/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.texera.amber.engine.architecture.coordinator.promisehandlers

import com.twitter.util.Future
import org.apache.texera.common.config.ApplicationConfig
import org.apache.texera.amber.core.virtualidentity.{ActorVirtualIdentity, PhysicalOpIdentity}
import org.apache.texera.amber.core.workflow.{PhysicalLink, PortIdentity}
import org.apache.texera.amber.engine.architecture.coordinator.{
  CoordinatorAsyncRPCHandlerInitializer,
  ExecutionStatsUpdate,
  LeastLoadedRoutingService,
  RuntimeStatisticsPersist
}
import org.apache.texera.amber.engine.architecture.deploysemantics.layer.WorkerExecution
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.{
  AddPartitioningRequest,
  AsyncRPCContext,
  EmptyRequest,
  QueryStatisticsRequest,
  StatisticsUpdateTarget
}
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState.COMPLETED
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.{
  EmptyReturn,
  WorkerMetricsResponse
}
import org.apache.texera.amber.engine.architecture.scheduling.config.LeastLoadedRoutingConfig
import org.apache.texera.amber.engine.architecture.sendsemantics.partitionings.RoundRobinPartitioning
import org.apache.texera.amber.engine.architecture.worker.statistics.WorkerStatistics
import org.apache.texera.amber.util.VirtualIdentityUtils

/** Get statistics from all the workers, and (when TEXERA_LEAST_LOADED_ROUTING
  * is on) use them to steer batch-level round-robin senders toward whichever
  * receiver currently has the least queued work.
  *
  * Everything above the "--- least-loaded routing ---" marker below is
  * unmodified stock Texera (as of the `main` branch this was built against).
  * Everything below the marker is the addition.
  *
  * possible sender: coordinator(by statusUpdateAskHandle)
  */
trait QueryWorkerStatisticsHandler {
  this: CoordinatorAsyncRPCHandlerInitializer =>

  private var globalQueryStatsOngoing = false

  // Minimum of the two timer intervals converted to nanoseconds.
  // A full-graph worker query is skipped and served from cache when the last completed
  // query falls within this window, avoiding redundant worker RPCs.
  private val minQueryIntervalNs: Long =
    Math.min(
      ApplicationConfig.getStatusUpdateIntervalInMs,
      ApplicationConfig.getRuntimeStatisticsPersistenceIntervalInMs
    ) * 1_000_000L

  // Nanosecond timestamp of the last completed full-graph worker stats query.
  @volatile private var lastWorkerQueryTimestampNs: Long = 0L

  // Reads the current cached stats and forwards them to the appropriate client sink(s).
  private def forwardStats(updateTarget: StatisticsUpdateTarget): Unit = {
    val stats = cp.workflowExecution.getAllRegionExecutionsStats
    updateTarget match {
      case StatisticsUpdateTarget.UI_ONLY =>
        sendToClient(ExecutionStatsUpdate(stats))
      case StatisticsUpdateTarget.PERSISTENCE_ONLY =>
        sendToClient(RuntimeStatisticsPersist(stats))
      case StatisticsUpdateTarget.BOTH_UI_AND_PERSISTENCE |
          StatisticsUpdateTarget.Unrecognized(_) =>
        sendToClient(ExecutionStatsUpdate(stats))
        sendToClient(RuntimeStatisticsPersist(stats))
    }
  }

  // --- least-loaded routing ------------------------------------------------
  //
  // A receiver's true backlog is "tuples delivered - tuples processed", not
  // just "tuples processed" (all WorkerStatistics.inputTupleMetrics normally
  // reports). There is no protobuf field for "delivered" -- adding one needs
  // protoc/betterproto regeneration for both Scala and Python, unavailable in
  // this environment -- so the count is carried inside inputTupleMetrics
  // itself, tagged with a reserved PortIdentity that can never collide with a
  // real port (real ports are always non-negative). See
  // core/architecture/managers/statistics_manager.py for where it is written.
  private val RECEIVED_COUNT_SENTINEL_PORT = PortIdentity(-1, internal = true)

  // Mirrors _ROUTING_PREF_BASE in round_robin_partitioner.py exactly.
  // batch_size <= ROUTING_PREF_BASE, sent on the ordinary AddPartitioning RPC,
  // encodes "prefer the receiver at this index" as (ROUTING_PREF_BASE -
  // batch_size) rather than a real batch size. No new RPC and no protobuf
  // change: the field already exists and a size can never legitimately be
  // negative, so this range was free to repurpose.
  private val ROUTING_PREF_BASE = -1000

  /** Strips the sentinel entry out of a worker's raw statistics before they
    * reach anything else -- collectedResults feeds wExec.updateStats, which
    * feeds the dashboard via getAllRegionExecutionsStats. Left in place, it
    * would show up as a fake port in the operator's real per-port statistics.
    * Returns the cleaned stats and the extracted count separately.
    */
  private def extractReceivedCount(stats: WorkerStatistics): (WorkerStatistics, Option[Long]) = {
    val (sentinelEntries, realEntries) =
      stats.inputTupleMetrics.partition(_.portId == RECEIVED_COUNT_SENTINEL_PORT)
    if (sentinelEntries.isEmpty) {
      (stats, None)
    } else {
      (stats.copy(inputTupleMetrics = realEntries), Some(sentinelEntries.map(_.tupleMetrics.count).sum))
    }
  }

  /** Round-robin links in the executing regions, with the partitioning each
    * was configured with -- the same source the link-wiring handler used to
    * wire them, so this sees exactly what the workers were actually told.
    */
  private def liveRoundRobinLinks: Map[PhysicalLink, RoundRobinPartitioning] = {
    cp.workflowExecutionManager.getExecutingRegions.toSeq
      .flatMap(_.resourceConfig.toSeq)
      .flatMap(_.linkConfigs.toSeq)
      .collect {
        case (link, linkConfig) =>
          linkConfig.partitioning match {
            case rr: RoundRobinPartitioning => Some(link -> rr)
            case _                          => None
          }
      }
      .flatten
      .toMap
  }

  /**
    * Assign each sender on a live batch-level round-robin link a preferred
    * receiver, ranked by measured backlog (delivered - processed), instead of
    * the fixed rotation it would otherwise use.
    *
    * A no-op for any sender not actually running in batch-level mode: the
    * signal only has an effect where RoundRobinPartitioner.set_preferred_
    * receiver_index is read on the Python side, gated there on
    * TEXERA_BATCH_LEVEL_RR. Safe to send broadly rather than track which mode
    * each sender is in.
    */
  private def runLeastLoadedRouting(
      samples: Seq[(ActorVirtualIdentity, WorkerStatistics)],
      receivedCounts: Map[ActorVirtualIdentity, Long]
  ): Unit = {
    val links = liveRoundRobinLinks
    if (links.isEmpty) return

    // Per-WORKER processed count, deliberately not aggregated across an
    // operator's workers: the whole point is telling individual receivers
    // apart, which an operator-level sum would erase.
    val processedByWorker: Map[ActorVirtualIdentity, Long] = samples.map {
      case (wid, stats) => wid -> stats.inputTupleMetrics.map(_.tupleMetrics.count).sum
    }.toMap

    links.foreach {
      case (link, partitioning) =>
        val senders = partitioning.channels.map(_.fromWorkerId).distinct
        val receivers = partitioning.channels.map(_.toWorkerId).distinct
        val backlogByReceiver: Map[ActorVirtualIdentity, Long] = receivers.map { r =>
          val delivered = receivedCounts.getOrElse(r, 0L)
          val processed = processedByWorker.getOrElse(r, 0L)
          r -> math.max(0L, delivered - processed)
        }.toMap

        val assignments =
          LeastLoadedRoutingService.computeAssignments(senders, receivers, backlogByReceiver)

        assignments.foreach { a =>
          val index = receivers.indexOf(a.preferredReceiver)
          if (index >= 0) {
            val signal = partitioning.withBatchSize(ROUTING_PREF_BASE - index)
            workerInterface.addPartitioning(AddPartitioningRequest(link, signal), mkContext(a.sender))
            if (LeastLoadedRoutingConfig.verbose) {
              logger.info(
                s"[least-loaded-routing] ${a.sender.name} -> prefer ${a.preferredReceiver.name} " +
                  s"(backlog=${backlogByReceiver.getOrElse(a.preferredReceiver, 0L)})"
              )
            }
          }
        }
    }
  }
  // --- end least-loaded routing ---------------------------------------------

  override def coordinatorInitiateQueryStatistics(
      msg: QueryStatisticsRequest,
      ctx: AsyncRPCContext
  ): Future[EmptyReturn] = {
    // Avoid issuing concurrent full-graph statistics queries.
    // If a global query is already in progress, skip this request.
    if (globalQueryStatsOngoing && msg.filterByWorkers.isEmpty) {
      // A query is already in-flight: serve the last completed query's cached data,
      // or drop silently if no prior query has finished yet.
      if (lastWorkerQueryTimestampNs > 0) forwardStats(msg.updateTarget)
      return EmptyReturn()
    }

    var opFilter: Set[PhysicalOpIdentity] = Set.empty
    // Only enforce the single-query restriction for full-graph queries.
    if (msg.filterByWorkers.isEmpty) {
      if (System.nanoTime() - lastWorkerQueryTimestampNs < minQueryIntervalNs) {
        // Cache is still fresh: the faster timer already queried workers recently.
        forwardStats(msg.updateTarget)
        return EmptyReturn()
      }
      globalQueryStatsOngoing = true
    } else {
      // Map the filtered worker IDs (if any) to their corresponding physical operator IDs
      val initialOps: Set[PhysicalOpIdentity] =
        msg.filterByWorkers.map(VirtualIdentityUtils.getPhysicalOpId).toSet

      // Include all transitive upstream operators in the filter set
      opFilter = {
        val visited = scala.collection.mutable.Set.empty[PhysicalOpIdentity]
        val toVisit = scala.collection.mutable.Queue.from(initialOps)

        while (toVisit.nonEmpty) {
          val current = toVisit.dequeue()
          if (visited.add(current)) {
            val upstreamOps = cp.workflowScheduler.physicalPlan.getUpstreamPhysicalOpIds(current)
            toVisit.enqueueAll(upstreamOps)
          }
        }

        visited.toSet
      }
    }

    // Traverse the physical plan in reverse topological order (sink to source),
    // grouped by layers of parallel operators.
    val layers = cp.workflowScheduler.physicalPlan.layeredReversedTopologicalOrder

    // Accumulator to collect all (exec, wid, state, stats) results
    val collectedResults =
      scala.collection.mutable.ArrayBuffer.empty[(WorkerExecution, WorkerMetricsResponse, Long)]

    // Same replies, cleaned of the sentinel, for least-loaded routing.
    val receiverSamples =
      scala.collection.mutable.ArrayBuffer.empty[(ActorVirtualIdentity, WorkerStatistics)]
    val receivedCounts = scala.collection.mutable.HashMap.empty[ActorVirtualIdentity, Long]

    // Recursively process each operator layer sequentially (top-down in reverse topo order)
    def processLayers(layers: Seq[Set[PhysicalOpIdentity]]): Future[Unit] =
      layers match {
        case Nil =>
          // All layers have been processed
          Future.Done

        case layer +: rest =>
          // Issue statistics queries to all eligible workers in the current layer
          val futures = layer.toSeq.flatMap { opId =>
            // Skip operators not included in the filtered subset (if any)
            if (opFilter.nonEmpty && !opFilter.contains(opId)) {
              Seq.empty
            } else {
              cp.workflowExecution.getLatestOperatorExecutionOption(opId) match {
                // Operator region has not been initialized yet; skip in this polling round.
                case None       => Seq.empty
                case Some(exec) =>
                  // Skip completed operators
                  if (exec.getState == COMPLETED) {
                    Seq.empty
                  } else {
                    // Select all workers for this operator
                    val workerIds = exec.getWorkerIds

                    // Send queryStatistics to each worker and update internal state on reply
                    workerIds.map { wid =>
                      workerInterface.queryStatistics(EmptyRequest(), wid).map { resp =>
                        // Strip the sentinel (if present) before this reply
                        // reaches anything else: collectedResults feeds the
                        // dashboard, and it must never see it.
                        val (cleanedStats, receivedCount) =
                          extractReceivedCount(resp.metrics.workerStatistics)
                        val cleanedResp =
                          if (receivedCount.isDefined)
                            resp.copy(metrics = resp.metrics.copy(workerStatistics = cleanedStats))
                          else resp
                        receivedCount.foreach(c => receivedCounts.update(wid, c))

                        collectedResults.addOne(
                          (exec.getWorkerExecution(wid), cleanedResp, System.nanoTime())
                        )
                        // Full-graph rounds only. A filtered round covers a
                        // subset of workers; folding a partial view into the
                        // ranking would read an un-polled receiver's silence
                        // as "no backlog" instead of "no data this round".
                        if (LeastLoadedRoutingConfig.enabled && msg.filterByWorkers.isEmpty) {
                          receiverSamples.addOne((wid, cleanedStats))
                        }
                      }
                    }
                  }
              }
            }
          }

          // After all worker queries in this layer complete, process the next layer
          Future.collect(futures).flatMap(_ => processLayers(rest))
      }

    // Start processing all layers and forward stats to the appropriate sink(s) on completion.
    processLayers(layers).map { _ =>
      collectedResults.foreach {
        case (wExec, resp, timestamp) =>
          // State is ordered by the worker's logical version; stats by receipt time.
          wExec.updateState(resp.metrics.stateVersion, resp.metrics.workerState)
          wExec.updateStats(timestamp, resp.metrics.workerStatistics)
      }
      forwardStats(msg.updateTarget)
      if (LeastLoadedRoutingConfig.enabled && msg.filterByWorkers.isEmpty) {
        // Statistics reporting must not depend on this: run it after the
        // stats have been forwarded, and never let it escape -- an exception
        // here must not freeze globalQueryStatsOngoing at true.
        try runLeastLoadedRouting(receiverSamples.toSeq, receivedCounts.toMap)
        catch {
          case t: Throwable => logger.warn(s"[least-loaded-routing] round failed: $t")
        }
      }
      // Record the completion timestamp before releasing the lock so that any timer
      // firing in between sees a valid cache entry rather than triggering a redundant query.
      if (globalQueryStatsOngoing) {
        lastWorkerQueryTimestampNs = System.nanoTime()
        globalQueryStatsOngoing = false
      }
      EmptyReturn()
    }
  }

}
