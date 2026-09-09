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

package org.apache.texera.amber.engine.architecture.scheduling.config

/**
  * The only two settings least-loaded routing needs.  Both default off, so an
  * unconfigured build is stock: nothing here changes existing behaviour
  * unless TEXERA_LEAST_LOADED_ROUTING=true is set, and even then it only
  * takes effect on a link whose senders are running in batch-level round
  * robin mode (TEXERA_BATCH_LEVEL_RR=true on the Python side; see
  * round_robin_partitioner.py).
  */
object LeastLoadedRoutingConfig {

  private def envFlag(name: String): Boolean =
    sys.env.get(name).exists(v => v.equalsIgnoreCase("true") || v == "1")

  /** Turn the mechanism on. */
  lazy val enabled: Boolean = envFlag("TEXERA_LEAST_LOADED_ROUTING")

  /** Log every routing decision at INFO, not just warnings/errors. */
  lazy val verbose: Boolean = envFlag("TEXERA_LEAST_LOADED_ROUTING_VERBOSE")
}
