# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import os as _os
import typing
from loguru import logger
from overrides import overrides
from typing import Iterator

from core.architecture.sendsemantics.partitioner import Partitioner
from core.models import Tuple
from core.models.state import State
from core.util import set_one_of
from proto.org.apache.texera.amber.core import ActorVirtualIdentity
from proto.org.apache.texera.amber.engine.architecture.rpc import EmbeddedControlMessage
from proto.org.apache.texera.amber.engine.architecture.sendsemantics import (
    Partitioning,
    RoundRobinPartitioning,
)

# Batch-level round robin: accumulate a sender's whole output in ONE buffer and
# send each full batch to a single receiver, rotating receivers between
# batches, instead of routing every tuple into one of R per-receiver buffers.
#
# This is the foundation least-loaded routing sits on: a per-tuple router has
# no "next batch" whose destination could be redirected -- it ships
# continuously. A per-sender single buffer gives exactly one ship decision per
# batch, which is what the routing preference below overrides.
_BATCH_LEVEL_RR = _os.environ.get("TEXERA_BATCH_LEVEL_RR", "").strip().lower() in (
    "1",
    "true",
)

# Least-loaded routing: the controller assigns each sender a preferred
# receiver, ranked by measured backlog, instead of a fixed rotation. No new
# RPC and no protobuf change -- reuses the existing AddPartitioning call's
# batch_size field, which can never legitimately be negative:
#   batch_size <= _ROUTING_PREF_BASE  encodes  index = _ROUTING_PREF_BASE - batch_size
# The controller (QueryWorkerStatisticsHandler.scala) computes the SAME index
# independently, from the SAME deterministic channel-dedup order this class
# uses to build self.receivers, so no name ever needs to cross the JVM/Python
# boundary -- only a small integer does. Only meaningful in batch-level mode;
# harmless no-op otherwise.
_ROUTING_PREF_BASE = -1000


class RoundRobinPartitioner(Partitioner):
    def __init__(self, partitioning: RoundRobinPartitioning, worker_id: str = None):
        super().__init__(set_one_of(Partitioning, partitioning))
        self.batch_size = partitioning.batch_size
        # Indexed by round_robin_index to choose the downstream worker to send to.
        self.receivers = self.build_receiver_batches(partitioning.channels)
        self.round_robin_index = 0

        # Batch-level mode state. self.receivers still holds the ordered
        # receiver list (its per-receiver lists go unused in this mode).
        self._batch: typing.List[Tuple] = []
        # Set via set_preferred_receiver_index; None until the first ranking
        # arrives, or if least-loaded routing is off entirely.
        self._preferred_receiver_id = None

        if _BATCH_LEVEL_RR and worker_id is not None:
            # Stagger the rotation by sender index. A sender emits only a
            # handful of full batches -- often fewer than there are receivers
            # -- so if every sender started at receiver 0 the first few
            # receivers would take all the work and the rest would get none.
            #
            # The index comes from this sender's position among the link's
            # senders, derived from the partitioning's own channel list rather
            # than parsed out of the worker id -- deterministic, and the same
            # data every sender already has, so it cannot drift out of sync.
            senders = list(
                dict.fromkeys(
                    channel.from_worker_id.name for channel in partitioning.channels
                )
            )
            if worker_id in senders:
                self.round_robin_index = senders.index(worker_id) % len(self.receivers)
            else:
                logger.warning(
                    f"batch-level round robin: {worker_id} is not among this "
                    f"link's senders {senders}; rotation not staggered, which "
                    f"can leave later receivers idle"
                )

    def set_preferred_receiver_index(self, index: int) -> None:
        """
        Override the next batch-level ship target.

        A no-op outside batch-level mode: self._preferred_receiver_id is
        simply never read by the tuple-level ship path below, so this is safe
        to call unconditionally regardless of which mode this sender is
        actually running.
        """
        if 0 <= index < len(self.receivers):
            self._preferred_receiver_id = self.receivers[index][0]
        else:
            logger.warning(
                f"least-loaded routing: index {index} out of range for "
                f"{len(self.receivers)} receivers; preference ignored"
            )

    def _ship_target(self) -> ActorVirtualIdentity:
        """
        Batch-level mode only: who the next full batch goes to.

        The least-loaded preference wins when set; otherwise falls back to
        staggered rotation. round_robin_index still advances every ship
        either way, so if the preference stops updating, rotation resumes
        from a sane position instead of restarting at index 0.
        """
        if self._preferred_receiver_id is not None:
            return self._preferred_receiver_id
        return self.receivers[self.round_robin_index][0]

    @overrides
    def add_tuple_to_batch(
        self, tuple_: Tuple
    ) -> Iterator[typing.Tuple[ActorVirtualIdentity, typing.List[Tuple]]]:
        if _BATCH_LEVEL_RR:
            self._batch.append(tuple_)
            if len(self._batch) >= self.batch_size:
                target = self._ship_target()
                yield target, self._batch
                self._batch = []
                self.round_robin_index = (self.round_robin_index + 1) % len(
                    self.receivers
                )
            return

        receiver, batch = self.receivers[self.round_robin_index]
        batch.append(tuple_)
        if len(batch) == self.batch_size:
            yield receiver, batch
            self.receivers[self.round_robin_index] = (receiver, list())
        self.round_robin_index = (self.round_robin_index + 1) % len(self.receivers)

    @overrides
    def flush(
        self, to: ActorVirtualIdentity, ecm: EmbeddedControlMessage
    ) -> Iterator[typing.Union[EmbeddedControlMessage, typing.List[Tuple]]]:
        if _BATCH_LEVEL_RR:
            # The pending batch has one destination -- the current preference
            # or rotation target -- so it is emitted on that receiver's call
            # only, before the marker, and never duplicated to the others.
            if self._batch and self._ship_target() == to:
                yield self._batch
                self._batch = []
                self.round_robin_index = (self.round_robin_index + 1) % len(
                    self.receivers
                )
            yield ecm
            return

        for receiver, batch in self.receivers:
            if receiver == to:
                if len(batch) > 0:
                    yield batch
                    batch.clear()
                yield ecm

    @overrides
    def flush_state(
        self, state: State
    ) -> Iterator[
        typing.Tuple[ActorVirtualIdentity, typing.Union[State, typing.List[Tuple]]]
    ]:
        if _BATCH_LEVEL_RR:
            if self._batch:
                target = self._ship_target()
                yield target, self._batch
                self._batch = []
                self.round_robin_index = (self.round_robin_index + 1) % len(
                    self.receivers
                )
            for receiver, _ in self.receivers:
                yield receiver, state
            return

        for receiver, batch in self.receivers:
            if len(batch) > 0:
                yield receiver, batch
                batch.clear()
            yield receiver, state
