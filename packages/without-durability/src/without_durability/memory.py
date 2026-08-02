from __future__ import annotations

import asyncio
from collections import defaultdict
from collections import deque
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from datetime import timedelta
from itertools import count
from time import monotonic

from without_durability.seams import Delivery
from without_durability.seams import Fenced
from without_durability.seams import Pass

# Both seams over ordinary dicts, shipped rather than kept in a test directory, because
# the whole design says a store is injected and this is the store a test should inject.
# They are doubles rather than mocks: every mechanism (the load, the record, the resume,
# the queue, the timer's claim) runs for real and only the storage is swapped, so a test
# against them exercises the same code paths a server-backed one does.
#
# What that buys is a suite with no container in it. What it costs is the one thing a
# single process cannot stand in for: these hold their state in this process's memory, so
# nothing here says whether a *second* process would see the same exclusion. That is
# exactly what the store-backed suites are for, and it is why meeting the protocol's
# requirements here is not optional (see `MemoryCheckpointer`).

# What an effect is for a store whose datastore is a dict: a function over that dict,
# returning the value to record. A Redis store's is a Lua script and a SQL store's is a
# callback over an open transaction's cursor. Nothing is shared between them but the
# position in `transact`.
type MemoryEffect = Callable[[dict[str, object]], object]


@dataclass(frozen=True, slots=True)
class MemoryCheckpointer:
    """
    A `Checkpointer` keeping one dict per workflow, and one claim beside it.

    It meets the protocol's requirements rather than approximating them, which is the
    only way a test against it says anything about a real store: tokens rise per
    workflow, a write below the fence raises `Fenced`, and a key already recorded is
    never overwritten. Every method is synchronous between its `await`s, which is this
    store's version of a Lua script or a transaction: nothing can interleave halfway
    through a claim or a conditional write, so it enforces atomicity the way a real store
    does rather than pretending the question does not arise.

    A workflow whose checkpoint is a dict in this process is durable across exactly
    nothing, so this is for tests and for driving a workflow in a script, not for a
    deployment. Every other store here is the same code with the dict somewhere else.
    """

    hashes: dict[str, dict[str, object]] = field(default_factory=lambda: defaultdict(dict))
    tokens: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    held_until: dict[str, float] = field(default_factory=lambda: defaultdict(float))
    # This store's *other* data, standing in for the application tables that live
    # alongside a checkpoint. `transact` is only meaningful over something like it.
    data: dict[str, object] = field(default_factory=dict)

    async def load(self, workflow: str) -> dict[str, object]:
        return dict(self.hashes[workflow])

    async def claim(self, workflow: str, lease: timedelta) -> Pass | None:
        if self.held_until[workflow] > monotonic():
            return None
        self.tokens[workflow] += 1
        self.held_until[workflow] = monotonic() + lease.total_seconds()
        return Pass(workflow=workflow, token=self.tokens[workflow])

    async def record(self, holder: Pass, key: str, value: object) -> object:
        if holder.token < self.tokens[holder.workflow]:
            raise Fenced(f"pass {holder.token} of {holder.workflow!r} was superseded")
        return await self.supply(holder.workflow, key, value)

    async def transact(self, holder: Pass, key: str, effect: MemoryEffect) -> object:
        """
        Run `effect` over this store's own data and record it, without an `await` between.

        The in-memory answer to the question Redis answers with a script and SQL with a
        transaction: this
        store's datastore is `data`, so an effect is a function over `data`, and single
        threaded code with no suspension point is its transaction. Which is the point of
        `Effect` being a type parameter, since nothing about `LuaEffect` would fit here.
        """
        if holder.token < self.tokens[holder.workflow]:
            raise Fenced(f"pass {holder.token} of {holder.workflow!r} was superseded")
        recorded = self.hashes[holder.workflow]
        if key in recorded:
            return recorded[key]
        recorded[key] = effect(self.data)
        return recorded[key]

    async def supply(self, workflow: str, key: str, value: object) -> object:
        return self.hashes[workflow].setdefault(key, value)

    async def release(self, holder: Pass) -> None:
        # The token stays, so the next claim outranks this one: releasing hands the
        # workflow back, it does not rewind the fence.
        if holder.token == self.tokens[holder.workflow]:
            self.held_until[holder.workflow] = 0.0


@dataclass(frozen=True, slots=True)
class MemoryScheduler:
    """
    A `Scheduler` keeping the queue in a deque, the sleepers in a dict, and, like the
    stream it stands in for, the deliveries nobody has answered for yet.

    `outstanding` is the part worth having a double for: a delivery stays there until
    `done`, so a test can drop one on the floor the way a dying worker would and watch
    `reclaim` pick it up. `next_ready` waits on an event rather than returning
    immediately, mirroring the blocking read: a worker with nothing to do parks instead
    of spinning, and a test that hands it a workflow gets a pass the moment it does.
    """

    queue: deque[str] = field(default_factory=deque)
    sleeping: dict[str, datetime] = field(default_factory=dict)
    outstanding: dict[str, tuple[Delivery, float]] = field(default_factory=dict)
    arrived: asyncio.Event = field(default_factory=asyncio.Event)
    receipts: count[int] = field(default_factory=count)

    async def prepare(self) -> None:
        """Nothing to set up: a dict is its own consumer group."""

    async def make_ready(self, workflow: str) -> None:
        self.queue.append(workflow)
        self.arrived.set()

    async def wake_at(self, workflow: str, when: datetime) -> None:
        self.sleeping[workflow] = when

    async def wake_due(self, now: datetime) -> tuple[str, ...]:
        # No `await` between the removal and the enqueue, which is this double's
        # version of the Lua script: nothing else can observe a workflow that has left
        # the sleepers and not yet reached the queue.
        due = tuple(workflow for workflow, when in self.sleeping.items() if when <= now)
        for workflow in due:
            del self.sleeping[workflow]
            self.queue.append(workflow)
        if due:
            self.arrived.set()
        return due

    async def next_ready(self, within: timedelta) -> Delivery | None:
        if not self.queue:
            with suppress(TimeoutError):
                async with asyncio.timeout(within.total_seconds()):
                    await self.arrived.wait()
            self.arrived.clear()
        if not self.queue:
            return None
        delivery = Delivery(workflow=self.queue.popleft(), receipt=str(next(self.receipts)))
        self.outstanding[delivery.receipt] = (delivery, monotonic())
        return delivery

    async def reclaim(self, idle: timedelta) -> Delivery | None:
        stale = monotonic() - idle.total_seconds()
        taken = next((delivery for delivery, since in self.outstanding.values() if since <= stale), None)
        if taken is not None:
            # Taking it over restarts its clock, as the real reclaim does, so a delivery
            # this worker is now holding is not immediately reclaimable again.
            self.outstanding[taken.receipt] = (taken, monotonic())
        return taken

    async def done(self, delivery: Delivery) -> None:
        # Silent about a receipt it never issued, as `XACK` is: acknowledging twice, or
        # acknowledging something already taken over, is not an error anywhere.
        self.outstanding.pop(delivery.receipt, None)
