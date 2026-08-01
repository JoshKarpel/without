from __future__ import annotations

import asyncio
from collections import defaultdict
from collections import deque
from contextlib import suppress
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from datetime import timedelta
from itertools import count
from time import monotonic

from integration.durable.wakeups import Delivery

# The two stores the durable toys talk through, in memory. They are doubles rather
# than mocks: every mechanism here (the load, the record, the resume, the queue, the
# timer's claim) runs for real, and only the storage is swapped, which is the payoff
# of injecting both seams. The container-backed tests run the same code against Redis.


@dataclass(frozen=True, slots=True)
class MemoryCheckpoints:
    """A `Checkpoints` keeping one dict per workflow."""

    hashes: dict[str, dict[str, object]] = field(default_factory=lambda: defaultdict(dict))

    async def load(self, workflow: str) -> dict[str, object]:
        return dict(self.hashes[workflow])

    async def record(self, workflow: str, key: str, value: object) -> None:
        self.hashes[workflow][key] = value


@dataclass(frozen=True, slots=True)
class MemoryWakeups:
    """
    A `Wakeups` keeping the queue in a deque, the sleepers in a dict, and, like the
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
