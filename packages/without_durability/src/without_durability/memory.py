from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from datetime import timedelta
from itertools import count
from time import monotonic

from without_durability.codec import JSON
from without_durability.codec import CheckpointCodec
from without_durability.interfaces import LEASE
from without_durability.interfaces import Delivery
from without_durability.interfaces import Entry
from without_durability.interfaces import Fenced
from without_durability.interfaces import Pass
from without_durability.interfaces import Recorded
from without_durability.interfaces import Written
from without_durability.interfaces import check_duration
from without_durability.interfaces import inbox_key
from without_durability.stepwise import now_utc

# Both interfaces over ordinary dicts, shipped rather than kept in a test directory, because
# the whole design says a store is injected and this is the store a test should inject.
# They are doubles rather than mocks: every mechanism (the load, the record, the resume,
# the queue, the timer's claim, the codec) runs for real and only the storage is swapped.
#
# The codec is on that list for a reason worth stating plainly, because leaving it off is
# the natural thing to do and it is what makes a double lie. A dict can hold a value
# directly, so encoding into it looks like ceremony; but then a step's result comes back
# by identity here and through a round trip everywhere else, and every property that
# depends on the round trip passes in the suite and fails in production. So `hashes`
# holds `Stored`, which carries the *encoding* exactly as a Redis hash field or a `TEXT`
# column does.
#
# What that buys is a suite with no container in it. What it costs is the one thing a
# single process cannot stand in for: nothing here says whether a *second* process would
# see the same exclusion, which is exactly what the store-backed suites are for.

# What an effect is for a store whose datastore is a dict: a function over that dict,
# returning the value to record. A Redis store's is a Lua script and a SQL store's is a
# callback over an open transaction's cursor. Nothing is shared between them but the
# position in `transact`.
type MemoryEffect = Callable[[dict[str, object]], object]


@dataclass(frozen=True, slots=True)
class Stored:
    """
    One record as this store keeps it: the encoding, and when the winning write landed.

    A value per record rather than a second mapping beside `hashes`, because two mappings
    kept in step are a state that can be wrong and this cannot: a key either has a record
    or has none, and the record carries its own time. It is the row the SQL stores keep,
    with the column names spelled out.
    """

    encoded: str
    at: datetime


@dataclass(frozen=True, slots=True)
class MemoryCheckpointer:
    """
    A `Checkpointer` keeping one dict per workflow, and one claim beside it.

    It meets the protocol's requirements rather than approximating them, which is the
    only way a test against it says anything about a real store: tokens rise per
    workflow, a write below the fence raises `Fenced`, and a key already recorded is
    never overwritten. Every method is synchronous between its `await`s, which is this
    store's version of a Lua script or a transaction.

    A workflow whose checkpoint is a dict in this process is durable across exactly
    nothing, so this is for tests and for driving a workflow in a script, not for a
    deployment. `hashes` holds what the codec produced rather than what a step returned,
    so reading a checkpoint back means `load` rather than reaching into it.
    """

    # Plain dicts rather than `defaultdict`s, because the annotation is the interface: a
    # `defaultdict` field typed as `dict` accepts a `dict` at construction and then fails
    # on the first write, which is a store the type says you may build and the code says
    # you may not. What the default lookup bought was two characters at each read, and
    # what it cost was that every read of an unknown workflow silently grew the mapping.
    hashes: dict[str, dict[str, Stored]] = field(default_factory=dict)
    tokens: dict[str, int] = field(default_factory=dict)
    held_until: dict[str, float] = field(default_factory=dict)
    codec: CheckpointCodec[str] = JSON
    # The clock every record is stamped with. The other two stores read their server's,
    # which here would be `datetime.now(UTC)`; taking it as an argument is what lets a test
    # stamp records from a clock it moves rather than waiting out a real interval, and it
    # is deliberately *not* the clock a claim is measured by, which stays `monotonic` for
    # the reason every lease does.
    now: Callable[[], datetime] = now_utc
    # This store's *other* data, standing in for the application tables that live
    # alongside a checkpoint. `transact` is only meaningful over something like it, and
    # it holds ordinary values rather than encoded ones: it stands in for the
    # application's own tables, whose shape is the application's business and not this
    # store's to encode.
    data: dict[str, object] = field(default_factory=dict)

    async def load(self, workflow: str) -> dict[str, object]:
        # Reading a workflow is not creating one, and this is the call a status endpoint
        # makes for an id nobody has ever submitted.
        return {key: self.codec.decode(held.encoded) for key, held in self.hashes.get(workflow, {}).items()}

    async def history(self, workflow: str) -> dict[str, Written]:
        return {
            key: Written(value=self.codec.decode(held.encoded), at=held.at)
            for key, held in self.hashes.get(workflow, {}).items()
        }

    async def claim(self, workflow: str, lease: timedelta) -> Pass | None:
        if self.held_until.get(workflow, 0.0) > monotonic():
            return None
        token = self.tokens.get(workflow, 0) + 1
        self.tokens[workflow] = token
        self.held_until[workflow] = monotonic() + lease.total_seconds()
        return Pass(workflow=workflow, token=token)

    async def record(self, holder: Pass, key: str, value: object) -> Recorded:
        # Indexed rather than defaulted, here and in `transact` and `release`: a `Pass`
        # exists only because `claim` wrote a token for that workflow, so a miss is a
        # broken invariant and worth a `KeyError` rather than a `0` that fences nobody.
        if holder.token < self.tokens[holder.workflow]:
            raise Fenced(f"pass {holder.token} of {holder.workflow!r} was superseded")
        encoded = self.codec.encode(value)
        # `setdefault` is the whole of first-writer-wins, and it reports the winner by
        # handing back what is now stored: ours when we won, and the earlier writer's
        # when we did not. Comparing encodings rather than values is what makes a tie
        # (two passes that ran the same effect) count as winning for both, and comparing
        # the *encoding* rather than the whole `Stored` is what keeps it a tie: the
        # earlier writer's time is not this call's, so the records would never compare
        # equal and every tie would be reported as a loss.
        stored = self.hashes.setdefault(holder.workflow, {}).setdefault(key, Stored(encoded=encoded, at=self.now()))
        return Recorded(value=self.codec.decode(stored.encoded), first=stored.encoded == encoded)

    async def transact(self, holder: Pass, key: str, effect: MemoryEffect) -> object:
        """
        Run `effect` over this store's own data and record it, without an `await` between.

        The in-memory answer to the question Redis answers with a script and SQL with a
        transaction: this store's datastore is `data`, so an effect is a function over
        `data`, and single-threaded code with no suspension point is its transaction.
        Which is the point of `Effect` being a type parameter, since nothing about
        `LuaEffect` would fit here.

        Having no suspension point is only half a transaction, and the other half is the
        rollback. An effect that raises partway, or one whose result the codec refuses,
        would otherwise leave `data` moved and nothing recorded, which is the state the
        protocol forbids outright and the state a replay then compounds by running the
        effect again over data it already moved. So the mapping is snapshotted first and
        put back on any exception, which is what `transacted` gets from `ROLLBACK` in the
        SQLite store and a script gets from Redis running it to completion or not at all.

        The snapshot is shallow, which bounds what this double can stand in for: an
        effect that reaches *inside* a value in `data` (appending to a list it holds)
        mutates something the restore hands back unchanged. That is the same bound the
        rest of this store has, since a dict is not a datastore, and an effect written
        the way a real one is (replace the entry, do not edit it in place) stays inside
        it.
        """
        if holder.token < self.tokens[holder.workflow]:
            raise Fenced(f"pass {holder.token} of {holder.workflow!r} was superseded")
        recorded = self.hashes.setdefault(holder.workflow, {})
        if key not in recorded:
            before = dict(self.data)
            try:
                encoded = self.codec.encode(effect(self.data))
            except BaseException:
                self.data.clear()
                self.data.update(before)
                raise
            recorded[key] = Stored(encoded=encoded, at=self.now())
        return self.codec.decode(recorded[key].encoded)

    async def supply(self, workflow: str, key: str, value: object) -> object:
        stored = self.hashes.setdefault(workflow, {}).setdefault(
            key, Stored(encoded=self.codec.encode(value), at=self.now())
        )
        return self.codec.decode(stored.encoded)

    async def append(self, workflow: str, value: object) -> Entry:
        """
        File `value` under the next key in this workflow's inbox.

        The position is how many records the workflow already has, which is the same
        number the Redis store takes from `HLEN` and for the same reasons: nothing here
        ever removes a record and first-writer-wins means nothing is ever replaced, so the
        count only rises and a key it yields cannot already be taken. Being synchronous
        between its `await`s is what makes it atomic, which is this store's version of a
        Lua script.

        It counts every record rather than only the inbox's, so a workflow's entries are
        numbered with gaps wherever a step was recorded between two appends. That is
        exactly what the interface allows: the keys sort into append order, which is the
        whole of what a consumer reads them for.
        """
        recorded = self.hashes.setdefault(workflow, {})
        key = inbox_key(len(recorded))
        recorded[key] = Stored(encoded=self.codec.encode(value), at=self.now())
        return Entry(key=key, value=self.codec.decode(recorded[key].encoded))

    async def discard(self, workflow: str) -> int:
        """
        Forget every record this workflow has, and raise its fence so a live pass cannot
        write more.

        The token is taken *up* rather than removed, which is the whole of what makes this
        safe against a pass in flight: that pass keeps its `Pass` and believes it still
        owns the workflow, and the number it carries is now below the fence, so its next
        `record` raises `Fenced`. Deleting the token instead would hand the next claim a 1
        that a pass holding 7 outranks, and the deleted workflow would fill back up.

        The lease goes with it, so the workflow is claimable again immediately: what is
        being kept is the ordering, not the claim.

        Only a workflow that *has* a token gets one, so discarding an id nobody has claimed
        writes nothing: a `Pass` exists only because `claim` wrote a token, so where there
        is no token there is no pass to fence, and minting one would leave a tombstone for
        a workflow that never ran.
        """
        removed = self.hashes.pop(workflow, {})
        superseded = self.tokens.get(workflow)
        if superseded is not None:
            self.tokens[workflow] = superseded + 1
            self.held_until[workflow] = 0.0
        return len(removed)

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
    # How long a delivery stays this taker's, which `reclaim` measures against and which
    # `worker.work` reads to decide how long to claim the workflow for. A field rather
    # than the constant read inline, like every other scheduler here, so a test can shrink
    # it and so the two windows cannot be set to different numbers.
    lease: timedelta = LEASE

    def __post_init__(self) -> None:
        check_duration("a lease", self.lease)

    async def prepare(self) -> None:
        """Nothing to set up: a dict is its own consumer group."""

    async def make_ready(self, workflow: str) -> None:
        self.queue.append(workflow)
        self.arrived.set()

    async def wake_at(self, delivery: Delivery, when: datetime) -> None:
        # Two structures, so there is nothing to compare about *freshness*: a wakeup that
        # arrived while the pass was running is in `queue`, and this writes to `sleeping`.
        #
        # What is compared is whether the delivery is still live at all, which is the other
        # requirement on this call and the one a `cancel` needs: a delivery this store no
        # longer holds was cancelled underneath its worker, so writing the deadline would
        # put a deleted workflow back among the sleepers to be woken and run from the top.
        # Answering for the delivery is what it shares with `done`, and is unconditional
        # because acknowledging something already gone is not an error anywhere.
        if self.outstanding.pop(delivery.receipt, None) is not None:
            self.sleeping[delivery.workflow] = when

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

    async def cancel(self, workflow: str) -> None:
        """
        Drop every wakeup this store holds for the workflow, whichever structure it is in.

        All three, because a workflow can be in any of them and the caller has no way to
        know which: waiting in the queue, waiting on a clock, or out with a worker that
        has not answered for it yet. That last one is why `wake_at` checks `outstanding`,
        since dropping the delivery here is what tells the pass still running that its
        workflow is gone.
        """
        self.sleeping.pop(workflow, None)
        stale = [receipt for receipt, (delivery, _since) in self.outstanding.items() if delivery.workflow == workflow]
        for receipt in stale:
            del self.outstanding[receipt]
        # Rebuilt rather than filtered in place: `deque` has no bulk removal, and removing
        # by value while iterating is the shape that silently skips a duplicate, which one
        # workflow queued twice is.
        queued = [queued for queued in self.queue if queued != workflow]
        self.queue.clear()
        self.queue.extend(queued)

    async def done(self, delivery: Delivery) -> None:
        # Silent about a receipt it never issued, as `XACK` is: acknowledging twice, or
        # acknowledging something already taken over, is not an error anywhere.
        self.outstanding.pop(delivery.receipt, None)
