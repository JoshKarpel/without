# The same durability, without the graph: a workflow is an ordinary async function
# whose effects are named. `run.step(key, effect, parse)` runs the effect once and
# records what it returned; resuming means calling the function again, where each step
# it reaches hands back what is already recorded instead of running. The whole engine is
# the dict lookup in `Run.step`.
#
# What is re-executed is the code *between* the steps, and that is the one rule this
# mechanism asks for: effects live in steps, the code around them is pure. Temporal and
# DBOS state that rule as workflow determinism, and their recovery re-runs the same code
# for the same reason; here it is the functional-core/imperative-shell split the rest of
# this repo already keeps.
#
# Keying by an explicit *name* rather than by position in a history is what makes that
# rule mild: reordering two independent steps changes nothing, and inserting a step
# ahead of an existing one changes nothing, since the new step runs and the old one
# still finds its record. What no longer holds is the graph's eager check, because the
# set of keys a function will use is not knowable until it has used them.
#
# The other half is suspension. A step that cannot finish now stops the pass instead of
# blocking, which is what buys the things a graph cannot express at all: a three-day
# settlement window, and a value another process has yet to write. Both are the same
# idea as a recorded result, so both are entries in the same mapping.
#
# Suspension is an exception *inside* the pass and a value *at* its boundary, and the
# split is deliberate. Unwinding a workflow written as straight-line code needs an
# exception, since there is no other way to stop in the middle of somebody else's
# function. But a caller deciding what to do next is asking a question with three
# answers, and a sealed union of three answers is a better way to ask it than a `try`
# whose `except` clause the type checker cannot check for completeness. So `resume`
# catches its own signal and hands back an `Outcome`.

from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Iterator
from contextlib import suppress
from dataclasses import dataclass
from dataclasses import field
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from itertools import islice
from typing import Never

from without_durability.interfaces import INBOX
from without_durability.interfaces import Checkpointer
from without_durability.interfaces import Contended
from without_durability.interfaces import Entry
from without_durability.interfaces import Fenced
from without_durability.interfaces import Interruption
from without_durability.interfaces import Pass

type StepKey = str
# How a recorded value re-enters the workflow as the type the workflow declared.
#
# It takes `object` because that is honestly what a checkpoint holds: the store's codec
# carries every key uniformly and can only promise to hand something back, never what.
# It returns `T` because a parser that ran is a *proof* the value has that shape, which
# a `cast` is not. So this is the one direction that has to be per-step, where the codec
# is per-store: only the workflow knows that `"items"` is a mapping of sku to cents.
#
# It is one function rather than half of a per-step codec, because encoding is uniform:
# a step that encoded for itself would have to lower its value into something the
# store's codec also understands, which is a second pass over the same data for nothing.
type Parse[T] = Callable[[object], T]


class Suspended(Interruption):
    """
    The pass cannot go further until `key` is recorded. Nothing has failed.

    A control-flow signal wearing an exception's clothes, so a workflow written as
    straight-line code can stop in the middle of itself. It is how a suspension travels
    *through* a workflow body, not how it is reported: `resume` catches these and hands
    back an `Outcome`, so a driver matches over three values rather than catching this.

    Error handling *around* a workflow must let this through: a suspended workflow is
    one that is going fine, and unwinding it would undo work it is still counting on.
    Being an `Interruption` is what makes that structural rather than a rule to
    remember, and it is why these stay public despite `resume` absorbing them: a
    workflow author needs to know what must not be caught.

    Public to *name*, then, and not to raise. The ways of waiting are its subclasses, and
    each carries what a driver needs to answer it; this base carries only the fact that a
    pass stopped, which no driver can act on. `resume` turns one
    raised directly into an ordinary failure of that workflow rather than letting it
    through (see there for why that is the kinder of the two).
    """

    def __init__(self, key: StepKey, waiting: str) -> None:
        self.key = key
        super().__init__(f"suspended at {key!r}, waiting {waiting}")


class InputNeeded(Suspended):
    """
    The pass is waiting on a value only something outside it can supply (`Run.awaiting`).

    Nobody schedules a wakeup for this, because no clock will satisfy it: the thing
    that writes the value is what makes the workflow ready again.
    """

    def __init__(self, key: StepKey) -> None:
        super().__init__(key, "to be told")


class MessageNeeded(Suspended):
    """
    The pass is waiting on the workflow's inbox having something new in it (`Run.receive`).

    Nobody schedules a wakeup for this either, and for `InputNeeded`'s reason: no clock
    satisfies it, and whoever delivers the next message is what makes the workflow ready.
    What differs is how it is answered. `InputNeeded`'s key is an *address*, so a client
    holding it calls `arrive(workflow, key, value)`; this one's key names the read step
    that stopped, which nobody writes to, and the answer is `deliver(workflow, value)`.
    Sharing a type would mean a key that is sometimes somewhere to write and sometimes a
    diagnostic, which is exactly the shape `Sleeping` and `Waiting` are kept apart to
    avoid.
    """

    def __init__(self, key: StepKey) -> None:
        super().__init__(key, "to be sent something")


class ScheduledWakeup(Suspended):
    """
    The pass is waiting out a deadline it chose itself (`Run.sleep`).

    `due` is that deadline, and it is not optional, which is the reason this is its own
    type rather than a field on `Suspended`. The two ways of waiting are structurally
    different: this one carries a moment to schedule, and `InputNeeded` carries nothing
    because there is nothing to schedule. One class with a nullable `due` would make
    those two states the same shape and leave every consumer to re-derive them, which is
    the same reason `Sleeping` and `Waiting` are separate on the way back out.
    """

    def __init__(self, key: StepKey, due: datetime) -> None:
        self.due = due
        super().__init__(key, f"until {due.isoformat()}")


def now_utc() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True, slots=True)
class Run[Effect = Never]:
    """
    One pass at a workflow: what it has already committed to, and how to commit more.

    Built by `resume` and threaded through the workflow function as its first
    argument, so what a step *is* stays visible at the call site rather than being
    inferred from a decorator. `recorded` is the checkpoint loaded once at the top of
    the pass and kept current as the pass adds to it, so a step reads memory rather
    than the store. `holder` is this pass's claim, and carrying it is what lets a step
    write at all: there is no way to record without one.
    """

    holder: Pass
    checkpointer: Checkpointer[Effect]
    recorded: dict[StepKey, object]
    now: Callable[[], datetime] = now_utc
    claimed: set[StepKey] = field(default_factory=set)
    # The deadlines this pass has written and not yet reached, which `resume` reports when
    # no `ScheduledWakeup` arrives to report them. A `sleep` in one branch of a task group
    # is cancelled the instant another branch raises, and the record is durable by then, so
    # the exception is the one part of a suspension that can go missing. See `resume`.
    waking: dict[StepKey, datetime] = field(default_factory=dict)

    @property
    def workflow(self) -> str:
        return self.holder.workflow

    async def step[T](self, key: StepKey, effect: Callable[[], Awaitable[object]], parse: Parse[T]) -> T:
        """
        Run `effect` once across every pass of this workflow, recording what it returns.

        `parse` is what makes the return type true rather than asserted, and it is
        required for that reason. What this hands back is never the object `effect`
        produced: it is what the *store* holds, read back through the store's
        `CheckpointCodec`, so a step returning a tuple is handed a list under the
        default `JsonCodec` on the very pass that ran it. A `cast` here would be a lie
        on every path rather than only after a crash.

        The effect's own return type is deliberately not tied to `parse`'s. What goes
        in and what comes out are related by encode-then-decode, which is not the
        identity, so requiring one type for both would assert something false. `sleep`
        is the proof rather than the exception: it records an ISO string and reads back
        a `datetime`. A richer codec narrows what a parser has to repair and no codec
        removes it, since `pydantic_core` renders a tuple as a JSON array too: the
        codec is a transport concern uniform over every key, and this is a meaning
        concern particular to one.

        The record is written before the step returns, so the workflow never proceeds
        on a result the store has not accepted. When two passes both ran the effect,
        the first to record wins and the second is handed the winner's value, so from
        there they proceed identically rather than diverging on which capture id is
        real. That makes the duplicate harmless downstream rather than preventing it,
        which is what the claim is for. Which of the two happened is on the `Recorded`
        and is deliberately ignored: a step has no dependents holding the loser's
        value, where `run_durably` fed its node's result downstream before the write.

        Cancellation is what separates those two sentences, and the write is held past
        it deliberately. Once `effect` has returned, the thing it did has happened:
        cancelling the write now does not undo the charge, it only removes the record
        of it, so the next pass performs it again. And a step is cancelled in the
        ordinary course of a fan-out, not only in a crash, since a workflow that spawns
        a capture per line item ends its siblings when one of them is declined. Every
        sibling that had already called the gateway would be charged twice.

        So the write is a task the step *shields* rather than an ordinary await, and a
        cancelled step waits for it before unwinding. It is still cancellation: what
        the caller waits for is one store round trip, which is the same bound the
        worker's own release has, rather than whatever the cancelled effect was stuck
        on. The claim is still held while it lands (a release keeps the token, so a
        write in flight is not fenced by it), which is what makes the record valid.

        It waits through *repeated* cancellation, which is not stubbornness but the
        ordinary case. One cancelled pass delivers two: a fan-out gathered under
        `asyncio.gather` cancels its children when the pass is cancelled, and the gather
        returns as soon as the first child answers, so the caller's own teardown cancels
        the rest a second time. Honouring the second one is dropping a write whose
        gateway call has already happened, which is the charge this whole shape exists
        to keep. The wait is still bounded by the store, not by the workflow.
        """
        self.claim(key)
        return await self.perform(key, effect, parse)

    async def perform[T](self, key: StepKey, effect: Callable[[], Awaitable[object]], parse: Parse[T]) -> T:
        """
        `step` once the name has been claimed: the lookup, the effect, and the write.

        Split out for `receive`, which has to claim its key *before* it knows whether it
        will run an effect at all, since a pass that suspends on an empty inbox should
        still have reported a duplicate step name. Claiming inside here instead would
        make that either a double claim or no claim.
        """
        if key in self.recorded:
            return parse(self.recorded[key])
        recording = asyncio.ensure_future(self.checkpointer.record(self.holder, key, await effect()))
        try:
            recorded = await asyncio.shield(recording)
        except asyncio.CancelledError:
            # `wait` rather than an `await`, because this one is finishing somebody
            # else's business: the write's own failure belongs to the pass being torn
            # down, and raising it here would replace the cancellation with it.
            while not recording.done():
                with suppress(asyncio.CancelledError):
                    await asyncio.wait([recording])
            # A write that landed is part of this pass whether or not the step that made
            # it survived to say so, and the mapping is what the rest of the pass reads:
            # `sleep` looks here for the deadline it was cancelled before it could raise.
            if recording.exception() is None:
                self.recorded[key] = recording.result().value
            raise
        self.recorded[key] = recorded.value
        return parse(recorded.value)

    async def transact[T](self, key: StepKey, effect: Effect, parse: Parse[T]) -> T:
        """
        Perform `effect` and record it in one commit, so the step is *exactly* once.

        The difference from `step` is the failure it removes rather than the work it
        does. `step` runs the effect and then writes the record, so a crash in between
        leaves the effect done and unrecorded and the next pass repeats it:
        at-least-once. Here the store performs the work and writes the record together,
        so there is no in-between for a crash to land in.

        The price is that `effect` has to be something the *store* can perform, which
        means it has to live in the store: a Lua script over keys in the same Redis, a
        callback over a cursor in the same SQL transaction. An effect that leaves the
        datastore (a payment gateway, a carrier) cannot be in the commit, is not a
        transaction anyone can offer, and belongs in `step` with an idempotency key.
        That boundary is a fact about distributed transactions rather than a limitation
        of this interface, which is why `Effect` is a type parameter and not a shared
        interface.

        `parse` is required for the reason it is on `step`, and more plainly: what the
        effect returns is produced by the *store* (a Lua script's reply, a cursor's
        row), so there is no Python type to infer even before the codec touches it.
        """
        self.claim(key)
        if key in self.recorded:
            return parse(self.recorded[key])
        stored = await self.checkpointer.transact(self.holder, key, effect)
        self.recorded[key] = stored
        return parse(stored)

    async def sleep(self, key: StepKey, duration: timedelta) -> None:
        """
        Wait out `duration`, across crashes, by suspending until the recorded deadline.

        The *deadline* is what gets recorded, not the duration, which is the whole
        point: a crash on day two of a three-day wait must not restart the clock. The
        first pass computes and stores it, every later pass reads it back, and the
        wait ends when a pass arrives after it.

        The deadline is noted on the `Run`, and noted on the cancellation path too,
        because the raise is the part that can go missing. A `sleep` in one branch of a
        task group is cancelled the moment another branch raises, and the write is held
        past that cancellation (see `step`), so the ordinary way to end up with a durable
        deadline and no `ScheduledWakeup` is not a crash but an ordinary fan-out. Left
        unnoted, no pass reports it and no driver schedules it: the workflow holds a
        deadline that was supposed to end a wait, and waits out a clock that will never
        fire.
        """

        async def deadline_from_now() -> str:
            return (self.now() + duration).isoformat()

        try:
            deadline = await self.step(key, deadline_from_now, parsing_deadline(key))
        except asyncio.CancelledError:
            recorded = self.recorded.get(key)
            if recorded is not None:
                self.waking[key] = parse_deadline(key, recorded)
            raise
        if self.now() < deadline:
            self.waking[key] = deadline
            raise ScheduledWakeup(key, due=deadline)

    async def awaiting[T](self, key: StepKey, parse: Parse[T]) -> T:
        """
        The value another process recorded under `key`, suspending until there is one.

        A signal, without a mailbox: whoever has the answer (an HTTP handler taking an
        approval, a webhook) writes one field into this workflow's checkpoint and asks
        for another pass. Because the wait is a *recorded value* rather than a message
        delivered to a running process, it outlives the process that was waiting and
        can be satisfied by any other.

        This is the value a caller is least able to assume anything about, since it
        crossed a trust boundary: a step at least chose its own effect, where here the
        workflow reads what an HTTP handler put there.
        """
        self.claim(key)
        if key not in self.recorded:
            raise InputNeeded(key)
        return parse(self.recorded[key])

    async def receive(
        self,
        key: StepKey,
        *,
        after: StepKey | None = None,
        limit: int | None = None,
    ) -> tuple[Entry, ...]:
        """
        The entries delivered to this workflow after `after`, suspending until there are any.

        `awaiting`'s role over a stream. Where that waits on one named value written once,
        this waits on a log the outside world appends to (`Durable.deliver`), and the
        workflow reads it with a cursor of its own rather than naming each message in
        advance.

        `after` is that cursor, and it is the caller's to carry: pass the key of the last
        entry you took, and the next call picks up behind it. Threading it explicitly is
        what makes two independent readers inside one workflow work without a rule about
        it, and what keeps the cursor a value rather than hidden state on the `Run`.

        It never returns empty. With nothing new it raises `MessageNeeded`, so the pass
        ends as `Listening` and whoever delivers next re-queues the workflow, which means
        `received[-1].key` is total and threading the cursor needs no branch. `pending` is
        the variant for a caller that wants whatever is there and would rather carry on.

        `limit` bounds the take, and is how a workflow consumes *part* of what has
        arrived: a consumer that treats the first new message as opening a unit of work
        and everything behind it as belonging to that unit cannot advance its cursor past
        the lot, so it takes one and leaves the rest for the next call.

        Reading the inbox is a **step**, and for the reason everything else here is one: a
        live read of a log somebody is still writing to gives two passes different
        answers. What gets recorded is the key of the last entry taken, so a replay hands
        back the same entries rather than whatever has arrived since. It is a *reference*
        rather than a copy, which is sound precisely because entries are immutable: the
        keys it names still hold what they held.

        No store round trip is needed to read them, either. Entries are ordinary records,
        so they are already in `recorded`, which is the snapshot this pass loaded at the
        top. An entry appended mid-pass is therefore invisible to this pass, which is
        correct rather than a limitation: the append made the workflow ready, so the next
        pass sees it.
        """
        self.claim(key)
        available = self.delivered(after, limit)
        if not available and key not in self.recorded:
            raise MessageNeeded(key)

        # Only reached when `available` is non-empty: a recorded bound was written by a
        # pass that took at least one entry, and nothing removes an entry, so a replay
        # under the same cursor sees at least what that pass saw.
        async def last_taken() -> object:
            return available[-1].key

        return through(available, await self.perform(key, last_taken, parsing_bound(key)))

    async def pending(
        self,
        key: StepKey,
        *,
        after: StepKey | None = None,
        limit: int | None = None,
    ) -> tuple[Entry, ...]:
        """
        The entries delivered after `after`, or nothing at all, without suspending.

        `receive` for a workflow that wants to fold in anything waiting and carry on
        regardless: a long-running unit of work checking for a cancellation, a reducer
        draining what has piled up since its last turn.

        It records how far it read exactly as `receive` does, including when it read
        nothing, and that write is the point rather than bookkeeping. Left unrecorded, a
        replay would re-evaluate against a fuller inbox and hand this pass entries the
        first one never saw, which is the divergence the whole step mechanism exists to
        prevent.
        """
        self.claim(key)
        available = self.delivered(after, limit)

        async def last_taken() -> object:
            return available[-1].key if available else after

        return through(available, await self.perform(key, last_taken, parsing_bound(key)))

    def delivered(self, after: StepKey | None, limit: int | None) -> tuple[Entry, ...]:
        """
        The inbox past `after`, as this pass sees it, in the order the entries were appended.

        Two guarantees carry the order, and both are the store's. The records arrive from
        `load` in the order they were first recorded, which is what puts these in append
        order among everything else the workflow has done; and `append` mints keys that
        sort, which is what makes the `>` against a cursor mean "later than". A store
        meeting one and not the other is a store this reads wrongly, which is why the
        conformance suite pins both.
        """
        unread = (
            Entry(key=key, value=value)
            for key, value in self.recorded.items()
            if key.startswith(INBOX) and (after is None or key > after)
        )
        return tuple(islice(unread, limit))

    def claim(self, key: StepKey) -> None:
        """
        Reserve `key` for this pass, refusing a name already used in it.

        Two steps sharing a name is the failure this mechanism is most exposed to: the
        second silently inherits the first's result, and no amount of re-running
        reveals it. The graph rejects a duplicate node key when the graph is built;
        the closest thing available here is to reject it the moment the second one is
        reached, which happens on every pass rather than only after a crash.

        The `INBOX` prefix is refused here for the same reason and at the same moment. A
        step named `inbox:3` would be read back by `receive` as a message somebody
        delivered, silently, and no amount of re-running would reveal it. The store owns
        that key space, so a workflow reaching into it is a collision worth failing on
        rather than a naming style.
        """
        if key.startswith(INBOX):
            raise ValueError(f"{key!r} is in the inbox key space the store assigns; name the step something else")
        if key in self.claimed:
            raise ValueError(f"{key!r} was already used in this pass; two steps cannot share a name")
        self.claimed.add(key)


def parse_deadline(key: StepKey, recorded: object) -> datetime:
    """The deadline `sleep` recorded, or a loud failure if the store holds something else."""
    if not isinstance(recorded, str):
        raise TypeError(f"{key!r} holds {recorded!r}, which is not a deadline this workflow wrote")
    return datetime.fromisoformat(recorded)


def through(available: tuple[Entry, ...], bound: StepKey | None) -> tuple[Entry, ...]:
    """
    The entries a read took, given everything in front of its cursor and how far it went.

    One rule covers both the pass that read and every pass that replays it, because the
    bound is a key rather than a count: `None` is a read that took nothing and had nothing
    before it, a bound equal to the caller's own cursor is a read that took nothing, and
    anything else names the last entry taken. A replay sees a fuller inbox and is cut back
    to exactly the same entries by the same comparison.
    """
    if bound is None:
        return ()
    return tuple(entry for entry in available if entry.key <= bound)


def parse_bound(key: StepKey, recorded: object) -> StepKey | None:
    """How far a read got, or a loud failure if the store holds something else."""
    if recorded is not None and not isinstance(recorded, str):
        raise TypeError(f"{key!r} holds {recorded!r}, which is not a cursor this workflow wrote")
    return recorded


def parsing_bound(key: StepKey) -> Parse[StepKey | None]:
    """`parse_bound` as the one-argument parser `perform` takes, closed over its key."""

    def parse(recorded: object) -> StepKey | None:
        return parse_bound(key, recorded)

    return parse


def parsing_deadline(key: StepKey) -> Parse[datetime]:
    """`parse_deadline` as the one-argument parser `step` takes, closed over its key."""

    def parse(recorded: object) -> datetime:
        return parse_deadline(key, recorded)

    return parse


@dataclass(frozen=True, slots=True)
class Completed[T]:
    """The pass ran the workflow to the end, and `value` is what it returned."""

    value: T


@dataclass(frozen=True, slots=True)
class Sleeping:
    """
    The pass stopped at a deadline the workflow chose, and nothing is owed but time.

    `due` is that deadline, read back from the checkpoint rather than recomputed, so a
    crash on day two of a three-day wait does not restart the clock. A driver schedules
    a wakeup for it.
    """

    key: StepKey
    due: datetime


@dataclass(frozen=True, slots=True)
class Waiting:
    """
    The pass stopped on a value only something outside it can supply.

    There is no deadline here and deliberately none to invent: no clock satisfies this
    wait, so a driver schedules nothing and whoever writes the value under `key` is what
    makes the workflow ready again.
    """

    key: StepKey


@dataclass(frozen=True, slots=True)
class Listening:
    """
    The pass stopped on the workflow's inbox, with nothing new in it.

    A driver does exactly what it does for `Waiting`, and that is not a reason to make
    them one type. `Waiting.key` is an *address*: a client holding it answers with
    `arrive(workflow, key, value)`. `key` here names the read step that stopped, which
    nobody writes to, and the answer is `deliver(workflow, value)`, addressed to the
    workflow rather than to any key. Folding the two together would leave a key that is
    sometimes somewhere to write and sometimes a diagnostic, which is the denormalized
    shape `Sleeping` and `Waiting` are already kept apart to avoid, and would leave a
    consumer asking "is this workflow waiting to be *told* something or waiting to be
    *sent* something" with nothing but its own step names to answer from.
    """

    key: StepKey


# What one pass at a workflow came to. It is a sealed union rather than one type with
# nullable fields because the answers have genuinely different shapes, and a driver that
# matches over them is told by the type checker when it has missed one.
type Outcome[T] = Completed[T] | Sleeping | Waiting | Listening


async def resume[T, Effect](
    holder: Pass,
    checkpointer: Checkpointer[Effect],
    body: Callable[[Run[Effect]], Awaitable[T]],
    *,
    now: Callable[[], datetime] = now_utc,
) -> Outcome[T]:
    """
    Make one pass at a claimed workflow, from whatever it has already recorded.

    Call it after a crash, after a wakeup, or after a value it was waiting on arrives:
    each call runs `body` from the top and reaches further than the last, and calling it
    on a finished workflow performs no effects at all.

    It returns what the pass came to rather than raising when the pass stops short, so
    "the workflow finished", "it is waiting out a deadline", and "it is waiting to be
    told" arrive as three values a caller matches over. What to *do* about each is still
    the caller's (schedule the wakeup, do nothing until the approval lands, hold the
    process open until `due`), which is the point: this reports, the driver decides.

    Only the two suspensions are converted. Anything the workflow's own code raises
    propagates untouched, including `Fenced` and `Contended`, because losing the
    workflow is not an outcome of a pass but a statement that this pass was never
    entitled to one.

    A `Suspended` that is neither of the two is the one thing rewritten rather than
    passed along, and the reason is what it would otherwise cost. `Outcome` has no arm
    for it and a driver has no way to answer it, so it can only travel outward; and it
    travels as an `Interruption`, which every sensible `except Exception` in a driver is
    built to miss, so one workflow raising it would take down the loop running every
    other workflow. Re-raised as an ordinary exception it is what it actually is: that
    workflow's mistake, and nobody else's.

    It takes a claim rather than making one, for the same reason it returns rather than
    raises. Whether to wait for a contended workflow, come back later, or fail is the
    driver's call: a worker holding a queue delivery wants one answer and a test driving
    a workflow it owns wants another. `claimed` is the second of those.

    An interruption raised inside a task group arrives wrapped, and is unwrapped here
    rather than being left to the driver, because wrapped is exactly where the harm is. A
    workflow that fans its steps out with `asyncio.TaskGroup` raises a
    `BaseExceptionGroup` when one of them suspends or loses the claim, and a group whose
    leaves are `BaseException`s is not itself an `Exception`: it is neither an `Outcome`,
    nor something a driver's `except (Fenced, Contended)` matches, nor something its
    `except Exception` can reach. So the one workflow that fanned out would take down the
    loop running every other one, whichever of the two happened to it. Both are unwrapped,
    for the same reason and by the same rule.

    Losing the claim wins over everything else in the group. It says this pass may not
    write at all, so a sibling's failure beside it is a consequence rather than a second
    piece of news, and reporting the sibling instead would tell a driver to log a workflow
    failure for a workflow that is fine and being advanced by somebody else.

    Several branches can suspend in one group, and a pass has one outcome, so a deadline
    wins over a wait for input. Both are answered eventually (the wakeup fires, and
    whoever writes the value queues the workflow either way), but only the deadline is
    answered by *this* driver: reporting the wait would leave nothing scheduled for a
    branch that asked for a clock. The earliest deadline wins among several, since a pass
    that wakes too early suspends again and one that wakes too late has kept a branch
    waiting for nothing.

    A deadline `Run.sleep` recorded counts even when its own suspension never arrived,
    which is what `waking` is for. A group cancels its remaining branches the instant one
    of them raises, so a branch that had just written its deadline can be cancelled
    between the write and the raise: the deadline is durable, and without this nothing
    reports it, so the driver schedules nothing and a workflow waits out a clock that will
    never fire. Reading it off the `Run` instead of off the exception is what makes the
    deadline-wins rule deliverable rather than merely stated.
    """
    run = Run(holder=holder, checkpointer=checkpointer, recorded=await checkpointer.load(holder.workflow), now=now)
    try:
        return Completed(await body(run))
    except Fenced, Contended:
        raise
    except Suspended as raised:
        return stopped_at([raised], run.waking)
    except BaseExceptionGroup as group:
        return unwound(group, run.waking)


def leaves(group: BaseException) -> Iterator[BaseException]:
    """Every exception in a group, however deeply the task groups nested it."""
    if isinstance(group, BaseExceptionGroup):
        for nested in group.exceptions:
            yield from leaves(nested)
    else:
        yield group


def unwound(
    group: BaseExceptionGroup[BaseException], waking: dict[StepKey, datetime]
) -> Sleeping | Waiting | Listening:
    """
    What a pass came to when its fan-out raised, or the failure re-raised without its
    suspensions.

    The split is by what each leaf says about *this pass's right to run*. A lost claim is
    raised bare, so a driver's `except (Fenced, Contended)` sees what it was written to
    see. Anything else that is not a suspension is a genuine failure of the workflow, and
    is re-raised with the suspensions taken out: left in, they would keep the group a
    `BaseExceptionGroup` and it would be invisible to the driver all over again.

    Taking them out is what this can do rather than a guarantee about what is left. A leaf
    that is a `BaseException` and neither a suspension nor a lost claim (a helper that
    called `sys.exit`, say) keeps the remainder a `BaseExceptionGroup` and so keeps it past
    a driver's `except Exception`, which for a worker means the process ends. That is the
    right answer for `SystemExit` and a poor one for the workflow, which is left to be
    retried into the next worker; a workflow that wants to fail should raise an ordinary
    exception.
    """
    for lost in leaves(group):
        if isinstance(lost, Fenced | Contended):
            raise lost from group
    _suspensions, failures = group.split(Suspended)
    if failures is not None:
        raise failures from None
    return stopped_at([each for each in leaves(group) if isinstance(each, Suspended)], waking)


def stopped_at(suspensions: list[Suspended], waking: dict[StepKey, datetime]) -> Sleeping | Waiting | Listening:
    """
    What a pass that stopped came to, given every suspension that reached its edge and
    every deadline it wrote on the way.

    The two sources are not redundant. `waking` holds what `Run.sleep` recorded, including
    a deadline whose suspension was cancelled before it could be raised, which is the one
    a driver would otherwise never hear about; the raised `ScheduledWakeup`s cover a
    workflow that raised one itself rather than through `sleep`. Neither is filtered by
    whether the deadline has since matured, because a wakeup scheduled for a moment
    already past is answered by a pass that runs immediately, finds the wait over, and
    carries on. Nothing is lost by being early.

    A `Suspended` that is neither of the two kinds is the one thing rewritten rather than
    passed along, and the reason is what it would otherwise cost. `Outcome` has no arm for
    it and a driver has no way to answer it, so it can only travel outward; and it travels
    as an `Interruption`, which every sensible `except Exception` in a driver is built to
    miss, so one workflow raising it would take down the loop running every other
    workflow. Re-raised as an ordinary exception it is what it actually is: that
    workflow's mistake, and nobody else's.
    """
    deadlines = dict(waking)
    for each in suspensions:
        if isinstance(each, ScheduledWakeup):
            deadlines.setdefault(each.key, each.due)
    if deadlines:
        key, due = min(deadlines.items(), key=lambda waiting: (waiting[1], waiting[0]))
        return Sleeping(key=key, due=due)
    # The two ways of waiting on the outside world are answered identically by a driver
    # (schedule nothing), so which one a fan-out reports when it produced both is
    # arbitrary; what it must not be is unstable, since two passes reporting different
    # outcomes for the same suspended workflow is a difference an operator would chase.
    # Addressed first, because it is the one a client can act on from the outcome alone.
    for each in suspensions:
        if isinstance(each, InputNeeded):
            return Waiting(key=each.key)
    for each in suspensions:
        if isinstance(each, MessageNeeded):
            return Listening(key=each.key)
    unreportable = suspensions[0]
    raise TypeError(
        f"{type(unreportable).__name__} is not a suspension a pass can report: "
        f"a workflow waits with `Run.sleep`, `Run.awaiting`, or `Run.receive`"
    ) from unreportable
