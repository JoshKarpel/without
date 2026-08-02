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
# The other half is suspension. A step that cannot finish now raises `Suspended` instead
# of blocking, which is what buys the things a graph cannot express at all: a three-day
# settlement window, and a value another process has yet to write. Both are the same
# idea as a recorded result, so both are entries in the same mapping.

from __future__ import annotations

from collections.abc import Awaitable
from collections.abc import Callable
from dataclasses import dataclass
from dataclasses import field
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Never

from without_durability.seams import Checkpointer
from without_durability.seams import Interruption
from without_durability.seams import Pass

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
    straight-line code can stop in the middle of itself. Catch this to mean "the pass
    stopped short"; catch one of the two subclasses below to mean a particular reason.

    Error handling *around* a workflow must let this through: a suspended workflow is
    one that is going fine, and unwinding it would undo work it is still counting on.
    Being an `Interruption` is what makes that structural rather than a rule to
    remember.
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


class ScheduledWakeup(Suspended):
    """
    The pass is waiting out a deadline it chose itself (`Run.sleep`).

    `due` is that deadline, and it is not optional, which is the reason this is its own
    type rather than a field on `Suspended`. A driver decides what to do with a
    suspension by asking *which kind* it is, and the two answers are structurally
    different: this one carries a moment to schedule, and `InputNeeded` carries nothing
    because there is nothing to schedule. One class with a nullable `due` would make
    those two states the same shape and leave every consumer to re-derive them.
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
        """
        self.claim(key)
        if key in self.recorded:
            return parse(self.recorded[key])
        recorded = await self.checkpointer.record(self.holder, key, await effect())
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
        of this seam, which is why `Effect` is a type parameter and not a shared
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
        """

        async def deadline_from_now() -> str:
            return (self.now() + duration).isoformat()

        deadline = await self.step(key, deadline_from_now, parsing_deadline(key))
        if self.now() < deadline:
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

    def claim(self, key: StepKey) -> None:
        """
        Reserve `key` for this pass, refusing a name already used in it.

        Two steps sharing a name is the failure this mechanism is most exposed to: the
        second silently inherits the first's result, and no amount of re-running
        reveals it. The graph rejects a duplicate node key when the graph is built;
        the closest thing available here is to reject it the moment the second one is
        reached, which happens on every pass rather than only after a crash.
        """
        if key in self.claimed:
            raise ValueError(f"{key!r} was already used in this pass; two steps cannot share a name")
        self.claimed.add(key)


def parse_deadline(key: StepKey, recorded: object) -> datetime:
    """The deadline `sleep` recorded, or a loud failure if the store holds something else."""
    if not isinstance(recorded, str):
        raise TypeError(f"{key!r} holds {recorded!r}, which is not a deadline this workflow wrote")
    return datetime.fromisoformat(recorded)


def parsing_deadline(key: StepKey) -> Parse[datetime]:
    """`parse_deadline` as the one-argument parser `step` takes, closed over its key."""

    def parse(recorded: object) -> datetime:
        return parse_deadline(key, recorded)

    return parse


async def resume[T, Effect](
    holder: Pass,
    checkpointer: Checkpointer[Effect],
    body: Callable[[Run[Effect]], Awaitable[T]],
    *,
    now: Callable[[], datetime] = now_utc,
) -> T:
    """
    Make one pass at a claimed workflow, from whatever it has already recorded.

    Call it after a crash, after a wakeup, or after a value it was waiting on arrives:
    each call runs `body` from the top and reaches further than the last, and calling
    it on a finished workflow performs no effects at all. It raises a `Suspended` when
    the pass stops short, and which one says what the pass is waiting on, leaving what
    happens next to the caller (schedule the wakeup, do nothing until the approval
    lands, hold the process open until `due`): that policy belongs to whoever is
    driving rather than to this function.

    It takes a claim rather than making one, for the same reason. Whether to wait for a
    contended workflow, come back later, or fail is the driver's call: a worker holding
    a queue delivery wants one answer and a test driving a workflow it owns wants
    another. `claimed` is the second of those.
    """
    return await body(
        Run(holder=holder, checkpointer=checkpointer, recorded=await checkpointer.load(holder.workflow), now=now)
    )
