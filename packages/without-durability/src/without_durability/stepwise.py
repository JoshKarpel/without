# The same durability, without the graph: a workflow is an ordinary async function
# whose effects are named. `run.step(key, effect, parse)` runs the effect once and
# records what it returned; resuming means calling the function again, where each step
# it reaches hands back what is already recorded instead of running. The whole engine is
# the dict lookup in `Run.step`.
#
# Every step takes a parser, and that is the one piece of ceremony here that earns its
# place. A step never hands back the object its effect produced: it hands back what the
# *store* holds, read through a codec, so the value has already been somewhere that does
# not preserve Python types. Annotating the effect and casting would be a lie on the
# first pass, not merely after a crash, since a tuple comes back a list the moment it is
# recorded. A parser is the same thing the rest of this repo does at every boundary, and
# unlike a cast it can fail loudly when the store holds something else.
#
# What is re-executed is the code *between* the steps, and that is the one rule this
# mechanism asks for: effects must live in steps, the code around them must be pure. Temporal
# and DBOS state that rule as workflow determinism (their recovery re-runs the same
# code for the same reason); here it is the functional-core/imperative-shell split the
# rest of this repo already keeps, so a workflow that respects it is resumable and one
# that does not double-charges the card in the code between two steps.
#
# Keying by an explicit *name* rather than by position in a history is what makes the
# rule that mild. Reordering two independent steps changes nothing, and inserting a
# step ahead of an existing one changes nothing: the new step runs and the old one
# still finds its record. What no longer holds is the graph's eager check, since the
# set of keys a function will use is not knowable until it has used them, where a
# `CompiledGraph` rejects a checkpoint key naming no node before running anything.
#
# The other half is suspension. A step that cannot finish now raises `Suspended`
# instead of blocking, which is what buys the things a graph cannot express at all: a
# three-day settlement window, and a value another process has yet to write. Both are
# the same idea as a recorded result, so both are just entries in the same mapping.
#
# `Run.transact` is the one place a step is more than bookkeeping. Naming the effect at
# the call site is what makes it possible: `step` is handed a thunk and can only run it
# and write afterwards, where `transact` is handed something the *store* can perform, so
# the work and the record become one commit and that step stops being at-least-once. It
# is available exactly for effects that live in the store, which is a fact about
# distributed transactions rather than about any store's feature list.

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
# It is one function rather than half of a per-step codec, and the asymmetry is the same
# one `CheckpointCodec` has from the other side. Encoding is uniform, so the store does
# it once for every key; a step that encoded for itself would have to lower its value
# into something the store's codec also understands, which is a second pass over the
# same data for nothing (see `CheckpointCodec`).
type Parse[T] = Callable[[object], T]


class Suspended(Interruption):
    """
    The pass cannot go further until `key` is recorded. Nothing has failed.

    A control-flow signal wearing an exception's clothes, so a workflow written as
    straight-line code can stop in the middle of itself. `due` is set when the wait
    is a deadline the workflow itself chose (`Run.sleep`) and `None` when it is on a
    value only the outside world can supply (`Run.awaiting`), which is exactly the
    difference between scheduling a wakeup and waiting to be told.

    Error handling *around* a workflow must let this through, for the same reason
    `run_saga` does not compensate on cancellation: a suspended workflow is one that
    is going fine, and unwinding it would undo work it is still counting on. Being an
    `Interruption` is what makes that structural rather than a rule to remember, since
    the `except Exception` a workflow author writes around a flaky gateway no longer
    reaches it.
    """

    def __init__(self, key: StepKey, due: datetime | None = None) -> None:
        self.key = key
        self.due = due
        until = f" until {due.isoformat()}" if due is not None else ""
        super().__init__(f"suspended at {key!r}{until}")


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
    than the store.

    `holder` is this pass's claim on the workflow, and carrying it is what lets a step
    write: there is no way to record without one, which is the invariant made
    structural rather than remembered.
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
        on every path rather than only after a crash. Parsing at the point where the
        value re-enters the workflow is the same move the rest of this repo makes at
        every boundary, and it is the only one that can be checked.

        The effect's own return type is deliberately not tied to `parse`'s. What goes
        in and what comes out are related by encode-then-decode, which is not the
        identity, so requiring one type for both would assert something false. `sleep`
        is the proof rather than the exception: it records an ISO string and reads back
        a `datetime`, and a step that records a mapping to rebuild a domain value does
        the same thing. `effect` therefore promises only something the codec can carry,
        which no annotation expresses, and `parse` says what the workflow gets.

        It also decides how much a codec has to carry. A richer codec narrows what a
        parser has to repair, and no codec removes it: `pydantic_core` renders a tuple
        as a JSON array too, because the round trip is lossy by nature rather than by
        the stdlib's weakness. So the codec is a transport concern, uniform over every
        key, and this is a meaning concern, particular to one.

        The record is written before the step returns, so the workflow never proceeds
        on a result the store has not accepted.

        What comes back from `record` is what the *store* holds, not necessarily what
        this effect returned. The difference shows up when two passes both ran the
        effect: the first to record wins, the second is handed the winner's value, and
        from there they proceed identically instead of two workflows diverging on which
        capture id is real. The duplicate effect is not prevented by that, only made
        harmless downstream; preventing it is what the claim is for, and the two
        together are why a step is written this way.

        Which of the two happened is on the `Recorded` and is deliberately ignored here.
        A step has already been told the winner's value and has no dependents holding
        the loser's, so adopting it and carrying on is the whole response; `run_durably`
        is the caller that cannot, because a graph fed its node's result downstream
        before the write.
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
        leaves the effect done and unrecorded and the next pass repeats it: at-least-once.
        Here the store performs the work and writes the record together, so there is no
        in-between for a crash to land in, and re-running the workflow performs nothing.

        The price is that `effect` has to be something the *store* can perform, which
        means it has to live in the store: a Lua script over keys in the same Redis, a
        callback over a cursor in the same Postgres transaction. An effect that leaves the datastore (a
        payment gateway, a carrier) cannot be in the commit, is not a transaction anyone
        can offer, and belongs in `step` with an idempotency key. That boundary is a fact
        about distributed transactions rather than a limitation of this seam, which is
        why `Effect` is a type parameter and not a shared interface.

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
            raise Suspended(key, due=deadline)

    async def awaiting[T](self, key: StepKey, parse: Parse[T]) -> T:
        """
        The value another process recorded under `key`, suspending until there is one.

        A signal, without a mailbox: whoever has the answer (an HTTP handler taking an
        approval, a webhook) writes one field into this workflow's checkpoint and asks
        for another pass. Because the wait is a *recorded value* rather than a message
        delivered to a running process, it outlives the process that was waiting and
        can be satisfied by any other.

        This is the value most worth parsing and the one a caller is least able to
        assume anything about: it crossed a trust boundary, written by something that
        shares nothing with this workflow but an id. A step at least chose its own
        effect; here the workflow is reading what an HTTP handler put there.
        """
        self.claim(key)
        if key not in self.recorded:
            raise Suspended(key)
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
    it on a finished workflow performs no effects at all. It raises `Suspended` when
    the pass stops short, leaving what happens next to the caller (schedule a wakeup,
    do nothing until the approval lands, hold the process open until `due`), because
    that policy belongs to whoever is driving rather than to this function.

    It takes a claim rather than making one, for the same reason. Whether to wait for a
    contended workflow, come back later, or fail is the driver's call: a worker holding
    a queue delivery wants one answer and a test driving a workflow it owns wants
    another. `shell.claimed` is the second of those.
    """
    return await body(
        Run(holder=holder, checkpointer=checkpointer, recorded=await checkpointer.load(holder.workflow), now=now)
    )
