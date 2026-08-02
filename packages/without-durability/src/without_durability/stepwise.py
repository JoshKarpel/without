# The same durability, without the graph: a workflow is an ordinary async function
# whose effects are named. `run.step(key, effect)` runs the effect once and records
# what it returned; resuming means calling the function again, where each step it
# reaches hands back what is already recorded instead of running. The whole engine is
# the dict lookup in `Run.step`.
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
from typing import cast

from without_durability.seams import Checkpointer
from without_durability.seams import Pass

type StepKey = str


class Suspended(Exception):
    """
    The pass cannot go further until `key` is recorded. Nothing has failed.

    A control-flow signal wearing an exception's clothes, so a workflow written as
    straight-line code can stop in the middle of itself. `due` is set when the wait
    is a deadline the workflow itself chose (`Run.sleep`) and `None` when it is on a
    value only the outside world can supply (`Run.awaiting`), which is exactly the
    difference between scheduling a wakeup and waiting to be told.

    Error handling *around* a workflow must let this through, for the same reason
    `run_saga` does not compensate on cancellation: a suspended workflow is one that
    is going fine, and unwinding it would undo work it is still counting on.
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

    async def step[T](self, key: StepKey, effect: Callable[[], Awaitable[T]]) -> T:
        """
        Run `effect` once across every pass of this workflow, recording what it returns.

        The recorded value is what later passes get back, so it round-trips through
        the store's codec: a step that returns a value the codec cannot carry hands
        back something else after a crash (JSON here, so keep step results JSON-native,
        the same constraint the graph's nodes carry). The record is written before the
        step returns, so the workflow never proceeds on a result the store has not
        accepted.

        What comes back from `record` is what the *store* holds, not necessarily what
        this effect returned, and this returns that. The difference shows up when two
        passes both ran the effect: the first to record wins, the second is handed the
        winner's value, and from there they proceed identically instead of two
        workflows diverging on which capture id is real. The duplicate effect is not
        prevented by that, only made harmless downstream; preventing it is what the
        claim is for, and the two together are why a step is written this way.
        """
        self.claim(key)
        if key in self.recorded:
            return cast(T, self.recorded[key])
        stored = await self.checkpointer.record(self.holder, key, await effect())
        self.recorded[key] = stored
        return cast(T, stored)

    async def transact(self, key: StepKey, effect: Effect) -> object:
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

        The result comes back as `object` rather than the effect's own type, because
        what is returned is what the *store* holds after the commit: read back from the
        record on a later pass, so it round-trips through the codec like any step.
        """
        self.claim(key)
        if key in self.recorded:
            return self.recorded[key]
        stored = await self.checkpointer.transact(self.holder, key, effect)
        self.recorded[key] = stored
        return stored

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

        deadline = parse_deadline(key, await self.step(key, deadline_from_now))
        if self.now() < deadline:
            raise Suspended(key, due=deadline)

    async def awaiting(self, key: StepKey) -> object:
        """
        The value another process recorded under `key`, suspending until there is one.

        A signal, without a mailbox: whoever has the answer (an HTTP handler taking an
        approval, a webhook) writes one field into this workflow's checkpoint and asks
        for another pass. Because the wait is a *recorded value* rather than a message
        delivered to a running process, it outlives the process that was waiting and
        can be satisfied by any other. The value crosses a trust boundary, so it comes
        back as `object` for the workflow to parse.
        """
        self.claim(key)
        if key not in self.recorded:
            raise Suspended(key)
        return self.recorded[key]

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
