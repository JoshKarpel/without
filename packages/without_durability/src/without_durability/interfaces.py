# The interfaces, and nothing else. Every runner here talks through them and every store
# implements them, and all three live in one file because the third exists to say how
# the first two compose: a reader deciding what a store owes should not have to visit
# three files to find out.
#
# What they own is the guarantee, which is the one thing a caller cannot supply for
# itself. A protocol of `load` and `record` alone has no way to say "only if nobody else
# is running this" or "only if I am still the one who may write", so two passes at one
# workflow both see a step unrecorded and both perform its effect. `Pass` and the
# requirements on `Checkpointer` are where that is repaired; why it is repaired *here*
# rather than in a server or a required database is
# docs/without-durability/guarantees.md.

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
from typing import Never
from typing import Protocol

# How long a claim is good for, and how long a delivery stays its taker's. It has to
# exceed the longest a pass can honestly take, since a pass that outlives its claim finds
# its writes `Fenced` and has to start over.
#
# One number for both, defined once here and read by every store and the worker, because
# the two are not independent: a delivery that becomes reclaimable before its holder's
# claim expires is handed to a worker that cannot yet write, which spends a pass to
# discover it. A store that wants a different window sets `Scheduler.lease`, and `work`
# follows it.
LEASE = timedelta(minutes=1)


def check_duration(name: str, duration: timedelta) -> None:
    """
    Refuse a duration that is not positive, where the value enters.

    Every timing here is an amount of time to let pass, and not one of them has a
    meaningful zero: a lease already expired when granted excludes nobody, a `poll` or a
    `tick` of zero spins, and a blocking read bounded by zero turns the worker's pull
    into a busy loop. At the boundary rather than at the point of use, because a duration
    assembled from an unset setting (`timedelta(seconds=settings.lease_seconds)`) is
    exactly how a zero arrives.

    Two durations are deliberately *not* run through this, because they are thresholds
    rather than intervals and their zero means something: `reclaim`'s `idle` (take over
    anything outstanding, however recently it was delivered) and SQLite's `busy_timeout`
    (do not wait for the write lock at all).

    What no check reaches is the bound that decides correctness, that a lease exceed the
    longest a pass can honestly take. Only the deployment knows that, so this rules out
    the values that are nonsense rather than certifying the ones that are not.
    """
    if duration <= timedelta():
        raise ValueError(f"{name} must be a positive duration, but got {duration}")


@dataclass(frozen=True, slots=True)
class Pass:
    """
    The right to run one pass at a workflow, and the proof of it.

    `token` is a *fencing* token, not an identifier: it rises with every claim on this
    workflow, so comparing two of them says which pass is the newer one. That is what
    makes the exclusion survive a stalled process, which a lease alone cannot. A holder
    that pauses past its lease keeps its `Pass` and believes it still owns the workflow;
    the store is what knows better, because the next claim raised the number and every
    write carries one. See `Checkpointer.record`.
    """

    workflow: str
    token: int


@dataclass(frozen=True, slots=True)
class Recorded:
    """
    What a store holds under a step's key after a write, and whether this pass put it there.

    `value` is what the store holds, *decoded*: the caller's when it won and the winner's
    when it did not. It is read back through the codec either way rather than handed back
    as it came, so a value that does not survive the round trip shows that on the first
    pass rather than surprising the second.

    `first` is the part a caller cannot work out for itself, and the reason `record`
    returns a value rather than a bare `object`. Equality between what a pass handed in
    and what came back answers a different question, since a result crosses a
    `CheckpointCodec` both ways: a step returning a tuple gets a list back from a JSON
    codec *having won outright*, and a runner comparing the two would report a race that
    never happened. Only the store sees both encodings, so only the store can say. It is
    true when the encoding stored is this pass's own, which counts a tie as a win for
    both: two passes that ran the same effect have nothing to disagree about.

    Separating the two is what makes the equality worth testing rather than something to
    avoid. Once `first` answers "did I win", comparing `value` against what went in
    answers "did this value survive its own store", which is the check `run_durably`
    makes.
    """

    value: object
    first: bool


class Interruption(BaseException):
    """
    A control-flow signal from the durable machinery, not a failure of the work.

    It descends from `BaseException` for the reason `asyncio.CancelledError` does: an
    `except Exception` written to handle a workflow's *own* errors (a gateway declined, a
    row was missing) must not silently absorb a signal about whether this pass may run at
    all. Catching them is deliberate, by name, or not at all.
    """


class Contended(Interruption):
    """Another pass holds this workflow, so this caller does not get to run one."""


class Fenced(Interruption):
    """
    A write from a pass that has been superseded, refused rather than applied.

    Raised when a `Pass` outlives its claim and someone else has since taken the
    workflow. It means this pass has lost, not that the workflow has: whoever holds the
    newer claim carries on, and the right response is to stop, since every subsequent
    write would be refused too. Which is exactly why it is an `Interruption`, since
    compensating a saga or logging a failure are responses to the *workflow* going wrong
    and both are wrong here.
    """


class Checkpointer[Effect = Never](Protocol):
    """
    Where a workflow's completed work is kept, and who is currently allowed to add to it.

    The narrow interface a durable runner talks through, so the store is injected rather than
    reached for: a Redis hash, a Postgres table, or a SQLite file in production, a plain
    dict in a test. Its keys are plain names rather than `without_dag`'s `NodeKey`,
    because the store is the piece the two mechanisms share: a graph records under its
    node names (`run_durably`) and an ordinary function under its step names
    (`stepwise`), and the store cannot tell, nor should it.

    The requirements are the whole reason this protocol is not just a mapping. A runner
    cannot construct exclusion out of an interface with no way to express it, so a store that
    cannot meet them cannot make a workflow safe to run.

    - `load` MUST return the values recorded for that workflow so far, and an empty
      mapping for one that has never run.
    - `load` MUST return them in the order they were first recorded. A workflow's records
      have two independent writers, the pass through `record` and anything outside it
      through `supply`, and neither can order itself against the other: a counter either
      side keeps is read from a stale snapshot or observed and then raced, so both reach
      for the same next number and the tie has to be invented. The store is the only thing
      that sees every write, which makes it the only thing that can say. First-writer-wins
      already decides what a key holds; this says the same writer decides where it sits, so
      a losing write moves neither the value nor the position. The order is the guarantee
      and the number behind it is not: it is a `dict`, which preserves insertion order, so
      a caller reads the order by iterating and no implementation owes a sequence anyone
      can see.
    - `claim` MUST grant at most one live `Pass` per workflow, and MUST issue tokens that
      strictly increase per workflow, so that a later claim always outranks an earlier
      one. It returns `None` when someone else holds the workflow.
    - `record` MUST refuse a write whose token is below the highest claimed for that
      workflow, raising `Fenced`, and MUST NOT overwrite a key that is already recorded.
      It returns a `Recorded`: the value stored *after* the call, so two passes that both
      ran an effect at least agree on its result rather than diverging, and whether that
      value is this pass's own.
    - `record` and `supply` MUST make the value durable before returning.
    - Every value MUST cross the store's `CheckpointCodec` in both directions, so that
      what `load` and `record` hand back is what a later pass will read rather than what
      this one happened to pass in. A store that skips the round trip on the way out is a
      store whose tests pass and whose resumed workflows see something else.
    - `transact` MUST run the effect at most once across every pass of a workflow,
      returning the recorded value without re-running when the step is already recorded,
      and it MUST NOT leave the effect applied without its record or the reverse.

    `transact` is the one that changes the guarantee rather than protecting it. `record`
    is a second round trip after an effect already happened, so a crash in between leaves
    the effect done and unrecorded: at-least-once, the bound every durable engine lands
    on. Performing the work and writing the record in one commit closes that, for the
    effects a store can perform itself. What bounds *that* is neither this interface nor a
    store's feature list but the fact that you can only transact within one datastore
    (see docs/without-durability/guarantees.md).

    `Effect` is how a store expresses such a piece of work, and it is a type parameter
    because there is no shared answer: a Lua script over keys in the same Redis, a
    callback handed a cursor inside an open SQL transaction, a function over an in-memory
    store's own dict. A store with nothing to offer here uses `Never`, which makes
    `transact` uncallable rather than absent, since a caller cannot produce a value of
    that type. That is also the default, so bare `Checkpointer` reads as "any store,
    never mind what it can co-commit": an effect only ever goes *in*, so the parameter is
    contravariant and `Checkpointer[Never]` is the supertype every concrete store
    satisfies.

    Only `record` reports who won, and the asymmetry is deliberate. It is the one write
    whose caller has a decision to make, since `run_durably` has already handed a node's
    result to that node's dependents by the time it writes. `supply` is called from
    outside any pass by a client that wants the stored value and nothing else, and
    `transact` runs at most once by construction, so neither has a race to report.
    """

    async def load(self, workflow: str) -> dict[str, object]: ...

    async def claim(self, workflow: str, lease: timedelta) -> Pass | None: ...

    async def record(self, holder: Pass, key: str, value: object) -> Recorded: ...

    async def transact(self, holder: Pass, key: str, effect: Effect) -> object: ...

    async def supply(self, workflow: str, key: str, value: object) -> object: ...

    async def release(self, holder: Pass) -> None: ...


@dataclass(frozen=True, slots=True)
class Delivery:
    """
    One wakeup, taken by a worker and not yet acknowledged.

    The `receipt` is what makes the queue crash-safe: it names the entry the store is
    still holding on this worker's behalf, so acknowledging is a separate act from
    receiving and a worker that dies between them leaves the wakeup to be taken over
    rather than losing it.
    """

    workflow: str
    receipt: str


class Scheduler(Protocol):
    """
    Where a workflow's *right to run* is kept, apart from what it has done.

    The interface the API and the worker share: the API makes a workflow ready, a worker
    takes the next ready one and says when it is `done` with it. Injected like the
    checkpoint store, so the worker is drivable from a dict in a test.

    The requirements are about *not losing a wakeup*, since a lost one is a workflow that
    never runs again, and they are stated as properties rather than as mechanics because
    the implementations reach them by different routes. One is a Redis stream beside a
    sorted set; the rest are a single structure scored by when a workflow becomes
    visible, where `wake_due`, `reclaim`, and `prepare` all have little or nothing to do.
    An implementation MUST guarantee that:

    - a workflow passed to `make_ready` is eventually yielded by some `next_ready`, even
      if the worker holding it dies mid-pass, and even if the wakeup arrives *while* a
      pass on that workflow is running;
    - `wake_due` moves each workflow it reports in one durable step, since one that
      removes a deadline and then queues the workflow loses it whenever it dies in
      between (an implementation with nothing to move satisfies this trivially);
    - `wake_at` answers for its delivery and sets the workflow's next pass in one step,
      and MUST NOT overwrite a wakeup that arrived since that delivery was taken.

    That last one is why `wake_at` takes a `Delivery` rather than a workflow id, and it
    is the same move `wake_due` and `Durable.arrive` make. Scheduling and acknowledging
    were two calls with a rule about their order, which a protocol cannot enforce and a
    caller can get wrong; worse, on a store that holds one entry per workflow they are a
    read-modify-write over a value somebody else may have just written, so a confirmation
    that landed while the pass was ending was overwritten by the deadline the pass chose
    and waited days for a clock instead of running at once. Naming the transition instead
    means the store compares the receipt it handed out, which is the one thing that can
    tell the two apart.

    What is deliberately *not* required is that a workflow reach only one worker at a
    time. The stream will happily hand two deliveries for one workflow to two consumers,
    and that is safe because exclusion belongs to `Checkpointer.claim` rather than here:
    this interface answers "who owes a pass", the checkpoint store answers "who may write".

    `lease` is how long a delivery stays its taker's, and it is on the store rather than
    an argument to every call because the same number has to bound the *checkpoint*
    claim. The implementations reach it by different routes (an idle threshold `reclaim`
    measures against, or the invisibility a visibility-scored queue writes when it takes
    one) and it is the answer to the same question either way, so `work` reads it here
    and claims the workflow for exactly as long. Taking the two from different places
    fails quietly: a delivery reclaimed while its holder can still write is a pass spent
    finding out that somebody else owns the workflow.

    A `wake_at` deadline is the *caller's* clock, where a lease is measured by the
    store's, because a workflow chooses its own deadline (`Run.sleep` records one) and
    nothing else can say what it meant. So a wakeup lands early or late by whatever the
    two clocks disagree by, which bounds how promptly a sleep ends rather than
    correctness: a pass that wakes too early finds its deadline unreached and suspends
    again.
    """

    @property
    def lease(self) -> timedelta: ...

    async def prepare(self) -> None: ...

    async def make_ready(self, workflow: str) -> None: ...

    async def wake_at(self, delivery: Delivery, when: datetime) -> None: ...

    async def wake_due(self, now: datetime) -> tuple[str, ...]: ...

    async def next_ready(self, within: timedelta) -> Delivery | None: ...

    async def reclaim(self, idle: timedelta) -> Delivery | None: ...

    async def done(self, delivery: Delivery) -> None: ...


class Durable[Effect = Never](Protocol):
    """
    Both stores a workflow needs, and the transitions that have to be atomic across them.

    This exists because holding a `Checkpointer` and a `Scheduler` side by side is not
    simpler to use *correctly*. Making a workflow runnable is two writes to two places,
    and a caller that does them in the wrong order, or does the first and dies, leaves a
    workflow that is recorded and unreachable. `Scheduler.wake_due` already answers that
    shape of problem by naming the *transition* rather than exposing its halves, so that
    a caller cannot hold a claimed-but-unqueued id at all; `arrive` is the same move one
    level up.

    The two stores stay separate underneath, because they genuinely can be separate: a
    Postgres checkpoint beside an SQS queue is an ordinary deployment, and forbidding it
    would be bundling a mechanism to fix an interface. What this interface changes is who
    carries the coupling. Callers get one call with no ordering to get right, and what
    varies between implementations is not whether `arrive` exists but what it
    *guarantees*.

    - `arrive` MUST record `value` under `key` with first-writer-wins, exactly as
      `Checkpointer.supply` does, and MUST make the workflow ready. It returns the value
      stored after the call, the caller's if it won and the existing one if it did not.
    - `arrive` SHOULD be a single commit where the two stores are one datastore, which is
      what a store built on one database or one file can offer.
    - Where they are not, it MUST record before it queues (`SplitDurable`). The two
      failures are not symmetric: recorded-and-unqueued is a workflow waiting for a
      wakeup that a resubmission supplies, and queued-with-nothing-recorded is a pass
      that wakes, finds nothing to do, and drops the value on the floor.

    `Effect` is `Checkpointer`'s, threaded through so that a caller holding a `Durable`
    can still reach a store's `transact`, and defaulting to `Never` for the same reason.
    """

    @property
    def checkpointer(self) -> Checkpointer[Effect]: ...

    @property
    def scheduler(self) -> Scheduler: ...

    async def arrive(self, workflow: str, key: str, value: object) -> object: ...


@dataclass(frozen=True, slots=True)
class SplitDurable[Effect = Never]:
    """
    A `Durable` over two stores that are not one datastore, so `arrive` is two writes.

    The general composition, and the one that admits it cannot co-commit. It is what a
    Redis deployment uses (the checkpoint hash and the queue live in one Redis, but on a
    cluster they are deliberately in different slots, which is the same thing as being in
    different stores), and what any pairing of unrelated products uses.

    The order is the whole of what it can offer, and it is not arbitrary: the record goes
    first, so a crash in the window leaves a workflow that has the value and lacks the
    wakeup, which anything asking again supplies. The reverse would queue a pass that
    wakes to find nothing recorded and answers for the delivery, losing the value
    outright.
    """

    checkpointer: Checkpointer[Effect]
    scheduler: Scheduler

    async def arrive(self, workflow: str, key: str, value: object) -> object:
        stored = await self.checkpointer.supply(workflow, key, value)
        await self.scheduler.make_ready(workflow)
        return stored


async def claimed(checkpointer: Checkpointer, workflow: str, lease: timedelta = LEASE) -> Pass:
    """
    Claim `workflow`, or raise because someone else has it.

    The form for a caller that expects to win: a test, or a runner driving a workflow it
    owns outright. A worker taking deliveries off a queue wants `claim` itself, because
    losing the race is ordinary there and the answer is to come back later rather than to
    fail.
    """
    check_duration("a lease", lease)
    holder = await checkpointer.claim(workflow, lease)
    if holder is None:
        raise Contended(f"another pass holds {workflow!r}")
    return holder
