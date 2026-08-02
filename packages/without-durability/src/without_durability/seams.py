# The contracts, and nothing else. Every runner in this package talks through them and
# every store implements them, which is why they are worth reading first and worth
# keeping in one file.
#
# What they own is the guarantee, and that is the one thing a caller cannot supply for
# itself. A protocol of `load` and `record` alone is too weak to be safe at any scale: it
# has no way to say "only if nobody else is running this" or "only if I am still the one
# who may write", so two passes at one workflow both see a step unrecorded and both
# perform its effect, and no amount of care in the runner fixes it. `Pass` and the
# requirements on `Checkpointer` are where that is repaired. Which is also the point of
# the whole exercise: Temporal puts the exclusion in a server so the storage can be
# anything, DBOS puts it in Postgres so there need be no server, and either way it lives
# *below* the workflow code. Here it lives in the seam, which means a store gets to say
# how much of it it can offer.
#
# All three seams live here together because the third exists to say how the first two
# compose, and a reader deciding what a store owes should not have to visit three files
# to find out.

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
# discover it. Keeping a second copy per store would let the two drift silently, so a
# store that wants a different window sets `Scheduler.lease` and the worker follows it
# (see `Scheduler` and `worker.work`).
LEASE = timedelta(minutes=1)


def check_duration(name: str, duration: timedelta) -> None:
    """
    Refuse a duration that is not positive, where the value enters.

    Every timing here is an amount of time to let pass, and not one of them has a
    meaningful zero: a lease that has already expired when it is granted excludes nobody,
    a `poll` or a `tick` of zero spins, and a blocking read bounded by zero turns the
    worker's pull into a busy loop over the queue. So the check is on all of them rather
    than on the one that matters most, which is the lease: its failure is the only silent
    one (two passes then run the same unrecorded step and nothing raises at all), but
    "this one is quiet and those merely burn a core" is a poor reason to leave a
    footgun in, and one comparison at construction removes the question for every one of
    them.

    At the boundary rather than at the point of use, because a duration assembled from
    configuration (`timedelta(seconds=settings.lease_seconds)`, with the setting unset) is
    exactly how a zero arrives, and by the time a claim is being granted with it there is
    nothing left to say.

    Two durations are deliberately *not* run through this, because they are thresholds
    rather than intervals and their zero means something: `reclaim`'s `idle` (take over
    anything outstanding, however recently it was delivered) and SQLite's `busy_timeout`
    (do not wait for the write lock at all).

    What no check reaches is the bound that decides correctness, that a lease exceed the
    longest a pass can honestly take. Only the deployment knows that (see the guide's
    gaps), so this rules out the values that are nonsense rather than certifying the ones
    that are not.
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

    `first` is the part a caller cannot work out for itself, and the reason `record`
    returns a value rather than a bare `object`. Equality between what a pass handed in
    and what came back is not the same question: a result crosses a `CheckpointCodec` on
    the way in and out, so a step returning a tuple gets a list back from a JSON codec
    *having won outright*. A runner comparing those two would report a race that never
    happened. The store is the only party that sees both encodings, so it is the one that
    answers, and this carries the answer instead of leaving it to be inferred.

    It is true when the encoding stored is this pass's own, which covers a tie: two passes
    that ran the same effect and produced the same value have nothing to disagree about,
    whichever of them wrote first.

    Separating the two is what makes the equality *worth* testing rather than something to
    avoid. Once `first` answers "did I win", comparing `value` against what went in answers
    the other question cleanly: did this value survive its own store. `run_durably` makes
    exactly that check, and it is only sound because it is no longer overloaded onto the
    first one.

    `value` is what the store holds, *decoded*, which is the caller's value when it won and
    the winner's when it did not. It is read back through the codec either way rather than
    handed back as it came, so a value that does not survive the round trip does so on the
    first pass rather than surprising the second.
    """

    value: object
    first: bool


class Interruption(BaseException):
    """
    A control-flow signal from the durable machinery, not a failure of the work.

    It descends from `BaseException` for the reason `asyncio.CancelledError` does: an
    `except Exception` written to handle a workflow's *own* errors (a gateway declined, a
    row was missing) must not silently absorb a signal about whether this pass may run at
    all. Every one of these means something specific about the pass rather than about the
    workflow, and swallowing one leaves a caller carrying on from a position it no longer
    holds. Catching them is deliberate, by name, or not at all.
    """


class Contended(Interruption):
    """Another pass holds this workflow, so this caller does not get to run one."""


class Fenced(Interruption):
    """
    A write from a pass that has been superseded, refused rather than applied.

    Raised when a `Pass` outlives its claim and someone else has since taken the
    workflow. It means this pass has lost, not that the workflow has: whoever holds the
    newer claim carries on, and the right response is to stop, since every subsequent
    write would be refused too.

    Which is exactly why it is an `Interruption`. Compensating a saga, retrying a step, or
    logging a failure are all responses to the *workflow* going wrong, and every one of
    them is wrong here: the workflow is fine and someone else is advancing it, so a loser
    that unwinds is undoing work the winner is still building on.
    """


class Checkpointer[Effect = Never](Protocol):
    """
    Where a workflow's completed work is kept, and who is currently allowed to add to it.

    `Effect` is how *this* store expresses a piece of work it can perform and record in
    one commit, and it is a type parameter because there is no shared answer: a Redis
    store takes a Lua script over keys in that same Redis, a Postgres or SQLite one takes
    a callback handed a cursor inside the open transaction, and an in-memory one takes a
    function over its own dict (`memory.MemoryCheckpointer`). A store with nothing to offer
    here uses `Never`, which makes `transact` uncallable rather than absent, since a
    caller cannot produce a value of that type. That is also the default, so bare
    `Checkpointer` reads as "any store, never mind what it can co-commit". An effect only
    ever goes *in*, so the parameter is contravariant and `Checkpointer[Never]` is the
    supertype every concrete store satisfies: code that does not transact keeps the plain
    annotation and still accepts all of them.

    The narrow seam a durable runner talks through, so the store is injected rather than
    reached for: a Redis hash, a Postgres table, or a SQLite file in production, a plain
    dict in a test. Its keys are plain names rather than `without_dag`'s `NodeKey`,
    because the store is the piece the two mechanisms share: a graph records under its
    node names (`graph.run_durably`) and an ordinary function under its step names
    (`stepwise`), and the store cannot tell, nor should it.

    Four requirements, and they are the whole reason this protocol is not just a mapping.
    A store that cannot meet them cannot make a workflow safe to run, and the point of
    naming them here is that the *store* is where the guarantee has to live: a runner
    cannot construct exclusion out of a seam that has no way to express it.

    - `load` MUST return the values recorded for that workflow so far, and an empty
      mapping for one that has never run.
    - `claim` MUST grant at most one live `Pass` per workflow, and MUST issue tokens that
      strictly increase per workflow, so that a later claim always outranks an earlier
      one. It returns `None` when someone else holds the workflow.
    - `record` MUST refuse a write whose token is below the highest claimed for that
      workflow, raising `Fenced`, and MUST NOT overwrite a key that is already recorded.
      It returns a `Recorded`: the value stored *after* the call, which is the caller's if
      it won and the existing one if it did not, so two passes that both ran an effect at
      least agree on its result rather than diverging, and whether that value is this
      pass's own, which only the store can say.
    - `record` and `supply` MUST make the value durable before returning.
    - Every value MUST cross the store's `CheckpointCodec` in both directions, so that what
      `load` and `record` hand back is what a later pass will read rather than what this
      one happened to pass in. A store that skips the round trip on the way out is a store
      whose tests pass and whose resumed workflows see something else.

    `transact` is the one that changes the guarantee rather than protecting it, and it
    is why `Effect` exists. `record` is a second round trip after an effect already
    happened, so a crash in between leaves the effect done and unrecorded: at-least-once,
    the bound every durable engine lands on. `transact` closes that for the effects it
    can, by performing the work and writing the record in a single commit. It MUST run
    the effect at most once across every pass of a workflow, returning the recorded value
    without re-running when the step is already recorded, and it MUST NOT leave the
    effect applied without its record or the reverse.

    Only `record` reports who won, and the asymmetry is deliberate rather than an
    omission. It is the one write whose caller has a decision to make: `run_durably` has
    already handed a node's result to that node's dependents by the time it writes, so
    losing means the run is downstream of a value the store rejected and must stop.
    `supply` is called from outside any pass by a client that wants the stored value and
    nothing else, and `transact` runs at most once across every pass by construction, so
    there is no race for either of them to report. Giving all four the same return type
    would make them look alike without making any of them simpler.

    What bounds it is not this seam and not the store's feature list: you can only
    transact within one datastore. A Lua script commits atomically over Redis data, a
    Postgres transaction over Postgres data, and neither reaches the other or a payment
    gateway. So `transact` is available exactly for effects that live where the
    checkpoint lives, and everything else stays `record` plus an idempotent effect keyed
    by the workflow id. Postgres is not privileged here, it just tends to be where the
    data already is.
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

    The seam the API and the worker share: the API makes a workflow ready, a worker
    takes the next ready one and says when it is `done` with it. Injected like the
    checkpoint store, so the worker is drivable from a dict in a test.

    The requirements are about *not losing a wakeup*, since a lost one is a workflow
    that never runs again, and they are stated as properties rather than as mechanics
    because the implementations reach them by different routes. One is a Redis stream
    beside a sorted set; the others are a single structure scored by when a workflow
    becomes visible, where `wake_due`, `reclaim`, and `prepare` all have little or nothing
    to do. An implementation MUST guarantee that:

    - a workflow passed to `make_ready` is eventually yielded by some `next_ready`, even
      if the worker holding it dies mid-pass, and even if the wakeup arrives *while* a
      pass on that workflow is running;
    - `wake_due` moves each workflow it reports in one durable step, since one that
      removes a deadline and then queues the workflow loses it whenever it dies in
      between (an implementation with nothing to move satisfies this trivially);
    - a `wake_at` survives a `done` for a delivery taken before it, because the worker
      calls them in that order and the acknowledgement must not undo the scheduling.

    That last one is the tell that this seam does not stand alone, and `Durable` is what
    answers it: a requirement phrased as "because the caller does them in that order" is
    coupling the protocol carries without expressing.

    What is deliberately *not* required is that a workflow reach only one worker at a
    time. The stream will happily deliver two scheduler for one workflow to two consumers,
    and that is safe because exclusion belongs to `Checkpointer.claim` rather than here:
    this seam answers "who owes a pass", the checkpoint store answers "who may write".

    `lease` is how long a delivery stays its taker's, and it is on the store rather than
    an argument to every call because the same number has to bound the *checkpoint*
    claim. The implementations reach it by different routes (an idle threshold `reclaim`
    measures against, or the invisibility a visibility-scored queue writes when it takes
    one) and it is the answer to the same question either way, so `worker.work` reads it
    here and claims the workflow for exactly as long. A worker that took the two from
    different places would eventually take one shorter than the other, and the failure is
    quiet: a delivery reclaimed while its holder can still write is a pass spent finding
    out that somebody else owns the workflow. That is why this is the one knob and why
    turning it means constructing the scheduler with a different one.

    A `wake_at` deadline is the *caller's* clock, where a lease is measured by the
    store's. A workflow chooses its own deadline (`Run.sleep` records one), and nothing
    else can say what it meant, so the skew a lease is deliberately protected from is one
    a deadline still carries: a wakeup lands early or late by whatever the two clocks
    disagree by. It is a bound on how promptly a sleep ends rather than on correctness,
    since a pass that wakes too early finds its deadline unreached and suspends again.
    """

    @property
    def lease(self) -> timedelta: ...

    async def prepare(self) -> None: ...

    async def make_ready(self, workflow: str) -> None: ...

    async def wake_at(self, workflow: str, when: datetime) -> None: ...

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
    workflow that is recorded and unreachable. `Scheduler.wake_due` already answers exactly
    this shape of problem, by naming the *transition* rather than exposing its halves, so
    that a caller cannot hold a claimed-but-unqueued id at all. `arrive` is that same move
    one level up, and the fact that it had not been made was an inconsistency in this
    design rather than a matter of taste.

    The two stores stay separate underneath, because they genuinely can be separate: a
    Postgres checkpoint beside an SQS queue is an ordinary deployment, and forbidding it
    would be bundling a mechanism to fix a contract. What this seam changes is who
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
    cluster they are deliberately in different slots, which is the same thing as being
    in different stores), and what any pairing of unrelated products uses.

    The order is the whole of what it can offer, and it is not arbitrary: the record goes
    first, so a crash in the window leaves a workflow that has the value and lacks the
    wakeup. That state is recoverable by anything that asks again, which for the API is
    an ordinary client retry under the same idempotency key. The reverse order would
    queue a pass that wakes to find nothing recorded and answers for the delivery, which
    loses the value outright.
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
    owns outright. A worker taking scheduler off a queue wants `claim` itself, because
    losing the race is ordinary there and the answer is to come back later rather than
    to fail.
    """
    check_duration("a lease", lease)
    holder = await checkpointer.claim(workflow, lease)
    if holder is None:
        raise Contended(f"another pass holds {workflow!r}")
    return holder
