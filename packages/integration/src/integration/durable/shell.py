# The durable half: run a compiled graph against a store, so an interrupted run
# resumes where it stopped instead of starting over. It is deliberately tiny,
# because `CompiledGraph` already emits and accepts exactly the value a durable
# workflow needs: `stream` yields `(node key, result)` as each step lands, and
# `checkpoint=` takes that same mapping back. The engine is a loop.
#
# What this shell is *not* is a workflow framework. There is no scheduler, no
# worker fleet, no retry policy: the caller decides when to run and how often, and
# a run is just an `await`. That keeps the control flow visible in the caller's
# code, which is the same reason `without` is a library rather than a framework.
#
# What the seam *does* own is the guarantee, and that is the one thing a caller
# cannot supply for itself. A protocol of `load` and `record` alone is too weak to
# be safe at any scale: it has no way to say "only if nobody else is running this"
# or "only if I am still the one who may write", so two passes at one workflow both
# see a step unrecorded and both perform its effect, and no amount of care in the
# runner fixes it. `Pass` and the requirements on `Checkpoints` are where that is
# repaired. Which is also the point of the whole exercise: Temporal puts the
# exclusion in a server so the storage can be anything, DBOS puts it in Postgres so
# there need be no server, and either way it lives *below* the workflow code. Here
# it lives in the seam, which means a store gets to say how much of it it can offer.

from __future__ import annotations

from collections.abc import Callable
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import timedelta
from typing import Protocol
from typing import cast

from without_dag import CompiledGraph
from without_dag import NodeKey

# How long a claim is good for. It has to exceed the longest a pass can honestly take,
# since a pass that outlives its claim finds its writes `Fenced` and has to start over.
LEASE = timedelta(minutes=1)


@dataclass(frozen=True, slots=True)
class Pass:
    """
    The right to run one pass at a workflow, and the proof of it.

    `token` is a *fencing* token, not an identifier: it rises with every claim on this
    workflow, so comparing two of them says which pass is the newer one. That is what
    makes the exclusion survive a stalled process, which a lease alone cannot. A holder
    that pauses past its lease keeps its `Pass` and believes it still owns the workflow;
    the store is what knows better, because the next claim raised the number and every
    write carries one. See `Checkpoints.record`.
    """

    workflow: str
    token: int


class Contended(Exception):
    """Another pass holds this workflow, so this caller does not get to run one."""


class Fenced(Exception):
    """
    A write from a pass that has been superseded, refused rather than applied.

    Raised when a `Pass` outlives its claim and someone else has since taken the
    workflow. It means this pass has lost, not that the workflow has: whoever holds the
    newer claim carries on, and the right response is to stop, since every subsequent
    write would be refused too.
    """


class Checkpoints(Protocol):
    """
    Where a workflow's completed work is kept, and who is currently allowed to add to it.

    The narrow seam a durable runner talks through, so the store is injected rather than
    reached for: a Redis hash in production (`store.RedisCheckpoints`), a plain dict in a
    test. Its keys are plain names rather than this module's `NodeKey`, because the store
    is the piece the two mechanisms here share: a graph records under its node names
    (`run_durably`) and an ordinary function under its step names (`stepwise`), and the
    store cannot tell, nor should it.

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
      It returns the value that is stored *after* the call, which is the caller's if it
      won and the existing one if it did not, so two passes that both ran an effect at
      least agree on its result rather than diverging.
    - `record` and `supply` MUST make the value durable before returning.

    What no implementation can offer is a `record` that runs in the same transaction as
    the effect it is recording, unless the effect writes to the same store. That gap is
    what keeps every step at-least-once (see `run_durably`), and it is the one thing on
    this list that a store can be *unable* to provide rather than merely fail to.
    """

    async def load(self, workflow: str) -> dict[str, object]: ...

    async def claim(self, workflow: str, lease: timedelta) -> Pass | None: ...

    async def record(self, holder: Pass, key: str, value: object) -> object: ...

    async def supply(self, workflow: str, key: str, value: object) -> object: ...

    async def release(self, holder: Pass) -> None: ...


async def claimed(checkpoints: Checkpoints, workflow: str, lease: timedelta = LEASE) -> Pass:
    """
    Claim `workflow`, or raise because someone else has it.

    The form for a caller that expects to win: a test, or a runner driving a workflow it
    owns outright. A worker taking wakeups off a queue wants `claim` itself, because
    losing the race is ordinary there and the answer is to come back later rather than
    to fail.
    """
    holder = await checkpoints.claim(workflow, lease)
    if holder is None:
        raise Contended(f"another pass holds {workflow!r}")
    return holder


async def run_durably[*Ins, Out](
    run: CompiledGraph[*Ins, Out],
    checkpoints: Checkpoints,
    holder: Pass,
    *values: *Ins,
) -> Out:
    """
    Run `run` under a claimed workflow, recording each step and resuming from what
    is already recorded.

    Call it again with a fresh claim on the same workflow after a crash (or a
    timeout, or a redeploy) and the steps that finished are not re-entered: their
    results come back from the store, and only what was in flight or unstarted
    runs. Call it again after a *completed* run and nothing runs at all, which is
    what makes the whole call idempotent rather than merely restartable. The
    inputs are passed positionally every time, because an entry is not part of the
    checkpoint (it lives wherever the request itself does).

    The record is written before the next result is pulled, and `stream` is
    pull-driven, so no step *downstream* of a completed one starts until that
    one's result is durable: the write is a barrier, not a background flush.
    Siblings already in flight keep running, which is the point of the fan-out.

    A record that comes back holding something else means another pass recorded
    that node first. Unlike `stepwise`, this cannot simply adopt the winner's
    value: the graph handed its own to the node's dependents the moment the node
    finished, so the run is already downstream of a value the store rejected, and
    the only honest move left is to stop. Holding a claim makes that rare, and it
    is `Fenced` rather than this when the claim has lapsed.

    The exactly-once claim reaches exactly as far as that write: a crash between a
    step's effect and its record leaves the effect done and unrecorded, so the
    resumed run repeats it. That is at-least-once, the same bound every durable
    engine lands on unless the effect and the record share a transaction, which
    this seam cannot offer (see `Checkpoints`). The answer is the one they all
    give: make the effect itself idempotent, by passing the workflow as the
    payment gateway's idempotency key, rather than pretending the gap is closable
    here.
    """
    checkpoint = await checkpoints.load(holder.workflow)
    async for key, value in run.stream(*values, checkpoint=checkpoint):
        stored = await checkpoints.record(holder, key, value)
        if stored != value:
            raise Contended(f"{key!r} was recorded by another pass at {holder.workflow!r}")
        checkpoint[key] = stored
    # Every node has run or been restored, so the output is in hand either way. It
    # is read from the checkpoint rather than from the call, because a fully
    # recorded workflow runs nothing and so has nothing to return.
    return cast(Out, checkpoint[run.output])


async def run_saga[In, Out, Reached, Undone](
    forward: CompiledGraph[In, Out],
    unwind: CompiledGraph[Reached, Undone],
    reached: Callable[[Mapping[NodeKey, object]], Reached],
    checkpoints: Checkpoints,
    holder: Pass,
    value: In,
    *,
    lease: timedelta = LEASE,
) -> Out:
    """
    Run `forward` durably, and on failure compensate with `unwind` before re-raising.

    The saga, and it is this short because the checkpoint is a *value*: what the
    forward run achieved is a mapping of results, so deciding what to give back is
    a pure function of it (`reached` parses that mapping into whatever the
    compensation needs) rather than a replay log or an engine to interrogate. There
    is nothing automatic about which step compensates which, and deliberately so:
    that pairing is domain knowledge, and even the engines that formalize sagas
    leave the author to write both halves.

    The rollback runs through `run_durably` too, under its own key and its own
    claim, so it is checkpointed exactly like the forward run: a crash mid-rollback
    resumes it instead of refunding twice. Its claim is separate because it is a
    separate workflow, and taking it can fail the same way any claim can, which is
    the honest outcome: two processes compensating the same saga at once is what
    the exclusion exists to prevent. The original failure is re-raised once the
    compensation lands, because compensating does not make the workflow succeed; a
    failure *inside* the compensation propagates in its place, carrying the original
    as its context, since a half-unwound saga is the more urgent problem.

    Cancellation is not caught. It is not a failed workflow but a stopped one, and
    unwinding on the way out would fire the compensations while their forward steps
    may still be running.
    """
    try:
        return await run_durably(forward, checkpoints, holder, value)
    except Exception:
        unwinding = await claimed(checkpoints, f"{holder.workflow}:unwind", lease)
        try:
            await run_durably(unwind, checkpoints, unwinding, reached(await checkpoints.load(holder.workflow)))
        finally:
            await checkpoints.release(unwinding)
        raise
