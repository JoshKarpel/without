# Run a compiled graph against a store, so an interrupted run resumes where it stopped
# instead of starting over. It is deliberately tiny, because `CompiledGraph` already
# emits and accepts exactly the value a durable workflow needs: `stream` yields
# `(node key, result)` as each step lands, and `checkpoint=` takes that same mapping
# back. The engine is a loop.
#
# What this is *not* is a workflow framework. There is no scheduler, no worker fleet,
# no retry policy: the caller decides when to run and how often, and a run is just an
# `await`. That keeps the control flow visible in the caller's code, which is the same
# reason `without` is a library rather than a framework.
#
# This is the only module here that depends on `without-dag`. The other mechanism
# (`stepwise`) needs no graph at all, and the two share nothing but the `Checkpointer`
# seam, which is the point: one checkpoint, two ways to spend it.

from __future__ import annotations

from collections.abc import Callable
from collections.abc import Mapping
from datetime import timedelta
from typing import cast

from without_dag import CompiledGraph
from without_dag import NodeKey

from without_durability.seams import LEASE
from without_durability.seams import Checkpointer
from without_durability.seams import Contended
from without_durability.seams import Pass
from without_durability.seams import claimed


async def run_durably[*Ins, Out](
    run: CompiledGraph[*Ins, Out],
    checkpointer: Checkpointer,
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
    node's effect and its record leaves the effect done and unrecorded, so the
    resumed run repeats it. That is at-least-once, and the answer for anything
    that leaves the datastore is the one every durable engine gives: make the
    effect itself idempotent, by passing the workflow as the payment gateway's
    idempotency key, rather than pretending the gap is closable here.

    A graph gets no `transact`, and the reason is the graph rather than the store.
    `Checkpointer.transact` closes that gap for effects the store can perform
    itself, but it closes it by making the effect and the record one call, and a
    node is an ordinary async function this runner only sees the *result* of. The
    stepwise mechanism reaches it because a step names its effect at the call site
    (`run.transact(...)`); expressing that here would mean a node type that hands
    the graph an effect instead of running one.
    """
    checkpoint = await checkpointer.load(holder.workflow)
    async for key, value in run.stream(*values, checkpoint=checkpoint):
        stored = await checkpointer.record(holder, key, value)
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
    checkpointer: Checkpointer,
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

    The rollback's id is the workflow's own with `:unwind` appended, which puts one
    constraint on ids in exchange for needing no second namespace: an id that already
    ends in `:unwind` addresses another workflow's rollback. Deriving a sibling by
    suffixing is what makes that true, so it holds against any store rather than being
    a property of how one of them builds keys, and like the rest of the id contract it
    is stated rather than checked.
    """
    try:
        return await run_durably(forward, checkpointer, holder, value)
    except Exception:
        unwinding = await claimed(checkpointer, f"{holder.workflow}:unwind", lease)
        try:
            await run_durably(unwind, checkpointer, unwinding, reached(await checkpointer.load(holder.workflow)))
        finally:
            await checkpointer.release(unwinding)
        raise
