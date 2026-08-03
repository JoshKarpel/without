# Run a compiled graph against a store, so an interrupted run resumes where it stopped
# instead of starting over. It is deliberately tiny, because `CompiledGraph` already
# emits and accepts exactly the value a durable workflow needs: `stream` yields
# `(node key, result)` as each step lands, and `checkpoint=` takes that same mapping
# back. The engine is a loop.
#
# There is no scheduler here, no worker fleet, and no retry policy: the caller decides
# when to run and how often, and a run is just an `await`.
#
# There is no saga here either, and that is a claim rather than a gap. A compensation is
# another graph, so unwinding one workflow is running a second one: a `try` around this
# call and another call to it in the `except`, under an id the application chose. Shipping
# that as a function would add nothing but a name for the rollback's workflow, which is a
# name in the *caller's* id namespace, so the library would be reserving a suffix that
# every id-minting site then has to know about and refuse. The pairing of a step with its
# undo is domain knowledge either way (every engine that formalizes sagas leaves the
# author to write both halves), so what is left to abstract is the eight lines that hold
# them. See docs/without-durability/index.md, which writes them out.
#
# This is the only module here that depends on `without-dag`. The other mechanism
# (`stepwise`) needs no graph at all, and the two share nothing but the `Checkpointer`
# interface, which is the point: one checkpoint, two ways to spend it.

from __future__ import annotations

from typing import cast

from without_dag import CompiledGraph

from without_durability.interfaces import Checkpointer
from without_durability.interfaces import Contended
from without_durability.interfaces import Pass


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

    A record the store did not take from this pass means another pass recorded
    that node first. Unlike `stepwise`, this cannot simply adopt the winner's
    value: the graph handed its own to the node's dependents the moment the node
    finished, so the run is already downstream of a value the store rejected, and
    the only honest move left is to stop. Holding a claim makes that rare, and it
    is `Fenced` rather than this when the claim has lapsed. Whether the store took
    the value is `Recorded.first` rather than something inferred by comparison,
    since a result crosses a `CheckpointCodec` and a run that won outright can be
    handed back something unequal.

    With that separated out, the comparison becomes the *other* check worth making,
    and this is the one place able to make it, because it holds both values at once.
    A graph feeds a node's result straight to its dependents, so without it they see
    a `tuple` on the pass that computed the node and a `list` on the pass that
    restored it, with no crash needed for the two to disagree. So a node whose
    result does not survive its own store is refused on the pass that wrote it,
    naming the node. That is also why a graph needs no per-node parser where
    `stepwise` does: verifying beats parsing when you still hold what you sent.

    A graph gets no `transact`, and the reason is the graph rather than the store.
    Closing the at-least-once gap means making the effect and the record one call,
    and a node is an ordinary async function this runner only sees the *result* of.
    A step reaches it because it names its effect at the call site
    (`run.transact(...)`); expressing that here would mean a node type that hands
    the graph an effect instead of running one. So a crash between a node's effect
    and its record repeats the effect, and the answer for anything leaving the
    datastore is the ordinary one: make it idempotent under the workflow id.

    A graph whose output is one of its own *entries* is refused rather than run.
    `evaluate` supports that identity plan, because an entry it was handed is a
    value it can return; here the output is read back out of the checkpoint, and an
    entry is the one thing a checkpoint never holds (it is fed positionally on
    every call, which is why `Graph.of` keys entries by position). So the run would
    record every node correctly and then fail looking for a key that was never
    going to be there.
    """
    if run.output in run.inputs:
        raise ValueError(
            f"{run.output!r} is one of this graph's entries rather than a node, and an entry is "
            f"never recorded, so there would be nothing to resume from or to return"
        )
    checkpoint = await checkpointer.load(holder.workflow)
    async for key, value in run.stream(*values, checkpoint=checkpoint):
        recorded = await checkpointer.record(holder, key, value)
        if not recorded.first:
            raise Contended(f"{key!r} was recorded by another pass at {holder.workflow!r}")
        if recorded.value != value:
            raise TypeError(
                f"{key!r} returned {value!r}, which this store reads back as {recorded.value!r}: "
                f"a node whose result does not survive the checkpoint cannot be resumed from one"
            )
        checkpoint[key] = recorded.value
    # Every node has run or been restored, so the output is in hand either way. It
    # is read from the checkpoint rather than from the call, because a fully
    # recorded workflow runs nothing and so has nothing to return.
    return cast(Out, checkpoint[run.output])
