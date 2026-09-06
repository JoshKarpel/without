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

import asyncio
from collections.abc import Mapping
from collections.abc import Sequence
from collections.abc import Set as AbstractSet
from contextlib import aclosing
from contextlib import suppress
from typing import cast

from without_dag import CompiledGraph
from without_dag import NodeKey

from without_durability.interfaces import Checkpointer
from without_durability.interfaces import Contended
from without_durability.interfaces import Pass
from without_durability.interfaces import Recorded


def survives(sent: object, restored: object) -> bool:
    """
    Whether a node's result came back out of the store as the same value it went in as.

    Equality is not that question, and the gap between them is where this check earns its
    place. A value whose round trip *changes its type* while comparing equal passes an
    `==` test and still breaks the thing the test exists to protect: an `IntEnum` node
    result is stored as its number and restored as a plain `int`, so the pass that
    computed it feeds its dependents `Tier.GOLD` and the pass that restored it feeds them
    `1`. Nothing raised, and the two passes computed different answers.

    So the type has to match at every level rather than only at the top, since the same
    substitution inside a container is the same divergence with a container around it.
    That includes a mapping's *keys*, which is the place it is easiest to miss: `key in
    restored` is answered by hashing and `==`, so an enum key finds its own number and
    reports a match. The stored key itself is fetched back and compared as a value, which
    is what makes the check reach it.

    Below the containers this falls back to equality, coerced to a `bool` so the return
    type is the one the signature promises. The coercion is not a safeguard: `bool(x)` is
    truthiness by definition, so a value whose `__eq__` answers elementwise still reports
    whatever its result is truthy for. Nothing here can repair a value whose equality does
    not answer a yes-or-no question.

    Iterative rather than recursive, because the recursion is over data whose depth the
    *codec* sets rather than this module: the stdlib's JSON encoder is C and happily
    round-trips a structure hundreds of levels deep, so a recursive walk would raise
    `RecursionError` out of a function annotated `-> bool` and fail a run whose value
    survived its store perfectly well.

    The pairs already compared are remembered, which is what keeps the walk finite. A
    codec that preserves references (a pickle, anything with a reference table) meets both
    of `CheckpointCodec`'s requirements and can carry a value that reaches itself, and a
    tree with parent pointers is an ordinary domain value rather than a contrived one; a
    walk without a memo would follow the cycle until it ran out of memory. The same memo
    turns shared substructure from exponential into linear, since a value reachable by
    many paths is compared once.
    """
    pending: list[tuple[object, object]] = [(sent, restored)]
    # By identity, because that is the question being memoized: two objects already
    # compared answer the same way again. They are reachable from `sent` and `restored`
    # for the whole walk, so nothing here is comparing a recycled id.
    compared: set[tuple[int, int]] = set()
    while pending:
        one, other = pending.pop()
        if (id(one), id(other)) in compared:
            continue
        compared.add((id(one), id(other)))
        if type(one) is not type(other):
            return False
        if isinstance(one, Mapping) and isinstance(other, Mapping):
            if len(one) != len(other):
                return False
            # The stored keys by identity of value, so a key can be compared as the value
            # it is rather than only looked up by equality.
            keys = {key: key for key in other}
            for key, value in one.items():
                if key not in keys:
                    return False
                pending.append((key, keys[key]))
                pending.append((value, other[key]))
            continue
        if isinstance(one, AbstractSet) and isinstance(other, AbstractSet):
            if len(one) != len(other):
                return False
            members = {member: member for member in other}
            for member in one:
                if member not in members:
                    return False
                pending.append((member, members[member]))
            continue
        # `str` and `bytes` are sequences of themselves, so they would walk forever on
        # anything but the empty one; they are values here, which the equality below
        # answers.
        if isinstance(one, Sequence) and isinstance(other, Sequence) and not isinstance(one, str | bytes):
            if len(one) != len(other):
                return False
            pending.extend(zip(one, other, strict=True))  # pragma: no mutate - lengths compared just above
            continue
        if not bool(one == other):
            return False
    return True


async def written(checkpointer: Checkpointer, holder: Pass, key: NodeKey, value: object) -> Recorded:
    """
    Record a node's result, and hold the write past a cancellation of this run.

    The node's effect has already happened by the time this is called, so cancelling the
    write does not undo it: it removes the record of it, and the pass that takes the
    workflow over performs it again. Cancellation is ordinary here rather than a crash,
    since a graceful shutdown or a rolling deploy cancels passes in flight, so without
    this the parcel goes twice on an ordinary Tuesday afternoon.

    It is `Run.step`'s shape, for `Run.step`'s reason, and it waits through repeated
    cancellation for the same one: the caller waits for a store round trip rather than for
    whatever the run was doing, and the claim is still held while it lands, since a
    release keeps the token.
    """
    recording = asyncio.ensure_future(checkpointer.record(holder, key, value))
    try:
        return await asyncio.shield(recording)
    except asyncio.CancelledError:
        # `wait` rather than an `await`: the write's own failure belongs to the run being
        # torn down, and raising it here would replace the cancellation with it.
        while not recording.done():
            with suppress(asyncio.CancelledError):
                await asyncio.wait([recording])
        raise


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
    result does not survive its own store fails the run on the pass that wrote it,
    naming the node. That is also why a graph needs no per-node parser where
    `stepwise` does: verifying beats parsing when you still hold what you sent.

    What that check is, exactly, is a diagnostic and not a repair, and the difference
    is worth stating because the failure reads like one. The store took the value
    before it could be compared (`record` is what produces the value to compare
    *against*), so the reshaped result is durable by the time this raises and a later
    pass will resume from it and run to completion, feeding dependents the restored
    shape with nothing left to complain. The run that discovers it is therefore the
    only one that can, which is what makes raising on it worth doing and why the
    answer is a codec that carries the value rather than a retry.

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
    # `aclosing`, because every way out of this loop but the last one is an exception, and
    # an abandoned async generator is not a stopped one. The nodes in flight when a pass is
    # `Fenced` are its *siblings*, still running their effects; without this they are held
    # alive by the traceback and run to completion, charging a card the winning pass is
    # also charging, after this call has already told its caller to stop. Closing the
    # stream throws into the generator, whose own `finally` cancels them.
    async with aclosing(run.stream(*values, checkpoint=checkpoint)) as completions:
        async for key, value in completions:
            recorded = await written(checkpointer, holder, key, value)
            if not recorded.first:
                raise Contended(f"{key!r} was recorded by another pass at {holder.workflow!r}")
            if not survives(value, recorded.value):
                raise TypeError(
                    f"{key!r} returned {value!r}, which this store reads back as {recorded.value!r}: "
                    f"a node whose result does not survive the checkpoint is resumed as something else"
                )
            checkpoint[key] = recorded.value
    # Every node has run or been restored, so the output is in hand either way. It
    # is read from the checkpoint rather than from the call, because a fully
    # recorded workflow runs nothing and so has nothing to return.
    return cast(Out, checkpoint[run.output])
