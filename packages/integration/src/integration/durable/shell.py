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

from __future__ import annotations

from collections.abc import Callable
from collections.abc import Mapping
from typing import Protocol
from typing import cast

from without_dag import CompiledGraph
from without_dag import NodeKey


class Checkpoints(Protocol):
    """
    Where a workflow's completed steps are kept, keyed by workflow.

    The narrow seam the durable runner talks through, so the store is injected
    rather than reached for: a Redis hash in production (`store.RedisCheckpoints`),
    a plain dict in a test. `load` MUST return the results recorded for that
    workflow so far, an empty mapping for one that has never run, and `record`
    MUST make one result durable before it returns.
    """

    async def load(self, workflow: str) -> dict[NodeKey, object]: ...

    async def record(self, workflow: str, key: NodeKey, value: object) -> None: ...


async def run_durably[*Ins, Out](
    run: CompiledGraph[*Ins, Out],
    checkpoints: Checkpoints,
    workflow: str,
    *values: *Ins,
) -> Out:
    """
    Run `run` under an idempotency key, recording each step and resuming from what
    is already recorded.

    Call it again with the same `workflow` after a crash (or a timeout, or a
    redeploy) and the steps that finished are not re-entered: their results come
    back from the store, and only what was in flight or unstarted runs. Call it
    again after a *completed* run and nothing runs at all, which is what makes the
    whole call idempotent rather than merely restartable. The inputs are passed
    positionally every time, because an entry is not part of the checkpoint (it
    lives wherever the request itself does).

    The record is written before the next result is pulled, and `stream` is
    pull-driven, so no step *downstream* of a completed one starts until that
    one's result is durable: the write is a barrier, not a background flush.
    Siblings already in flight keep running, which is the point of the fan-out.

    The exactly-once claim reaches exactly as far as that write: a crash between a
    step's effect and its record leaves the effect done and unrecorded, so the
    resumed run repeats it. That is at-least-once, the same bound every durable
    engine lands on, and the answer is the same one they give: make the effect
    itself idempotent (pass `workflow` as the payment gateway's idempotency key),
    rather than pretending the gap is closable here.
    """
    checkpoint = await checkpoints.load(workflow)
    async for key, value in run.stream(*values, checkpoint=checkpoint):
        await checkpoints.record(workflow, key, value)
        checkpoint[key] = value
    # Every node has run or been restored, so the output is in hand either way. It
    # is read from the checkpoint rather than from the call, because a fully
    # recorded workflow runs nothing and so has nothing to return.
    return cast(Out, checkpoint[run.output])


async def run_saga[In, Out, Reached, Undone](
    forward: CompiledGraph[In, Out],
    unwind: CompiledGraph[Reached, Undone],
    reached: Callable[[Mapping[NodeKey, object]], Reached],
    checkpoints: Checkpoints,
    workflow: str,
    value: In,
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

    The rollback runs through `run_durably` too, under its own key, so it is
    checkpointed exactly like the forward run: a crash mid-rollback resumes it
    instead of refunding twice. The original failure is re-raised once the
    compensation lands, because compensating does not make the workflow succeed; a
    failure *inside* the compensation propagates in its place, carrying the original
    as its context, since a half-unwound saga is the more urgent problem.

    Cancellation is not caught. It is not a failed workflow but a stopped one, and
    unwinding on the way out would fire the compensations while their forward steps
    may still be running.
    """
    try:
        return await run_durably(forward, checkpoints, workflow, value)
    except Exception:
        await run_durably(unwind, checkpoints, f"{workflow}:unwind", reached(await checkpoints.load(workflow)))
        raise
