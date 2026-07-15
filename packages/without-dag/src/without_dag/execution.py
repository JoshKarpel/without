from __future__ import annotations

import asyncio
from collections import Counter
from collections import deque
from collections.abc import AsyncGenerator
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Hashable
from collections.abc import Iterable
from collections.abc import Mapping
from dataclasses import dataclass
from graphlib import TopologicalSorter

from without import cancel_futures

type NodeKey = Hashable


@dataclass(frozen=True, slots=True)
class Node:
    """
    One async step in a graph, named by `key` and wired by `dependencies`.

    The narrow seam a graph-defining frontend lowers onto: a `Node` is a value,
    not a place. `run` receives its dependencies' results as a tuple in
    `dependencies` order and returns this node's single result. Results cross
    this seam as `object` (the executor cannot know each step's type); a typed
    frontend restores precision above it, exactly as `without-web`'s `Extractor`
    values are collected with `object` values and re-typed by `into`.
    """

    key: NodeKey
    dependencies: tuple[NodeKey, ...]
    run: Callable[[tuple[object, ...]], Awaitable[object]]


@dataclass(frozen=True, slots=True)
class Plan:
    """
    A node set compiled into its input-independent scheduling structure.

    Everything a run needs that does *not* depend on the input values: the nodes
    by key, the dependency edges (as the graph `graphlib` wants), and the number
    of consumers per key (so a result can be freed once its last dependent has
    read it). Computed once and reused across runs as a value, so a fixed graph
    driven per event never rebuilds any of it.
    """

    by_key: Mapping[NodeKey, Node]
    dependencies: Mapping[NodeKey, tuple[NodeKey, ...]]
    consumers: Mapping[NodeKey, int]

    @classmethod
    def of(cls, nodes: Iterable[Node]) -> Plan:
        """Compile a node set into a reusable `Plan`."""
        by_key = {node.key: node for node in nodes}
        dependencies = {key: node.dependencies for key, node in by_key.items()}
        consumers: Counter[NodeKey] = Counter()
        for node in by_key.values():
            consumers.update(node.dependencies)
        return cls(by_key=by_key, dependencies=dependencies, consumers=consumers)


async def drive(
    plan: Plan,
    inputs: Mapping[NodeKey, object],
    limit: int | None,
) -> AsyncGenerator[tuple[NodeKey, object]]:
    """
    Run a compiled `Plan`, yielding each `(key, result)` the instant it completes.

    The streaming core, and the *events* half of the model. Yields completions in
    whatever order nodes finish; the only ordering guarantee is the causal one, a
    node after the dependencies it consumed. `inputs` pre-supplies the values of
    source keys, marked done without running and never yielded. `limit` caps how
    many nodes run concurrently (`None` is unbounded).

    Scheduling drains completions off an `asyncio.Queue` that each spawned future
    feeds via a done-callback attached once at spawn, rather than
    `asyncio.wait(..., FIRST_COMPLETED)` re-registering a callback on every
    in-flight future each step (O(W) callback churn per completion for a graph W
    nodes wide). It reuses `without.limit_concurrency`'s bounded-concurrency shape
    but not its call, since the scheduler needs the completed task's `NodeKey` to
    unlock successors, which that lazy source hides. Acyclicity is proven by `TopologicalSorter.prepare`,
    which raises `graphlib.CycleError`. Each node runs once; a result is dropped
    as soon as its last dependent has captured it. A node that raises fails the
    whole run, cancelling in-flight siblings, which is also how closing the
    iterator early tears the run down.
    """
    if limit is not None and limit < 1:
        raise ValueError(f"limit must be at least 1 or None, but got {limit}")

    sorter: TopologicalSorter[NodeKey] = TopologicalSorter(plan.dependencies)
    sorter.prepare()
    consumers: Counter[NodeKey] = Counter(plan.consumers)
    results: dict[NodeKey, object] = dict(inputs)
    running: dict[asyncio.Future[object], NodeKey] = {}
    completed: asyncio.Queue[asyncio.Future[object]] = asyncio.Queue()
    ready: deque[NodeKey] = deque(sorter.get_ready())
    try:
        while sorter.is_active():
            while ready and (limit is None or len(running) < limit):
                key = ready.popleft()
                if key in results:
                    sorter.done(key)
                    ready.extend(sorter.get_ready())
                    continue
                if key not in plan.by_key:
                    raise KeyError(f"{key!r} is neither a supplied input nor a defined node")
                node = plan.by_key[key]
                args = tuple(results[dependency] for dependency in node.dependencies)
                for dependency in node.dependencies:
                    consumers[dependency] -= 1
                    if consumers[dependency] == 0:
                        del results[dependency]
                future = asyncio.ensure_future(node.run(args))
                running[future] = key
                future.add_done_callback(completed.put_nowait)
            done = await completed.get()
            key = running.pop(done)
            value = done.result()
            results[key] = value
            sorter.done(key)
            ready.extend(sorter.get_ready())
            yield key, value
    finally:
        await cancel_futures(running)


async def evaluate(plan: Plan, target: NodeKey, inputs: Mapping[NodeKey, object], limit: int | None) -> object:
    """
    Run every node in `plan` and return `target`'s value: the *behavior* read.

    A consumer of `drive` that runs the whole graph and keeps the one value the
    caller wants, dropping the rest. `target` is a node whose completion supplies
    the value, or a supplied input returned directly (an identity plan). There is
    deliberately no early return on `target`: the graph is run to completion, so
    the result reflects the whole graph and every node's effects have happened.

    A `target` that is neither a defined node nor a supplied input raises
    `KeyError`, matching `drive`, rather than silently reading back as `None`.
    """
    if target not in plan.by_key and target not in inputs:
        raise KeyError(f"{target!r} is neither a supplied input nor a defined node")
    # `target` is either a node, whose completion `drive` yields (and overwrites
    # this), or a supplied input, which `drive` never yields: default to its value
    # so an identity graph (the output is an input) returns it.
    result: object = inputs.get(target)
    async for key, value in drive(plan, inputs, limit):
        if key == target:
            result = value
    return result
