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
from contextlib import aclosing
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
    is collected as `Extractor[object]` and re-typed by `into`.
    """

    key: NodeKey
    dependencies: tuple[NodeKey, ...]
    run: Callable[[tuple[object, ...]], Awaitable[object]]


async def executed(
    nodes: Iterable[Node],
    inputs: Mapping[NodeKey, object],
    limit: int | None,
) -> AsyncGenerator[tuple[NodeKey, object]]:
    """
    Run every node, yielding each `(key, result)` the instant it completes.

    The streaming core, and the *events* half of the model: where `execute`
    samples one final value (a behavior), `executed` reports every completion as
    it happens, in whatever order nodes finish. The only ordering guarantee is
    the causal one: a node is always yielded after the dependencies it consumed.
    `inputs` pre-supplies the values of source keys, which are marked done
    without running and never yielded.

    `limit` caps how many nodes run concurrently; `None` leaves it unbounded.
    Scheduling replicates the shape of `without.limit_concurrency`
    (`asyncio.wait(..., return_when=FIRST_COMPLETED)`) rather than calling it:
    the scheduler needs the completed task's `NodeKey` to unlock successors, and
    that per-completion identity is exactly what `limit_concurrency`'s lazy
    source hides. Acyclicity is proven at the boundary by
    `graphlib.TopologicalSorter.prepare`, which raises `graphlib.CycleError`.

    Each node runs once; its result is memoized and fed to every dependent, so a
    diamond's shared ancestor executes a single time with no glitch, and a
    result is dropped as soon as its last dependent has captured it. A node that
    raises fails the whole run: the exception surfaces and any in-flight siblings
    are cancelled on the way out, which is also how closing the iterator early
    tears the run down.
    """
    if limit is not None and limit < 1:
        raise ValueError(f"limit must be at least 1 or None, but got {limit}")

    by_key = {node.key: node for node in nodes}
    sorter: TopologicalSorter[NodeKey] = TopologicalSorter()
    pending_consumers: Counter[NodeKey] = Counter()
    for node in by_key.values():
        sorter.add(node.key, *node.dependencies)
        pending_consumers.update(node.dependencies)
    sorter.prepare()

    results: dict[NodeKey, object] = dict(inputs)
    running: dict[asyncio.Future[object], NodeKey] = {}
    ready: deque[NodeKey] = deque(sorter.get_ready())
    try:
        while sorter.is_active():
            while ready and (limit is None or len(running) < limit):
                key = ready.popleft()
                if key in results:
                    sorter.done(key)
                    ready.extend(sorter.get_ready())
                    continue
                if key not in by_key:
                    raise KeyError(f"{key!r} is neither a supplied input nor a defined node")
                node = by_key[key]
                args = tuple(results[dependency] for dependency in node.dependencies)
                for dependency in node.dependencies:
                    pending_consumers[dependency] -= 1
                    if pending_consumers[dependency] == 0:
                        del results[dependency]
                running[asyncio.ensure_future(node.run(args))] = key
            done, _ = await asyncio.wait(running, return_when=asyncio.FIRST_COMPLETED)
            for future in done:
                key = running.pop(future)
                value = future.result()
                results[key] = value
                sorter.done(key)
                ready.extend(sorter.get_ready())
                yield key, value
    finally:
        await cancel_futures(running)


async def execute(
    nodes: Iterable[Node],
    target: NodeKey,
    inputs: Mapping[NodeKey, object],
    limit: int | None,
) -> object:
    """
    Run the graph needed to produce `target` and return its value.

    The *behavior* half of the model, layered on `executed`: only the transitive
    dependencies of `target` run, so a branch the output does not demand is never
    started, and since `target` is the sole terminal of that pruned graph it
    completes last. `inputs` pre-supplies the values of source keys. `limit` caps
    concurrency (`None` is unbounded). A failing node propagates and cancels its
    in-flight siblings; a wide graph holds only the values still in play.
    """
    by_key = {node.key: node for node in nodes}
    needed = _needed(by_key, target, inputs)
    async with aclosing(executed([by_key[key] for key in needed], inputs, limit)) as completions:
        async for key, value in completions:
            if key == target:
                return value
    return inputs[target]


def _needed(by_key: Mapping[NodeKey, Node], target: NodeKey, inputs: Mapping[NodeKey, object]) -> set[NodeKey]:
    needed: set[NodeKey] = set()
    frontier = [target]
    while frontier:
        key = frontier.pop()
        if key in inputs or key in needed:
            continue
        if key not in by_key:
            raise KeyError(f"{key!r} is neither a supplied input nor a defined node")
        needed.add(key)
        frontier.extend(by_key[key].dependencies)
    return needed
