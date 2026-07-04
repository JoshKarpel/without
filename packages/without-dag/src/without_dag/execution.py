from __future__ import annotations

import asyncio
from collections import Counter
from collections import deque
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
    is collected as `Extractor[object]` and re-typed by `into`.
    """

    key: NodeKey
    dependencies: tuple[NodeKey, ...]
    run: Callable[[tuple[object, ...]], Awaitable[object]]


async def execute(
    nodes: Iterable[Node],
    target: NodeKey,
    inputs: Mapping[NodeKey, object],
    limit: int | None,
) -> object:
    """
    Run the graph needed to produce `target`, at most `limit` nodes in flight.

    The execution core. `limit` caps how many nodes run concurrently; `None`
    leaves it unbounded, running every ready node at once. Only the transitive
    dependencies of `target` run, so a branch the output does not demand is never
    started. `inputs` pre-supplies the values of source keys (a graph's entry
    points), which are marked done without running. Each node runs once; its
    result is memoized and fed to every dependent, so a diamond's shared ancestor
    executes a single time with no glitch.

    Scheduling replicates the shape of `without.limit_concurrency`
    (`asyncio.wait(..., return_when=FIRST_COMPLETED)`) rather than calling it:
    the scheduler needs the completed task's `NodeKey` to unlock successors, and
    that per-completion identity is exactly what `limit_concurrency`'s lazy
    source hides. Acyclicity is proven at the boundary by
    `graphlib.TopologicalSorter.prepare`, which raises `graphlib.CycleError`.

    A node that raises fails the whole run: the exception surfaces from its task
    and any in-flight siblings are cancelled on the way out. A node's result is
    dropped as soon as its last dependent has captured it, so a wide graph holds
    only the values still in play rather than every result ever produced.
    Returns the value of `target`.
    """
    if limit is not None and limit < 1:
        raise ValueError(f"limit must be at least 1 or None, but got {limit}")

    by_key = {node.key: node for node in nodes}
    needed = _needed(by_key, target, inputs)

    sorter: TopologicalSorter[NodeKey] = TopologicalSorter()
    pending_consumers: Counter[NodeKey] = Counter()
    for key in needed:
        sorter.add(key, *by_key[key].dependencies)
        pending_consumers.update(by_key[key].dependencies)
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
                results[key] = future.result()
                sorter.done(key)
                ready.extend(sorter.get_ready())
    finally:
        await cancel_futures(running)

    return results[target]


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
