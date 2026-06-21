# Recover a DAG from declared inputs and render it to mermaid. Execution order
# is not written by the user; it is recovered from what each node declares it
# needs, so the decorator can return the function unchanged and control flow
# stays plain Python. First step toward visualizing the inferred graph shape.

from __future__ import annotations

import inspect
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import overload


class CycleError(Exception):
    """Raised when the declared inputs do not form a DAG."""


@dataclass(frozen=True, slots=True)
class Node:
    """A unit of work and the names of the inputs it declared it needs."""

    name: str
    inputs: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class Graph:
    """An immutable view of nodes and the edges recovered from their inputs."""

    nodes: Mapping[str, Node]

    def edges(self) -> tuple[tuple[str, str], ...]:
        """Directed ``(source, target)`` edges: an input flows into the node that needs it."""
        return tuple((source, node.name) for node in self.nodes.values() for source in node.inputs)

    def topological_order(self) -> tuple[str, ...]:
        """Recover a valid execution order, or raise ``CycleError`` if none exists.

        Inputs that are not themselves registered nodes (external sources like a
        socket or a config mount) are treated as already-satisfied roots.
        """
        incoming = {name: sum(1 for i in node.inputs if i in self.nodes) for name, node in self.nodes.items()}
        dependents: dict[str, list[str]] = {name: [] for name in self.nodes}
        for node in self.nodes.values():
            for source in node.inputs:
                if source in self.nodes:
                    dependents[source].append(node.name)

        ready = [name for name, count in incoming.items() if count == 0]
        order: list[str] = []
        while ready:
            name = ready.pop()
            order.append(name)
            for dependent in dependents[name]:
                incoming[dependent] -= 1
                if incoming[dependent] == 0:
                    ready.append(dependent)

        if len(order) != len(self.nodes):
            unresolved = sorted(set(self.nodes) - set(order))
            raise CycleError(f"declared inputs form a cycle among: {', '.join(unresolved)}")
        return tuple(order)

    def to_mermaid(self) -> str:
        """Render the recovered graph as a mermaid ``flowchart``.

        External inputs (names that are not registered nodes) render as rounded
        source nodes so the boundary between the outside world and the DAG is
        visible at a glance.
        """
        lines = ["flowchart TD"]
        externals = {source for source, _ in self.edges() if source not in self.nodes}
        for external in sorted(externals):
            lines.append(f"    {external}([{external}])")
        for node in self.nodes.values():
            if not node.inputs:
                lines.append(f"    {node.name}")
            for source in node.inputs:
                lines.append(f"    {source} --> {node.name}")
        return "\n".join(lines)


class Registry:
    """Collects nodes declared with ``@node`` and builds a ``Graph`` from them.

    Construct your own for isolation (tests, separate apps) rather than reaching
    for a global, so two graphs never bleed into each other.
    """

    def __init__(self) -> None:
        self._nodes: dict[str, Node] = {}

    @overload
    def node[F: Callable[..., object]](self, fn: F) -> F: ...

    @overload
    def node[F: Callable[..., object]](
        self,
        fn: None = None,
        *,
        name: str | None = None,
        inputs: Iterable[str] | None = None,
    ) -> Callable[[F], F]: ...

    def node[F: Callable[..., object]](
        self,
        fn: F | None = None,
        *,
        name: str | None = None,
        inputs: Iterable[str] | None = None,
    ) -> F | Callable[[F], F]:
        """Register a function as a node, returning it unchanged.

        Inputs are declared, not ordered. By default they are inferred from the
        function's parameter names; pass ``inputs`` to override. The function is
        returned untouched, so calling it stays ordinary Python.
        """

        def register(func: F) -> F:
            node_name = name or func.__name__
            declared = tuple(inputs) if inputs is not None else tuple(inspect.signature(func).parameters)
            if node_name in self._nodes:
                raise ValueError(f"node {node_name!r} is already registered")
            self._nodes[node_name] = Node(name=node_name, inputs=declared)
            return func

        if fn is None:
            return register
        return register(fn)

    def graph(self) -> Graph:
        return Graph(nodes=dict(self._nodes))
