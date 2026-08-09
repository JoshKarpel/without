from __future__ import annotations

from collections.abc import AsyncGenerator
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field
from types import MappingProxyType
from typing import Generic
from typing import TypeVar
from typing import cast
from typing import overload

from without_dag.execution import Node
from without_dag.execution import NodeKey
from without_dag.execution import Plan
from without_dag.execution import drive
from without_dag.execution import evaluate

# Covariant: `T` names the node's result but is a phantom (no field holds it), so
# a `Handle[int]` is a `Handle[object]`. That is what lets the `node` runtime
# collect a heterogeneous mix of dependency handles as `*deps: Handle[object]`
# while the overloads keep each one's precise type. The legacy `TypeVar` is
# needed because PEP 695 infers an unused parameter as invariant; the variance is
# sound here, so we state it explicitly (see `without_web.extractors.Extractor`).
_T_co = TypeVar("_T_co", covariant=True)


@dataclass(frozen=True, slots=True)
class Handle(Generic[_T_co]):  # noqa: UP046 - PEP 695 infers a phantom parameter as invariant; the covariant TypeVar is deliberate (see above)
    """
    A typed reference to a node's future result.

    The token the builder hands back from `of`/`node` and takes back as a
    dependency. `T` is phantom: the handle carries only the node's `key`, but the
    type flows through the wiring so a downstream step is checked against the
    types of the handles it depends on. Because a caller can only pass handles
    that already exist, a cycle is unrepresentable through this API.
    """

    key: NodeKey


_NO_CHECKPOINT: Mapping[NodeKey, object] = MappingProxyType({})


@dataclass(frozen=True, slots=True)
class CompiledGraph[*Ins, Out]:
    """
    A frozen graph that *is* an async callable `(*Ins) -> Out`.

    `build` returns one of these. It runs one bounded-concurrency execution per
    call, seeding each entry the graph was opened over with the matching
    positional argument and returning the value of its `output`. A single-input
    graph is a plain `Callable[[In], Awaitable[Out]]`, so it lifts into a
    `Processor` with `from_map`. The scheduling structure is compiled once at
    `build` into `_plan` and reused by every call and `stream`, so a graph driven
    per event never recomputes it. `nodes` is kept so the structure is
    *recoverable* (a future diagram is derived from this one declaration, not
    maintained beside it).

    Both the call and `stream` take a `checkpoint` of `{node key: result}`, the
    shape `stream` itself emits. A node named in it is not run: its result is
    taken as given and fed to its dependents, so a run picks up where a previous
    one stopped. That is the whole of resumption, because a node's key is a name
    the *user* chose (see `node`), so the pairs survive the process that produced
    them: sink them to a Redis hash or a table row under the workflow's
    idempotency key, and hand the hash back as the checkpoint after a crash.
    """

    nodes: tuple[Node, ...]
    inputs: tuple[NodeKey, ...]
    output: NodeKey
    limit: int | None
    _plan: Plan

    async def __call__(self, *values: *Ins, checkpoint: Mapping[NodeKey, object] = _NO_CHECKPOINT) -> Out:
        result = await evaluate(self._plan, self.output, self._seed(values, checkpoint), self.limit)
        return cast(Out, result)

    def stream(
        self, *values: *Ins, checkpoint: Mapping[NodeKey, object] = _NO_CHECKPOINT
    ) -> AsyncGenerator[tuple[NodeKey, object]]:
        """
        Run the whole graph, yielding each node's `(key, result)` as it completes.

        The streaming counterpart to calling the graph: `__call__` samples the
        single `output` value (a behavior), `stream` reports every completion as
        it happens (the events), letting the caller react as results land or read
        several outputs. The positional arguments seed the graph's inputs,
        checked against `*Ins` exactly as the call is; match a yielded key against
        a `Handle`'s `key` to pick out a node's result.

        A node supplied by `checkpoint` is skipped, so a resumed run yields only
        what this run actually computed. Collecting those pairs back into the
        checkpoint is what advances it, and a checkpoint collected from a run is
        closed under ancestry (a node completes only after its dependencies did),
        so resuming from it re-runs exactly the nodes that had not finished.
        """
        return drive(self._plan, self._seed(values, checkpoint), self.limit)

    def _seed(self, values: tuple[object, ...], checkpoint: Mapping[NodeKey, object]) -> dict[NodeKey, object]:
        seeded = dict(zip(self.inputs, values, strict=True))
        if not checkpoint:
            return seeded
        # A key naming no node is the shape of a checkpoint written by a *different*
        # graph (a renamed node, an older version of the workflow). Resuming from it
        # would silently re-run steps whose effects already happened, so the mismatch
        # is surfaced here rather than after the graph has run against it.
        unknown = checkpoint.keys() - self._plan.by_key.keys()
        if unknown:
            raise KeyError(f"checkpoint keys {sorted(unknown)} name no node in this graph")
        seeded.update(checkpoint)
        return seeded


@dataclass(frozen=True, slots=True)
class Graph[*Ins]:
    """
    A builder that records async steps and returns typed `Handle`s.

    `of` opens a graph over its entry types and hands back a tuple of one `Handle`
    per type, `node` names a step and wires it to the handles it depends on, and `build` freezes the
    result into a `CompiledGraph`. Because the graph carries its entry pack in
    its type (`Graph[*Ins]`), `build` needs only the output handle: it recovers
    the inputs the graph already knows, so there is no second place to keep in
    sync. The builder itself is a frozen value; the only mutation is appending to
    its interior list of recorded nodes. Each step's function receives its
    dependencies' results as positional arguments in the order its handles were
    passed.
    """

    _nodes: list[Node] = field(default_factory=list)
    _input_keys: tuple[NodeKey, ...] = ()

    # [[[cog import cog; from dag_ladders import emit; cog.outl(emit("of")) ]]]
    @overload
    @staticmethod
    def of() -> tuple[
        Graph[()],
        tuple[()],
    ]: ...

    @overload
    @staticmethod
    def of[A](
        a: type[A],
        /,
    ) -> tuple[
        Graph[A],
        tuple[Handle[A]],
    ]: ...

    @overload
    @staticmethod
    def of[A, B](
        a: type[A],
        b: type[B],
        /,
    ) -> tuple[
        Graph[A, B],
        tuple[Handle[A], Handle[B]],
    ]: ...

    @overload
    @staticmethod
    def of[A, B, C](
        a: type[A],
        b: type[B],
        c: type[C],
        /,
    ) -> tuple[
        Graph[A, B, C],
        tuple[Handle[A], Handle[B], Handle[C]],
    ]: ...

    @overload
    @staticmethod
    def of[A, B, C, D](
        a: type[A],
        b: type[B],
        c: type[C],
        d: type[D],
        /,
    ) -> tuple[
        Graph[A, B, C, D],
        tuple[Handle[A], Handle[B], Handle[C], Handle[D]],
    ]: ...

    @overload
    @staticmethod
    def of[A, B, C, D, E](
        a: type[A],
        b: type[B],
        c: type[C],
        d: type[D],
        e: type[E],
        /,
    ) -> tuple[
        Graph[A, B, C, D, E],
        tuple[Handle[A], Handle[B], Handle[C], Handle[D], Handle[E]],
    ]: ...

    @overload
    @staticmethod
    def of[A, B, C, D, E, F](
        a: type[A],
        b: type[B],
        c: type[C],
        d: type[D],
        e: type[E],
        f: type[F],
        /,
    ) -> tuple[
        Graph[A, B, C, D, E, F],
        tuple[Handle[A], Handle[B], Handle[C], Handle[D], Handle[E], Handle[F]],
    ]: ...

    @overload
    @staticmethod
    def of[A, B, C, D, E, F, G](
        a: type[A],
        b: type[B],
        c: type[C],
        d: type[D],
        e: type[E],
        f: type[F],
        g: type[G],
        /,
    ) -> tuple[
        Graph[A, B, C, D, E, F, G],
        tuple[Handle[A], Handle[B], Handle[C], Handle[D], Handle[E], Handle[F], Handle[G]],
    ]: ...

    @overload
    @staticmethod
    def of[A, B, C, D, E, F, G, H](
        a: type[A],
        b: type[B],
        c: type[C],
        d: type[D],
        e: type[E],
        f: type[F],
        g: type[G],
        h: type[H],
        /,
    ) -> tuple[
        Graph[A, B, C, D, E, F, G, H],
        tuple[Handle[A], Handle[B], Handle[C], Handle[D], Handle[E], Handle[F], Handle[G], Handle[H]],
    ]: ...

    @overload
    @staticmethod
    def of[A, B, C, D, E, F, G, H, J](
        a: type[A],
        b: type[B],
        c: type[C],
        d: type[D],
        e: type[E],
        f: type[F],
        g: type[G],
        h: type[H],
        j: type[J],
        /,
    ) -> tuple[
        Graph[A, B, C, D, E, F, G, H, J],
        tuple[Handle[A], Handle[B], Handle[C], Handle[D], Handle[E], Handle[F], Handle[G], Handle[H], Handle[J]],
    ]: ...

    @overload
    @staticmethod
    def of[A, B, C, D, E, F, G, H, J, K](
        a: type[A],
        b: type[B],
        c: type[C],
        d: type[D],
        e: type[E],
        f: type[F],
        g: type[G],
        h: type[H],
        j: type[J],
        k: type[K],
        /,
    ) -> tuple[
        Graph[A, B, C, D, E, F, G, H, J, K],
        tuple[
            Handle[A], Handle[B], Handle[C], Handle[D], Handle[E], Handle[F], Handle[G], Handle[H], Handle[J], Handle[K]
        ],
    ]: ...
    # [[[end]]]
    @staticmethod
    def of(*inputs: type[object]) -> tuple[object, ...]:
        """
        Open a graph over `inputs`, returning it and a tuple of one `Handle` per entry type.

        An entry's key is its position (`input:0`, ...) rather than a name the
        caller supplies, because an entry is fed positionally on every call and so
        never has to be recovered the way a node's result does. A node may not
        take one of these keys; `node` rejects the collision.
        """
        handles: tuple[Handle[object], ...] = tuple(Handle(f"input:{index}") for index in range(len(inputs)))
        graph: Graph[*tuple[object, ...]] = Graph(_input_keys=tuple(handle.key for handle in handles))
        return (graph, handles)

    # [[[cog cog.outl(emit("node")) ]]]
    @overload
    def node[T](
        self,
        key: NodeKey,
        fn: Callable[[], Awaitable[T]],
        /,
    ) -> Handle[T]: ...

    @overload
    def node[T, A](
        self,
        key: NodeKey,
        fn: Callable[[A], Awaitable[T]],
        a: Handle[A],
        /,
    ) -> Handle[T]: ...

    @overload
    def node[T, A, B](
        self,
        key: NodeKey,
        fn: Callable[[A, B], Awaitable[T]],
        a: Handle[A],
        b: Handle[B],
        /,
    ) -> Handle[T]: ...

    @overload
    def node[T, A, B, C](
        self,
        key: NodeKey,
        fn: Callable[[A, B, C], Awaitable[T]],
        a: Handle[A],
        b: Handle[B],
        c: Handle[C],
        /,
    ) -> Handle[T]: ...

    @overload
    def node[T, A, B, C, D](
        self,
        key: NodeKey,
        fn: Callable[[A, B, C, D], Awaitable[T]],
        a: Handle[A],
        b: Handle[B],
        c: Handle[C],
        d: Handle[D],
        /,
    ) -> Handle[T]: ...

    @overload
    def node[T, A, B, C, D, E](
        self,
        key: NodeKey,
        fn: Callable[[A, B, C, D, E], Awaitable[T]],
        a: Handle[A],
        b: Handle[B],
        c: Handle[C],
        d: Handle[D],
        e: Handle[E],
        /,
    ) -> Handle[T]: ...

    @overload
    def node[T, A, B, C, D, E, F](
        self,
        key: NodeKey,
        fn: Callable[[A, B, C, D, E, F], Awaitable[T]],
        a: Handle[A],
        b: Handle[B],
        c: Handle[C],
        d: Handle[D],
        e: Handle[E],
        f: Handle[F],
        /,
    ) -> Handle[T]: ...

    @overload
    def node[T, A, B, C, D, E, F, G](
        self,
        key: NodeKey,
        fn: Callable[[A, B, C, D, E, F, G], Awaitable[T]],
        a: Handle[A],
        b: Handle[B],
        c: Handle[C],
        d: Handle[D],
        e: Handle[E],
        f: Handle[F],
        g: Handle[G],
        /,
    ) -> Handle[T]: ...

    @overload
    def node[T, A, B, C, D, E, F, G, H](
        self,
        key: NodeKey,
        fn: Callable[[A, B, C, D, E, F, G, H], Awaitable[T]],
        a: Handle[A],
        b: Handle[B],
        c: Handle[C],
        d: Handle[D],
        e: Handle[E],
        f: Handle[F],
        g: Handle[G],
        h: Handle[H],
        /,
    ) -> Handle[T]: ...

    @overload
    def node[T, A, B, C, D, E, F, G, H, J](
        self,
        key: NodeKey,
        fn: Callable[[A, B, C, D, E, F, G, H, J], Awaitable[T]],
        a: Handle[A],
        b: Handle[B],
        c: Handle[C],
        d: Handle[D],
        e: Handle[E],
        f: Handle[F],
        g: Handle[G],
        h: Handle[H],
        j: Handle[J],
        /,
    ) -> Handle[T]: ...

    @overload
    def node[T, A, B, C, D, E, F, G, H, J, K](
        self,
        key: NodeKey,
        fn: Callable[[A, B, C, D, E, F, G, H, J, K], Awaitable[T]],
        a: Handle[A],
        b: Handle[B],
        c: Handle[C],
        d: Handle[D],
        e: Handle[E],
        f: Handle[F],
        g: Handle[G],
        h: Handle[H],
        j: Handle[J],
        k: Handle[K],
        /,
    ) -> Handle[T]: ...
    # [[[end]]]
    def node[T](self, key: NodeKey, fn: Callable[..., Awaitable[T]], *deps: Handle[object]) -> Handle[T]:
        """
        Add a node named `key` computing `fn` from the handles it depends on,
        returning its result handle.

        `fn` is called with the dependencies' results as positional arguments in
        the order their handles are passed. The overloads above tie each handle's
        type to `fn`'s matching parameter, so a mismatch is a static error.

        The caller supplies `key` rather than the builder minting one: uniqueness
        alone would be cheaper to generate, but a name chosen in the source is the
        same on the other side of a crash, which is what lets a run's
        `(key, result)` pairs be stored and handed back as a `checkpoint`. It must
        be distinct from every key already in the graph, node or entry, since a
        duplicate would otherwise quietly displace the node it collides with.
        """
        if key in self._input_keys:
            raise ValueError(f"{key!r} is the key of one of this graph's inputs, so no node may take it")
        if any(node.key == key for node in self._nodes):
            raise ValueError(f"{key!r} is already the key of a node in this graph")

        dependencies = tuple(dep.key for dep in deps)

        async def run(args: tuple[object, ...]) -> object:
            return await fn(*args)

        self._nodes.append(Node(key=key, dependencies=dependencies, run=run))
        return Handle(key)

    def build[Out](self, *, output: Handle[Out], limit: int | None = None) -> CompiledGraph[*Ins, Out]:
        """
        Freeze the recorded steps into a callable graph over the graph's inputs.

        The scheduling structure is compiled once here, so running the graph
        repeats no graph analysis. `limit` caps how many nodes run concurrently;
        it defaults to `None`, which leaves concurrency unbounded (every ready
        node runs at once). Pass an integer to cap it when the steps contend for
        a scarce resource.
        """
        if limit is not None and limit < 1:
            raise ValueError(f"limit must be at least 1 or None, but got {limit}")

        nodes = tuple(self._nodes)
        return CompiledGraph(
            nodes=nodes,
            inputs=self._input_keys,
            output=output.key,
            limit=limit,
            _plan=Plan.of(nodes),
        )
