# without-dag

Concurrent execution of DAG-shaped async workflows, liftable into a `without`
`Processor`. A `Processor[In, Out]` is otherwise an opaque stream-to-stream
closure; this package lets the *inside* of a per-event step be a graph of async
sub-steps that run with bounded concurrency and recombine into one output. It is
the value-level fan-out/fan-in the substrate leaves room for: one input value
drives many concurrent computations, distinct from the stream-level splitting a
`distribute`/`merge` vocabulary would do. See the
[`without_dag` API reference](../without-dag/reference.md) for the full surface.

Like [`without-asgi`](../without-asgi/index.md), this package handles only the *mechanism*,
execution, and leaves the graph-defining *policy* to a frontend. The two layers
live here for now but meet at one narrow interface, so a different frontend (a YAML
loader, a signature-inspecting decorator) could be layered on later without either
side importing the other's opinions.

## The execution core

A set of `Node` values is compiled once into a `Plan`, then run with at most
`limit` nodes in flight. `Plan.of` does the compiling; `drive` and `evaluate`
run it:

```python
class Plan:
    @classmethod
    def of(cls, nodes: Iterable[Node]) -> Plan: ...

async def drive(plan: Plan, inputs: Mapping[NodeKey, object], limit: int | None
) -> AsyncGenerator[tuple[NodeKey, object]]: ...

async def evaluate(plan: Plan, target: NodeKey, inputs: Mapping[NodeKey, object], limit: int | None
) -> object: ...
```

A `Node` is the interface: a value carrying its `key`, its ordered `dependencies`, and
an async `run` that takes its dependencies' results as a tuple and returns its
own. A `NodeKey` is a `str`: any hashable would do to tell nodes apart in memory,
but a key that survives the process is what lets a run's `(key, result)` pairs be
written to a store and handed back later, so the interface is narrowed to the one shape
every store can hold (a frontend wanting richer keys encodes them into one).
Results cross the interface as `object`, the same honest move `without-web` makes
when it collects a heterogeneous mix of `object`-valued `Extractor`s; the typed frontend
restores precision above it. There is only the compiled form: a caller runs a
graph by compiling a `Plan` and reusing it, the way an HTTP client owns one
pooled session rather than a fresh connection per request. The input-independent
work (nodes by key, dependency edges, consumer counts) is done once in `Plan.of`,
so a graph driven per event never repeats it.

Two properties fall out of the model:

- **Each node runs once.** Its result is memoized and fed to every dependent, so
  a diamond's shared ancestor executes a single time with no glitch.
- **Acyclicity is proven at the boundary.** The scheduler sits on stdlib
  [`graphlib.TopologicalSorter`](https://docs.python.org/3/library/graphlib.html),
  whose `prepare` raises `graphlib.CycleError` on a cycle. Scheduling drains
  completions off an `asyncio.Queue` fed by a done-callback attached once per
  spawned node, rather than `asyncio.wait(..., return_when=FIRST_COMPLETED)`
  re-registering a callback on every in-flight future each step (which is
  quadratic in the width of a graph). It keeps the bounded-concurrency shape of
  `without.limit_concurrency` rather than calling it, because the scheduler needs
  the completed task's key to unlock its successors, and that per-completion
  identity is what `limit_concurrency`'s lazy source hides.

A node that raises fails the whole run: the exception surfaces and any in-flight
siblings are cancelled via `without.cancel_futures`.

`inputs` pre-supplies values by key, and the scheduler asks nothing about where a
key came from: a key it finds there is marked done without running and is never
yielded, whether it names a source the graph is opened over or a node whose result
an earlier run already computed. That single rule is the whole of resumption, and
the typed frontend spends it in `checkpoint` below.

`drive` and `evaluate` are the *events* and *behavior* reads of one scheduler,
mirroring the substrate's own split. `drive` yields each `(key, result)` the
instant it completes (in whatever order nodes finish, a node always after the
dependencies it consumed), useful to react as results land or to read several
outputs. `evaluate` is a consumer of it that keeps the one value `target`
produces and drops the rest. Both run the whole graph: there is no target-based
pruning, since fixing the target up front is not a real early return and the
graph is small (its size is bounded by the per-event work, not the stream). A
node with no path to the output still runs.

`drive` is pull-driven: the DAG advances only as the consumer iterates. To drive
it in the background instead (so the graph makes progress while a slower consumer
catches up), wrap it with `without.spool`, which drives any stream ahead of its
consumer through a bounded queue on a background task.

## The typed frontend

`Graph` is a builder that threads value types through the wiring. `Graph.of`
opens a graph over its entry types and hands back the graph plus a tuple of one
`Handle` per type, `node` names a step and wires it to the handles it depends on (an arity-overload
ladder ties each `Handle[X]` to the matching parameter of the step's function),
and `build` freezes the result into a `CompiledGraph[*Ins, Out]` so its call is
checked for argument count and types:

```python
from without_dag import Graph

async def fetch(request: Request) -> Fetched: ...
async def parse(fetched: Fetched) -> Parsed: ...
async def render(fetched: Fetched, parsed: Parsed) -> Report: ...

graph, (request,) = Graph.of(Request)
fetched = graph.node("fetched", fetch, request)
parsed = graph.node("parsed", parse, fetched)             # parse must take a Fetched
report = graph.node("report", render, fetched, parsed)    # render must take (Fetched, Parsed)
run = graph.build(output=report, limit=4)

result: Report = await run(some_request)
```

Passing `parse` a handle whose type does not line up with its parameter is a mypy
error, not a runtime surprise. Because a step can only depend on handles that
already exist, a cycle is unrepresentable through this API; graphlib's check is a
backstop for the object interface.

The name leads because a node *is* named before it is defined, and because the key
is the caller's to choose rather than the builder's to mint. Uniqueness alone would
be cheaper to generate, but a name written in the source is the same name on the
other side of a crash, which is what makes a result recoverable at all. A key must
be distinct from every other in the graph, its entries included; entries are keyed
by position (`input:0`, ...) since they are fed positionally on every call and so
never have to be recovered.

The graph carries its entry types in its own type (`Graph[*Ins]`), so `build`
takes only the output handle: it recovers the inputs the graph already knows,
rather than making you list them a second time and keep the two in sync. A
general DAG may take several: `graph, (a, b) = Graph.of(A, B)` opens two entries,
and the compiled graph is called `run(a_value, b_value)` with the count and types
checked. Wrong arity or a mismatched value type is a static error.

`build` compiles the scheduling structure once, into the same object-interface `Plan`:
the nodes by key, the dependency edges, and the consumer counts are all
input-independent, so a `CompiledGraph` driven per event runs the nodes without
re-analyzing the graph each time. `build` *is* the typed graph's `Plan.of`.

The behavior/events duality is typed here too: `run(*inputs)` samples the single
`output`, while `run.stream(*inputs)` drives the precompiled `Plan` to yield
each node's `(key, result)` as it completes (match a yielded key against a
`Handle`'s `key` to pick one out). Both check the inputs against `*Ins`.

## Resuming from a checkpoint

A **checkpoint** is a mapping of node key to result: exactly what `stream` emits,
and exactly what both `run(...)` and `run.stream(...)` take back as `checkpoint=`.
A node named in it is not run. Its result is taken as given and fed to its
dependents, so a run picks up where an earlier one stopped:

```python
checkpoint: dict[str, object] = {}
try:
    async for key, result in run.stream(request):
        await store.hset(f"workflow:{workflow_id}", key, encode(result))  # durable before the next step
        checkpoint[key] = result
except Exception:
    ...  # a later process reloads the hash and hands it back
report = await run(request, checkpoint=checkpoint)
```

Two properties make that loop a durable workflow rather than a cache. `stream` is
pull-driven, so nothing downstream of a completed step starts until the consumer
asks for the next result, which makes the write a barrier rather than a background
flush. And a checkpoint collected from a run is closed under ancestry, because a
node completes only after its dependencies did, so resuming re-runs exactly the
nodes that had not finished. A checkpoint that covers the whole graph runs nothing
at all, which is what makes a re-run idempotent rather than merely restartable.

A supplied node counts as consuming its dependencies, even though it never runs.
That is what keeps a resumed run's memory bounded by what is left to do: the
scheduler drops a result once its last dependent has read it, and a checkpointed
node is the last thing that will ever want its ancestors' values, so treating it
as having read them is the difference between freeing them and pinning them for
the whole run.

The inputs are still passed positionally on every call, because an entry is not
part of the checkpoint: it lives wherever the request itself does (the queue
message, the request row). A key that names no node is rejected, since that is the
shape of a checkpoint written by a different version of the graph, and resuming
from it would silently redo work whose effects already happened. Renaming a node
is therefore a breaking change to its in-flight workflows, which is the honest
reading of what a rename means and the reason engines like Temporal grow
[worker versioning](https://docs.temporal.io/production-deployment/worker-deployments/worker-versioning);
in user space the same job is done by versioning the workflow id, or the graph,
and deciding deliberately how the old ones drain.

What that loop does not give you is exclusion. It makes an *interrupted* run
resumable, which is a statement about one runner over time; it says nothing about
two runners at once, and two processes driving the same workflow id both read a
step as unfinished and both perform its effect. That belongs to whatever holds the
checkpoint rather than to this package, since the store is the only party that
sees every writer, and it is what Temporal builds a server for and DBOS requires
Postgres for.

[`without-durability`](../without-durability/index.md) is that loop, written
once, with the missing half supplied: `run_durably` runs it against an injected
store under a claim every write is fenced against, and `run_saga` adds the
compensating half, where a failed run's checkpoint is parsed into "how far did we
get" and drives a rollback graph that is itself checkpointed.

## Lifting into a Processor

A `Stream` carries one value per event, so only a *single-input* graph lifts into
a `Processor`. In that case a `CompiledGraph` is an async `(In) -> Out` callable,
which is exactly what `from_map` wants: this package adds no wrapper of its own,
because there is nothing to add.

```python
from without import collect, from_map, stream_from_iterable

processor = from_map(run)                      # Processor[Request, Report]
reports = await collect(processor(stream_from_iterable([request_a, request_b])))
```

Each event drives one bounded-concurrency DAG execution and yields its output, so
the graph composes with `compose`, `stream_from_iterable`, `collect`, and the rest of the
substrate unchanged. When a step needs several values, group them into one input
object (a dataclass or tuple) so the graph keeps a single entry and still lifts.
