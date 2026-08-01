# without-dag

Concurrent execution of DAG-shaped async workflows, liftable into a `without`
`Processor`. A `Processor[In, Out]` is otherwise an opaque stream-to-stream
closure; this package lets the *inside* of a per-event step be a graph of async
sub-steps that run with bounded concurrency and recombine into one output. It is
the value-level fan-out/fan-in the substrate leaves room for: one input value
drives many concurrent computations.

`Graph` is the typed frontend: a builder that threads value types through the
wiring so a mismatched dependency is a mypy error and a cycle is unrepresentable.
It compiles once into a `CompiledGraph`, an async callable reused per event:

```python
from without_dag import Graph

async def fetch(request: Request) -> Fetched: ...
async def parse(fetched: Fetched) -> Parsed: ...
async def render(fetched: Fetched, parsed: Parsed) -> Report: ...

graph, (request,) = Graph.of(Request)
fetched = graph.node("fetched", fetch, request)
parsed = graph.node("parsed", parse, fetched)
report = graph.node("report", render, fetched, parsed)
run = graph.build(output=report, limit=4)

result: Report = await run(some_request)
```

Each node runs once (memoized, so a diamond's shared ancestor executes a single
time with no glitch), acyclicity is proven at the boundary via stdlib
`graphlib`, and a single-input graph is an async `(In) -> Out` callable, exactly
what `from_map` lifts into a `Processor`.

A node's key is a name you choose, so it means the same thing after a crash:
`run.stream(...)` yields each `(key, result)` as it lands, and handing that mapping
back as `run(..., checkpoint=...)` skips every step it names. Sink the pairs to a
Redis hash or a table row under a workflow's idempotency key and the graph is a
resumable durable workflow, with no engine underneath it.

See the
[`without-dag` guide](https://without.help/without-dag/)
(with the [API reference](https://without.help/reference/without_dag/))
for the full surface: the object-seam execution core (`Plan`, `drive`,
`evaluate`), the typed frontend, resuming from a checkpoint, and lifting into a
processor.
