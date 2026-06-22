# Checkpoint 5

A snapshot of where `without` stands, succeeding `CHECKPOINT_4.md`. For the
original pitch see `BIG_IDEA.md`; for the critical review and open design
questions see `REVIEW_BIG_IDEA.md`.

## What changed since Checkpoint 4

This session reorganized the toy KV server to make its functional-core /
imperative-shell seam physical, and in doing so crystallized a framing that
should headline the library's rationale from here on (see the next section).

- **The `kv` toy is now a package split along the core/shell seam.** It was a
  single `kv.py` mixing the generic transport's `In`/`Out` type variables with
  the KV protocol's concrete `Request`/`Reply`, which read as a muddle.
  It is now:
  - `without_integration.kv.core`: the pure functional core. `parse_request`,
    `apply` (the keyspace fold), `encode_reply`, the immutable `Store`, and
    `make_store`. All `Request`/`Reply`, no sockets, no asyncio, no `In`/`Out`.
  - `without_integration.kv.shell`: the imperative shell. A generic
    line-server transport (`Send`, `Connected`, `serve`, parameterized by
    `In`/`Out` and a `decode`/`encode` codec) plus `make_responder`, the wiring
    that lifts the pure core into a fold over `Connected[Request, Reply]` and
    runs it.
- **Tests split to match**: `tests/kv/test_core.py` (parse, encode, keyspace
  threading) and `tests/kv/test_shell.py` (the `make_responder` fold with an
  injected `send`, and the over-the-wire server tests).
- The split resolved the `In`/`Out`-vs-`Request`/`Reply` confusion structurally:
  `core` is now 100% `Request`/`Reply` with zero `In`/`Out`, the transport's
  generics live only in `shell`, and the dependency direction is canonical
  (shell depends on core, never the reverse).

## Rationale: `without` is a principled way to write a shell

This is a headline framing for the project, co-equal with the narrow-waist bet
below, and it should factor into how the library is motivated going forward.

The functional-core / imperative-shell split is not just an implementation
tactic the toy happens to use; it is *what the library is for*. You write your
domain logic as a pure core, steps lifted by `from_map` / `from_scan` /
`from_fold` / `from_sink`, and `without` gives you the vocabulary for assembling
the imperative shell that runs that core against the world: sources at the
inbound edge, behaviors via `sample`, the edge connectors, the leaves at the
outbound edge, and the `send`-down output shape. The payoff is the
functional-core payoff: the interesting logic is testable without mocks (pass
data in, assert on data out), and I/O is pushed to a thin, composable edge.

The `kv` package is the concrete demonstration: `kv.core` is pure and tested
with `stream`/`collect`, `kv.shell` is the runner, and the file boundary *is*
the core/shell boundary. Splitting it that way is the lesson.

## The bet (the narrow waist)

The other co-equal rationale, unchanged: Python has many frameworks with
similar-but-subtly-different shapes (ASGI apps, Kafka consumers, asyncio
protocols, config reloaders) that do not interoperate because none of them names
the shared lower layer. `without` names that layer as a narrow contract so the
pieces compose. It should feel like a library (your control flow stays visible),
not a framework. It is a peer of the ASGI/WSGI narrow-waist analogy, one level
*below* the frameworks; it is the layer you would *write* FastAPI on top of, not
a FastAPI competitor.

## The 2x2 builder matrix

The core names every combination of two axes: does the step thread **state**,
and does the processor **emit a stream** (the interior, composable) or
**terminate** (a leaf at the outbound edge)?

|               | emit a stream (`Processor`) | terminate (a leaf)        |
|---------------|-----------------------------|---------------------------|
| **stateless** | `from_map`                  | `from_sink`               |
| **stateful**  | `from_scan`                 | `from_fold`               |

- `from_map(step)`: each event maps to one output, no state. `step: In -> Out`.
- `from_scan(initial, step)`: threads state and emits one output per event.
  `step: (In, S) -> Transition[S, Out]`. This is a **scan** (Haskell `scanl`,
  `itertools.accumulate`), not a reduce: it yields every intermediate. It was
  `from_reducer` through Checkpoint 3; the rename corrects the misnomer.
- `from_sink(step)`: consumes a stream for its effects, emits no output stream.
  `step: In -> None`. The stateless terminus, dual to `from_map`.
- `from_fold(initial, step)`: threads state and yields only the final
  accumulated value when the stream ends (a true reduce). `step: (In, S) -> S`.
  The stateful terminus, dual to `from_scan`.

Naming is operational and uniform: `map`, `scan`, `fold`, `sink` name what each
*does*. The two terminuses are the dual of sources: sources sit at the inbound
edge producing a stream from the world, leaves sit at the outbound edge
consuming one into the world. Supporting types:
`Sink[In] = Callable[[Stream[In]], Awaitable[None]]` and
`Fold[In, S] = Callable[[Stream[In]], Awaitable[S]]`; a `Fold[In, object]` is the
umbrella a consumer-runner accepts, since both a sink and any fold satisfy it.

## The model (as it stands)

- **Processors all the way down, with named edges.** The interior is
  `Processor` (stream to stream), composed so one's output is another's input.
  Sources sit at the inbound edge (impure streams from the world); leaves
  (`from_sink`, `from_fold`) sit at the outbound edge, consuming a stream into
  effects or a final value. The runtime that wires and runs these is a thin
  interpreter, not a concept users model with.
- **Decoupled-IO core.** Logic is built from a step lifted by one of the four
  builders. A step MAY `await` contained I/O, reading dependencies from injected
  `Context` values, provided the effect does not escape the entrypoint.
- **Output by passing `send` down.** When one stateful processor multiplexes
  many clients (the KV server's single shared fold), each event carries the
  reply channel for its origin, so the processor sends rather than the runtime
  routing. This is the ASGI shape (`scope`/`receive`/`send`) and keeps the
  keyspace a threaded value while still fanning replies to the right client.
- **Two edge *types* (events vs. behaviors).** An *event* edge feeds outputs
  onward; a *behavior* edge (`sample`) exposes a stream's latest value as a
  `Context`.

## Workspace layout

A `uv` workspace of flat, version-locked packages. The root
(`without-workspace`) is a virtual project holding shared dev tooling and config.

- `packages/without` (import `without`): the core. `contracts` (`Processor`,
  `Stream`, `Context`, `Transition`, `Sink`, `Fold`, and the four builders
  `from_map`, `from_scan`, `from_fold`, `from_sink`), `wiring` (`Sample`,
  `pipe`, `sample`, `distribute`, `tee`, `broadcast`, `route`, `merge`,
  `stream_from_queue`), `tasks` (`background_task`), and `testing` (`stream`,
  `collect`, `tick`). Zero runtime dependencies.
- `packages/without-env` (import `without_env`): a static `Context` parsed from
  environment variables with `pydantic-settings`.
- `packages/without-configmap` (import `without_configmap`): a behavior source
  backed by a Kubernetes ConfigMap mount.
- `packages/without-integration` (import `without_integration`): depends on
  `without` and every plugin; holds cross-plugin tests and the `kv` toy server,
  now split into `kv.core` (pure keyspace) and `kv.shell` (generic line-server
  transport plus the wiring that runs the core). Not meant to be distributed.

## Status

Done and verified (mypy strict clean across 22 source files, 51 tests passing,
ruff lint + format clean):

- The 2x2 builder matrix (`from_map`, `from_scan`, `from_fold`, `from_sink`)
  with `Sink`/`Fold` types, and `stream_from_queue`.
- The `kv` toy server, reorganized along the core/shell seam: a pure `kv.core`
  keyspace tested directly with `stream`/`collect`, under a `kv.shell` line
  server that funnels every connection into one shared fold and delivers replies
  via a per-connection `send` channel that deliberately does not `drain` (so one
  slow client cannot stall the shared consumer).

## Open questions and next steps

1. **HTTP / ASGI** (the demanding validation). Working hypothesis: a long-lived
   processor that *spawns a short-lived processor per request*, each
   encapsulating the stream of events arriving on that request, mirroring ASGI's
   `application(scope, receive, send)`. The toy already adopted the `send`-down
   output shape. The fork to resolve when HTTP makes it concrete: the KV
   server's *single shared fold* (keyspace as a threaded value, no shared place)
   versus *per-request processors plus shared state behind them* in a
   lifespan-scoped `Context` (the ASGI shape, but reintroducing a shared
   resource).

Smaller candidates surfaced earlier:

- A **dynamic-merge** connector for a changing set of sources (the server case
  `merge` does not cover).
- Whether `serve`'s `Fold[..., object]` consumer parameter deserves a named
  `Leaf` umbrella type.

Deferred, to bring back deliberately (carried forward):

- **Graph/DAG recovery and visualization**, rebuilt on `graphlib`. The
  contained-effects relaxation weakens static dependency visibility.
- Known-hard dataflow/FRP problems: glitches on diamond dependencies, feedback
  cycles, and teardown order.
- `without.testing.tick` advances the loop a single step; the deterministic fix
  is an explicit "await next update" signal on a sampled `Context`.

Documentation debt: `BIG_IDEA.md` and the earlier checkpoints still describe the
model as an "async reducer". With the rename that is more accurately an **async
scan**, and the functional-core / imperative-shell framing above should be folded
into `BIG_IDEA.md`'s rationale when it is next revised.
