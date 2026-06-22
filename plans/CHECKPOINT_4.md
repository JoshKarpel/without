# Checkpoint 4

A snapshot of where `without` stands, succeeding `CHECKPOINT_3.md`. For the
original pitch see `BIG_IDEA.md`; for the critical review and open design
questions see `REVIEW_BIG_IDEA.md`.

## What changed since Checkpoint 3

This session built the toy line-protocol server (open question #1 from
Checkpoint 3) and, in doing so, worked out the output side of the model: how a
multiplexed server delivers replies, and what the full set of step builders
should be. All implemented and verified.

- **The toy KV server exists** (`packages/without-integration/kv.py`), a
  Redis-ish line-protocol TCP server. It proves the two things the checkpoint
  asked for: long-lived processor state (the keyspace is threaded as an
  immutable value through a single fold, never a shared mutable place) and a
  processor that emits outputs in response to a request stream.
- **The 2x2 builder matrix** (see below) is the important outcome. The two
  existing builders were renamed and two new ones added, so the core now names
  all four combinations of {stateless, stateful} x {emit a stream, terminate}.
- **Output is delivered ASGI-style, not routed.** Each connection's events
  carry a `send` callable bound to that connection; the processor replies by
  calling `send` (contained I/O). There are no connection ids, no mailbox
  registry, and no output dispatcher. This is the shape ASGI uses
  (`scope`/`receive`/`send`): the output channel is passed *down into* the
  processor rather than returned to a router. An earlier design with
  `FromClient`/`ToClient` envelopes, a `multiplex` combinator, and a per-client
  mailbox registry was built and then removed once the `send`-down shape made
  the surrogate connection ids unnecessary.
- **`stream_from_queue` added** to `without.wiring`: a push-to-pull source
  adapter that turns a queue into a `Stream`. A source that *pushes* (a server's
  accept loop, a callback client, a pub/sub subscriber) drops values into a
  queue; this exposes that queue as the pull-based `Stream` the rest of
  `without` consumes. Finding worth keeping: `merge` is a *static* N-to-1 fan-in
  (a fixed set of sources known up front), but a server's set of connections is
  *dynamic*, so the fan-in is a shared inbox queue any newly accepted connection
  writes to, not `merge`. A dynamic-merge connector is a candidate addition.
- **`background_task` left as-is after investigation.** A nested-cancellation
  guard (re-raise unless `current_task().cancelling() == 0`, borrowed from a
  sibling project) was tried, then reverted: experiments showed it is
  behaviorally identical to the existing `suppress(CancelledError)` for
  `background_task`'s exact shape, so it was speculative handling for a path that
  could not be demonstrated. A genuinely useful test
  (`test_background_task_surfaces_a_worker_exception_on_exit`) was kept.

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
  `itertools.accumulate`), not a reduce: it yields every intermediate, not just
  a final value. It was called `from_reducer` through Checkpoint 3; the rename
  corrects the misnomer.
- `from_sink(step)`: consumes a stream for its effects, emits no output stream.
  `step: In -> None`. The stateless terminus, dual to `from_map`.
- `from_fold(initial, step)`: threads state and yields only the final
  accumulated value when the stream ends (a true reduce). `step: (In, S) -> S`.
  The stateful terminus, dual to `from_scan`.

Naming is operational and uniform: `map`, `scan`, `fold`, `sink` name what each
*does*, replacing the earlier agent-noun mix (`mapper`/`reducer`). The two
terminuses are the dual of sources: sources sit at the inbound edge producing a
stream from the world, leaves sit at the outbound edge consuming one into the
world. The implicit "drain the output and discard it" loop is now a named leaf.

The supporting types: `Sink[In] = Callable[[Stream[In]], Awaitable[None]]` and
`Fold[In, S] = Callable[[Stream[In]], Awaitable[S]]`. A `Fold[In, object]` is
the umbrella a consumer-runner accepts, since both a sink and any fold satisfy
it.

## The bet

Python has many frameworks with similar-but-subtly-different shapes (ASGI apps,
Kafka consumers, asyncio protocols, config reloaders) that do not interoperate
because none of them names the shared lower layer. `without` names that layer as
a narrow contract so the pieces compose. It should feel like a library (your
control flow stays visible), not a framework.

## Scope: what this is and is not

`without` is not a FastAPI competitor; it is the layer you would *write* FastAPI
on top of. FastAPI is one instantiation of the model (an HTTP request handler
with a lifespan context manager); `without` provides the abstractions (the
`Processor`, the leaves, the behavior-`Context`, the edge connectors) that let
you model that yourself and reuse the *same* abstractions across domains: an
HTTP server, a queue processor, a config reloader. It is a peer of the
ASGI/WSGI narrow-waist analogy, one level *below* the frameworks.

## The model (as it stands)

- **Processors all the way down, with named edges.** The interior is
  `Processor`: a transformation from a stream of inputs to a stream of outputs,
  composed so one processor's output is another's input. Sources sit at the
  inbound edge (impure streams from the world), and leaves (`from_sink`,
  `from_fold`) sit at the outbound edge, consuming a stream into effects or a
  final value. The runtime that wires and runs these is a thin interpreter, not
  a concept users model with.
- **Decoupled-IO core.** A processor's logic is built from a step lifted by one
  of the four builders. A step MAY `await` contained I/O, reading dependencies
  from injected `Context` values, provided the effect does not escape the
  entrypoint. I/O is separated into source streams at the edge, behaviors via
  `sample`, effects contained in the step, and now replies via a `send` channel
  carried on the event, not banned.
- **Output by passing `send` down.** When one stateful processor multiplexes
  many clients (the KV server's single shared fold), each event carries the
  reply channel for its origin, so the processor sends rather than the runtime
  routing. This keeps the keyspace a threaded value while still fanning replies
  back to the right client, and it is the ASGI shape.
- **Two edge *types* (events vs. behaviors).** Unchanged: an *event* edge feeds
  outputs onward; a *behavior* edge (`sample`) exposes a stream's latest value
  as a `Context`.

## Workspace layout

A `uv` workspace of flat, version-locked packages. The root
(`without-workspace`) is a virtual project (`package = false`) holding shared
dev tooling and config.

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
  `without` and every plugin; holds cross-plugin tests, and now the `kv` toy
  server as a validation artifact (not meant to be distributed).

## Status

Done and verified (mypy strict clean across 19 source files, 51 tests passing,
ruff lint + format clean):

- The 2x2 builder matrix: `from_map`, `from_scan`, `from_fold`, `from_sink`,
  with `Sink`/`Fold` types.
- `stream_from_queue`, the push-to-pull source adapter.
- The toy KV server: a pure core (`parse_request`, `apply`, `encode_reply`, the
  immutable `Store`) tested directly with `stream`/`collect`, under an asyncio
  TCP shell (`serve`) that funnels every connection into one shared fold and
  delivers replies via a per-connection `send` channel.

## Open questions and next steps

1. **HTTP / ASGI** (was open question #2). The real test of whether the contract
   pays rent versus a plain `async def handle(request)`. Working hypothesis: a
   long-lived processor that *spawns a short-lived processor per request*, each
   encapsulating the stream of events arriving on that request, mirroring ASGI's
   `application(scope, receive, send)`. The toy already adopted the `send`-down
   output shape. The fork to resolve when HTTP makes it concrete: the KV server's
   *single shared fold* (keyspace as a threaded value, no shared place) versus
   *per-request processors plus shared state behind them* in a lifespan-scoped
   `Context` (the ASGI shape, but reintroducing a shared resource).

Smaller candidates surfaced this session:

- A **dynamic-merge** connector for a changing set of sources (the server case
  `merge` does not cover).
- Whether `serve`'s `Fold[..., object]` consumer parameter deserves a named
  `Leaf` umbrella type.

Deferred, to bring back deliberately (carried from Checkpoint 3):

- **Graph/DAG recovery and visualization**, rebuilt on `graphlib`. The
  contained-effects relaxation weakens static dependency visibility; declared
  context parameters keep the *declared* deps visible, ad-hoc I/O does not.
- Known-hard dataflow/FRP problems: glitches on diamond dependencies, feedback
  cycles, and teardown order.
- `without.testing.tick` advances the loop a single step; the deterministic fix
  is an explicit "await next update" signal on a sampled `Context`.

Documentation debt: `BIG_IDEA.md` and the earlier checkpoints describe the model
as an "async reducer". With the rename, that is more accurately an **async
scan**; the language should be refreshed when those docs are next touched.
