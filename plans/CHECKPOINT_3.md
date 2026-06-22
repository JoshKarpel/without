# Checkpoint 3

A snapshot of where `without` stands, succeeding `CHECKPOINT_2.md`. For the
original pitch see `BIG_IDEA.md`; for the critical review and open design
questions see `REVIEW_BIG_IDEA.md`.

## What changed since Checkpoint 2

This session turned to the *output* side of a processor: how its outputs are
distributed to consumers, and what a processor is allowed to do while producing
them. All implemented and verified.

- **Reframed from "sans-IO" to "decoupled IO".** The earlier framing claimed the
  core does no I/O, but that was only ever true of the pure reducer; the point
  was never to ban I/O but to *separate* it into the right abstractions so the
  parts stay reusable (sources at the edge, behaviors via `sample`, effects
  contained in the step). I/O still happens; it is decoupled, not absent. A
  processor MAY `await` I/O while handling an event (a database query, a
  closed-lifespan sub-request), reading dependencies from injected `Context`
  values. The one rule: an effect MUST NOT escape the entrypoint. A processor
  awaits its I/O to completion and MUST NOT hand a half-open resource (an open
  socket, an unfinished task it does not own) back to the runtime. Testability
  is injecting fake `Context` dependencies (plain DI). The honest cost: once
  processors can do ad-hoc I/O, the DAG-recovery bet weakens, because only
  *declared* context dependencies stay statically visible.
- **One reducer, always async.** Given the reframing (we do not mind processors
  doing I/O), the sync/async split stopped earning two functions. There is now a
  single `from_reducer` whose `step` is `async`, so it MAY `await` contained I/O;
  a `step` that does no I/O is just an `async def` that never awaits. The
  separate sync `from_reducer` and the short-lived `from_async_reducer` are both
  gone, collapsed into this one.
- **Context wiring is an explicit factory closure.** No new API: a processor
  declares its contexts as named, typed parameters of a factory
  (`make_handler(config: Context[Config], pool: Context[Pool]) -> Processor`),
  and the async reducer reads them with `config.current()`. Explicit DI,
  typeable, no `**kwargs` injection magic. This keeps the library-not-framework
  north star.
- **Five output-side connectors added** to `without.wiring`, all queue-backed
  with no new runtime dependencies (`more-itertools` is synchronous, so it does
  not apply to async streams):
  - `distribute(source, processor, workers)`: competing consumers. Each event
    is handled *once*, by whichever of `workers` independent processor instances
    is free; their outputs merge into one stream. The bounded-concurrency
    primitive (the 100-workers-off-one-queue case). Bounded queues give
    end-to-end backpressure.
  - `tee(source, branches, buffer=1)`: structural fan-out. Every branch sees
    every value, in order (the broadcast edge). A context manager; every branch
    MUST be consumed concurrently. `buffer` is how many values a branch may run
    ahead (default 1 keeps memory O(branches) and the slowest branch gating;
    larger lets a fast branch pull ahead; 0 is unbounded, no backpressure).
  - `broadcast(source, *processors)`: the processor-level convenience over
    `tee`, then merge. Each processor sees the whole input stream; their outputs
    interleave. The fan-out twin of `distribute`.
  - `route(source, *types)`: partition by the runtime type of each value, one
    branch per type, each value to exactly one branch. A value matching no
    listed type raises rather than being dropped. This is how a processor that
    emits a heterogeneous union (a `Response` *and* a background `KickOff`)
    splits its output to distinct sinks: the "return a response and also kick
    off a background task" case.
  - `merge(*sources)`: the N-to-1 fan-in dual. Folds many independent sources
    into one stream in completion order. `distribute` and `broadcast` use it
    internally to merge their results.
- Every internal pump uses `try/finally` to always emit a stop sentinel, so a
  source/processor error surfaces at the
  consumer instead of deadlocking it.

## The bet

Python has many frameworks with similar-but-subtly-different shapes (ASGI apps,
Kafka consumers, asyncio protocols, config reloaders) that do not interoperate
because none of them names the shared lower layer. `without` names that layer as
a narrow contract so the pieces compose. It should feel like a library (your
control flow stays visible), not a framework.

## Scope: what this is and is not

`without` is not a FastAPI competitor. It is the layer you would *write* FastAPI
on top of. FastAPI is one instantiation of the model (an HTTP request handler
with a lifespan context manager); `without` provides the abstractions (the
`Processor`, the behavior-`Context`, the edge connectors) that let you model
that yourself and reuse the *same* abstractions across domains: an HTTP server,
a queue processor, a config reloader. It is a peer of the ASGI/WSGI narrow-waist
analogy, one level *below* the frameworks, not a framework itself. The win is
that the HTTP lifespan handler and the Redis-queue worker are built from one
vocabulary.

## The model (as it stands)

- **Processors all the way down.** The only thing a user writes is a
  `Processor`: a transformation from a stream of inputs to a stream of outputs.
  One processor's output stream becomes another's input stream. There is no
  privileged "executor"; the runtime that wires and runs processors is a thin
  interpreter, not a concept users model with.
- **Decoupled-IO core.** A processor's logic is an async reducer,
  `(event, state) -> Transition(state, outputs)`, lifted by `from_reducer`. The
  `step` MAY `await` contained I/O, reading dependencies from injected `Context`
  values, provided the effect does not escape the entrypoint; a `step` that does
  no I/O is just an `async def` that never awaits. I/O is separated into source
  streams at the edge, behaviors via `sample`, and effects contained in the
  step, not banned.
- **Two edge *types* (events vs. behaviors).** An *event* edge feeds outputs
  onward; a *behavior* edge (`sample`) exposes a stream's latest value as a
  `Context` (latest-wins, no backpressure). "Context" is not a separate kind of
  thing; it is how a reader connects to a processor's output.
- **Event-edge connectors, by cardinality.** `pipe` (1 to 1, pure composition),
  `distribute` (1 to N competing, each event once), `tee`/`broadcast` (1 to N
  fan-out, each event to all), `route` (1 to N by variant, each event to its
  type's branch), and `merge` (N to 1 fan-in, several sources folded into one).
  The behavior edge stays `sample`. `sample`, `distribute`, `tee`, `broadcast`,
  `route`, and `merge` each need something running (a `background_task`), so they
  are the visible seams where the imperative shell shows up.

## Workspace layout

A `uv` workspace of flat, version-locked packages (no namespace packages). Each
package is its own top-level import. The root (`without-workspace`) is a virtual
project (`package = false`) holding shared dev tooling and config.

- `packages/without` (import `without`) — the core. `contracts` (`Processor`,
  `Stream`, `Context`, `Transition`, `from_reducer`), `wiring` (`Sample`,
  `pipe`, `sample`, `distribute`, `tee`, `broadcast`, `route`, `merge`), `tasks`
  (`background_task`, a `with`-scoped background task), and `testing` (`stream`,
  `collect`, `tick`). Zero runtime dependencies.
- `packages/without-env` (import `without_env`) — a static `Context` parsed from
  environment variables with `pydantic-settings`.
- `packages/without-configmap` (import `without_configmap`) — a behavior source
  backed by a Kubernetes ConfigMap mount: `watch_config` watches the mount
  directory (catching the atomic `..data` swap) and reparses a single YAML file
  on each change via `read_yaml_file`; read it through `without.sample`.
- `packages/without-integration` (import `without_integration`) — not a real
  package: depends on `without` and every plugin and holds cross-plugin tests.

## Status

Done and verified (mypy strict clean across 17 source files, 22 tests passing,
ruff lint + format clean):

- The decoupled-IO contract: a single async `from_reducer` whose `step` may
  `await` contained I/O.
- The full set of edge connectors: `pipe`, `sample`, `distribute`, `tee`,
  `broadcast`, `route`, `merge`, plus `background_task`.
- Two plugins: env vars (static context) and ConfigMap (changing context),
  proving both halves of the events/behaviors model, and a cross-plugin test
  showing them coexist.

Tooling matches the counterweight convention: `uv_build` backend, Python 3.13,
mypy strict, ruff at line-length 120, a 7-day `exclude-newer` cooldown, and a
`justfile` (`just test` runs mypy + pytest).

## Open questions and next steps

In the recommended order, with the hardest validation last:

1. A toy line-protocol server (Redis-ish) to prove long-lived processor state
   and a processor that emits outputs in response to a request stream. The
   output connectors and the async `from_reducer` are now in place to support it.
2. HTTP (sans-IO deps). The real test of whether the contract pays rent versus a
   plain `async def handle(request)`, now reframed by the scope note: the bar is
   "could you write something FastAPI-shaped on top of this," not "is this
   better than FastAPI."

Deferred, to bring back deliberately:

- **Graph/DAG recovery and visualization**, rebuilt on `graphlib`. Note the
  contained-effects relaxation weakens static dependency visibility; declared
  context parameters keep the *declared* deps visible, ad-hoc I/O does not.
- Known-hard problems inherited from dataflow/FRP: glitches on diamond
  dependencies, and feedback cycles and teardown order. (Backpressure now has a
  first answer on the event edge: the new connectors use bounded queues.)
- `without.testing.tick` advances the event loop a single step, so `sample`
  tests rely on the source draining in one activation. The deterministic fix is
  an explicit "await next update" signal on the sampled context
  (`await config.changed()`), which also has standalone value for consumers that
  want to react to a behavior changing.
