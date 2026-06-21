# Checkpoint 1

A snapshot of where `without` stands, as a resumption point. For the original
pitch see `BIG_IDEA.md`; for the critical review and open design questions see
`REVIEW_BIG_IDEA.md`.

## The bet

Python has many frameworks with similar-but-subtly-different shapes (ASGI apps,
Kafka consumers, asyncio protocols, config reloaders) that do not interoperate
because none of them names the shared lower layer. `without` names that layer as
a narrow contract so the pieces compose. It should feel like a library (your
control flow stays visible), not a framework.

## The model (as it stands)

- **Processors all the way down.** The only thing a user writes is a
  `Processor`: a transformation from a stream of inputs to a stream of outputs.
  One processor's output stream is another's input stream. There is no
  privileged "executor"; the runtime that wires and runs processors is a thin
  interpreter, not a concept users model with.
- **Sans-IO core.** A processor's logic is a pure reducer, `(event, state) ->
  Transition(state, outputs)`, built into a stream processor by `from_reducer`,
  which supplies the scan loop. I/O lives only in source streams at the boundary,
  which keeps the interior pure and testable without mocks.
- **Two edge types (events vs. behaviors).** An *event* edge feeds every output
  onward (`pipe`, which is just composition). A *behavior* edge exposes a
  stream's *latest* value as a `Context` (`sample`, latest-wins, no
  backpressure). "Context" is not a separate kind of thing; it is how a reader
  connects to a processor's output. `sample` is the one connector that needs
  something running, so it is the visible seam where the imperative shell shows
  up.
- **DAG recovered, not authored.** `@node` declares a unit of work and its
  inputs; the graph (execution order) is recovered from declared inputs and
  rendered to mermaid for visibility. The decorator returns the function
  unchanged.

## Workspace layout

A `uv` workspace of flat, version-locked packages (no namespace packages). Each
package is its own top-level import. The root (`without-workspace`) is a virtual
project (`package = false`) holding shared dev tooling and config.

- `packages/without` (import `without`) — the core. `contracts` (`Processor`,
  `Stream`, `Context`, `Transition`, `from_reducer`), `graph` (`@node`,
  `Registry`, mermaid + topological order), `wiring` (`Sample`, `pipe`,
  `sample`), `tasks` (`background_task`, a `with`-scoped background task), and
  `testing` (`stream`, `collect`, `tick`).
- `packages/without-env` (import `without_env`) — a static `Context` parsed from
  environment variables with `pydantic-settings`.
- `packages/without-configmap` (import `without_configmap`) — a behavior source
  backed by a Kubernetes ConfigMap mount: `watch_config` watches the mount
  directory (catching the atomic `..data` swap) and reparses a single YAML file
  on each change via `read_yaml_file`; read it through `without.sample`. This is
  the first context that actually changes.
- `packages/without-integration` (import `without_integration`) — not a real
  package: depends on `without` and every plugin so they are exercised together
  in cross-package tests. New plugins get added to its dependencies.

## Status

Done and verified (mypy strict clean across 17 source files, 20 tests passing,
ruff lint + format clean):

- The core contract, recast around stream-to-stream processors with the pure
  reducer as the testable kernel.
- DAG recovery from declared inputs, topological order, and mermaid rendering.
- The two edge connectors, `pipe` and `sample`.
- Two plugins: env vars (static context) and ConfigMap (changing context),
  proving both halves of the events/behaviors model end to end.

Tooling matches the counterweight convention: `uv_build` backend, Python 3.13,
mypy strict, ruff at line-length 120, a 7-day `exclude-newer` cooldown, and a
`justfile` (`just test` runs mypy + pytest).

## Open questions and next steps

In the recommended order, with the hardest validation last:

1. A toy line-protocol server (Redis-ish) to prove long-lived processor state
   and a processor that emits outputs in response to a request stream.
2. HTTP (sans-IO deps). The real test of whether the contract pays rent versus a
   plain `async def handle(request)`.

Known-hard problems inherited from dataflow/FRP, to decide deliberately rather
than discover: backpressure on event edges, glitches on diamond dependencies
(the DAG-from-declared-inputs idea makes diamonds common), and feedback cycles
and teardown order.

Smaller TODO already on the record: `without.testing.tick` advances the event
loop a single step, so `sample` tests rely on the source draining in one
activation. The deterministic fix is an explicit "await next update" signal on
the sampled context (an `await config.changed()`), which also has standalone
value for consumers that want to react to a behavior changing.
