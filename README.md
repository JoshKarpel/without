# without

A decoupled-IO substrate for connecting streams of events to stateful processors
backed by contexts, aiming for maximum concurrency from testable,
dependency-injected code. I/O is not banned, it is separated into the right
abstractions (sources at the edge, behaviors via `sample`, effects contained in
a processor's step) so the parts stay reusable.

The bet: Python has many frameworks with similar-but-subtly-different shapes
(ASGI apps, Kafka consumers, asyncio protocols, config reloaders) that do not
interoperate because none of them names the shared lower layer. `without` names
that layer as a narrow interface, so the pieces compose. It is meant to feel like
a library (your control flow stays visible) rather than a framework.

See [`PHILOSOPHY.md`](PHILOSOPHY.md) for the design rationale: the stateful
stream processor as a universal way to model computation, and an ecosystem of
thin layers you can descend, remix, and replace. The full
documentation, narrative guides, an API reference recovered from the source
docstrings, and the derived package dependency graph, lives at
<https://without.help/>.

## Layout

This is a `uv` workspace of flat, version-locked packages (no namespace
packages). Each package is its own top-level import.

- `packages/without` — the core: the interfaces every plugin speaks
  (`without.interfaces`), the stream edge connectors (`without.wiring`), and a
  `with`-scoped background task helper (`without.tasks`). Distributed on PyPI as
  `without-core` (the bare `without` name is unavailable there); imported as
  `without`.
- `packages/without-env` — first plugin: a static `Context` parsed from
  environment variables (`pydantic-settings`). Imported as `without_env`.
- `packages/without-configmap` — config from a Kubernetes mount (`watchfiles` +
  `pydantic`); the context-updated-by-a-stream half of the model. Imported as
  `without_configmap`.
- `packages/without-asgi` — adapters that turn an ASGI app's `receive`/`send`
  into typed event streams and back, in *both* directions (so it serves an app
  adapter and an ASGI server equally). The boundary only, no routing or
  framework. Imported as `without_asgi`.
- `packages/without-web` — an opinionated HTTP/WebSocket router over
  `without-asgi`: trie matching, typed path params, 405-vs-404, mounting, scoped
  middleware, exception handlers, and OpenAPI. Imported as `without_web`.
- `packages/without-http` — an `asyncio` ASGI **server** (and HTTP client) built
  on the sans-IO `h11`/`h2`/`wsproto` state machines: `serving(app)` owns the socket
  and the wire protocol (HTTP/1.1, HTTP/2, and WebSockets) and drives any ASGI app.
  Imported as `without_http`.
- `packages/without-dag` — bounded-concurrency execution of DAG-shaped async
  workflows: a `Graph` builder threads value types through the wiring, and a
  single-input graph is an async callable that `from_map` lifts straight into a
  `Processor`. Imported as `without_dag`.
- `packages/without-durability` — durable workflows over `without-dag`: a
  checkpoint any process can read, plus the store interfaces (`Checkpointer`,
  `Scheduler`, `Durable`) that make "one writer at a time" enforceable rather than
  hoped for. Imported as `without_durability`.
- `packages/without-durability-redis`, `packages/without-durability-postgres`,
  `packages/without-durability-sqlite` — one store each, so the core pulls no
  driver. Redis reaches every guarantee with small Lua scripts, the SQL stores with
  ordinary transactions, and SQLite needs no server at all.
- `packages/without-logging` — a logging pipeline: stdlib log records parsed into
  immutable `Record` values at a `capture` boundary, filtered and enriched as
  processors, drained to a sink the app owns. Imported as `without_logging`.
- `packages/integration` — not a real package (and never published: its name
  sits outside the `without*` family, so the publish workflow skips it): depends
  on `without` and every plugin so they can be exercised together in
  cross-package tests. Imported as `integration`.

The [package dependency graph](https://without.help/architecture/package-graph/)
(each arrow is "depends on") is derived from the declared dependencies and
rendered on the documentation site.

Planned plugins, in the order they should be attempted:

1. `without-env` — config from env vars; a static context. **(done)**
2. `without-configmap` — config from a k8s mount (`watchfiles` + `pydantic`);
   proves the context-updated-by-a-stream loop. **(done)**
3. a toy line-protocol server (Redis-ish); proves long-lived processor state.
   **(done: `integration.kv`)**
4. HTTP (sans-IO deps); the real test of the interface. **(done: `without-asgi`
   adapters, the `without-web` router, and `without-http` (an `h11`/`h2`/`wsproto`
   ASGI server plus client, serving HTTP/1.1, HTTP/2, and WebSockets). HTTP/3 and
   WebSockets-over-HTTP/2 are documented fast-follows.)**

## Development

```bash
uv sync
just test        # start the services in compose.yaml, then mypy + pytest
just durations   # profile the suite (slowest fixtures, setup, calls, teardown)
just docs        # serve the documentation site with live reload
```

A few tests drive a real backing service rather than a fake. `just test` starts the
services in [`compose.yaml`](compose.yaml) with [docker](https://www.docker.com) or
[podman](https://podman.io), whichever it finds, and stops them again when it exits;
install either to run those tests, or let them skip.
