# without

A decoupled-IO substrate for connecting streams of events to stateful processors
backed by contexts, aiming for maximum concurrency from testable,
dependency-injected code. I/O is not banned, it is separated into the right
abstractions (sources at the edge, behaviors via `sample`, effects contained in
a processor's step) so the parts stay reusable.

## The bet

Python has many frameworks with similar-but-subtly-different shapes (ASGI apps,
Kafka consumers, asyncio protocols, config reloaders) that do not interoperate
because none of them names the shared lower layer. `without` names that layer as
a narrow contract, so the pieces compose. It is meant to feel like a library
(your control flow stays visible) rather than a framework.

The [Philosophy](philosophy.md) page is the durable rationale: the narrow-waist
bet, the stream/processor/context substrate, functional-core/imperative-shell,
and values-over-places. Read it first to get the mindset the code is shaped
around.

## The substrate

Three types carry the whole model (`without.contracts`):

- A `Stream[T]` is an asynchronous sequence of values: the one shape every
  connection takes, whoever does the I/O. A socket, a file watcher, a clock, and
  an in-memory list are all just streams.
- A `Processor[In, Out]` transforms a stream of inputs into a stream of outputs.
  It is the only node type and the only thing a user writes.
- A `Context[T]` is a stream viewed as its latest sampled value: `current()`
  reads the latest and never blocks, the way long-lived state (config, a pool)
  is read.

## The packages

This is a [`uv`](https://docs.astral.sh/uv/) workspace of flat, version-locked
packages. Each is its own top-level import.

- [`without`](without/index.md): the core contracts every plugin speaks, the
  stream connectors, and a `with`-scoped background task helper.
- [`without-env`](without-env/index.md): a static `Context` parsed from
  environment variables with `pydantic-settings`.
- [`without-configmap`](without-configmap/index.md): config from a Kubernetes
  mount, the context-updated-by-a-stream half of the model.
- [`without-asgi`](without-asgi/index.md): adapters that turn an ASGI app's
  `receive`/`send` into typed event streams and back, in both directions.
- [`without-web`](without-web/index.md): an opinionated HTTP/WebSocket router
  with trie matching, typed path params, mounting, scoped middleware, exception
  handlers, and OpenAPI.
- [`without-http`](without-http/index.md): an `asyncio` ASGI server and HTTP
  client built on the sans-IO `h11`/`h2`/`wsproto` state machines.
- [`without-dag`](without-dag/index.md): bounded-concurrency execution of
  DAG-shaped async workflows, liftable straight into a `Processor`.

The [package dependency graph](architecture/package-graph.md) is derived from the
declared dependencies, and each package's API reference (its `Reference` page,
e.g. [`without`](without/reference.md)) is recovered from the source docstrings.

## Installing

```bash
pip install without-core      # the core contracts (imported as `without`)
pip install without-web       # plus whichever plugins you need
```

## Development

```bash
uv sync
just test        # start the services in compose.yaml, then mypy + pytest
just docs        # serve this site with live reload
```

A few tests drive a real backing service rather than a fake. `just test` starts the
services in `compose.yaml` with [podman](https://podman.io) and stops them again when
it exits; install podman to run them, or let them skip.

For contributing to `without` itself, see the
[Releasing](contributing/releasing.md) runbook for how the workspace is
versioned and published, and the
[Mutation testing](contributing/mutation-testing.md) guide for driving each
package's suite to zero surviving mutants.
