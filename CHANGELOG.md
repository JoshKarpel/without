# Changelog

## Unreleased

### Added

- **`without-web`**: reverse routing. `url_for(route, values)` renders a route back to a concrete
  path from the values for its path parameters, the inverse of the trie walk. It is a plain
  function of the route *value* (routes are identified by value, no registry), each value fed back
  through its converter to prove it round-trips (parse, don't validate, in reverse). Because
  `mount` bakes any prefix into the route, a route is a self-contained value whose segments are its
  full path, so reversing needs no router and holds no hidden prefix: a handler links by referencing
  a route value (immutable), and a websocket handler reverses an HTTP route to link to its resource
  with the same call.
- **`without-http`**: granular client request timeouts. A `Timeout` value bounds each phase
  independently (`connect`, `read`, `write`, `pool`), each an *inactivity* bound that re-arms on
  progress and is disabled by default (a deadline is the caller's policy, not the transport's). A
  timeout raises a typed `ConnectTimeout` / `ReadTimeout` / `WriteTimeout` / `PoolTimeout` under
  `HTTPTimeout` (itself a `TimeoutError`), so a caller can tell how far the request got and retry
  the right ones. Also: a per-host connection bound (`max_connections_per_host`, whose acquire-wait
  the `pool` axis guards, unbounded by default) and gating of HTTP/2 stream issuance against the
  server's `SETTINGS_MAX_CONCURRENT_STREAMS`.

### Changed

- **`without-web`**: routing and mounting reworked around self-contained route values. `mount(prefix,
  *middleware)` and `ws_mount(...)` are transforms that bake the prefix (and per-route middleware)
  into routes, reusable and usable as decorators; `delegate(prefix, app)` and `ws_delegate(...)`
  mount an opaque BYO app as a black box with the prefix-trimmed scope. This replaces the former
  `Mount`/`WebsocketMount` wrapper (a transparent sub-router is now just its baked routes), so a
  route carries its own full path — matching, OpenAPI, and reverse routing all read it directly, and
  a nested opaque app is trimmed by its full accumulated prefix by construction. Reverse routing is
  now the free `url_for` function rather than a `Router.url_for` method plus a `url_for()` extractor
  injected through `Match`.
- **`without-http`**: the client sends the request body concurrently with reading the response
  (consumer-driven duplex) instead of sending it whole first. A server can now answer early (a `413`,
  a redirect) without deadlocking a large upload, and a caller can drive genuine bidirectional
  streaming over HTTP/2 by feeding a queue-backed request body in reaction to the response. Connection
  teardown is a single release-exactly-once path shared by the background sender and the response body.

## 0.0.1

### Added

- **`without-core`** (imported as `without`): the narrow-waist core. The `Stream` / `Processor` / `Context`
  contracts, the builders (`from_map`, `from_scan`, `from_sink`, `from_fold`, and
  the polarity-dual predicate filters `from_selector` / `from_filter`), the wiring
  connectors (`compose`, which also composes a processor onto a terminal `Sink`;
  `tee`, its terminal fan-out counterpart, splitting a stream across several `Sink`
  branches so a shared prefix runs once; `sample`, `stream_from_iterable`,
  `stream_from_queue`, `collect`, `buffer`, `stack`), and the `with`-scoped task
  helpers
  (`background_task`, `limit_concurrency`, `sleep_forever`, `cancel_futures`,
  `as_async_iterator`).
- **`without-env`**: a static `Context` loaded once from environment variables
  with `pydantic-settings`.
- **`without-configmap`**: a behavior source backed by a Kubernetes ConfigMap
  mount, reloaded with `watchfiles` (watches the mount directory to catch the
  atomic `..data` symlink swap).
- **`without-asgi`**: adapters between an ASGI app's `receive`/`send` and typed
  event streams, complete in both the app and server directions, plus
  `make_asgi_app` and the unopinionated routing/middleware vocabulary.
- **`without-web`**: an opinionated HTTP/WebSocket router with trie matching,
  typed path parameters, converters, extractors, 405-vs-404, mounting, scoped
  middleware, exception handlers, and structure-recovered OpenAPI.
- **`without-http`**: an `asyncio` ASGI server and connection-pooling HTTP client
  built on the sans-IO `h11`/`h2`/`wsproto` state machines, serving HTTP/1.1,
  HTTP/2, and WebSockets (over the HTTP/1.1 upgrade), with TLS, keep-alive,
  streaming and buffered bodies, trailers, and client middleware.
- **`without-dag`**: bounded-concurrency execution of DAG-shaped async workflows,
  a typed `Graph` builder, and a single-input `CompiledGraph` that lifts straight
  into a `Processor` via `from_map`.
- **`without-logging`**: a logging pipeline. Stdlib log records parsed into
  immutable `Record` values at a `capture` boundary (stdlib as a one-way source),
  the message resolved and any exception captured as a structured
  `TracebackException` at that edge (no live traceback carried downstream, and its
  formatting left to the app), filtered with the core `from_selector` (plus the
  `at_least` level predicate) and enriched with `add_fields`, drained to a sink the
  app owns (or several at once, each with its own tail, through the core `tee`).
  Per-call-site context binds at the edge with the scoped `bind(**fields)` context
  manager and the `merge_context` `Record -> Record` enrichment composed into the
  default parser (the structlog-style `bind_contextvars` equivalent), since the
  pipeline runs off the caller's task and cannot recover it. Optional opt-in
  renderers `render_json` (fields flat) and `render_console` (human line) cover the
  common encodings without the core forcing one, with
  the timestamp and exception encodings injected: `exception_to_dict` (structured
  frames) or `exception_to_text` (flat traceback), and `iso_timestamp` by default.
  `offload` bridges a
  blocking worker onto a dedicated thread (delivering items in bursts, so the
  worker flushes when it catches up, no per-write thread hop) so file I/O stays off
  the event loop. Destination-shaped writers take strings (render a `Record` to text
  with a `from_map(Record -> str)` in front) and own the newline framing:
  `to_rotating_file` owns the byte count and clock, rotating on any combination of
  `max_bytes` (size), `max_age` (relative interval), and `schedule` (absolute
  wall-clock boundaries, built from times of day with `at_times`); `to_stream` writes
  to a caller-owned text stream (`sys.stderr`, a socket) without closing it.
- Documentation site (mkdocs-material + mkdocstrings): narrative guides, an API
  reference recovered from the source docstrings, and a package dependency graph
  derived from the workspace `pyproject.toml` files.
