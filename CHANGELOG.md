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
  independently (`connect`, `read`, `write`, `pool`), each a `timedelta` and an *inactivity* bound
  that re-arms on progress, disabled by default (a deadline is the caller's policy, not the
  transport's). Each axis applies through its own bound (`connecting()`, `reading()`, `writing()`,
  `pooling()`), so the axis-to-error mapping lives on `Timeout` rather than at every call site. A
  timeout raises a typed `ConnectTimeout` / `ReadTimeout` / `WriteTimeout` / `PoolTimeout` under
  `HTTPTimeout` (itself a `TimeoutError`), so a caller can tell how far the request got and retry
  the right ones. Also: per-host connection bounds and gating of HTTP/2 stream issuance against the
  server's `SETTINGS_MAX_CONCURRENT_STREAMS`. `max_connections_per_host` bounds concurrent HTTP/1.1
  connections to one origin (the acquire-wait the `pool` axis guards); `max_keepalive_per_host`
  bounds how many *idle* connections are retained per origin once a burst subsides, so the pool ramps
  up under load but settles back down when quiet. Both unbounded by default, and must be `>= 1` when
  set.
- **`without-http`**: TCP keepalive on pooled client connections, on by default and configured with a
  `TCPKeepalive` value on the pool (`idle`/`interval` as `timedelta`s, `count`, or `None` to disable).
  The kernel probes
  an otherwise-idle connection and drops it when a peer has vanished *silently* (a crash, a partition, a
  NAT dropping the flow), which a clean server-side close does not: that sends a `FIN` the pool already
  detects before reuse. This matters most because request timeouts are disabled by default, so nothing
  else would notice a dead idle socket until a request hung on it.
- **`without-asgi`**: `file_response(path)` streams a file as the `ResponseStart` + `ResponseBody` event
  stream a handler yields, with `Content-Type` guessed from the suffix (`mimetypes.guess_file_type`,
  overridable) and `Content-Length` from `stat`, the body read in `chunk_size` pieces off the event loop
  (`asyncio.to_thread`) so a large file is never buffered whole. It is a coroutine, not an async
  generator: awaiting it runs the `stat` up front, so a missing file raises `FileNotFoundError` before
  any `ResponseStart` is emitted and a handler can still answer a clean `404`. Reads and writes are
  lockstep by default; wrap the result in `spool` for read-ahead.
- **`without-asgi`**: `headers`, a module of pure functions over the raw ASGI header pairs
  (`RawHeaders`) rather than a wrapper type. `get_all` returns every value under a name as an
  immutable tuple and `first` the first (for singleton fields, where a duplicate is a protocol
  violation); `add`, `replace`,
  `remove`, `subset`, and `merge` are `RawHeaders -> RawHeaders` transforms. All match field names
  case-insensitively (RFC 9110) and preserve duplicates, so a multi-valued `Set-Cookie` survives
  intact. `RawHeaders` is the one representation the ASGI spec fixes on both edges, so operating on
  it directly keeps reads a scan and writes a straight pass-through, no value to wrap or unwrap.
- **`without-web`**: `once` and `optional`, parse adapters for singleton request fields. Each
  lifts a one-value `parse` into the tuple-taking form `query_param`/`header_param` feed: `once`
  requires the value exactly once (returning `V`), `optional` allows zero or one (returning
  `V | None`, `None` when absent). A duplicated value raises `ValueError` in both (a duplicated
  singleton violates RFC 9110 §5.3). Reading a single value stays a policy the call site chooses
  rather than a second extractor.

### Changed

- **`without-web`**: query and header extractor `parse` callbacks now receive an immutable `tuple`
  of values rather than a `list` (`query_param`, `header_param`, and the `once`/`optional`
  adapters), and `Request.query_params` values are tuples. The parsed request is a value no
  consumer can mutate out from under another (values over places); a `parse` typed on `list` must
  widen to `tuple`.
- **`without-core`** (imported as `without`): the `buffer` wiring connector is renamed `spool`, and its
  `maxsize` argument renamed `ahead`, so `spool(source, ahead=n)` reads as the read-ahead it is (drive a
  source ahead of its consumer through a bounded queue on a background task). Behavior is unchanged.

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
  streaming over HTTP/2: the request head is sent before the first body chunk is produced, so both a
  client-speaks-first duplex (feed a queue-backed body in reaction to the response) and a
  server-speaks-first one (let the server respond before any body chunk is ready) work. Connection
  teardown is a single release-exactly-once path shared by the background sender and the response body.
  Closing an early-answered HTTP/1.1 connection is now a bounded *lingering close* (a half-close `FIN`
  plus a short, fixed drain window, never draining to end-of-input) rather than a reset that could
  race ahead of and discard the response the server already sent, and the client stops streaming its
  body the moment the peer half-closes rather than writing on into a closing connection. See the new
  [Security](https://without.help/without-http/security/) page.

### Fixed

- **`without-asgi`**: `make_asgi_app` now closes the inbound stream when a connection handler exits,
  so a handler that abandons the request body early (reads part of it, then returns) has the inbound
  generator's `finally` run deterministically instead of leaving it suspended for garbage collection.
  This is the server-side mirror of the client folding connection release into its response-body
  generator; the handler's inbound stream is wrapped in `aclosing`, covering both the HTTP and
  WebSocket paths.

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
