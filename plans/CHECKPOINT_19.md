# Checkpoint 19

A snapshot of where `without` stands, succeeding `CHECKPOINT_18.md`. For the prior
state see `CHECKPOINT_18.md`; for the original pitch see `BIG_IDEA.md`.

This checkpoint adds `without-http`: an `asyncio` ASGI **server** (and an HTTP
client) built on the sans-IO `h11` and `wsproto` state machines. It implements
`plans/WITHOUT_HTTP.md`, but with a corrected architecture (recorded in that
plan's new "Revision" section): the written plan assumed `without-http` would run
`without` processors directly and that the HTTP vocabulary would be extracted into
a shared `without-http-types` package. The user corrected the intent mid-flight:
`without-web` should keep working with *any* ASGI server, and `without-http` simply
*is* an ASGI server. That simplification dropped two whole pieces of the plan (the
`without-http-types` extraction and the `from_asgi` adapter) and replaced them with
a smaller change to `without-asgi`.

Everything is green: mypy clean (81 source files), full pytest suite passing,
pre-commit clean.

## The layering decision

- **`without-http` is an ASGI server, not a processor transport.** It owns the
  socket and the wire protocol and drives any ASGI app through the plain
  `app(scope, receive, send)` contract. So a `without-web` router (via
  `make_asgi_app`) runs over it unchanged, and so does any third-party ASGI app,
  interchangeably with uvicorn. There is no inverse adapter to write because the
  server speaks ASGI natively.
- **`without-web` is untouched.** It still depends only on `without-asgi`. The
  planned `without-http-types` package was not created; the vocabulary stayed
  where it was.
- **`without-asgi` became a bidirectional codec instead.** An app adapter parses
  the dicts a server sends and encodes the dicts an app sends back; a *server*
  needs the duals. Added: `encode_scope` / `encode_http_scope` /
  `encode_websocket_scope` (typed scope to dict), `encode_inbound` /
  `encode_websocket_inbound` / `encode_lifespan_event` (typed event to the dict
  `receive` returns), and `parse_outbound` / `parse_websocket_outbound` /
  `parse_lifespan_reply` (the dict an app `send`s to typed). The websocket
  text/binary frame codec moved to `without_asgi.types` (`encode_websocket_data` /
  `decode_websocket_data`) so both directions share it, and `SupportsFileno` is now
  `@runtime_checkable` so `parse_outbound` can narrow a zero-copy `file`. Every
  pair round-trips, tested with `parse(encode(x)) == x`.

## What `without-http` ships

- **`serve(app, ...)`** runs an ASGI app over HTTP until cancelled;
  **`serving(app, ...)`** is the testable async-context-manager seam that binds a
  socket (`port=0` picks a free one), yields the bound `(host, port)`, and shuts
  down cleanly (stops accepting, cancels in-flight connections, runs lifespan
  shutdown).
- **HTTP/1.1 via `h11`** with keep-alive (`start_next_cycle` for sequential
  requests), `HEAD` body suppression, a `500` for a crashing handler, and a `400`
  for a malformed request. The pure core `h11_wire` maps an `h11.Request` to an
  `HttpScope`, body events to `Inbound`, and `Outbound` to `h11` events.
- **The ASGI lifespan cycle**, with the standard "the app raised before acking
  startup, so lifespan is unsupported, serve anyway" fallback (`lifespan.py`,
  `run_lifespan`). A genuine startup/shutdown failure raises `LifespanError`.
- **WebSockets over the HTTP/1.1 upgrade via `wsproto`** (`ws_wire` +
  `_serve_websocket`): full-duplex (a reader pump feeds inbound frames to a queue
  the app's `receive` drains while `send` writes outbound frames), with
  `websocket.close`-before-`accept` becoming a `403`, ping/pong handled
  transparently, and a client close surfacing to the app as
  `websocket.disconnect`.
- **A client as a mandated `Session`** (aiohttp look-and-feel): `open_session()` /
  `Session.request(method, url, ...)` over `asyncio.open_connection`, driving the
  same `h11` core in the client direction (TLS for `https`). v1 opens one
  connection per request; connection pooling is the documented follow-up behind
  the same surface.
- **Client middleware that reuses the server's `stack`.** A client exchange
  (`ClientRequest -> ClientResponse`) is the dual of a server handler, so a
  `ClientMiddleware` is `Middleware[object, ClientExchange, ClientRequest]` and
  composes with `without_asgi.routing.stack`. Shipped: `default_headers` and
  `follow_redirects`.

## Substrate and tooling touch-ups

- **`without.sleep_forever()`** (in `without.tasks`): suspend until cancelled, the
  idiom `serve` uses for its run loop. Replaces the unintuitive
  `await asyncio.Event().wait()`.
- **`pytest-timeout`** with a global 30s per-test timeout (`timeout_method =
  "thread"`, xdist-safe), so a deadlocked async server test fails loudly instead
  of hanging the suite. This was prompted by a real deadlock found during the
  WebSocket work (an app that loops on the lifespan scope instead of raising).
- **`testpaths = ["packages/**/tests"]`** (was `["packages"]`): a glob that scopes
  collection to the test directories.

## A mypy footgun worth remembering

Defining an async-generator handler (a `Processor[Inbound, Outbound]` written as
`async def`) *in the same module that imports `without_http`* trips a mypy
protocol-recognition artifact: with `wsproto`/`h2` in the same analysis, mypy
intermittently stops recognizing `AsyncIterator <: Stream` and rejects the handler
assignment. Explicit variance on `Stream` does not fix it (the failure is
structural subtyping, not variance). The fix used in the `without-http` tests is to
keep the handler-style apps out of the test module: its WebSocket tests serve
*raw* ASGI apps (plain `ASGIApp` callables, no `Processor` typing), and the
`make_asgi_app` + typed-handler path is exercised in `integration` instead (where
no module both imports `without_http` and defines async-generator handlers, so the
artifact never fires).

## Verification

- **Pure cores** unit-tested: `h11_wire` (scope/inbound/outbound mapping),
  `ws_wire` (handshake detection, scope, outbound mapping), and the new
  `without-asgi` reverse codecs (round-trips).
- **Server end-to-end** over loopback with `httpx`: GET/POST/HEAD, keep-alive,
  lifespan-threaded state, a `500` on crash; WebSockets with a small `wsproto`
  client (echo, reject-before-accept, client-close to disconnect).
- **Client end-to-end** against the loopback server: GET, POST body,
  `default_headers` observed server-side, `follow_redirects` following a `302`.
- **Composition** (`integration/tests/test_without_http.py`): the `todos`
  `without-web` router served over `without-http`, reached by both `httpx` and the
  `without-http` client, the `404` exception mapping, client middleware supplying
  the `/admin` authorization header, and the `/todos/session` WebSocket route.

## Open questions and next steps

New, from this checkpoint:

1. **HTTP/2 (`h2`)** is the headline follow-up: ALPN plus cleartext
   prior-knowledge detection, concurrent multiplexed per-stream app invocations
   with `WINDOW_UPDATE` flow control, reusing the same server-direction codecs.
   Design is settled in `plans/WITHOUT_HTTP.md` ("Gaps to address later"); deferred
   for its flow-control/concurrency intricacy. WebSockets-over-HTTP/2 (RFC 8441)
   and HTTP/3 (`aioquic`) sit behind it.
2. **Client connection pooling** behind the `Session` surface (keep-alive reuse,
   per-host limits, eventual h2 multiplexing). The aiohttp-style mandated session
   is the place it lands; v1 is connection-per-request.
3. **TLS/ALPN** on the server (`serving` taking an `ssl` context); needed for h2
   negotiation and for serving `https` directly.

Carried forward from Checkpoint 18, still open:

1. No shared components / `$ref` in OpenAPI output; couples with `url_for` /
   reverse-routing.
2. `todos` persistence is stubbed (`POST` echoes); a live `Context[TodoList]`
   updated by a fold would exercise the actor-model question.
3. Opaque-mount prefixes are literal-only.
4. Intra-workspace deps unpinned; a packaging gap for publishing.
5. Unconsumed input streams: `make_asgi_app` never `aclose()`s the inbound stream
   at the boundary.
6. The actor-model question (`ACTOR_MODEL.md`); static `Context` ceremony; a
   dynamic-merge connector; graph/DAG recovery on `graphlib`; known-hard FRP
   problems.

Operational, carried: CI on the `proof-of-concept` branch (PR #1, draft) has two
flaky concurrency tests in `kv/test_shell.py` that pass locally.

Documentation debt (carried): `BIG_IDEA.md` still calls the model an "async
reducer" (it is an async *scan*).
