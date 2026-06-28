# `without-http` — own the HTTP transport, sans-IO

## Context

`without` already has `without-asgi`: a *boundary adapter* that turns an ASGI
server's `receive`/`send` callables into typed `Inbound`/`Outbound` event
streams and runs a per-connection (fractally per-request) `Processor[Inbound,
Outbound]`. It does **not** own the transport — uvicorn (or another ASGI server)
owns the socket and the HTTP wire protocol; `without-asgi` only translates dicts.

The goal of `without-http` is to **own the transport layer ourselves** while
letting a sans-IO HTTP library (`h11` for HTTP/1.1, `h2` for HTTP/2) own the
wire-protocol state machine. We read/write socket bytes via `asyncio`, feed them
through the sans-IO library, and expose the same without-style streams:
request parts in, response parts out — for **both a server and a client**.

This forces a clean realization that the existing code already hints at: the
HTTP request/response *vocabulary* living inside `without-asgi` is not
ASGI-specific. ASGI and raw-socket-h11/h2 are two **boundaries** that both
produce and consume the same vocabulary. So we extract the vocabulary into a
dependency-light package both can share, and the result is full composition:

- A `without-web` router runs unchanged under uvicorn (`make_asgi_app`) **or**
  over `without-http`'s socket server (`serve`).
- *Any* ASGI app (without-asgi-based or third-party like FastAPI) runs over
  `without-http` via a new `from_asgi` adapter — so a user can pair
  `without-asgi` with **any transport provider**: uvicorn or `without-http`,
  interchangeably (this was an explicit requirement).
- A `without-http` **client** exposes its request/response as streams, so the
  **same middleware vocabulary** (`stack`/`wrap`/`Middleware`) used for server
  handlers applies to client exchanges (auth injection, redirects,
  decompression, retry, logging).

## Design decisions (resolved with the user)

- **Both server and client** in the first cut.
- **`h11` (HTTP/1.1) + `h2` (HTTP/2)** for the HTTP wire, **plus `wsproto`** for
  **WebSockets in v1** — `without-http` serves WebSockets too, for parity with
  `without-asgi`. WebSockets ride an HTTP/1.1 `Upgrade`; `wsproto` is the sans-IO
  WebSocket state machine layered over the connection after the handshake.
- **Package split** (the key decision):
  - `without-http-types` — pure, transport-agnostic HTTP + WebSocket vocabulary
    + middleware vocabulary. Depends only on `without`. **No `h11`/`h2`.**
  - `without-http` — the `h11`/`h2`-backed TCP transport (server + client).
    Depends on `without-http-types` + `h11` + `h2`. Knows nothing about ASGI.
  - `without-asgi` — refactored to depend on `without-http-types` (still **no**
    `h11`/`h2`), keeping only ASGI-specific framing; gains `from_asgi`.
  - `without-web` — repointed to consume the vocabulary from
    `without-http-types`; drops its `without-asgi` dependency.
- **Middleware is shared** and direction-agnostic: a client exchange is a
  `Processor` of the same shape as a server handler, so `stack`/`wrap`/
  `Middleware`/`buffered` move to `without-http-types` and serve server **and**
  client.
- **The WebSocket vocabulary moves to `without-http-types`**, so both
  `without-asgi` and `without-http` serve WebSockets from the same vocabulary and
  `without-web` depends only on `without-http-types`.
- **Deferred (see "Gaps to address later"):** WebSockets over HTTP/2, and
  HTTP/3. v1 does HTTP/1.1 + HTTP/2 request/response and WebSockets over the
  HTTP/1.1 upgrade only.

## Resulting package graph

```
without                       (substrate: Stream, Processor, Context, builders, wiring)
  ^
without-http-types            (pure HTTP + WebSocket + middleware vocabulary)   dep: without
  ^              ^                       ^
without-asgi   without-http            without-web
(ASGI dicts    (h11/h2/wsproto TCP    (router/handlers/extractors)
 <-> vocab;     server + client;       dep: without, without-http-types
 from_asgi)     middleware)
 dep:           dep: without-http-types,
 without-http-  h11, h2, wsproto
 types
```

No cycles. `without-http` never imports `without-asgi`; the user composes them
(`serve(host, port, from_asgi(asgi_app))`).

## Work breakdown

Each phase is independently shippable and keeps the full test suite green
(`just test` = mypy strict + pytest). Order follows "make the change easy, then
make the easy change": extract the foundation first as a pure refactor, then add
new capability on top.

### Phase 1 — Extract `without-http-types` (pure refactor, no new behavior)

New package `packages/without-http-types/` (mirror the `pyproject.toml` /
src-layout of `packages/without-asgi/`; dependency: `without` only;
`requires-python = ">=3.14"`).

Move the transport-agnostic types out of `without-asgi` into it:

- **Body/connection events** (from `without_asgi/inbound.py` and
  `outbound.py`): `RequestBody`, `Disconnect`, `Inbound`; `ResponseStart`,
  `ResponseBody`, `Response`, `encode_response`; the `RawHeaders` alias (generic
  HTTP, currently in `without_asgi/types.py`).
- **HTTP-generic outbound extensions**: `ServerPush`, `EarlyHint`,
  `ResponseTrailers` (real HTTP/2 / HTTP semantics). The `Outbound` union here
  is the HTTP-generic set. **Leave** the ASGI-server-offload extensions
  (`ZeroCopySend`, `PathSend`, `ResponseDebug`) in `without-asgi`, which extends
  the union with them for its own `encode_outbound`.
- **Request head**: introduce `RequestHead` holding the genuinely-HTTP fields
  from `HttpScope` (`method`, `path`, `raw_path`, `query_string`, `headers`,
  `http_version`, `scheme`, plus `authority`, `client`, `server`). This is what
  routers/handlers and the client use. `without-asgi`'s `HttpScope` composes a
  `RequestHead` plus ASGI framing (`asgi`, `root_path`, `extensions`). **Open
  detail to settle during implementation:** whether `without-web` matches on
  `RequestHead` directly (preferred — minimizes ASGI coupling) and how
  `root_path`/mounting maps onto it.
- **WebSocket vocabulary**: `WebsocketScope`, `WebsocketInbound`
  (`WebsocketConnect`/`Receive`/`Disconnect`), `WebsocketOutbound`
  (`WebsocketAccept`/`Send`/`Close`/`ResponseStart`/`ResponseBody`),
  `WebsocketText`/`WebsocketBinary`/`WebsocketData`. Moves now so `without-web`
  depends only on `without-http-types`. (`without-http` won't serve WebSockets
  in v1; this is for `without-web` + the future `wsproto` transport.)
- **Handler/middleware vocabulary** (from `without_asgi/app.py` and
  `routing.py`): `HttpHandler`, `HttpRouter[T]`, `WebsocketHandler`,
  `WebsocketRouter[T]`; `Lifespan[T]` (already ASGI-agnostic); `Middleware`,
  `HttpMiddleware`, `WebsocketMiddleware`, `stack`, `wrap`, `buffered`, and the
  `respond`/`read_body` helpers `buffered` needs. These are pure
  `Processor`/`Stream` combinators with no ASGI dependency.

Refactor `without-asgi` to import all of the above from `without-http-types` and
re-export the names it currently exposes (keep its public API stable). It
retains: raw ASGI types (`RawScope`, `RawMessage`, `Receive`, `Send`,
`ASGIApp`), `narrow_*`, ASGI scope/event parsing (`parse_scope`,
`parse_http_scope`, `parse_inbound`, `parse_lifespan_event`), `encode_outbound`
/ `encode_lifespan_reply`, lifespan event/reply messages
(`Startup`/`Shutdown`/`*Complete`/`*Failed`), the ASGI-only extension events,
shell adapters (`http_inbound`/`http_outbound`/etc.), and `make_asgi_app`.

Repoint `without-web` (`packages/without-web/src/without_web/`) imports from
`without_asgi` to `without_http_types` for the vocabulary, and drop
`without-asgi` from its `pyproject.toml` dependencies. Update its tests/helpers
imports accordingly.

**Verification:** `just test` passes unchanged (no behavior change). The diff is
imports + file moves + `pyproject.toml` dependency edits.

### Phase 2 — `without-http` server (h11 + h2 HTTP, then wsproto WebSockets)

New package `packages/without-http/` (dep: `without-http-types`, `h11`, `h2`,
`wsproto`; add to `uv.lock` respecting the 7-day cooldown).

- **Sans-IO core (pure, unit-testable, no sockets)** — mirror how `without-asgi`
  keeps parse/encode pure:
  - h11: map `h11` events → `(RequestHead, Stream[Inbound])` and
    `Outbound` → `h11` send events → bytes. Functions take/return values and an
    `h11.Connection`, no I/O.
  - h2: same shape over `h2.connection.H2Connection`, one request per stream id.
  - wsproto: map `wsproto` events → `WebsocketInbound` and `WebsocketOutbound`
    → `wsproto` sends → bytes, again pure over a `wsproto` connection object.
- **Transport shell (server):** `serve(host, port, *, http: HttpRouter[T],
  websocket: WebsocketRouter[T] = ..., lifespan: Lifespan[T])` using
  `asyncio.start_server`. Per connection: run `lifespan` once at server start
  (reuse the `Lifespan[T]` context-manager type; the server holds the state and
  threads it per request). Per request: build `RequestHead` + inbound stream
  from wire bytes, call the router → `Processor`, drain `Outbound` → wire bytes.
  Handle **keep-alive** (`h11` `start_next_cycle` for sequential requests on one
  connection; h2 multiplexed streams each spawn a per-request processor — the
  per-request-processor model from Checkpoint 6). ALPN selects h2 vs h11 (with
  an h2-prior-knowledge / h1-default fallback).
- **WebSockets (v1):** detect the HTTP/1.1 `Upgrade` handshake off the `h11`
  request, hand the connection to `wsproto`, build a `WebsocketScope` +
  `Stream[WebsocketInbound]`, call the `websocket` router → `Processor`, and
  drain `WebsocketOutbound` back through `wsproto` to the socket. Same
  per-connection-processor shape as HTTP and as `without-asgi`'s WebSocket path.
  WebSockets over HTTP/2 (extended CONNECT) is deferred (see future extensions).
- Reuse `read_body`, `buffered`, `stack`, `wrap` from `without-http-types`
  directly — server middleware works identically to `without-asgi`, for both
  HTTP and WebSocket handlers.

**Verification:** unit tests on the pure core (feed byte sequences, assert
`RequestHead`/`Inbound`/emitted bytes), mirroring `without-asgi`'s
parse/encode tests. End-to-end: start `serve` on an ephemeral port, drive it
with the Phase 4 client and/or `httpx`, assert responses; cover keep-alive and
both protocols.

### Phase 3 — `without-asgi` `from_asgi` (any transport provider)

Add `from_asgi(app: ASGIApp) -> HttpRouter[object]` (and a WebSocket sibling) to
`without-asgi`: the inverse of `make_asgi_app`. Given a vocabulary request
(`RequestHead` + inbound stream), it builds an ASGI scope dict, synthesizes
`receive` (vocabulary `Inbound` → ASGI receive dicts) and `send` (ASGI send
dicts → vocabulary `Outbound`, pushed to a queue), runs `app(scope, receive,
send)` as a background task, and yields decoded `Outbound`. Implemented with an
`asyncio.Queue` + `background_task` (from `without`).

This makes `serve(host, port, http=from_asgi(any_asgi_app))` run any ASGI app
over `without-http`, interchangeable with uvicorn.

**Verification:** unit-test `from_asgi` by driving a tiny known ASGIApp through
a synthetic inbound stream and asserting the `Outbound` sequence. Integration:
serve a `without-asgi`-based app (and optionally a Starlette app) via
`without-http` and hit it.

### Phase 4 — `without-http` client + client middleware

- **Client transport:** `connect(host, port)` / `request(head, body=...)` using
  `asyncio.open_connection`, driving the same pure h11/h2 core in client
  direction. The exchange is exposed as a `Processor` over request parts →
  response parts so it is a composable node, not a bespoke function.
- **Client middleware:** define `ClientExchange = Processor[<request parts>,
  <response parts>]` and `ClientMiddleware` as the matching `Middleware`
  instance, reusing `stack`/`wrap` from `without-http-types` unchanged. Ship a
  couple of example middlewares (default headers, redirect-follow) to prove the
  symmetry with server middleware.

**Verification:** client hits the Phase 2 server (loopback) and a public/local
HTTP server; assert a `wrap`-based middleware mutates the request/response
streams (e.g. injects a header observed server-side, or decompresses a gzipped
response).

### Phase 5 — integration examples + tests

In `packages/integration/`, add examples wiring the stack end-to-end and tests
that exercise composition:

- a `without-web` router served over `without-http` (`serve(..., http=router.dispatch)`);
- a `without-web` WebSocket route served over `without-http`;
- an ASGI app served over `without-http` (`serve(..., http=from_asgi(app))`);
- a client-with-middleware example hitting one of the above.

Docs:

- per-package `README.md` for `without-http-types` and `without-http` matching
  the style of the existing package READMEs;
- **a package dependency diagram in the root `README.md`** showing the full
  graph (`without` → `without-http-types` → {`without-asgi`, `without-http`,
  `without-web`}, plus `without-env`, `without-configmap`, `integration`), as a
  Mermaid `graph` so it renders on GitHub. Keep it current as packages are added;
- a new `plans/CHECKPOINT_19.md` capturing the layering decision and progress.

## Critical files

- New: `packages/without-http-types/` (pyproject + `src/without_http_types/...`).
- New: `packages/without-http/` (pyproject + `src/without_http/...`:
  pure `h11`/`h2`/`wsproto` core modules, `server.py`, `client.py`).
- Refactor: `packages/without-asgi/src/without_asgi/`
  (`inbound.py`, `outbound.py`, `scope.py`, `app.py`, `routing.py`,
  `shell.py`, `types.py`, `__init__.py`) — move vocabulary out, add `from_asgi`.
- Refactor: `packages/without-web/src/without_web/` (imports) +
  `packages/without-web/pyproject.toml` (drop `without-asgi` dep).
- Update: root `pyproject.toml` / `uv.lock` (workspace already globs
  `packages/*`; new deps `h11`, `h2`, `wsproto` with cooldown).
- Reuse, don't reinvent: `from_sink`/`from_scan`/`compose`/`background_task`
  and `asyncio.Queue` bridging patterns from `without` (`contracts.py`,
  `wiring.py`, `tasks.py`); the parse/encode test patterns from
  `packages/without-asgi/tests/`.

## Gaps to address later

These are explicitly out of v1 but should land as fast-follows. Both reuse v1
infrastructure (the HTTP/2 multiplexed-stream machinery, the WebSocket
vocabulary, the per-connection-processor model), so neither is a redesign.

### WebSockets over HTTP/2 (RFC 8441, "extended CONNECT")

v1 serves WebSockets only over the HTTP/1.1 `Upgrade`. Over HTTP/2 the handshake
is replaced by h2 headers: the server advertises
`SETTINGS_ENABLE_CONNECT_PROTOCOL = 1`; the client opens a stream with
`:method = CONNECT`, `:protocol = websocket`, `:scheme`/`:path`/`:authority`
(no `Sec-WebSocket-Key`/101); the server accepts with `:status = 200`; WebSocket
frames then flow as that stream's DATA frames.

- `h2` already supports extended CONNECT, so recognizing the request is small;
  the long-lived stream slots into the same multiplexed per-stream processor
  model (with WINDOW_UPDATE flow control) that v1's h2 HTTP responses use.
- **The real cost:** `wsproto`'s high-level `WSConnection` is bound to the h11
  handshake and can't be reused over h2. The H2 path must drive the lower-level
  `wsproto.frame_protocol.FrameProtocol` (frame encode/decode only). To avoid two
  divergent WebSocket paths, **factor the v1 WebSocket handler around the frame
  layer** so H1-vs-H2 differ only in handshake + framing transport; then H2
  becomes a near-free extension. Doing this factoring in v1 is cheap insurance
  even though v1 ships only the H1 transport.
- **The real risk:** few tools speak WS-over-H2, so end-to-end verification is
  thin. Validate the frame layer with the transport-independent
  [Autobahn TestSuite](https://github.com/crossbario/autobahn-testsuite) and a
  known-good peer where one can be found, rather than relying on
  `without-http` client ↔ server loopback alone (self-consistent but possibly
  spec-wrong). `without-http`'s client would also need the extended-CONNECT path.

### HTTP/3 (`aioquic` / `h3`)

HTTP/3 runs over QUIC (UDP), not TCP, so it cannot share the
`asyncio.start_server` / `asyncio.open_connection` transport at all — it needs
`aioquic` for the QUIC transport, with `h3`/`aioquic`'s HTTP/3 layer as the
sans-IO framing on top. This is a **separate transport path** within
`without-http`, parallel to the TCP h11/h2 path, producing and consuming the same
`without-http-types` vocabulary. The vocabulary and per-connection-processor
model carry over unchanged; the transport shell is new. Clearly the largest of
the deferred items.

## End-to-end verification

- `just test` green after every phase (mypy strict + pytest, `-Werror`).
- `pre-commit-autofix` clean.
- Manual: `serve` a `without-web` todos-style app on an ephemeral port, hit it
  with the `without-http` client and with `httpx`/`curl`; repeat with
  `from_asgi(asgi_app)`; exercise keep-alive, HTTP/2, and a WebSocket session.
