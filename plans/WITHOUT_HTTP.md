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

## Revision: what we actually built

The sections below this one are the *original* plan. During implementation the
user corrected its central premise, which simplified the design substantially.
This section records what was actually built; where it conflicts with the
original sections, this section wins.

The correction: `without-web` is not a transport, and it should keep working with
*any* ASGI server. `without-http` is itself **an ASGI server** (a uvicorn
alternative): it owns the socket and the wire protocol and drives any ASGI app
via `app(scope, receive, send)`. So the request/response *vocabulary* did not
need to be extracted into a shared package, and there is no inverse adapter to
write: the server speaks plain ASGI to apps.

Consequences (vs. the original plan):

- **No `without-http-types` package.** The vocabulary stayed in `without-asgi`.
  `without-web` is unchanged and still depends on `without-asgi`.
- **No `from_asgi`.** `without-http` runs any ASGI app natively, so there is
  nothing to adapt; `make_asgi_app`-built apps, bare `without-asgi` handlers, and
  third-party apps (Starlette/FastAPI) all just run.
- **`without-asgi` gained the reverse-direction codecs instead.** A server needs
  the dual of the app-side parse/encode: `encode_scope` /
  `encode_http_scope` / `encode_websocket_scope` (typed scope to dict),
  `encode_inbound` / `encode_websocket_inbound` / `encode_lifespan_event` (typed
  to the dict `receive` returns), and `parse_outbound` /
  `parse_websocket_outbound` / `parse_lifespan_reply` (the dict the app `send`s to
  typed). The vocabulary now round-trips both directions, and `without-http`
  works in typed values at the boundary while talking ASGI dicts to the app.
- **`without-http` shipped:** an `asyncio` ASGI server, `serve(app, ...)` and the
  testable `serving(app, ...)` context manager, over `h11` (HTTP/1.1) with
  keep-alive, the ASGI lifespan cycle (with the standard "app raised, so lifespan
  unsupported, serve anyway" fallback), and WebSockets over the HTTP/1.1 upgrade
  via `wsproto`. The pure wire cores (`h11_wire`, `ws_wire`) are unit-tested; the
  server and client are tested end-to-end over loopback (HTTP via `httpx`,
  WebSocket via a small `wsproto` client).
- **Client shipped as a mandated `Session`** (see the aiohttp note in Phase 4):
  `open_session()` / `Session.request(...)`, with client middleware reusing the
  server's `stack` (`add_headers`, `follow_redirects`). v1 opens a connection
  per request; pooling is the documented follow-up.
- **HTTP/2 was deferred** to a documented fast-follow (see "Gaps to address
  later"); the first cut is HTTP/1.1 + WebSockets, a complete ASGI server.
- **Substrate touch-up:** `without.sleep_forever()` was added for `serve`'s run
  loop, and the test suite gained `pytest-timeout` (a global per-test timeout) so
  a deadlocked async server test fails loudly instead of hanging.

Resulting package graph (no cycles):

```text
without
  ^
without-asgi   (ASGI <-> typed vocabulary, both directions; make_asgi_app)
  ^        ^
without-web   without-http   (asyncio ASGI server + client; deps h11, wsproto; h2 added when HTTP/2 lands)
(router)      dep: without, without-asgi
dep: without, without-asgi
```

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
- **Mandated session (aiohttp-style), connection pooling later.** The client
  surface should be a `Session` you open once and reuse, not a per-request
  free function: aiohttp's
  [request lifecycle](https://docs.aiohttp.org/en/v3.7.3/http_request_lifecycle.html)
  is the look-and-feel to follow, where almost all programs want one shared,
  long-lived session rather than one-shot requests. The session is the natural
  home for connection pooling (keep-alive reuse, per-host limits, HTTP/2 stream
  multiplexing over one connection): callers go through `async with
  session.request(...)` and the pool stays an implementation detail behind it.
  v1 can open a fresh connection per request inside the session; the pool is a
  contained follow-up that does not change the session API. A mandated session
  (no free `get`/`post` that hides a global pool) keeps lifetime and limits
  explicit and injected, matching the dependency-injection stance, and gives the
  pooling work one clear place to land.

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

These are explicitly out of the first shipped cut but should land as
fast-follows. They reuse the shipped infrastructure (the per-connection /
per-request processor model, the bidirectional ASGI codec, the WebSocket
vocabulary), so none is a redesign.

### HTTP/2 request/response (`h2`)

The first cut ships HTTP/1.1 (`h11`) plus WebSockets over the HTTP/1.1 upgrade
(`wsproto`), which together are a complete, tested ASGI server. HTTP/2 was
deferred from this cut: a correct h2 server needs concurrent multiplexed streams,
each driving its own ASGI app invocation, with `WINDOW_UPDATE` flow control and
careful lock discipline (the read loop and every stream's `send` share one
`H2Connection`, so a sender must not hold the connection lock while awaiting a
window update, or it deadlocks the read loop that would deliver it). That is a
large, intricate addition with real bug surface, so it is its own focused piece
rather than rushed in alongside the h11/ws work.

The design is settled and the `h2` API is proven (a happy-path request/response
round-trips through `h2.connection.H2Connection` already):

- **Detection.** Select h2 two ways: by ALPN (`h2` vs `http/1.1`) when serving
  over TLS, and by *prior knowledge* over cleartext, recognizing the connection
  preface `b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"` in the first bytes (peek before
  feeding `h11`, since `h11` would mis-parse `PRI` as an HTTP/1 method). Default
  to h11 when neither matches.
- **Per-stream apps.** Each `RequestReceived(stream_id)` builds an `HttpScope`
  from the h2 pseudo-headers (`:method`/`:path`/`:scheme`/`:authority`, the rest
  as ordinary headers, `:authority` folded into a synthesized `host`) and spawns
  a per-request app invocation, the same per-request-processor model the h11 path
  uses. `DataReceived` feeds that stream's inbound queue (and acknowledges flow
  control); `StreamEnded` closes the body; `StreamReset` cancels the app task.
- **Flow control.** A stream's `send` chunks the body by
  `local_flow_control_window(stream_id)` and `max_outbound_frame_size`, releasing
  the connection lock to await a per-stream event the read loop sets on
  `WindowUpdated`.
- **Codec reuse.** The same `without-asgi` server-direction codecs
  (`encode_http_scope`, `encode_inbound`, `parse_outbound`) carry over unchanged;
  only the wire mapping (h2 events ↔ typed vocabulary) is new, mirroring
  `h11_wire`.

WebSockets over HTTP/2 (below) build on this h2 stream machinery, so this lands
first.

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

#### One entrypoint, not a separate `serve_quic`

When HTTP/3 lands it MUST be reachable through the existing `serving` entrypoint,
not a parallel `serve_quic` the caller composes by hand. `serving` is the
imperative shell whose job is "bring up the server for this app", so owning all
three transports for one app is its concern; the vocabulary and per-request
processor layers stay transport-agnostic, and only the shell composes. With h3
enabled, `serving` brings up the TCP listener (h1/h2) and the QUIC/UDP listener
(h3) concurrently over the same app, and handles the glue the caller would
otherwise hand-write: binding TCP and UDP to the same port number, and injecting
the `Alt-Svc` response header into h1/h2 responses so clients discover h3.

Two constraints keep h3 from being unconditionally automatic, and both point to a
single **explicit opt-in, default off** parameter (`http3=True`) rather than
auto-enabling whenever `aioquic` happens to be importable:

- **`aioquic` is a heavy optional dependency** (a full QUIC + crypto stack). It
  MUST live behind an extra (`without-http[http3]`) so h1/h2-only users never
  drag it in.
- **QUIC is TLS-only.** There is no cleartext h3, so h3 can come up only when a
  cert is configured.

`serving` MUST reject illegal combinations at startup rather than silently
degrading (parse, don't validate): `http3=True` without TLS, or without `aioquic`
installed, fails loudly with a clear message. Auto-enabling on a stray importable
dependency is rejected as implicit magic: a call site should make plain that it
opens a UDP port, matching how the rest of `without` injects choices rather than
sniffing the environment.

#### HTTP/3 is an edge concern: TLS-terminating tier only

HTTP/3 cannot reach an app that sits behind upstream TLS termination (a service
mesh like Istio, an ingress gateway, any L7 proxy that terminates TLS). Because
QUIC welds TLS 1.3 into the transport, there is no plaintext HTTP/3 to forward
inward: "terminate TLS, then hand the app plaintext h3" describes something that
cannot exist. So h3 and upstream-TLS-termination are mutually exclusive at the app
boundary:

- **TLS terminated upstream** → the app receives plaintext, hence h1/h2c, hence
  not h3. The gateway terminates the client's QUIC connection (where h3's wins
  on the lossy public/mobile hop actually matter) and forwards h2/h1 upstream;
  the app runs `serving(app)` over plaintext TCP exactly as today, with
  `http3=False`. This is the overwhelmingly common production deployment, which is
  why default-off is correct.
- **App terminates its own TLS** (it is the public edge, or sits behind TLS
  passthrough) → it brings its own cert and runs QUIC directly with `http3=True`.
  This is where the dual-listener + `Alt-Svc` convenience pays off.

This makes one rule load-bearing, not merely tidy: **`Alt-Svc` auto-injection MUST
be gated on this process actually terminating h3.** A backend app must never
advertise `Alt-Svc`, since it would point clients at an h3 endpoint it does not
run, on a port nothing is listening on; the advertising belongs to whichever tier
terminates QUIC. A meshed app that wants to know the client used h3 reads it from
edge-supplied forwarded metadata (`Forwarded` / `X-Forwarded-Proto` /
`x-envoy-*`), not the ASGI scope, which honestly reports the hop the app is on.

### Request-concurrency shedding (503)

> Update (post-Checkpoint 22): the `max_concurrent_connections` cap discussed below
> was subsequently **dropped**. With HTTP/2 multiplexing it no longer tracks
> in-flight work, and the kernel listen backlog plus the `limit_concurrent_requests`
> middleware cover its job; `serving` now uses `asyncio.start_server`. The reasoning
> below is kept as the rationale for why request shedding lives in middleware.

The (since-removed) `max_concurrent_connections` was *connection admission*: at the
cap the server stopped accepting, so excess connections waited in the kernel's listen
backlog with no task and no handshake started. That was the leaner choice for the
HTTP/1.1-only server (effectively one in-flight request per connection).

It is **not** request-level overload shedding. Uvicorn's `--limit-concurrency`
takes the opposite approach: it accepts every connection and returns an immediate
`503` (with `Retry-After`) once over the limit. That gives the client a clear,
actionable "back off" signal and bounds *in-flight requests* rather than
connections, but it pays accept + (TLS) handshake + response cost per rejection.

The two are complementary, not either/or, so a 503 request-concurrency limit is a
reasonable addition *alongside* the connection cap: gate accepts to bound
resource consumption cheaply, and shed with `503` when request concurrency spikes.
This matters much more once **HTTP/2** lands, where one connection multiplexes many
concurrent requests and a connection cap no longer bounds in-flight work.

**Resolved: this is middleware, not transport code.** The 503 limit wraps the app
invocation, which is exactly the shape of an `HttpMiddleware`, so it ships as
`limit_concurrent_requests(limit, *, overloaded=<503 Response>)` in `without-asgi`'s
`routing`, not as a guard baked into `without-http`'s wire paths. The shed response
is a caller-supplied `Response` value (defaulting to `503` + `Retry-After: 1`), so
a JSON API or a `429` is just a different value, not a new code path. That dividing
line is principled and is the clean resolution of the connection-vs-request
confusion: **connection admission cannot be middleware** (it happens before any
scope or handler exists, so it stays the transport's `max_concurrent_connections`),
while **request shedding is middleware** (one implementation that applies under any
transport, h1/h2/h3, and even under uvicorn, because it wraps the handler rather
than the socket). Mounting it only on the HTTP router leaves long-lived WebSocket
connections uncounted; the shared budget is built once at app assembly and injected
through the closure.

The one case that would still justify a *transport-level* 503 in `without-http` is
bounding an **arbitrary hosted ASGI app** the server runs via plain ASGI (FastAPI,
Starlette), into which without-middleware cannot be injected. That is uvicorn's own
reason for putting the limit in the transport, and it is deferred until we care
about overload-protecting third-party hosted apps; without-native apps are fully
covered by the middleware.

## End-to-end verification

- `just test` green after every phase (mypy strict + pytest, `-Werror`).
- `pre-commit-autofix` clean.
- Manual: `serve` a `without-web` todos-style app on an ephemeral port, hit it
  with the `without-http` client and with `httpx`/`curl`; repeat with
  `from_asgi(asgi_app)`; exercise keep-alive, HTTP/2, and a WebSocket session.
