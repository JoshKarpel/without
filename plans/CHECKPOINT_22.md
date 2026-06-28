# Checkpoint 22

A snapshot of where `without` stands, succeeding `CHECKPOINT_21.md`. For the prior
state see `CHECKPOINT_21.md`; for the original pitch see `BIG_IDEA.md`.

This checkpoint builds out the **`without-http` client** into a real one: HTTP/2
multiplexing, HTTP/1.1 keep-alive reuse, cleartext h2c by prior knowledge, and
**streaming request and response bodies** (the full buffered/streaming matrix in
both directions). It lands the third item on Checkpoint 21's open list and then the
two follow-ups under it, plus a body-shape correction the user called for mid-way.

It also **simplifies the server**: the connection-admission cap is dropped and
`serving` goes back to `asyncio.start_server` (see below).

Everything is green: mypy clean (89 source files), full pytest suite (392 tests)
passing, pre-commit clean.

## Body shape: the full 2×2, streaming both ways

The first cut modeled the exchange as `request_bytes -> response_bytes`, which
cannot express streaming uploads or downloads. Corrected to mirror `without-web`'s
server handlers, where the core is a stream and buffering is a convenience:

- **Request body** is `content=` on `session.request`: `bytes` to buffer (gets a
  `content-length`), or a `Stream[bytes]` (any async iterable of chunks) to stream
  (gets `transfer-encoding: chunked` over h1; rides DATA frames over h2).
- **Response body** is a live stream: `async for chunk in response`, or `await
  response.read()` to buffer it whole.
- **The exchange stays value-based** (`ClientRequest -> ClientResponse`), *not*
  scope-based like the server handler. This is deliberate: the whole request is the
  value middleware transforms, so a `ClientMiddleware` can rewrite it (inject
  headers, change the URL on redirect, wrap the body). Header injection is therefore
  ordinary middleware (`add_headers`), not a built-in request knob: neither `Session`
  nor `open_session` carries a header field.

`ClientResponse` owns the body lifecycle: it is released exactly once, when the body
finishes. An h1 connection returns to the pool only if its body was read to the end
(a partial read closes it, since unread bytes remain on the wire); an h2 stream is
reset if abandoned early. `Session.request` finalizes the response on block exit, so
a body that is never read still releases its connection rather than stranding it.
(The release lives in `ClientResponse._finalize`, not in the body generator's
`finally`: closing an async generator that was never iterated skips its `finally`, so
relying on it leaked connections for unread responses.)

## HTTP/2 client connection model

`_Http2Connection` is the dual of the server's `_serve_h2_connection`: one
`h2.Connection` (`client_side=True`, `header_encoding=None`), a read loop feeding it
wire bytes and dispatching events, and per-request coroutines writing their own
stream out through it. One lock serializes all connection + writer access; `drain`
happens outside it.

- **Request-body flow control** is the server's, unchanged: a sender chunks by
  `min(remaining, local_flow_control_window, max_outbound_frame_size)` and blocks on a
  per-stream event the read loop sets on `WindowUpdated`, with the same lost-wakeup
  invariant (clear-and-wait under the lock; set only after a `WINDOW_UPDATE` under the
  lock).
- **Response-body flow control runs the other way:** the read loop queues received
  data as `(data, flow_controlled_length)` and *never* acknowledges it; the body
  consumer acknowledges each chunk as it reads it. So an unread response cannot outrun
  the flow-control window, which bounds the client's buffer (real backpressure, not
  ack-on-receipt buffering).
- **Failure isolation:** a `StreamReset` fails just that stream; connection teardown
  fails every pending stream and wakes blocked senders. A response abandoned early
  resets its stream (`abort`).

## Connection pooling

`Pool` keys connections by `Origin(scheme, host, port)` (a frozen dataclass, used
directly as the dict key):

- **HTTP/2:** one shared connection per origin, multiplexing. A per-origin lock
  serializes only *establishment* (TLS handshake + ALPN classify), so concurrent
  first-requests share one connection instead of racing open N; the lock is released
  before any request runs, so multiplexing is unserialized.
- **HTTP/1.1 keep-alive:** connections are kept and reused serially. An idle one is
  checked out per request and returned once its response body is read (`h11`
  `start_next_cycle` decides reusability: `Connection: close` or a socket close drops
  it). A pooled connection the server closed while idle is detected at checkout
  (`writer.is_closing()` / `reader.at_eof()`) and discarded for a fresh one; the rare
  close-after-checkout race surfaces as an error rather than an automatic replay,
  since streamed bodies are not replayable.
- **Protocol selection:** h2 by ALPN over TLS (`http2=True`, default; falls back to
  h1, remembered per origin in `_h11_only`), or over cleartext by *prior knowledge*
  with `http2_cleartext=True` (no negotiation: the caller asserts the server speaks
  h2c, and one that does not fails rather than falling back). Otherwise HTTP/1.1.

## `Session` construction

`Session` keeps a default-constructed `Pool` (`field(default_factory=Pool)`), so
`Session(...)` still constructs, but `open_session(...)` is the managed entrypoint:
it builds the pool with the requested `http2` / `http2_cleartext` / `ssl_context` and
`aclose()`s it on exit (cancelling h2 read loops, closing idle h1 sockets). Because
keep-alive now *retains* connections, a directly-constructed `Session` that makes
requests would leak them, so the test and integration call sites moved to
`open_session`.

## Server: connection cap dropped, back to `start_server`

`max_concurrent_connections` is removed. With HTTP/2 multiplexing a *connection* cap
no longer tracks in-flight work (one connection carries many requests), so the real
overload control is `limit_concurrent_requests` (request-level, transport-agnostic,
already shipped); a connection count's only remaining job is bounding raw
fds/memory, which the kernel listen backlog (`max_pending_connections`) and OS limits
already do.

Dropping the cap removed the one reason `serving` hand-rolled its accept loop
(gate admission *before* `accept`). So `serving` returns to `asyncio.start_server`,
which owns the accept loop (including transient-accept-error resilience with its
built-in `ACCEPT_RETRY_DELAY`) and binds every address `host` resolves to. Gone with
it: `_listen`, `_serve_accepted`, `_Slots`, the manual accept loop, and the
`accept_error_cooldown` parameter. Kept: the `in_flight` connection gauge (now a
trivial `_LiveConnections` counter wrapping the per-connection handler) on
`Server.in_flight`, and connection cancellation on shutdown (handlers register their
task in a set that `serving` cancels on exit).

## Verification

- **Pure/unit (`test_h2_wire.py`):** `request_headers` (pseudo-headers first,
  `Host`→`:authority`, hop-by-hop stripping) and `response_status_and_headers`.
- **HTTP/1.1 (`test_client.py`):** GET/POST, `add_headers` and `follow_redirects`
  middleware, keep-alive reuse (the same connection object is pooled and reused),
  stale-idle replacement, a streamed request body from an async iterator, a chunked
  streamed response read chunk by chunk, and cleartext h2c by prior knowledge.
- **HTTP/2 over TLS (`test_h2_client.py`, client ↔ its own server via `trustme`):**
  GET/POST, 8 concurrent requests multiplexed over **one** connection (asserted via
  the pool), a streamed request body, a 200 KB body larger than one flow-control
  window, and `add_headers` over h2.

## Open questions and next steps

`without-http` items still ahead:

1. **Consumer-driven request/response duplex** and **per-host pool limits.** A
   request currently sends its whole body before reading the response (correct for
   request/response HTTP, not full-duplex), and the pool keeps one h2 connection and
   an unbounded idle h1 list per origin. Also unhandled: a reused h2 connection
   hitting the server's `MAX_CONCURRENT_STREAMS` (open a second), and h2 stream-id
   exhaustion. `follow_redirects` re-issues the same body, so a redirect with a
   one-shot streaming body is not replayable (fine for the usual bodyless redirects).
2. **WebSockets over HTTP/2** (RFC 8441 extended CONNECT): needs `wsproto`'s
   lower-level `FrameProtocol` rather than its h11-bound `WSConnection`.
3. **HTTP/2 response extensions:** server push and trailers (real h2 semantics,
   deferred from the Checkpoint 21 cut; still `NotImplementedError`).
4. **HTTP/3** over QUIC (`aioquic`), a separate transport path. API and deployment
   constraints settled in `plans/WITHOUT_HTTP.md` (edge-tier-only, `http3=True`
   opt-in, dual TCP+UDP listeners, gated `Alt-Svc`).

Carried from earlier, still open: a transport-level 503 for arbitrary hosted ASGI
apps (without-native apps are covered by `limit_concurrent_requests` middleware);
OpenAPI shared components / `$ref`; `todos` persistence stubbed; opaque-mount
prefixes literal-only; intra-workspace deps unpinned; `make_asgi_app` never
`aclose()`s the inbound stream; the actor-model question (`ACTOR_MODEL.md`).
Documentation debt: `BIG_IDEA.md` still calls the model an "async reducer" (it is an
async *scan*).

Operational, carried: CI on `proof-of-concept` (PR #1, draft) has two flaky
concurrency tests in `kv/test_shell.py` that pass locally.
