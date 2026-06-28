# Checkpoint 21

A snapshot of where `without` stands, succeeding `CHECKPOINT_20.md`. For the prior
state see `CHECKPOINT_20.md`; for the original pitch see `BIG_IDEA.md`.

This checkpoint lands the headline `without-http` gap from Checkpoint 20's open
list: **HTTP/2** (`h2`), making `without-http` a server that speaks HTTP/1.1,
HTTP/2, and WebSockets. The design was settled in `plans/WITHOUT_HTTP.md`; this is
its implementation.

Everything is green: mypy clean (87 source files), full pytest suite (381 tests)
passing, pre-commit clean.

## What shipped

- **`h2` is a new dependency** of `without-http` (`h2>=4.1`, resolves to 4.3.0).
- **`h2_wire.py`** is the pure, sans-IO wire core, mirroring `h11_wire`:
  - `scope_from_h2_headers` builds the typed `HttpScope` from a request's
    pseudo-headers (`:method`/`:path`/`:authority`), folding `:authority` into a
    synthesized `host` when the request carries none. `http_version` is `"2"`; the
    `scheme` comes from the transport, not the client-asserted `:scheme`.
  - `response_headers` renders a response start as the h2 header block (`:status`
    first), lowercasing names (h2 requires it) and dropping the hop-by-hop headers
    that are illegal over h2 (`connection`, `transfer-encoding`, ...).
  - `early_hint_headers` renders a 103 informational response.
  - `H2_PREFACE` is the connection-preface constant used for cleartext detection.
- **`_serve_h2_connection`** in `server.py` is the `asyncio` shell driving one
  multiplexed HTTP/2 connection (the h11 path was extracted to
  `_serve_h11_connection` so `_serve_connection` is now just protocol selection).

## Protocol selection

`_serve_connection` picks the wire protocol per connection:

- **ALPN** over TLS: `ssl_object.selected_alpn_protocol() == "h2"` selects HTTP/2.
  `ALPN_PROTOCOLS` is now `("h2", "http/1.1")` in server-preference order, so a
  client offering h2 gets h2, otherwise http/1.1.
- **Prior knowledge** over cleartext: the first bytes are read and matched against
  the full h2 connection preface (`b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"`). This must
  be peeked before feeding `h11`, which would mis-parse `PRI` as an HTTP/1 method.
  The peeked bytes are then threaded into whichever path is chosen (seeded into the
  `h11.Connection`, or fed to the `H2Connection` as its first chunk).
- Everything else is HTTP/1.1.

## The HTTP/2 connection model

One `h2.Connection` per TCP connection, many request streams multiplexed over it,
each driving its own ASGI app invocation (the per-request-processor model from
Checkpoint 6). A read loop feeds wire bytes to the shared connection and dispatches
its events; every stream's `send` writes back through the same connection.

- **One lock serializes** all access to the `h2.Connection` object and the writer.
  Writes happen under the lock (`conn.send_*` then `writer.write(conn.data_to_send())`
  in the same critical section); `writer.drain()` always happens *outside* the lock,
  so draining never blocks other streams or the read loop.
- **Per-request streams.** `RequestReceived` builds the scope and spawns the app
  task; `DataReceived` feeds the stream's inbound queue and acknowledges flow
  control (reopening the client's window); `StreamEnded` closes the request body
  (`RequestBody(more_body=False)`); `StreamReset` delivers a `Disconnect`.
- **Body flow control.** A stream's body sender chunks by
  `min(remaining, local_flow_control_window(stream_id), max_outbound_frame_size)`.
  When the window is empty it blocks on a per-stream `asyncio.Event` the read loop
  sets on `WindowUpdated`.
- **The lost-wakeup invariant** (the subtle bit the design warned about): a sender
  that finds the window empty clears its wake event *while holding the lock*, and
  the read loop only ever sets that event after applying a `WINDOW_UPDATE` under the
  same lock. So the window can never grow between a sender's check and its wait,
  which would otherwise strand the response. This is why `drain()` is kept off the
  lock but the clear/check is kept on it.
- **Failure isolation.** A crashing (or incomplete) app sends a `500` if no
  response started yet, else resets the stream, without disturbing the other
  streams or the connection. On connection teardown the read loop cancels all
  in-flight stream tasks via `cancel_futures`.

The same `without-asgi` server-direction codecs (`encode_http_scope`,
`encode_inbound`, `parse_outbound`) carry over unchanged; only the wire mapping is
new, exactly as the design predicted.

## Scope of the first h2 cut

Matching the HTTP/1.1 path's outbound coverage: `ResponseStart`, `ResponseBody`
(with flow control), and `EarlyHint` (103) are supported; `ServerPush`,
`ResponseTrailers`, and the offload extensions (`ZeroCopySend`/`PathSend`/
`ResponseDebug`) raise `NotImplementedError`. Server push and trailers are genuine
h2 semantics and are noted as a follow-up, but kept out of this cut to hold the
bug surface to the core multiplexing/flow-control machinery. GET, POST bodies,
HEAD, keep-alive-equivalent multiplexing, and bidirectional flow control all work.

## Server refinements (follow-on)

A few server-side cleanups landed alongside the h2 work:

- **One accept loop.** `serving` no longer has separate capped/uncapped paths
  (`asyncio.start_server` for unlimited, a manual loop for the cap). It always drives
  one manual accept loop; admission is gated by acquiring a slot *before* `accept`,
  with the uncapped case folded away inside a new `_Slots` object. `_Slots` hides the
  semaphore, removes the per-call-site `None` checks, and exposes an `in_flight`
  gauge (the connections-side metric; the requests side is the middleware's budget).
  `serving` now yields a `Server` value (its bound `host`/`port` plus `in_flight`,
  and room for future fields) rather than a bare `(host, port)` tuple, so the gauge
  is reachable for metrics. Consequence: `limit_concurrency` is now unused (kept as a
  general `without` utility for now), and binding is consistently single-address
  (previously the unlimited path multi-bound via `start_server`, a silent
  cap-dependent difference).
- **Accept-error resilience.** The manual loop survives `sock_accept` failures
  instead of silently dying: it releases the reserved slot, skips `ECONNABORTED`
  immediately, and for resource-exhaustion (`EMFILE`/`ENFILE`/`ENOBUFS`/`ENOMEM`) or
  any unexpected error logs and pauses `accept_error_cooldown` (defaulting to
  asyncio's own `ACCEPT_RETRY_DELAY`) before retrying, so it neither busy-loops nor
  wedges. This restores what `start_server`'s built-in loop did for us before.
- **`serve` removed.** The run-until-cancelled wrapper was a one-liner over
  `serving` that re-declared its whole signature just to forward it (rotting on
  every new knob), and a real run loop (signals, several servers under `gather`) is
  the caller's call anyway. `serving` is now the sole entrypoint; hold it open with
  `without.sleep_forever()`.
- **Request-concurrency shedding** shipped as the `limit_concurrent_requests`
  middleware (see open question 5 and `WITHOUT_HTTP.md`).

## Verification

- **Pure/unit (`test_h2_wire.py`):** `scope_from_h2_headers` (pseudo-headers,
  `:authority`-to-`host` synthesis, explicit-host precedence, percent-decoding),
  `response_headers` (status-first, lowercasing, hop-by-hop stripping),
  `early_hint_headers`.
- **End-to-end cleartext h2c (`test_h2_server.py`):** a small raw `h2` client
  (one stream per connection, prior-knowledge preface) covers GET, POST body, HEAD,
  lifespan-state threading (the `make_asgi_app` path `without-web` uses), a crashing
  handler returning 500, and a response larger than one flow-control window.
- **End-to-end h2 over TLS (`test_tls.py`, via `httpx(http2=True)` + `trustme`):**
  ALPN negotiating h2 (and falling back to http/1.1 without it), `scheme == "https"`
  over h2, 8 concurrent requests multiplexed over one connection, and a 200 KB body
  round-tripping (both-directions flow control). The old "ALPN prefers http/1.1"
  test was replaced, since the server now prefers h2.

## Open questions and next steps

`without-http` items still ahead (all build on the shipped per-stream machinery):

1. **WebSockets over HTTP/2** (RFC 8441 extended CONNECT): needs `wsproto`'s
   lower-level `FrameProtocol` rather than its h11-bound `WSConnection`.
2. **HTTP/2 response extensions:** server push and trailers (real h2 semantics,
   deferred from this cut).
3. **HTTP/2 on the client.** The `Session` client is still HTTP/1.1, connection-
   per-request; pooling and h2 multiplexing are the follow-ups behind that surface.
4. **HTTP/3** over QUIC (`aioquic`), a separate transport path. The API and
   deployment constraints are now settled in `plans/WITHOUT_HTTP.md`: it lands
   through the existing `serving` (no separate `serve_quic`) behind an explicit
   `http3=True` opt-in (default off; needs a TLS cert and the `without-http[http3]`
   extra, both enforced loudly at startup), with `serving` owning the dual TCP+UDP
   listeners, same-port binding, and `Alt-Svc` injection.
   Crucially, h3 is an **edge-tier-only** concern: it cannot reach an app behind
   upstream TLS termination (Istio/gateways), since QUIC welds TLS into the
   transport and there is no plaintext h3 to forward inward. So a meshed app stays
   `http3=False` and serves h1/h2c as today, and `Alt-Svc` auto-injection is gated
   on this process actually terminating h3.
5. ~~A 503 request-concurrency limit alongside the connection-admission cap.~~
   **Shipped as middleware** (`limit_concurrent_requests` in `without-asgi`'s
   `routing`), since the limit wraps the app invocation rather than the socket, so
   it applies under any transport. It takes a caller-supplied `overloaded` response
   (default `503` + `Retry-After: 1`). The only piece left at the transport level is
   bounding an *arbitrary hosted ASGI app* (the uvicorn case), deferred; see the
   resolved note in `plans/WITHOUT_HTTP.md`. The placement rule for built-in
   middleware is recorded in `packages/without-asgi/CLAUDE.md`.

Carried from earlier, still open: OpenAPI shared components / `$ref`; `todos`
persistence stubbed; opaque-mount prefixes literal-only; intra-workspace deps
unpinned; `make_asgi_app` never `aclose()`s the inbound stream; the actor-model
question (`ACTOR_MODEL.md`). Documentation debt: `BIG_IDEA.md` still calls the
model an "async reducer" (it is an async *scan*).

Operational, carried: CI on `proof-of-concept` (PR #1, draft) has two flaky
concurrency tests in `kv/test_shell.py` that pass locally.
