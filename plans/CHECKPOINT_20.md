# Checkpoint 20

A snapshot of where `without` stands, succeeding `CHECKPOINT_19.md`. For the prior
state see `CHECKPOINT_19.md`; for the original pitch see `BIG_IDEA.md`.

This checkpoint attacks deferred work in `without-http` (Checkpoint 19's open
list): it adds **server-side TLS/ALPN** and **connection-admission limits**, and
extracts the bounded-concurrency idiom those limits need into a reusable
`without.tasks` primitive.

Everything is green: mypy clean (84 source files), full pytest suite (351 tests)
passing, pre-commit clean.

## TLS/ALPN on the server

- **`serving`/`serve` take `ssl_context`** (an `ssl.SSLContext`) plus
  `ssl_handshake_timeout` and `ssl_shutdown_timeout`. With a context set, the
  server speaks `https`/`wss` directly. The two timeout knobs are the natural
  companions of TLS (bounding the handshake and the closing `close_notify`); they
  are pass-throughs to `asyncio.start_server` / `connect_accepted_socket`.
- **Scheme is derived from the transport**, not threaded as a flag: a connection
  is secure when `writer.get_extra_info("ssl_object")` is present, so the scope's
  `scheme` becomes `https`/`wss` automatically.
- **`server_ssl_context(certfile, keyfile=None)`** (new `without_http.tls`) builds
  a server context for the common case and advertises `ALPN_PROTOCOLS`
  (`("http/1.1",)` for now; `"h2"` joins it when HTTP/2 lands). A caller needing
  more (encrypted key, client-cert verification) brings its own context, since
  `ssl_context` is injected.
- Tested with `trustme` (new dev dependency): an `https` request round-trips and
  reports `scheme == "https"`; a `wss` upgrade reports `wss`; ALPN negotiates
  `http/1.1` even when the client also offers `h2`.

## Connection-admission limits

Two knobs on `serving`/`serve`, with `backlog` renamed to say what it is:

- **`max_pending_connections`** (was `backlog`): the kernel listen backlog, the
  queue of connections the OS has accepted (TCP handshake done) but the server has
  not yet `accept()`-ed. The docstring spells out what happens when it fills: the
  OS drops/refuses further connection attempts (on Linux the `SYN` is dropped, so
  the client retransmits and eventually succeeds or times out); nothing queues in
  the process.
- **`max_concurrent_connections`** (default `None` = unlimited): a true cap on
  connections served at once. The key decision (driven by the user): at the cap we
  **do not accept** further connections, rather than accepting and parking them.
  Parking would spawn a task per connection blocked on a semaphore and complete the
  TCP/TLS handshake, wasting memory and "lying" to the client (accepted but not
  worked). Instead the excess waits in the kernel's pending queue with no task and
  no handshake started.

### How the gating works

- The **unlimited path is unchanged**: `asyncio.start_server` with its built-in
  accept loop. Lowest blast radius for the common case.
- The **bounded path drives its own accept loop**. It binds the listening socket
  itself (`socket.getaddrinfo` + `bind`/`listen`, a single address, which is what
  accept-gating needs), then feeds an async generator (`incoming()`, which awaits
  `sock_accept` only when pulled) through `limit_concurrency`. Because the driver
  pulls the next item only while below the limit, the `accept` itself is gated.
  Each accepted socket is wrapped in streams via `connect_accepted_socket`
  (performing the TLS handshake there too, so TLS composes with the cap), then run
  through the same `_serve_connection`.
- `server.sockets` returns `TransportSocket` wrappers that lack `.accept()`, which
  is why the bounded path owns a real `socket.socket` rather than borrowing
  `start_server`'s.

## New substrate primitives in `without.tasks`

The bounded-accept idiom is general, not HTTP-specific (it is the
[death.andgravity](https://death.andgravity.com/limit-concurrency) pattern the
Python rules already reference), so it lives in `without.tasks` and is re-exported
from `without`, along with two helpers it factored out:

```python
async def limit_concurrency[T](
    aws: AsyncIterable[Awaitable[T]] | Iterable[Awaitable[T]],
    limit: int,
) -> AsyncIterator[asyncio.Future[T]]: ...
```

It runs at most `limit` awaitables at once, yielding each as a `Future` when it
finishes, and **pulls the source lazily** (only while below the limit), so a lazy
side-effecting source (an accept loop) is never advanced past the limit. On early
exit or cancellation it cancels and awaits the in-flight set, so nothing leaks.

Two reusable helpers came out of writing it (both re-exported from `without`):

- **`cancel_futures(futures)`**: cancel an entire set, *then* await them all, so
  they tear down concurrently rather than one-at-a-time. Replaces the open-coded
  two-phase teardown that `limit_concurrency` and the server's unlimited shutdown
  path both had.
- **`as_async_iterator(items)`**: normalize a sync or async iterable into one
  async iterator, so `limit_concurrency` consumes both kinds through a single
  `await anext` path (cleaner than branching `next`/`anext` with a `type: ignore`).

Unit-tested: `limit_concurrency` (run-all, never-exceed-limit, lazy pulling,
cancel-on-exit, failure surfaced through the `Future`), `cancel_futures`
(cancel-then-await all, non-cancellation teardown error propagates), and
`as_async_iterator` (sync and async sources).

## Design notes worth keeping

- **`max_concurrent_connections` is connection admission, not request shedding.**
  A background comparison to uvicorn's `--limit-concurrency` (which accepts every
  connection and returns an immediate `503`) clarified the split: 503 is a clearer
  client signal and covers request-level overload (essential for HTTP/2, where one
  connection multiplexes many requests), but it still does accept + handshake +
  response work per rejection. Accept-gating is the leaner choice for the current
  HTTP/1.1 server (effectively one request per connection at a time). The two are
  complementary; a 503 request-concurrency limit is a reasonable later addition
  *alongside* the connection cap, and becomes more relevant once HTTP/2 lands.

## Verification

- **Pure/unit:** `limit_concurrency` (five behaviors); `server_ssl_context` ALPN
  via end-to-end negotiation.
- **Server end-to-end over loopback:** `https` GET reporting `scheme`, a `wss`
  upgrade reporting `scheme`, ALPN preferring `http/1.1` over an offered `h2`, the
  concurrency cap (a third connection is not even accepted while two are in
  flight, then served once a slot frees), and TLS composed with the cap.

## Open questions and next steps

Carried forward from Checkpoint 19, still the `without-http` headline items:

1. **HTTP/2 (`h2`)**: ALPN now advertises only `http/1.1`; HTTP/2 adds `"h2"` to
   `ALPN_PROTOCOLS`, cleartext prior-knowledge detection, and concurrent
   multiplexed per-stream app invocations with `WINDOW_UPDATE` flow control. Design
   settled in `plans/WITHOUT_HTTP.md`. WebSockets-over-HTTP/2 (RFC 8441) and HTTP/3
   (`aioquic`) sit behind it.
2. **Client connection pooling** behind the `Session` surface (keep-alive reuse,
   per-host limits, eventual h2 multiplexing). v1 is connection-per-request.
3. **A 503 request-concurrency limit** alongside the connection cap (see design
   note above), most useful once HTTP/2 multiplexing lands.

Carried from earlier, still open: OpenAPI shared components / `$ref`; `todos`
persistence stubbed; opaque-mount prefixes literal-only; intra-workspace deps
unpinned; `make_asgi_app` never `aclose()`s the inbound stream; the actor-model
question (`ACTOR_MODEL.md`). Documentation debt: `BIG_IDEA.md` still calls the
model an "async reducer" (it is an async *scan*).

Operational, carried: CI on `proof-of-concept` (PR #1, draft) has two flaky
concurrency tests in `kv/test_shell.py` that pass locally.
