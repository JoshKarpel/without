# without-http

A sans-IO-backed ASGI **server** and **HTTP client** for `without`. Where
[`without-asgi`](without-asgi.md) is the *app* side of the ASGI boundary (it turns
a server's `receive`/`send` into typed streams), `without-http` is the *server*
side: it owns the socket and the HTTP wire protocol, and drives any ASGI app via
`app(scope, receive, send)`. See the
[`without_http` API reference](../reference/without_http.md) for the full surface.

The wire-protocol state machines are themselves sans-IO libraries:
[`h11`](https://h11.readthedocs.io/) for HTTP/1.1,
[`h2`](https://python-hyper.org/projects/h2/) for HTTP/2, and
[`wsproto`](https://python-hyper.org/projects/wsproto/) for WebSockets.
`without-http` reads and writes socket bytes with `asyncio`, feeds them through
those state machines, and uses `without-asgi`'s server-direction codecs to
translate between typed events and the ASGI dicts an app expects.

## Server

```python
from without import sleep_forever
from without_asgi import make_asgi_app
from without_http import serving

app = make_asgi_app(lifespan, http=router.dispatch, websocket=sockets.dispatch)

async with serving(app, host="127.0.0.1", port=8000):
    await sleep_forever()   # run until cancelled
```

Because `without-http` speaks plain ASGI to the app, *any* ASGI app runs over it,
interchangeably with uvicorn: a [`without-web`](without-web.md) router, a bare
`without-asgi` handler, or a third-party app (Starlette, FastAPI).

`serving(app, ...)` is the entrypoint: an async context manager that drives the
lifespan cycle, binds the socket (pass `port=0` to let the OS pick), yields a
`Server`, and shuts down cleanly on exit. There is no separate run-until-cancelled
wrapper: hold the block open however you like, with `sleep_forever()` for the simple
case or your own loop (signal handling, several servers under `asyncio.gather`). The
yielded `Server` exposes the bound address and live metrics:

```python
async with serving(app, port=0) as server:
    ...  # hit http://{server.host}:{server.port}; server.in_flight is the live count
```

What the server handles:

- **Lifespan.** The app is run once with a `lifespan` scope for the server's
  lifetime: `startup` on entry, `shutdown` on exit. An app that does not support
  lifespan signals so by raising before it acks startup; the server then serves
  without a lifespan cycle (the standard ASGI fallback).
- **TLS.** Pass an `ssl.SSLContext` as `ssl_context` to serve `https`/`wss`
  directly (the scope's `scheme` becomes `https`/`wss`). `server_ssl_context`
  builds one for the common case, advertising the protocols the server speaks via
  ALPN. `ssl_handshake_timeout` and `ssl_shutdown_timeout` bound the TLS handshake
  and close.
- **HTTP/2.** Selected by ALPN (`h2`) over TLS, or by *prior knowledge* over
  cleartext (the h2 connection preface is sniffed off the first bytes, since `h11`
  would mis-parse `PRI` as an HTTP/1 method). Each request stream drives its own
  ASGI app invocation, so many run concurrently over one connection; a single lock
  serializes the shared `h2.Connection` and the writer, and body sends respect
  per-stream `WINDOW_UPDATE` flow control. The same `without-asgi` server-direction
  codecs carry over; only the wire mapping (`h2_wire`) is new.
- **Keep-alive.** Sequential requests on one HTTP/1.1 connection reuse it
  (`h11`'s `start_next_cycle`).
- **WebSockets** over the HTTP/1.1 `Upgrade`: the handshake is handed to `wsproto`,
  and the connection runs full-duplex (a reader pump feeds inbound frames to the
  app's `receive` while `send` writes outbound frames). A `websocket.close` sent
  before `websocket.accept` becomes an HTTP `403`, per the ASGI contract.
- **Isolation.** A crashing request handler is contained: it becomes a `500` (when
  no response has started yet) without taking the connection or server down.
- **Connections.** Served via `asyncio.start_server`, which owns the accept loop
  (surviving transient accept errors with its built-in retry delay) and binds every
  address `host` resolves to. `max_pending_connections` is the kernel listen backlog
  (the queue of accepted-by-the-OS-but-not-yet-served connections; when it fills, the
  OS drops or refuses further connection attempts). The server does not cap raw
  connections: the backlog and OS resource limits provide that backpressure, and
  `Server.in_flight` reports the live connection count for metrics. To bound in-flight
  *requests* (the right limit once one HTTP/2 connection multiplexes many requests),
  wrap the app in `limit_concurrent_requests`, which sheds with a `503`.

The pure wire cores (`h11_wire`, `h2_wire`, `ws_wire`) are sans-IO and unit-tested:
they map `h11`/`h2`/`wsproto` events to the typed `without-asgi` vocabulary and
back, with no sockets. The `asyncio` shell (`server.py`) is the only part that
touches I/O.

## Client

The client is a `ConnectionPool` you open once and make requests through, not free
`get`/`post` functions:

```python
from without_http import ConnectionPool

async with ConnectionPool() as pool:
    async with pool.request("GET", "http://127.0.0.1:8000/items") as (head, body):
        assert head.status == 200
        data = await body.read()
```

### The response: a `(head, body)` split

`pool.request` yields a `ClientResponse`, which is a `NamedTuple`, so take it whole or
unpack it as you like:

```python
async with pool.request("GET", url) as response:   # response.head, response.body
    ...
async with pool.request("GET", url) as (head, body):   # unpacked, types preserved
    ...
```

`head` is a `ResponseHead` (status + headers), a value you branch on immediately;
`body` is a `ResponseBody`, a live stream you consume separately. This mirrors how the
server *consumes* a request (a `scope` value plus a body stream): the structured head
is pulled out as a value so you can decide what to do before touching the body.

`head` is without-http's own inbound type, deliberately *not* without-asgi's outbound
`ResponseStart` even though the fields match: a type the parser fills from the wire
has no defaults (so a missing field fails loudly), while an outbound type an app
builds carries them for ergonomics. Same split as without-asgi's `RequestBody`
(inbound) versus `ResponseBody` (outbound).

### Buffered and streaming, both directions

Request and response bodies each cover the full buffered/streaming matrix, the
client mirror of `without-web`'s server handlers. The request body is `body=`
on `pool.request`: pass `bytes` to buffer it, or a `Stream[bytes]` (any async
iterable of chunks) to stream it. The response `body` is a live stream: iterate it
chunk by chunk, or `await body.read()` to buffer the whole thing.

```python
async def upload() -> AsyncIterator[bytes]:
    for path in paths:
        yield path.read_bytes()

async with pool.request("POST", url, body=upload()) as (head, body):
    async for chunk in body:          # stream the response as it arrives
        sink.write(chunk)
```

The connection is released when the body is finished: an HTTP/1.1 connection is
returned to the pool only if its body was read to the end (a partial read closes
it, since unread bytes remain on the wire), and an HTTP/2 stream is reset if
abandoned early. `pool.request` closes the body on block exit, so a body you never
read still releases its connection rather than stranding it.

### Trailers

A response can carry trailing headers after its body (gRPC's `grpc-status` is the
common case). The default path drops them: `async for chunk in body` and
`await body.read()` yield only `bytes`. When you know (out of band, by the
endpoint's contract) that trailers matter, opt in:

```python
data, trailers = await body.read_with_trailers()   # trailers: tuple[ResponseTrailers, ...]
# or, while streaming: async for item in body.events():  # bytes | ResponseTrailers
```

`read_with_trailers` returns *all* trailer blocks (an empty tuple if none), so a
consumer that requires them enforces that itself rather than the framework imposing
a failure on every response. Dropping trailers on the default path is a deliberate,
valid choice, not a swallowed error, so a server adding a trailer never breaks a
client that does not ask for it.

### Connection pooling

`ConnectionPool` keys connections by origin. HTTP/2 requests to one origin
multiplex over a single pooled connection; HTTP/1.1 connections are kept alive and
reused serially (an idle one is checked out per request and returned once its
response body is read). h2 is negotiated by ALPN over TLS
(`ConnectionPool(allow_http2=True)`, the default; pass a custom `ssl_context_factory` for a
private CA), or over cleartext by *prior knowledge* with `ConnectionPool(force_http2_cleartext=True)`
(no negotiation, so the caller is asserting the server speaks h2c); otherwise the
origin speaks HTTP/1.1.

```python
async with ConnectionPool(allow_http2=True, ssl_context_factory=make_ctx) as pool:
    # eight concurrent requests, multiplexed over one h2 connection
    bodies = await asyncio.gather(*(fetch(pool, n) for n in range(8)))
```

Open the pool as an async context manager so its connections are closed on exit; a
directly-constructed `ConnectionPool()` works for short-lived use but does not manage
the long-lived connections keep-alive retains.

### Client middleware

A client *exchange* (`ClientRequest -> ClientResponse`) is the dual of a server
handler, and a `ClientMiddleware` wraps one into another: `ClientExchange ->
ClientExchange`. That is the zero-context case of the **same** `stack` that composes
server middleware (a server middleware is `(handler, state, scope) -> handler`; a
client one needs no context because the request *is* the value it transforms), so the
one `stack` serves both. The pool carries a default `middleware` applied to every
request, and `pool.request(..., middleware=...)` composes more *inside* it for a
single call:

```python
from without_http import ConnectionPool, add_headers, follow_redirects, cookies, CookieJar, stack

jar = CookieJar()
async with ConnectionPool(middleware=add_headers((b"authorization", b"Bearer ..."))) as pool:
    async with pool.request("GET", url, middleware=stack(follow_redirects(), cookies(jar))) as (head, body):
        ...
```

Because the whole request is the value the exchange transforms (not a fixed scope),
middleware can rewrite it on the way out (inject headers, change the URL on redirect,
attach cookies) and wrap the response on the way back.

For the simple independent case, `wrap(request=, response=)` builds a middleware from a
request transform and/or a response transform, the client counterpart to
without-asgi's `wrap` (which wraps a handler's inbound/outbound streams). `add_headers`
is a one-liner over it: `wrap(request=lambda r: replace(r, headers=...))`. Reach for it
when the two sides are independent; a middleware whose sides share state (`cookies`) or
that loops (`follow_redirects`) is written directly as a `ClientExchange` wrapper.

```python
from without_http import ClientResponse, wrap

byte_counter = wrap(response=lambda r: ClientResponse(r.head, counting(r.body)))
```

Keep the pool's own middleware to *pure* decoration (default headers, redirect
following, retry): things that are values, not state. Anything carrying mutable,
request-spanning identity belongs in a value you own and pass per request. A
`CookieJar` is the canonical case: you construct the jar and hand it to `cookies(jar)`,
so cookie scope (application identity) stays independent of connection reuse
(transport) rather than both hiding in the pool. Two requests share cookies exactly
when they share a jar. Place `cookies` *inside* `follow_redirects` in a `stack` so each
redirect hop both sends and collects cookies.
