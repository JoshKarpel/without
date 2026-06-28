# without-http

A sans-IO-backed ASGI **server** (and HTTP client) for `without`. Where
[`without-asgi`](../without-asgi) is the *app* side of the ASGI boundary (it turns
a server's `receive`/`send` into typed streams), `without-http` is the *server*
side: it owns the socket and the HTTP wire protocol, and drives any ASGI app via
`app(scope, receive, send)`.

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
interchangeably with uvicorn: a [`without-web`](../without-web) router, a bare
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

The client is a reusable [aiohttp-style](https://docs.aiohttp.org/en/stable/client_quickstart.html)
session you open once and share, not free `get`/`post` functions:

```python
from without_http import open_session

async with open_session() as session:
    async with session.request("GET", "http://127.0.0.1:8000/items") as response:
        assert response.status == 200
        body = await response.read()
```

### Buffered and streaming, both directions

Request and response bodies each cover the full buffered/streaming matrix, the
client mirror of `without-web`'s server handlers. The request body is `content=`
on `session.request`: pass `bytes` to buffer it, or a `Stream[bytes]` (any async
iterable of chunks) to stream it. The response body is a live stream: iterate it
chunk by chunk, or `await response.read()` to buffer the whole thing.

```python
async def upload() -> AsyncIterator[bytes]:
    for path in paths:
        yield path.read_bytes()

async with session.request("POST", url, content=upload()) as response:
    async for chunk in response:          # stream the response as it arrives
        sink.write(chunk)
```

The connection is released when the body is finished: an HTTP/1.1 connection is
returned to the pool only if its body was read to the end (a partial read closes
it, since unread bytes remain on the wire), and an HTTP/2 stream is reset if
abandoned early. `session.request` finalizes the response on block exit, so a body
you never read still releases its connection rather than stranding it.

### Connection pooling

The session is the home for default headers, the middleware stack, and the
connection pool. HTTP/2 requests to one origin multiplex over a single pooled
connection; HTTP/1.1 connections are kept alive and reused serially (an idle one
is checked out per request and returned once its response body is read). h2 is
negotiated by ALPN over TLS (`open_session(http2=True)`, the default; pass a custom
`ssl_context` for a private CA), or over cleartext by *prior knowledge* with
`open_session(http2_cleartext=True)` (no negotiation, so the caller is asserting the
server speaks h2c); otherwise the origin speaks HTTP/1.1.

```python
async with open_session(http2=True, ssl_context=ctx) as session:
    # eight concurrent requests, multiplexed over one h2 connection
    bodies = await asyncio.gather(*(fetch(session, n) for n in range(8)))
```

Open the session with `open_session` so the pool's connections are closed on exit;
a directly-constructed `Session()` works for short-lived use but does not manage
long-lived pooled connections.

### Client middleware

A client *exchange* (`ClientRequest -> ClientResponse`) is the dual of a server
handler, so the **same** `Middleware` vocabulary and `stack` that wrap server
handlers wrap client exchanges. The session has no header field of its own:
cross-cutting request decoration (default headers, auth) is middleware. Two are
shipped:

```python
from without_http import open_session, add_headers, follow_redirects
from without_asgi.routing import stack

async with open_session(middleware=stack(
    add_headers((b"authorization", b"Bearer ...")),
    follow_redirects(max_hops=5),
)) as session:
    ...
```

A `ClientMiddleware` is `(state, inner_exchange, request) -> exchange`, the request
playing the role the scope plays server-side. Because the whole request is the value
the exchange transforms (not a fixed scope), middleware can rewrite it: inject
headers, change the URL on redirect, wrap the body.

## Deferred

The server speaks HTTP/1.1, HTTP/2, and WebSockets (over the HTTP/1.1 upgrade).
Still ahead, building on the shipped per-stream machinery (see
[`plans/WITHOUT_HTTP.md`](../../plans/WITHOUT_HTTP.md)):

- **WebSockets over HTTP/2** (RFC 8441 extended CONNECT), which needs `wsproto`'s
  lower-level frame layer rather than its h11-bound handshake connection.
- **HTTP/2 response extensions:** server push and trailers. The h2 wire mapping
  currently supports the response start/body and 103 early hints, and rejects the
  rest, mirroring the HTTP/1.1 path.
- **Consumer-driven request/response duplex** and per-host pool limits: today a
  request sends its whole body before reading the response (fine for request/response
  HTTP, not for full-duplex), and the pool keeps one h2 connection and an unbounded
  idle h11 list per origin.
- **HTTP/3** over QUIC (`aioquic`), a separate transport path producing the same
  vocabulary.
- **Transport-level overload shedding for arbitrary hosted ASGI apps.** Request-level
  shedding already ships as the `limit_concurrent_requests` middleware (which wraps the
  handler, so it applies under any transport); only a hosted third-party app, into
  which without-middleware cannot be injected, would need a 503 at the transport.
