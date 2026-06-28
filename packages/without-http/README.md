# without-http

A sans-IO-backed ASGI **server** (and HTTP client) for `without`. Where
[`without-asgi`](../without-asgi) is the *app* side of the ASGI boundary (it turns
a server's `receive`/`send` into typed streams), `without-http` is the *server*
side: it owns the socket and the HTTP wire protocol, and drives any ASGI app via
`app(scope, receive, send)`.

The wire-protocol state machines are themselves sans-IO libraries:
[`h11`](https://h11.readthedocs.io/) for HTTP/1.1 and
[`wsproto`](https://python-hyper.org/projects/wsproto/) for WebSockets.
`without-http` reads and writes socket bytes with `asyncio`, feeds them through
those state machines, and uses `without-asgi`'s server-direction codecs to
translate between typed events and the ASGI dicts an app expects.

## Server

```python
from without_asgi import make_asgi_app
from without_http import serve

app = make_asgi_app(lifespan, http=router.dispatch, websocket=sockets.dispatch)

await serve(app, host="127.0.0.1", port=8000)   # runs until cancelled
```

Because `without-http` speaks plain ASGI to the app, *any* ASGI app runs over it,
interchangeably with uvicorn: a [`without-web`](../without-web) router, a bare
`without-asgi` handler, or a third-party app (Starlette, FastAPI).

`serve(app, ...)` runs forever (until cancelled). For tests and programmatic
control, `serving(app, ...)` is an async context manager that binds the socket,
yields the bound `(host, port)` (pass `port=0` to let the OS pick), and shuts down
cleanly on exit:

```python
from without_http import serving

async with serving(app, port=0) as (host, port):
    ...  # hit http://{host}:{port}
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
- **Keep-alive.** Sequential requests on one HTTP/1.1 connection reuse it
  (`h11`'s `start_next_cycle`).
- **WebSockets** over the HTTP/1.1 `Upgrade`: the handshake is handed to `wsproto`,
  and the connection runs full-duplex (a reader pump feeds inbound frames to the
  app's `receive` while `send` writes outbound frames). A `websocket.close` sent
  before `websocket.accept` becomes an HTTP `403`, per the ASGI contract.
- **Isolation.** A crashing request handler is contained: it becomes a `500` (when
  no response has started yet) without taking the connection or server down.
- **Connection limits.** `max_pending_connections` is the kernel listen backlog
  (the queue of accepted-by-the-OS-but-not-yet-served connections; when it fills,
  the OS drops or refuses further connection attempts). `max_concurrent_connections`
  caps how many connections are served at once: at the cap the server stops
  *accepting*, so excess connections wait in the kernel queue without a parked task
  or a started TLS handshake, rather than being accepted and blocked. It is built on
  `without`'s `limit_concurrency`.

The pure wire core (`h11_wire`, `ws_wire`) is sans-IO and unit-tested: it maps
`h11`/`wsproto` events to the typed `without-asgi` vocabulary and back, with no
sockets. The `asyncio` shell (`server.py`) is the only part that touches I/O.

## Client

The client is a reusable [aiohttp-style](https://docs.aiohttp.org/en/stable/client_quickstart.html)
session you open once and share, not free `get`/`post` functions:

```python
from without_http import open_session

async with open_session() as session:
    async with session.request("GET", "http://127.0.0.1:8000/items") as response:
        assert response.status == 200
        body = response.body
```

The session is the home for default headers, the middleware stack, and (later) the
connection pool. v1 opens a connection per request; pooling is a contained
follow-up behind this same surface.

### Client middleware

A client *exchange* (`ClientRequest -> ClientResponse`) is the dual of a server
handler, so the **same** `Middleware` vocabulary and `stack` that wrap server
handlers wrap client exchanges. Two are shipped:

```python
from without_http import Session, default_headers, follow_redirects
from without_asgi.routing import stack

session = Session(middleware=stack(
    default_headers((b"authorization", b"Bearer ...")),
    follow_redirects(max_hops=5),
))
```

A `ClientMiddleware` is `(state, inner_exchange, request) -> exchange`, the request
playing the role the scope plays server-side.

## Deferred: HTTP/2

The first cut is HTTP/1.1 + WebSockets, a complete ASGI server. HTTP/2 (`h2`) is a
documented fast-follow: it needs concurrent multiplexed streams with
`WINDOW_UPDATE` flow control and careful lock discipline, so it is its own focused
piece. The design (ALPN plus cleartext prior-knowledge detection, per-stream app
invocations, the same server-direction codecs) is settled in
[`plans/WITHOUT_HTTP.md`](../../plans/WITHOUT_HTTP.md).
