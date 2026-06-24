# without-asgi

`without` adapters that turn an ASGI application's `receive`/`send` into typed
event streams and back. This package is *only* the boundary: it parses raw ASGI
event dicts into typed values, encodes typed values back into the dicts a server
expects, and exposes `receive` as a `Stream` and `send` as a `Sink`. Routing,
middleware, and handlers are left to the application, which hooks processors
together in its own code. The one piece of protocol the adapter does drive is
lifespan (see `make_asgi_app` below), because that is boundary work, not app
policy.

An ASGI app is `async def app(scope, receive, send)`. The adapters let the body
of that callable read as plain `without` wiring:

```python
from without_asgi import http_inbound, http_outbound, parse_http_scope

async def app(scope, receive, send):
    head = parse_http_scope(scope)
    handler = select(head)                          # your routing, your processor
    outbound = handler(http_inbound(receive))       # Stream[Inbound] -> Stream[Outbound]
    await http_outbound(send)(outbound)             # drive ASGI send
```

Because `receive` is already pull-based, `http_inbound` is a plain async
generator (no queue): the request's lifecycle *is* the stream's lifecycle, so it
ends on the final body chunk or a disconnect. `scope` (method, path) is known
once up front, so routing is an ordinary `scope -> Processor` choice rather than
a per-event stream split.

`make_asgi_app(lifespan, http=..., websocket=...)` builds the ASGI app: it drives
the lifespan protocol around a portable `Lifespan[T] = () ->
AbstractAsyncContextManager[T]`, setting the value up on `startup`, tearing it
down on `shutdown` (reporting setup/teardown errors as `lifespan.startup.failed` /
`lifespan.shutdown.failed`). Each connection scope is parsed into a typed
`HttpScope` / `WebsocketScope` and passed to that protocol's *router* with the
value threaded in: an `HttpRouter[T] = (T, HttpScope) -> Processor[Inbound,
Outbound]` (and the websocket equivalent) selects the `Processor` that serves the
connection. `make_asgi_app` then owns the receive/send wiring around it: it wraps
`receive` into the inbound stream, runs the returned `Processor`, and drains its
outbound stream into `send`, so a router and its handler only ever see streams,
never the raw callables or the lifespan scope. Both routers are optional, so an
app serves only what it provides; a connection whose router is absent is rejected
with `NotImplementedError`. The manual wiring shown above is the drill-under path
for a handler that needs the raw `receive`/`send`.

The `Lifespan` names no ASGI types on purpose, so the same value drives a non-ASGI
shell (a queue processor, a CLI, a test) unchanged; only the wrapper differs.
Interdependent resources compose inside the lifespan with nested `async with`,
which also orders teardown.

The pure half (`parse_http_scope`, `parse_inbound`, `encode_outbound`,
`encode_response`, and the lifespan equivalents) is sans-IO and tested without a
socket: build a `scope`, a scripted `receive`, and a capturing `send`, then call
`app` directly. See the `integration` package for a worked feature-flag service
that reads dynamic config from a `without-configmap` `Context`.
