# without-asgi

`without` adapters that turn an [ASGI](https://asgi.readthedocs.io/) application's
`receive`/`send` into typed event streams and back. This package is *only* the
boundary: it parses raw ASGI
event dicts into typed values, encodes typed values back into the dicts a server
expects, and exposes `receive` as a `Stream` and `send` as a `Sink`. Routing and
handlers are left to the application, and so is middleware apart from the few
pieces that need nothing but this vocabulary and therefore work under any router
and any server (concurrency and body-size limits, and negotiated response
compression). The one piece of protocol
the adapter does drive is lifespan, because that is boundary work, not app policy.

An ASGI app is `async def app(scope, receive, send)`. The adapters let the body
of that callable read as plain `without` wiring:

```python
from without_asgi import http_inbound, http_outbound, parse_http_scope


async def app(scope, receive, send):
    head = parse_http_scope(scope)
    handler = select(head)  # your routing, your processor
    outbound = handler(http_inbound(receive))  # Stream[Inbound] -> Stream[Outbound]
    await http_outbound(send)(outbound)  # drive ASGI send
```

`make_asgi_app(lifespan, http=..., websocket=...)` builds the ASGI app, driving
the lifespan protocol and wiring each connection's `receive`/`send` around the
`Processor` a router selects. The same typed vocabulary parses and encodes in
*both* directions, so a transport that owns the wire (like
[`without-http`](../without-http)) can talk ASGI to any app in typed values; and
the optional `without_asgi.routing` submodule ships the unopinionated
`Middleware` / `stack` / `wrap` / `buffered` tools you assemble a router from,
with `without_asgi.compression` holding the negotiated response compression built
on them.

For a full, opinionated router you don't have to hand-roll, the sibling
[`without-web`](../without-web) package snaps onto this boundary through nothing
but the `HttpRouter` type.

See the
[`without-asgi` guide](https://without.help/without-asgi/)
(with the [API reference](https://without.help/reference/without_asgi/))
for the full surface, including the codec's server direction and the middleware
body shapes.
