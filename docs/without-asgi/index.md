# without-asgi

`without` adapters that turn an [ASGI](https://asgi.readthedocs.io/) application's
`receive`/`send` into typed event streams and back. This package is *only* the
boundary: it parses raw ASGI
event dicts into typed values, encodes typed values back into the dicts a server
expects, and exposes `receive` as a `Stream` and `send` as a `Sink`. Routing,
middleware, and handlers are left to the application, which hooks processors
together in its own code. The one piece of protocol the adapter does drive is
lifespan (see `make_asgi_app` below), because that is boundary work, not app
policy. See the [`without_asgi` API reference](../without-asgi/reference.md) for
the full surface.

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
ends on the final body chunk or a disconnect. A handler that wants the whole body
folds that stream with `read_body`, which joins the `RequestBody` chunks and
raises `ClientDisconnect` if the client drops before the final one. `scope`
(method, path) is known once up front, so routing is an ordinary `scope ->
Processor` choice rather than a per-event stream split.

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
never the raw callables or the lifespan scope. Each protocol's router defaults to
one that refuses the connection, so an app serves a protocol only by passing its
own router to override the default; an unserved HTTP scope gets a `501 Not
Implemented` and an unserved WebSocket scope is closed before `accept` (a `403`).
The manual wiring shown above is the drill-under path for a handler that needs the
raw `receive`/`send`.

The `Lifespan` names no ASGI types on purpose, so the same value drives a non-ASGI
shell (a queue processor, a CLI, a test) unchanged; only the wrapper differs.
Interdependent resources compose inside the lifespan with nested `async with`,
which also orders teardown.

Writing a router is opinionated work (what a route matches on, how dispatch falls
back), so this package ships no router. The optional `without_asgi.routing`
submodule provides only the unopinionated tools you assemble one from: a
`Middleware` vocabulary, generic over the connection state `T`, the protocol's
handler, and scope (with `HttpMiddleware[T]` / `WebsocketMiddleware[T]` aliases),
so a middleware wraps a handler with the lifespan state and scope in hand
(`(T, handler, scope) -> handler`); state leads so a cross-cutting middleware can
read the same `T` the handler sees, while one that does not need it ignores the
argument; `stack`, which composes a sequence of middleware into one
(first outermost), so a stack of middleware is itself a `Middleware`; `wrap`, which
builds a middleware from scope-aware inbound and/or outbound stream transformers
(composing them around the handler, so a logging or header middleware is a
one-liner; `wrap` is the scope-only end, so its product ignores `T`); and
`buffered`, which adapts a `(state, scope, body) -> Response`
function into the `HttpRouter` shape for the common request/response case (it reads
as a decorator).
The `integration` package's `transform.router` shows a small
protocol-generic `Router` built from these, dispatching both an HTTP and a
WebSocket route.

For a full, opinionated router you don't have to hand-roll, the sibling
[`without-web`](../without-web/index.md) package provides trie matching with typed path
parameters, 405-vs-404, mounting, scoped middleware, exception handlers, and
OpenAPI. It snaps onto this boundary through nothing but the `HttpRouter` type
(`Router.dispatch` *is* one), so adopting it is opt-in and bring-your-own stays
first-class. The `integration` package's `todos` example is
built on it.

A `Middleware` wraps the whole handler, a `Processor[Inbound, Outbound]`, so it can
transform the inbound stream, the outbound stream, or both. The body is not a
special thing to reach for; it is the `RequestBody` events on the inbound stream
(and `ResponseBody` events on the way out), so a middleware that touches the body
just transforms those events before or after the inner handler. Two shapes:

- **Per-chunk**, which stays streaming: wrap `inputs` and re-`yield` each
  `RequestBody` with its `body` transformed and its `more_body` preserved, passing
  `Disconnect` through. The inner handler still receives the body incrementally.
- **Whole-body**, which buffers: `await read_body(inputs)` to join the chunks (it
  raises `ClientDisconnect` on a truncated body), do the work, then `yield` one
  `RequestBody(body=..., more_body=False)`. The inner handler sees a complete body
  in a single event and cannot tell it was re-synthesized; the tradeoff is that
  buffering forecloses streaming in the handler. The response body is symmetric:
  wrap the outbound stream and transform its `ResponseBody` events, the way the
  `transform` example's header middleware rewrites `ResponseStart`.

The pure half (`parse_http_scope`, `parse_inbound`, `encode_outbound`,
`encode_response`, and the lifespan equivalents) is sans-IO and tested without a
socket: build a `scope`, a scripted `receive`, and a capturing `send`, then call
`app` directly. See the `integration` package for a worked
text-transform service that reads the request body and dynamic config from a
`without-configmap` `Context`.

## The codec runs both directions

Everything above is the *app* side of the boundary: parse the dicts an ASGI
server hands an app (`parse_*`), encode the typed values the app sends back
(`encode_outbound`, `encode_lifespan_reply`). The vocabulary is also complete in
the *server* direction, which is what a transport provider needs to drive an app:

- `encode_scope` (and `encode_http_scope` / `encode_websocket_scope`) renders a
  typed scope into the dict an app expects, the dual of `parse_scope`.
- `encode_inbound` / `encode_websocket_inbound` / `encode_lifespan_event` build
  the dicts an app's `receive` returns, the duals of the `parse_*` events.
- `parse_outbound` / `parse_websocket_outbound` / `parse_lifespan_reply` classify
  the dicts an app passes to `send`, the duals of the `encode_*` reply encoders.

So the same typed vocabulary parses and encodes in both directions, and a server
that owns the wire can work in typed values at the boundary rather than raw dicts.
The sibling [`without-http`](../without-http/index.md) package is exactly that: an ASGI
server built on `h11`/`h2`/`wsproto` that uses these server-direction codecs to
talk ASGI to any app, `make_asgi_app`-built or third-party.
