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
    handler = select(head)  # your routing, your processor
    outbound = handler(http_inbound(receive))  # Stream[Inbound] -> Stream[Outbound]
    await http_outbound(send)(outbound)  # drive ASGI send
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
never the raw callables or the lifespan scope. It closes the inbound stream when
the handler exits (via `aclosing`), so a handler that abandons the request body
early has its `finally` run deterministically rather than leaving the generator
dangling for GC: the server-side mirror of the client folding connection release
into its response-body generator. Each protocol's router defaults to
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

## `Content`: a body and what it is

Encoding a value produces two things that have to travel together, the bytes and the
`content-type` naming them, and separating them is the same mistake at every call site.
`Content` pairs them, and carries no policy of its own:

```python
from without_asgi import Response, json_content

Response.from_content(201, json_content(todo))
```

`json_content(payload, *, dumps=...)` is one producer of a `Content`; a form, text, or
msgpack encoder is another, with equal standing. The *encoder* stays an argument, so an
app that needs sorted keys, a faster library, or one that knows its domain types passes
its own and changes nothing else:

```python
json_content(todo, dumps=lambda value: json.dumps(value, sort_keys=True))
```

The stdlib is the default because a default should add no dependency, and it is strict
where JSON is (`allow_nan=False`, so a `NaN` fails at the sender rather than at whoever
parses the response). Key order is left alone: sorting is a policy some callers want and
a cost every response would pay.

A `Content` describes what its bytes *are*; *transforming* an exchange (compressing a
request body, decoding a response) is middleware's job, in `without-http`'s client
vocabulary, where it streams instead of buffering and applies at whatever scope the
composition happens. A caller who wants one compressed request decorates the client
inline for that call (`gzip_compress()(client)`) rather than wrapping the value.

`Response.from_content(status, content, *, headers=())` layers the caller's headers over
the ones the content described, so a handler answering `application/problem+json` over a
JSON body says so there rather than rebuilding the body. The same value is what
`without-http`'s client takes as a request body (`request(client, "POST", url,
body=json_content(order))`), which is the point of it living here rather than in either
package above.

## Streaming a file

`file_response(path)` builds the outbound stream that serves a file: a
`ResponseStart` with `Content-Type` and `Content-Length` filled in, then the
`ResponseBody` chunks. It returns a `Stream[Outbound]`, so it drops straight into
a handler's output (or a `without-web` `Reply`), and `Response` stays the pure
buffered value it is; streaming is the other, already-existing arm rather than an
iterator smuggled into that value.

It exists because the offloaded-file ASGI extensions
(`http.response.pathsend`, `http.response.zerocopysend`) only pay off when the
transport can push bytes *below* Python (a native `sendfile`), which a pure-Python
asyncio server cannot do; rather than advertise an offload it can't honor, the
reusable-but-fiddly work moves here. That work is: guess the content type, compute
the length, and chunk the bytes off the event loop into the `ResponseBody` events
the framework already streams to `send`, so a large file is never slurped into one
`bytes`.

`file_response` is a coroutine, not an async generator, and the ordering is the
point. Awaiting it runs the `stat` up front, so a missing file raises
`FileNotFoundError` *before* any `ResponseStart` is emitted, while nothing is on
the wire yet and a handler can still answer a clean `404`. This is the
parse-don't-validate move, and precisely the wart `http.response.pathsend` cannot
avoid: its path is opened only *after* the status and headers have already been
sent, so a missing file there can only truncate a response that already claimed
`200`.

```python
from pathlib import Path

from without_asgi import Response, file_response


async def download(state, match) -> Reply:
    try:
        return await file_response(Path("/srv/report.pdf"))
    except FileNotFoundError:
        return Response(status=404, body=b"not found\n")
```

`Content-Type` is guessed from the suffix with `mimetypes.guess_file_type`
(falling back to `application/octet-stream`) and overridable with `content_type`;
any `headers` given are prepended, for a `content-disposition` say. The body is
read in `chunk_size` pieces via `asyncio.to_thread`, matching the package's
`pathlib.Path` + thread-offload file-I/O discipline, and the file is closed when
the stream ends or is closed early (`make_asgi_app` closes an abandoned outbound
stream, e.g. on a client disconnect mid-download). A `HEAD` request needs nothing
special: the transport drops the body chunks on the wire. Range requests (`206`)
and conditional requests (`ETag`, `Last-Modified`) are not modeled yet; a caller
that wants them sets the headers and status itself.

Reads and writes are lockstep by default: the next chunk is read only once the
consumer has drained the current one, so disk and socket never overlap. Because
the chunks are an ordinary `Stream`, read-ahead is opt-in composition rather than
a built-in, `spool` from the core:

```python
from without import spool

return spool(await file_response(path), ahead=2)
```

`spool` drives the file reads up to `ahead` chunks ahead of the socket writes
through a bounded queue on a background task, so the next `read` overlaps the
current chunk's send; `ahead` still bounds the memory held and applies
backpressure.

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
