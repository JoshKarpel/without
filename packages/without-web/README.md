# without-web

An opinionated HTTP and WebSocket router for
[`without-asgi`](../without-asgi). `without-asgi` deliberately ships *no*
router, only the unopinionated boundary (scope/event parsing) and composition
tools (`Middleware`, `stack`, `wrap`, `buffered`). `without-web` is the
opinionated layer on top: tuple patterns with typed parameters, typed request
extraction, 405-vs-404, mounting, scoped middleware, exception handlers, and
OpenAPI.

It snaps onto the boundary through nothing but the existing `HttpRouter` type:
`Router.dispatch` *is* an `HttpRouter[T]`, so `make_asgi_app(http=router.dispatch)`
just works, and bring-your-own (or no router at all) stays first-class. Neither
package imports the other's routing opinions.

```python
import json

from without_asgi import Response, make_asgi_app
from without_web import INT, Router, get, path_param

uid = path_param("id", INT)              # one token: a pattern segment AND a typed read

@get(t"/users/{uid}", uid)               # t-string pattern; `@get` returns a Route value
async def show_user(state, user_id: int):    # user_id is an int, no `assert isinstance`
    body = json.dumps({"id": user_id}).encode()
    return Response(status=200, headers=((b"content-type", b"application/json"),), body=body)

router = Router(routes=(show_user,), fallback=not_found)
app = make_asgi_app(lifespan, http=router.dispatch)
```

Encoding (the serializer, its options, the content type) is the application's
choice, so `without-web` ships no `json_response`-style helper: a handler returns
a `Response` (status, headers, bytes) however it likes. The same stance the
router takes toward schemas (`schema_for` is injected) it takes toward response
bodies.

## A router dispatches on the scope, never the body

`make_asgi_app` types the router as `(T, HttpScope) -> HttpHandler`. The router
never sees the inbound stream, so it cannot parse a body it never reads.
Dispatch is therefore a total, pure function of `method` + `path`. Everything
else (query, headers, body) may be *parsed* by an endpoint, but parsing it does
not change *which* handler is selected.

Path parameters are the one thing the router must own: only the route pattern
knows that `/users/42` binds `id=42`.

## Patterns: literal strings and t-strings

A route pattern is either a plain string for a **literal-only** path
(`@get("/todos")`, split into `Literal` segments) or a **t-string** (PEP 750,
Python 3.14) interpolating path-param tokens (`@get(t"/todos/{todo_id}", ...)`,
where `todo_id` is a `path_param(...)`/`catch_all(...)` value). A parameter must
occupy a whole segment.

The t-string carries the path *structure* for matching, but a `Template` erases
its interpolation types, so the token's type is recovered from the handler's
positional extractor list: the *same* `path_param(...)` value is interpolated
into the pattern (where it is the segment, matched and schemed through its
converter) *and* passed alongside (where it is the typed read), so the name,
converter, schema, and parsed type are declared exactly once. A brace in a
*plain* string is a build error pointing you to the t-string form rather than a
route that silently never matches.

## Layering: each layer owns the schema it parses

Whichever layer parses or produces a value is the single source of truth for
that value's schema. The router owns method, path, and path params; the handler
owns query, request body, and responses, each carried by the extractor that
parses it. OpenAPI is a *merge* of those self-descriptions, not a blob declared
in one place (see [`openapi`](#openapi-recover-a-description-from-structure)).

## Matching: a radix tree (trie)

The pattern's literal parts are split on `/` and folded into one immutable trie
whose nodes map a segment (a literal, a typed param slot, or a `catch_all`) to
child nodes, terminating in a `{method -> endpoint}` map. Matching is a pure
walk. Three things fall out of the structure rather than being authored by hand:

- **Precedence.** At each node, literal children beat typed-param children beat
  the catch-all. No "order your routes carefully" footgun.
- **405-vs-404.** Land on a node whose method map lacks the request method → a
  `405` with an `Allow` header. Dead-end the walk → the `fallback` (typically a
  `404`). This is the distinction an exact `method == m and path == p` router
  cannot express.
- **Mounting** is a subtree graft at a prefix node.

The one subtlety: a converter can *reject* (`INT` against `abc`), so the walk
backtracks to try sibling branches when a converter rejects, with a defined
resolution order (literal, then typed params, then catch-all).

### Converters parse, they don't validate

A `Converter` is a `str -> value` parser paired with the JSON Schema it parses
into, a plain value carrying its own `name`. The built-ins are exported as
values: `STR`, `INT`, `FLOAT`, `UUID`, and `PATH` (the catch-all). A converter
that raises `ValueError` *rejects* the segment, which makes that branch fail to
match and the walk backtrack (ultimately a 404), never a handler-side error. A
token carries its converter value straight into the trie, so an app adds its own
by constructing a `Converter` and using it in a `path_param`; there is no
registry to register it in.

## Reading the request: extractors

An `Extractor[V]` is parsing-as-a-value: a pure `Request -> V` paired with the
OpenAPI fragment it contributes. `path_param`, `query_param`, `header_param`,
`body`, `catch_all`, `http_scope`, and `websocket_scope` build them. An extractor
that raises *rejects* the request, mapped to a 4xx by the exception handlers; it
never decides which handler runs. `http_scope()`/`websocket_scope()` hand back
the unparsed scope, so "pass the scope down" and "parse parts of it" compose
instead of competing. The same `query_param`/`header_param`/`path_param` tokens
serve both HTTP and websocket handlers (`Request.scope` is
`HttpScope | WebsocketScope`).

`handle(*extractors, fn=...)` ties the extractor types to `fn`'s parameters via
an overload ladder, so a `path_param("id", INT)` paired with an `fn` that expects
a `str` is a mypy error, with no runtime introspection. It buffers the request
*input* (so a `body` extractor can read it) but does not force buffered *output*:
`fn` is always `async` (a handler must be able to `await` I/O), and may be an
`async def` that returns a `Response` (buffered, after async work) or an
`async def ... yield` that streams `Outbound` events. A single `_emit` dispatch
relays whichever, so the output mode is just what the handler hands back. A plain
`def ... return Response` is a type error: there would be no place to `await`.

`handle_stream(*extractors, fn=...)` is the streaming-*input* sibling: it leaves
the inbound stream untouched and hands it to `fn` as a trailing `Stream[Inbound]`
argument, so `fn` *is* the processor (no inner function), reading the live stream
as it arrives (a streaming upload, a long poll, a loop driven by request chunks).
The same overload ladder ties the extractor types, but the extractors are
scope-only (`path_param`/`query_param`/`header_param`/`http_scope`); a `body`
extractor is rejected, since buffering the body is exactly what a streaming route
avoids. The output is free here too (yield to stream, return or await a `Response`
to buffer), so the **input/output 2×2** is fully covered: input buffering is the
one build-time axis (`handle` vs `handle_stream`), output is always the handler's
return. The inbound stream is deliberately *not* an extractor: an `Extractor`
reads the parsed-once `Request` *value*, and a live stream is a consume-once
*place*, so it is passed as an argument rather than smuggled into `Request`.

`into(make, *extractors)` combines extractors into one that builds a typed value,
the escape hatch from the per-handler arity ceiling and the way to parse a group
of inputs into one model. Each extractor supplies one positional argument to
`make` (a constructor or factory), with the types tied the same way; the
constituents' OpenAPI fragments are carried through. It reuses the existing
tokens rather than re-reading the request. A frozen dataclass or `NamedTuple`
constructor works directly; a pydantic model (keyword-only init plus validators)
is wrapped in a small factory (`into(lambda a, b: M(x=a, y=b), ea, eb)`), so a
rejecting validator raises for the exception handlers to map.

The method decorators `get`/`post`/`put`/`patch`/`delete`/`head`/`options` are
`handle` plus a method and a pattern. `@get(pattern, *extractors)` co-locates the
route with the handler and **returns a `Route` value**: it registers nothing, so
assembly stays the explicit, declarative `Router(routes=(...))`. The `Router`
merges `Route`s that share a pattern, so `@get` and `@post` on one path combine
into a single method map.

Each method decorator carries a `.stream` form for streaming input:
`@post.stream(pattern, *extractors)` is to `handle_stream` what `@post` is to
`handle`. The handler *is* the processor, taking the live inbound stream as its
trailing argument, and a `body` extractor is rejected. See `integration.todos`'
`POST /todos/import`, which folds a newline-delimited stream into the list as it
arrives, acknowledging each line while later chunks are still in flight.

`buffered` (the `(state, match, body) -> Response` adapter) and the lower-level
`Endpoint[T, S, H] = (T, Match[S]) -> H` protocol remain for bring-your-own
handlers; `Match` carries the scope plus the already-parsed path params.

## Mounting

`Mount(prefix, target)` composes a sub-application as a value. Two cases:

- a **`without-web` `Router`** is grafted: its routes are prepended with the
  prefix, so matching and OpenAPI see straight through, and the sub-router's own
  `middleware` is carried onto each grafted route (so middleware on a mounted
  router applies to its whole subtree, and nowhere else);
- an **opaque `HttpRouter`** (a BYO router or another app) is handed the
  prefix-trimmed scope (ASGI `root_path` semantics) and treated as a black box.

## Middleware: router-wide, per-subtree, or per-route

A `Router`'s `middleware` runs on every dispatch. To scope middleware to *part* of
an app, two composing tools, no new mechanism:

- **A subtree:** give a mounted sub-`Router` its own `middleware` (see Mounting).
  It applies to every route under the mount and nothing outside it. This is the
  natural home for cross-cutting concerns like auth on `/admin`. See
  `integration.todos`, whose `admin` mount carries an `Authorization`-header gate.
- **One route:** `with_middleware(endpoint, *middleware)` wraps a single endpoint.
  An `Endpoint` builds the handler and a `Middleware` is `(T, H, S) -> H`, so this
  is just composition; the result is a narrower `Endpoint`. Use it per method, e.g.
  `route("/admin", get=with_middleware(list_admins, require_auth))`.

Both reuse the same `Middleware` vocabulary (`stack`, `wrap`, a plain
`(T, handler, scope) -> handler`) as the router-wide hook, including the ability to
read connection state and to replace the handler outright (a 401 without calling
the wrapped endpoint).

## Exception handlers

Exception handling is not a new mechanism: it is a `Middleware`. `catching(recover)`
wraps an endpoint, watches its outbound stream, and turns a raised exception into a
`Response`, reusing the existing `stack`/`wrap`/`compose` machinery. There is *no
registry*: `recover` is the app's policy, an ordinary
`(Exception) -> Awaitable[Response | None]` function. Write it as `match exc:` and
each case narrows to its real type (no `assert isinstance`, no cast); return `None`
to let the exception propagate.

```python
async def recover(exc: Exception) -> Response | None:
    match exc:
        case TodoNotFound():
            return Response(status=404, ...)   # exc is TodoNotFound here
        case _:
            return None                        # propagate

router = Router(routes=(...), middleware=stack(catching(recover)))
```

Because it is plain middleware, the app controls where it sits in the stack (an
outer `catching` handles what an inner one returns `None` for) and `recover` has
full control: re-raise, chain, or do async work. `catching_websocket` is the
sibling that maps to a `WebsocketClose`.

Honest limitation: once a `ResponseStart` has been emitted, the status line is
on the wire and cannot be rewritten, so the mapping applies only to exceptions
raised *before the first outbound event*. After that the exception re-raises.

## OpenAPI: recover a description from structure

`openapi(router, schema_for=...)` is a pure transform of the route table. For
each route it merges the router's half (path, methods, path-param schemas, which
it reads from the converters on the segments) with the endpoint's half. The
endpoint's half is recovered from the *extractors* that parse it: each
`query_param`/`header_param`/`body` carries its own OpenAPI fragment, so the
schema is declared in exactly the one place it is parsed, and `handle`/`@get`
assemble those into a self-describing endpoint.

Turning a captured type into JSON Schema is an injected concern, so `without-web`
stays schema-library agnostic: an extractor or `RouteSpec` may carry an
already-built JSON Schema mapping, or a type plus a `schema_for: type -> schema`
function the app supplies (pydantic's `model_json_schema`, or a dataclass
walker).

A request or response body is a `Body(media_type, shape)`. The shape is
`Single(schema)` for one whole document or `Sequence(item_schema)` for a
sequential media type (NDJSON, SSE `text/event-stream`, `application/json-seq`,
...): `Single` renders OpenAPI's `schema`, `Sequence` renders 3.2's `itemSchema`
(one item's shape), and the document is emitted as `3.2.0`. This is
*documentation only*: `without-web` is agnostic to the framing on the wire, the
media type is the app's string, and the handler emits the bytes. A
streaming-input route has no `body` extractor to recover an inbound schema from,
so it declares one directly: `@post.stream(..., request_body=Body(
"application/x-ndjson", Sequence(...)))`.

## WebSocket routing

`WebsocketRouter` reuses the same trie machinery with no method layer (so no
405): a connection either matches a path or falls to the `fallback`. Its
`dispatch` is a `WebsocketRouter[T]` for `make_asgi_app(websocket=...)`.

`@ws(pattern, *extractors)` is the websocket sibling of `@get`/`@post`: it ties
typed `path_param`/`query_param`/`header_param` tokens to the handler's arguments
and returns a `WebsocketRoute`. There is no body to buffer (a handshake carries
none, so a `body` extractor is rejected). The handler *is* the frame processor,
exactly as `@post.stream`'s is: it takes the live inbound frames as a trailing
`Stream[WebsocketInbound]` argument and yields `WebsocketOutbound` directly,
rather than returning a processor. Because the handler is the processor, a
websocket connection folds naturally: see `integration.todos`' `/todos/session`,
which threads a working `TodoList` across its inbound frames (a *scan* over the
connection), the bidirectional, long-lived sibling of the `POST /todos/import`
fold.

The `integration` package's `todos` example is built entirely on this package;
see it for a worked HTTP + WebSocket service.
