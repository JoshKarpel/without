# without-web

An opinionated HTTP and WebSocket router for
[`without-asgi`](../without-asgi/index.md). `without-asgi` deliberately ships *no*
router, only the unopinionated boundary (scope/event parsing) and composition
tools (`Middleware`, `stack`, `wrap`, `buffered`). `without-web` is the
opinionated layer on top: tuple patterns with typed parameters, typed request
extraction, 405-vs-404, mounting, scoped middleware, exception handlers, reverse
routing (`url_for`), and OpenAPI. See the
[`without_web` API reference](../without-web/reference.md) for the full surface.

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

The serializer stays the application's choice, so `without-web` decides nothing about
encoding: a handler returns a `Response` (status, headers, bytes) however it likes, the
same stance the router takes toward schemas (`schema_for` is injected). What a handler
does *not* have to re-derive is the pairing of encoded bytes with the `content-type`
naming them, which lives one layer down as
[`json_content` and `Response.from_content`](../without-asgi/index.md#content-a-body-and-what-it-is)
with the encoder still an argument.

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
converter, schema, and parsed type are declared exactly once. A plain string is
taken verbatim as a literal path, so a path parameter requires the t-string form.

## Layering: each layer owns the schema it parses

Whichever layer parses or produces a value is the single source of truth for
that value's schema. The router owns method, path, and path params; the handler
owns query, request body, and responses, each carried by the extractor that
parses it. OpenAPI is a *merge* of those self-descriptions, not a blob declared
in one place (see [OpenAPI](#openapi-recover-a-description-from-structure)).

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
- **Delegation** to an opaque sub-app is a black-box leaf at a prefix node.
  (Transparent mounting isn't a router concern at all: `mount(...)` bakes the
  prefix into the routes before they reach the router, see [Mounting](#mounting).)

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

An `Extractor[C, V]` is parsing-as-a-value: a pure `C -> V` (from the request
context `C` it reads) paired with the OpenAPI fragment it contributes.
`path_param`, `query_param`, `header_param`, `body`, `catch_all`, `http_scope`,
and `websocket_scope` build them. An extractor that raises *rejects* the request,
mapped to a 4xx by the exception handlers; it never decides which handler runs.
A rejecting `parse` (a `ValueError`) becomes an `ExtractionError` the extractor
raises with everything a `recover` policy needs gathered at the raise site: the
`field` it came from and the underlying error as a first-class `cause`, so a
policy answers `case ExtractionError(cause=ValidationError())` with a 422 and
`case ExtractionError()` with a 400 naming the `field`. Because that boundary is
one matchable type, a plain `ValueError` raised deeper in a handler stays a 500
instead of masquerading as a client 400. `http_scope()`/`websocket_scope()` hand
back the unparsed scope, so "pass the scope down" and "parse parts of it" compose
instead of competing.

The context `C` is what makes the wrong extractor on the wrong route a *static*
error rather than a runtime guard. It is a small lattice of request-head types:
the permissive `RequestHead` (`scope` is `HttpScope | WebsocketScope`), which
`path_param`/`query_param`/`header_param`/`catch_all` read, so those tokens serve
any route; `HttpRequestHead` (`scope` narrowed to `HttpScope`), which `http_scope`
needs; `WebsocketRequestHead` (`scope` is `WebsocketScope`), which `websocket_scope`
needs; and `BufferedRequest` (an `HttpRequestHead` plus the buffered `body`), which
`body` needs. Because an extractor is *contravariant* in `C`, a permissive
`Extractor[RequestHead, V]` slots into any handler, while a `body` token
(`Extractor[BufferedRequest, V]`) on a streaming or websocket route, or an
`http_scope` on a websocket route, is a mypy error at the call, with no runtime
check to fail later.

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
scope-only (`path_param`/`query_param`/`header_param`/`http_scope`, whose context
is the streaming route's `HttpRequestHead`); a `body` extractor is a static type
error, since its `BufferedRequest` context is exactly the buffering a streaming
route avoids. The output is free here too (yield to stream, return or await a
`Response` to buffer), so the **input/output 2×2** is fully covered: input
buffering is the
one build-time axis (`handle` vs `handle_stream`), output is always the handler's
return. The inbound stream is deliberately *not* an extractor: an `Extractor`
reads the parsed-once `RequestHead` *value*, and a live stream is a consume-once
*place*, so it is passed as an argument rather than smuggled into `RequestHead`.

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
trailing argument, and a `body` extractor is a static type error. See `integration.todos`'
`POST /todos/import`, which folds a newline-delimited stream into the list as it
arrives, acknowledging each line while later chunks are still in flight.

`buffered` (the `(state, match, body) -> Response` adapter) and the lower-level
`Endpoint[T, S, H] = (T, Match[S]) -> H` protocol remain for bring-your-own
handlers; `Match` carries the scope plus the already-parsed path params.

## Mounting

Mounting a sub-application has two genuinely different cases, and `without-web`
keeps them apart rather than folding both into one `Mount` object.

**Transparent — routes you own.** `mount(prefix, *middleware)` returns a *transform*
that bakes the prefix (and any per-route middleware) into each route you hand it,
returning plain routes whose path *already includes the prefix*:

```python
api = mount("/api", require_auth)          # a reusable mount point

routes = api(list_users, create_user)      # -> rebased routes, /api/... behind auth

@mount("/api")                             # or as a decorator on one route
@get(t"/users/{uid}", uid)
async def show_user(...): ...
```

Because the prefix is baked into the route's segments, a mounted route is still a
first-class, self-contained value: there is no `Mount` wrapper in the router,
matching and OpenAPI see the full path directly, and reverse routing (`url_for`)
needs no router and cannot lose the prefix (this is what dissolves the old
cross-router footgun, see [Reverse routing](#reverse-routing-url_for)). Nesting
composes: `mount("/api")(mount("/v1")(route))` is `/api/v1/...`. This is also the
clean shape for *distributing* a route group: a package ships a factory
`build(prefix, ...)` that constructs its mounted, interlinked routes together, and
the consumer picks the prefix.

**Opaque — a sub-app whose routes you cannot see.** `delegate(prefix, app)` mounts
a bring-your-own `HttpRouter` (a legacy app, a third-party mount) as a black box:
its routes cannot be baked, so it is handed the prefix-trimmed scope (ASGI
`root_path` semantics) and left alone. A `delegate` can itself be rebased by
`mount` (`mount("/admin")(delegate("/legacy", app))` sits at `/admin/legacy`), so a
nested opaque app is trimmed by its full accumulated path.

`ws_mount` and `ws_delegate` are the exact WebSocket siblings (see
[WebSocket routing](#websocket-routing)).

## Middleware: router-wide, per-prefix, or per-route

A `Router`'s `middleware` runs on every dispatch. To scope middleware to *part* of
an app, two composing tools, no new mechanism:

- **A prefix:** hand middleware to `mount(prefix, *middleware)` (see Mounting). It
  is baked onto every route placed under the mount and nothing outside it. This is
  the natural home for cross-cutting concerns like auth on `/admin`. See
  `integration.todos`, whose `admin = mount("/admin", require_authorization)`
  carries an `Authorization`-header gate. (Mount middleware is state-agnostic
  `HttpMiddleware[object]`, which auth and logging already are; state-specific
  middleware goes on a route with `with_middleware`.)
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

## Reverse routing: `url_for`

`url_for(route, values)` is the inverse of dispatch: given a route and the values
for its path parameters, it renders the concrete path the route would match at, so
a handler or template links to a route by its *value* rather than hand-assembling a
string that drifts when the path changes. It is a **plain function** of the route
value, no router involved:

```python
show_user = get(t"/users/{uid}", uid)   # a Route value you hold

@post("/users", new_user_body)
async def create_user(state, new):
    location = url_for(show_user, {"id": created.id})   # -> "/users/<id>"
    return Response(status=201, headers=((b"location", location.encode()),), body=...)
```

The handler references the `show_user` *value* (immutable, like a constant) and
calls a free function: it names no router, so there is no app-level singleton to
depend on and no runtime handler→router cycle. This works because a route is a
**self-contained value**: `mount` bakes any prefix into the route's segments (see
[Mounting](#mounting)), so the route's own path *is* its full path. Three things
follow:

- **Identity is the value, not a name.** You pass the route you already hold; there
  is no registry of names to keep in sync (the same stance the trie takes, see
  [Matching](#matching-a-radix-tree-trie)).
- **No router, no cross-router footgun.** Because the prefix lives *in the route*,
  not in a router, reversing needs no router and cannot miss a prefix that some
  other router knew about. A websocket handler reverses an HTTP `Route` to link to
  its resource with the same call, since neither is tied to a router.
- **Values are checked in reverse.** Each value is rendered and fed back through
  the segment's converter to prove it would parse straight back (parse, don't
  validate, in reverse): a value the converter would reject, one that does not
  round-trip, or one that spans multiple segments (a `/` in a single-segment
  param) raises, as does a missing or unknown parameter. A `catch_all` segment
  is the one place `/` is allowed. `url_for` reverses a `WebsocketRoute` the same
  way.

See `integration.todos`: `create_todo`'s `201` body carries the new todo's URL,
reversed from the `show_todo` route, and the `/todos/session` websocket puts that
*same* reversed URL in each reply, an HTTP route linked from a websocket handler
with nothing but the route value.

## WebSocket routing

`WebsocketRouter` reuses the same trie machinery with no method layer (so no
405): a connection either matches a path or falls to the `fallback`. Its
`dispatch` is a `WebsocketRouter[T]` for `make_asgi_app(websocket=...)`. Mounting
mirrors HTTP: `ws_mount(prefix, *middleware)` bakes a prefix into WebSocket routes
and `ws_delegate(prefix, app)` mounts an opaque WebSocket app as a black box (see
[Mounting](#mounting)).

`@ws(pattern, *extractors)` is the websocket sibling of `@get`/`@post`: it ties
typed `path_param`/`query_param`/`header_param` tokens to the handler's arguments
and returns a `WebsocketRoute`. Its context is `WebsocketRequestHead`, so a `body`
(or `http_scope`) token is a static type error: a handshake carries no body, and
its scope is a `WebsocketScope`, not an `HttpScope`. The handler *is* the frame
processor,
exactly as `@post.stream`'s is: it takes the live inbound frames as a trailing
`Stream[WebsocketInbound]` argument and yields `WebsocketOutbound` directly,
rather than returning a processor. Because the handler is the processor, a
websocket connection folds naturally: see `integration.todos`' `/todos/session`,
which threads a working `TodoList` across its inbound frames (a *scan* over the
connection), the bidirectional, long-lived sibling of the `POST /todos/import`
fold.

The `integration` package's `todos` example is built entirely
on this package; see it for a worked HTTP + WebSocket service.
