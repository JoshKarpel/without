# without-web

An opinionated HTTP and WebSocket router for
[`without-asgi`](../without-asgi). `without-asgi` deliberately ships *no*
router, only the unopinionated boundary (scope/event parsing) and composition
tools (`Middleware`, `stack`, `wrap`, `buffered`). `without-web` is the
opinionated layer on top: tuple patterns with typed parameters, typed request
extraction, 405-vs-404, mounting, exception handlers, and OpenAPI.

It snaps onto the boundary through nothing but the existing `HttpRouter` type:
`Router.dispatch` *is* an `HttpRouter[T]`, so `make_asgi_app(http=router.dispatch)`
just works, and bring-your-own (or no router at all) stays first-class. Neither
package imports the other's routing opinions.

```python
from without_asgi import make_asgi_app
from without_web import INT, Router, get, json_response, path_param

uid = path_param("id", INT)              # one token: a pattern segment AND a typed read

@get(t"/users/{uid}", uid)               # t-string pattern; `@get` returns a Route value
def show_user(state, user_id: int):      # user_id is an int, no `assert isinstance`
    return json_response(200, {"id": user_id})

router = Router(routes=(show_user,), fallback=not_found)
app = make_asgi_app(lifespan, http=router.dispatch)
```

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
`fn` returns `Reply = Response | Stream[Outbound]`, so a streaming handler
(`async def ... yield`) works directly.

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

`buffered` (the `(state, match, body) -> Response` adapter) and the lower-level
`Endpoint[T, S, H] = (T, Match[S]) -> H` protocol remain for bring-your-own
handlers; `Match` carries the scope plus the already-parsed path params.

## Mounting

`Mount(prefix, target)` composes a sub-application as a value. Two cases:

- a **`without-web` `Router`** is grafted: its routes are prepended with the
  prefix, so matching and OpenAPI see straight through;
- an **opaque `HttpRouter`** (a BYO router or another app) is handed the
  prefix-trimmed scope (ASGI `root_path` semantics) and treated as a black box.

## Exception handlers

`exception_handlers: Mapping[type[Exception], (Exc) -> Response]` is implemented
as a middleware that wraps each endpoint and maps caught exceptions to a
`Response`, reusing the existing `stack`/`wrap`/`compose` machinery rather than
inventing a new mechanism. An unmapped exception propagates.

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

## WebSocket routing

`WebsocketRouter` reuses the same trie machinery with no method layer (so no
405): a connection either matches a path or falls to the `fallback`. Its
`dispatch` is a `WebsocketRouter[T]` for `make_asgi_app(websocket=...)`.

`@ws(pattern, *extractors)` is the websocket sibling of `@get`/`@post`: it ties
typed `path_param`/`query_param`/`header_param` tokens to the handler's arguments
and returns a `WebsocketRoute`. There is no body to buffer (a handshake carries
none, so a `body` extractor is rejected), and the handler returns a
`WebsocketHandler` (the frame processor) rather than a `Response`.

The `integration` package's `todos` example is built entirely on this package;
see it for a worked HTTP + WebSocket service.
