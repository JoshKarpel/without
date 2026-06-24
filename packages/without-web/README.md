# without-web

An opinionated HTTP and WebSocket router for
[`without-asgi`](../without-asgi). `without-asgi` deliberately ships *no*
router, only the unopinionated boundary (scope/event parsing) and composition
tools (`Middleware`, `stack`, `wrap`, `buffered`). `without-web` is the
opinionated layer on top: path patterns with typed parameters, 405-vs-404,
mounting, exception handlers, and OpenAPI.

It snaps onto the boundary through nothing but the existing `HttpRouter` type:
`Router.dispatch` *is* an `HttpRouter[T]`, so `make_asgi_app(http=router.dispatch)`
just works, and bring-your-own (or no router at all) stays first-class. Neither
package imports the other's routing opinions.

```python
from without_asgi import make_asgi_app
from without_web import Router, route, buffered

@buffered
def show_user(state, match, body):           # match.params["id"] is an int
    ...

router = Router(
    routes=(route("/users/{id:int}", get=show_user),),
    fallback=not_found,
)
app = make_asgi_app(lifespan, http=router.dispatch)
```

## A router dispatches on the scope, never the body

`make_asgi_app` types the router as `(T, HttpScope) -> HttpHandler`. The router
never sees the inbound stream, so it cannot parse a body it never reads.
Dispatch is therefore a total, pure function of `method` + `path`. Everything
else (query, headers, body) may be *declared* on an endpoint for typing and
documentation, but declaring it does not change *which* handler is selected.

Path parameters are the one thing the router must own: only the route pattern
knows that `/users/42` binds `id=42`.

## Layering: each layer owns the schema it parses

Whichever layer parses or produces a value is the single source of truth for
that value's schema. The router owns method, path, and path params; the handler
owns query, request body, and responses. OpenAPI is a *merge* of those two
self-descriptions, not a blob declared in one place (see
[`openapi`](#openapi-recover-a-description-from-structure)).

## Matching: a radix tree (trie)

The route table is split on `/` and folded into one immutable trie whose nodes
map a segment (a literal, a typed param slot, or a `{rest:path}` catch-all) to
child nodes, terminating in a `{method -> endpoint}` map. Matching is a pure
walk. Three things fall out of the structure rather than being authored by hand:

- **Precedence.** At each node, literal children beat typed-param children beat
  the catch-all. No "order your routes carefully" footgun.
- **405-vs-404.** Land on a node whose method map lacks the request method → a
  `405` with an `Allow` header. Dead-end the walk → the `fallback` (typically a
  `404`). This is the distinction an exact `method == m and path == p` router
  cannot express.
- **Mounting** is a subtree graft at a prefix node.

The one subtlety: a converter can *reject* (`{id:int}` against `abc`), so the
walk backtracks to try sibling branches when a converter rejects, with a defined
resolution order (literal, then typed params, then catch-all).

### Converters parse, they don't validate

A converter is a `str -> value` parser paired with the JSON Schema it parses
into. The built-ins are `str` (default), `int`, `float`, `uuid`, and `path` (the
catch-all). A converter that raises `ValueError` *rejects* the segment, which
makes that branch fail to match and the walk backtrack (ultimately a 404),
never a handler-side error. Converters live in a registry injected into the
router, so an app adds its own by passing `converters=`.

## Endpoints and `Match`

`make_asgi_app`'s `HttpRouter` type is unchanged. The router's internal endpoint
protocol is richer: an `Endpoint[T, S, H] = (T, Match[S]) -> H`, where `Match`
carries the scope plus the already-parsed path params. `buffered` is the
web-flavored `(state, match, body) -> Response` adapter (the sibling of
`without_asgi.routing.buffered`, which hands the bare scope).

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
it knows from the converters) with the endpoint's half. An endpoint contributes
its half by answering `describe() -> RouteSpec`; the `describe(spec)` decorator
makes a `buffered` endpoint self-describing without changing its call shape, so
the body/query/response schema is declared in exactly the one place it is
parsed.

Turning a captured type into JSON Schema is an injected concern, so `without-web`
stays schema-library agnostic: a `RouteSpec` may carry an already-built JSON
Schema mapping, or a type plus a `schema_for: type -> schema` function the app
supplies (pydantic's `model_json_schema`, or a dataclass walker).

## WebSocket routing

`WebsocketRouter` reuses the same trie machinery with no method layer (so no
405): a connection either matches a path or falls to the `fallback`. Its
`dispatch` is a `WebsocketRouter[T]` for `make_asgi_app(websocket=...)`.

The `integration` package's `transform` example is built entirely on this
package; see it for a worked HTTP + WebSocket service.
