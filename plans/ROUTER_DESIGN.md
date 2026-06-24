# Design doc: a concrete (opinionated) router for `without`

This is a design exploration, not a committed implementation. It lays out the
feature taxonomy for a fancier router, the design options for each feature, and
the precise division of responsibility between the routing layer, the handler
layer, and the `without-asgi` boundary.

## Context

`without-asgi` deliberately ships **no router**, only unopinionated tools
(`Middleware`, `stack`, `wrap`, `buffered` in `routing.py`). The transform
example hand-rolls a 52-line `Router` (`integration/.../transform/router.py`)
that matches on exact `method == m and path == p`. That is enough for that app
but doesn't cover what FastAPI/Litestar/Starlette/Django give you: path
parameters, 405-vs-404, mounting, OpenAPI.

We want to *prove* an opinionated router you set up and hand to
`make_asgi_app`, **without** forcing anyone through it. Bring-your-own (or
no router at all) must stay first-class. This doc lays out the feature
taxonomy, the design options for each, and where the line falls between
`without-asgi` and a possible new `without-web`.

## The reframing that drives everything: a router dispatches on the *scope*

`make_asgi_app` types the router as:

```python
HttpRouter[T] = Callable[[T, HttpScope], HttpHandler]   # app.py:61
HttpHandler  = Processor[Inbound, Outbound]              # app.py:60
```

The router is a function of `(state, HttpScope) -> handler`. It **never sees
the inbound stream**: the body arrives as `RequestBody` events that only the
returned handler consumes. So the router is a pure function of the *scope*:
`method`, `path`, `query_string`, `headers`. Not the body.

This kills the "does the router need a parser?" question (it doesn't, it can't
parse a body it never reads) and yields a clean dispatch principle:

> **Dispatch depends only on `method` + `path`.** Everything else (query,
> headers, body) may be *declared* on a route for typing and documentation, but
> declaring it does not change *which* handler is selected. Matching stays a
> total, pure function of `(method, path)`.

This is a **deliberate opinionated restriction, not a limitation.** The scope
*does* carry `query_string` and `headers`, so a router technically *could* match
on them, dispatching a different handler for `?format=json` vs `?format=xml`. But
effectively no one does (FastAPI/Starlette/Litestar/Django all match on
method+path only; content negotiation is the handler's job), and allowing it
would make matching a function of an open-ended input, push query interpretation
into the routing layer, and split query ownership across both layers. We forbid
it. The payoff is the clean layering below: query belongs *wholly* to the
handler, and the match function has a small, total domain.

Path parameters are the one thing the router *must* own: only the route pattern
knows that `/users/42` binds `id=42`.

## Layering: each layer documents exactly what it parses or produces

This is the precision the design hinges on. State it as one rule:

> **Whichever layer parses or produces a value is the single source of truth
> for that value's schema.** No fact is declared in two places.

Sorting every concern by that rule:

| Concern | Parsed/produced by | Owns its schema |
|---|---|---|
| Method, path | router (matching) | router |
| **Path** params (`{id:int}`) | router (converters) | router |
| **Query** params | handler (reads `query_string`) | handler |
| **Request body** | handler (reads inbound stream) | handler |
| **Response(s)** + status codes | handler (emits outbound) | handler |

The router never sees the body or interprets the query, so it cannot and must
not be the source of those schemas: that would be denormalized state (the body
shape declared on the route *and* parsed in the handler, free to drift; see
parse-don't-validate). Instead:

> **The OpenAPI spec is a *merge*: the router contributes the path/method/
> path-param half it owns and *asks each handler* for the body/query/response
> half the handler owns.**

So an endpoint is not a bare callable: it is a value that can **describe
itself**. The same typed declaration that gives a handler its body parsing also
yields that body's schema: one value, two consumers (the handler's parse and the
router's spec). That is parse-don't-validate and single-source-of-truth landing
together. Mechanics in §6.

## Where the line falls: `without-asgi` vs `without-web`

**Criterion:** does the piece *translate the ASGI protocol*, or does it *express
an opinion about HTTP application structure*?

| Stays in `without-asgi` (boundary + unopinionated composition) | Moves to `without-web` (HTTP-app opinions) |
|---|---|
| `make_asgi_app`, scope/inbound/outbound parsing, shell | `Route` / `Mount` / `Router` with path patterns + converters |
| `Middleware`, `stack`, `wrap`, `buffered` | 405-vs-404 policy, `Allow` header |
| The `HttpRouter`/`WebsocketRouter` types | `Match`/`Request` value carrying scope + path params |
| | response helpers (`json_response`, ...), exception-handler middleware |
| | `openapi(router) -> dict` + (optional) Swagger UI |

**Decision: a new sibling package, `without-web`.** Reasons:

1. `REVIEW_TRANSFORM_EXAMPLE.md` already doubts routing belongs in
   `without-asgi` ("it is not clear that vocabulary belongs in `without-asgi`
   at all: the package's job is the ASGI boundary, not a routing DSL").
2. The only integration surface is the existing `HttpRouter` type:
   `without_web.Router.dispatch` **is** an `HttpRouter`, so
   `make_asgi_app(http=router.dispatch, ...)` just works and BYO/no-router stay
   first-class. That **re-proves the narrow-waist thesis a second time**: a
   routing package snaps onto the boundary package through nothing but the bare
   callable type, neither knowing about the other.
3. OpenAPI, Swagger UI, and response helpers pull in opinions (and possibly a
   schema dependency) that should not weigh down the boundary package.

An optional submodule in `without-asgi` was the alternative (same code,
different home) but complects boundary-translation with app-routing in one
package, so it is rejected.

## Feature designs

### 1. Path matching + typed path parameters

**Decision: compile the route table to a radix tree (trie), Litestar-style.**
The table is split on `/` into segments and folded into one immutable tree whose
nodes map a segment (a literal, a typed param slot, or a `{rest:path}` catch-all)
to child nodes, terminating in a `{method -> endpoint}` map. Matching is a pure
walk of that tree.

It is chosen over the two alternatives, Starlette's **regex per pattern**
(simple but opaque, and a per-route linear scan) and a **flat list of segment
patterns**. The reason is not raw speed (irrelevant at the scale needed to prove
the idea) but that the trie's structure *recovers* three things we would
otherwise have to author by hand, which is squarely on-theme (declarative,
values-over-places):

- **Precedence from structure.** At each node, literal children beat typed-param
  children beat the `{rest:path}` catch-all. No "order your routes carefully"
  footgun.
- **405-vs-404 from the walk.** Land on a node with no entry for the request
  method → 405; dead-end the walk → 404 (see §2).
- **Mounting as a subtree graft** at a prefix node (see §4).

**The one subtlety to document:** typed converters can *reject* (`{id:int}` vs
`abc`), so the walk is not a pure single descent: it must backtrack to try
sibling branches (e.g. a `{name:str}` param) when a converter rejects. So each
node needs a defined resolution order (literal → typed param → `str` param →
`{rest:path}`) and the walk needs backtracking. A flat segment-scan over the
route list is the trivial fallback if an initial slice wants minimal code before
the trie exists.

**Converters as parsers (parse, don't validate).** A converter is
`str -> T` that may reject: `str` (default), `int`, `float`, `uuid`,
`path`. A converter that rejects a segment means **that branch does not match**
(the walk backtracks; ultimately a 404 if nothing matches), not a handler-side
error. Converters are values in a registry, injected, not hardcoded, so apps
can add their own (dependency-injection rule).

**Where extracted params go.** Keep `make_asgi_app`'s `HttpRouter` type
unchanged; the router's *internal* endpoint protocol is richer. Define a value:

```python
@dataclass(frozen=True, slots=True)
class Match:
    scope: HttpScope
    params: Mapping[str, object]   # already-parsed by converters
```

and `Endpoint[T] = Callable[[T, Match], HttpHandler]`. A web-flavored
`buffered` then offers `(state, match, body) -> Response`. `router.dispatch`
still presents as `(T, HttpScope) -> HttpHandler` to `make_asgi_app`.

### 2. Method dispatch: 405 vs 404

Each terminal trie node holds a `{method -> endpoint}` map. The walk matches the
*path* first; then the method is looked up in that map. Node reached but method
absent → **405** with an `Allow` header listing the bound methods. Walk
dead-ends → **404**. This distinction is the main thing the exact-match
transform router can't express.

### 3. Query parameters: declare, don't dispatch

Per the dispatch principle, query parsing must not affect which handler runs;
per the layering rule, query schema belongs to the **handler** (it is what reads
`query_string`), not the route. So:

- **Extraction is the handler's** and is *not* on the matching path. The handler
  parses `head.query_string` itself (status quo: `mode_param`) or via a
  `without-web` typed helper.
- That same helper/declaration is what the handler reports as its query-param
  contribution when the router asks for OpenAPI (§6). The router does not
  separately declare query params; that would split the source of truth.

### 4. Mounting / sub-routers

A `Mount(prefix, sub_router)` is a trie node where the walk stops and delegates,
stripping the prefix using ASGI `root_path` semantics before handing off the
trimmed scope. Two cases:

- **Mounted `without-web` Router**: graft its trie as a subtree at the prefix
  node, so matching and OpenAPI see straight through (mounted routes contribute
  prefixed paths to the spec).
- **Opaque `HttpRouter`** (a BYO router or another app): the walk hands off the
  trimmed scope and treats the mount as a black box for OpenAPI.

Either way a Mount composes as a value (making-changes rule). WebSocket routing
reuses the same trie machinery (no `method`, so no 405 layer).

### 5. Exception handlers

The handler is a `Processor`; exceptions surface while its outbound stream is
consumed. A declarative
`exception_handlers: Mapping[type[Exception], Callable[[Exc], Response]]` is
implemented as a **middleware** that wraps each endpoint and maps caught
exceptions to a `Response`, reusing the existing `stack`/`wrap`/`compose`
machinery rather than inventing a new mechanism.

**Honest limitation to document:** once `ResponseStart` has been emitted, the
status line is on the wire and can't be rewritten. Exception → response mapping
is clean only for exceptions raised *before the first outbound event*; after
that the handler can abort but not re-status (error-handling rule: be honest
about what can't occur).

### 6. OpenAPI: recover a description from declared structure

Per the layering rule, the spec is a **merge of two self-descriptions**, not a
single declared blob:

- the **router** contributes what it owns: path pattern, methods, path-param
  schemas (it knows the converter types);
- each **endpoint** contributes what it owns (request body, query params,
  responses + status codes) via a small description value it exposes.

So define an endpoint as more than a callable: a value pairing the handler
factory with an optional **`describe() -> RouteSpec`**. `openapi(router)` is then
a **pure transform**: walk the table, and for each route merge the router's
path/method half with `endpoint.describe()`. The router *asks*; it never
restates the handler's half.

**One declaration, two consumers.** The elegant part: the same typed sugar that
gives a handler its body parsing produces its `describe()`. E.g. a typed
`buffered_json(RequestModel) -> ResponseModel` decorator captures the request
and response types *once*; the handler uses them to parse/serialize, and
`describe()` reflects them into schema. The body shape is declared exactly where
it is parsed, no drift.

Turning a captured type into JSON Schema is a *separate, injected* concern, so
`without-web` stays agnostic: an endpoint's `describe()` may yield an
already-built JSON-Schema value, or a type plus a `type -> schema` function the
app supplies (pydantic's `model_json_schema()` if present, else a minimal
dataclass walker). Functional core: the table is data, `openapi()` is pure, and
schema-from-type is injected.

This is the same "recover a description from declared structure" move that
`BIG_IDEA`/`REVIEW` promised for the DAG (`@node` + mermaid) pillar and never
delivered, here realized in the HTTP domain, and notably *recovered from the
handlers themselves* rather than re-declared. Worth calling out as the
philosophical payoff.

## How it snaps onto the entrypoint (the whole integration surface)

```python
@buffered_json(ShowUserBody)       # captures body/response types once:
def show_user(state, match, body): # ...used to parse here AND to describe() below
    ...

router = Router(
    routes=(
        route("/users/{id:int}", get=show_user),  # endpoints carry describe()
        Mount("/admin", admin_router),
    ),
    fallback=not_found,            # 404
    converters=DEFAULT_CONVERTERS, # injected, extensible
    exception_handlers={DomainError: to_400},
)
app = make_asgi_app(
    lifespan,
    http=router.dispatch,          # router.dispatch IS an HttpRouter[T]
    websocket=socket_router.dispatch,
)
spec = openapi(router)             # pure merge: router path/method half
                                   # + each endpoint.describe() -> OpenAPI 3.x
```

No change to `make_asgi_app`. BYO router or no router remains exactly as today.

## Out of scope (call out, don't build)

- A FastAPI-style **magic DI container** (`Depends`). `without` threads state
  explicitly through `(state, scope/match)`; magical signature injection
  contradicts the dependency-injection rule. State and contexts are passed, not
  resolved by name.
- Body/query *validation as the router's job*: it doesn't see the body, and
  query doesn't drive dispatch.
- Static file serving, URL reversal (`url_for`), templating: later, if ever.

## Decisions

| # | Decision | Rejected alternative |
|---|---|---|
| 1 | **New `without-web` package.** Routing is app opinion, not boundary translation; `Router.dispatch` snaps on via the `HttpRouter` type, re-proving the narrow waist. | Submodule in `without-asgi` (complects boundary with app-routing). |
| 2 | **Radix-tree (trie) matching** (§1). Precedence, 405-vs-404, and mounting are recovered from tree structure; node-level resolution order plus backtracking on converter rejection is the accepted cost. | Regex per pattern (opaque, linear scan); flat segment-scan (kept only as a pre-trie fallback for an initial slice). |
| 3 | **Endpoints self-describe** via `describe() -> RouteSpec`; the router merges its path/method half with the handler's body/query/response half (§6). | Re-declaring body/response on the route (denormalized state). |
| 4 | **Injected `type -> schema` function.** `without-web` stays dependency-free; the app supplies the converter (pydantic's `model_json_schema()`, else a minimal dataclass walker). | Hard pydantic dependency on the routing package. |
| 5 | **Query extraction is handler-owned** via a typed helper that also produces the handler's `describe()` contribution (§3). | Router-side query declaration (splits the source of truth). |

No open decisions remain. Next step, when moving toward code, is the
design-validation slice below.

## Validating the design (since this produces no code)

Validate the *design* by re-expressing the transform app's two routes
(`POST /transform`, `GET /modes`) and the WebSocket route on the proposed API,
on paper, and checking:

- `router.dispatch` still type-checks as `HttpRouter[Settings]` against
  `make_asgi_app` (mypy strict, the project's bar);
- the existing middleware (`access_log`, `request_digest`, ...) still attaches
  via `stack`, unchanged;
- `openapi(router)` produces a plausible spec by merging the router's
  path/method/path-param half with each endpoint's `describe()`, confirming the
  body/response schema is sourced from the handler, declared in exactly one
  place;
- a hand-built scope for `/transform` with a wrong method yields 405 + `Allow`,
  and an unknown path yields 404: the two cases the current exact-match router
  can't tell apart.
