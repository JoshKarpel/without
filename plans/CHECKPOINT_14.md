# Checkpoint 14

A snapshot of where `without` stands, succeeding `CHECKPOINT_13.md`. For the
original pitch see `BIG_IDEA.md`; for the critical review and open design
questions see `REVIEW_BIG_IDEA.md`; for the router design this checkpoint
realizes see `ROUTER_DESIGN.md`.

## What changed since Checkpoint 13

One large pass: the opinionated router sketched in `ROUTER_DESIGN.md` is now a
real package, `without-web`, plus a new `todos` example built entirely on it.
The `transform` example is untouched: it stays the hand-rolled-router
demonstration, and `todos` is the you-don't-have-to-hand-roll-one counterpart.

- **New package `without-web` (`packages/without-web/`).** Depends only on
  `without` and `without-asgi`. It snaps onto the boundary through nothing but
  the existing `HttpRouter` type: `Router.dispatch` *is* an `HttpRouter[T]`, so
  `make_asgi_app(http=router.dispatch)` just works and bring-your-own (or no
  router) stays first-class. That **re-proves the narrow-waist thesis a second
  time**: a routing package composes onto the boundary package through the bare
  callable type, neither importing the other's opinions.
  - `converters.py`: `str`/`int`/`float`/`uuid`/`path` as a `Converter` value
    pairing a `parse` (raises `ValueError` to *reject*, which backtracks the
    walk: parse, don't validate) with the JSON Schema the router owns for that
    path param. The registry is an immutable `MappingProxyType`, injected into a
    router, extensible by an app.
  - `patterns.py`: `parse_pattern`/`split_path` into `Literal`/`Param`/
    `CatchAll`. Whole-segment params only; catch-all must be last; matching is
    trailing-slash insensitive because patterns and targets split the same way.
  - `trie.py`: an immutable radix tree, generic over the leaf payload, built by
    a local mutable builder then frozen. The walk backtracks on converter
    rejection; precedence (literal -> typed param -> `str` param -> catch-all)
    is recovered from node structure, not registration order.
  - `router.py`: `Match` (scope + already-parsed params), `Endpoint`, `Route`,
    `Mount`, `route()`, `Router`, `WebsocketRoute`, `ws_route`,
    `WebsocketRouter`. `Router` compiles its table to the trie once in
    `__post_init__` (a frozen dataclass with the tree stored via
    `object.__setattr__`). `dispatch` resolves the leaf, distinguishes **405**
    (path matched, method absent -> `Allow` header) from **404** (dead-end ->
    `fallback`), grafts a mounted `without-web` `Router` (its routes prepended
    with the prefix) and delegates an opaque `HttpRouter` (handed the
    prefix-trimmed scope under ASGI `root_path` semantics).
  - `responses.py`: `json_response`/`text_response`, and a web-flavored
    `buffered` whose handler is `(state, match, body) -> Response` (the sibling
    of `without_asgi.routing.buffered`, which hands the bare scope).
  - `exceptions.py`: `catching` / `catching_websocket`, both built on one
    generic `_guarding` core.
  - `openapi.py`: `RouteSpec`/`describe`/`openapi()`. The spec is a **merge of
    two self-descriptions**: the router contributes path/method/path-param
    (it knows the converter schemas); each endpoint that answers `describe()`
    contributes body/query/responses. Turning a captured type into JSON Schema
    is an injected `schema_for`, so the package stays schema-library agnostic.
- **New example `integration/todos/`.** The canonical todo-list REST API,
  chosen because it exercises the whole design at once: `/todos/{id:int}` (typed
  path param), `GET` vs `POST` on `/todos` (method dispatch, so `PUT` -> 405),
  `?done=` (handler-owned query filter), `/admin` (grafted sub-router) and
  `/legacy` (opaque mount), `TodoNotFound`/`ValidationError` mapped by exception
  handlers, a websocket feed, and `todos_openapi()` resolving a pydantic model
  through `schema_for`. `todos.core` is the pure immutable `TodoList`;
  `todos.app` is the `without-web` wiring.
- **Tests and docs.** Unit tests for every `without-web` module (converters,
  patterns, trie incl. backtracking, router incl. 405/404/mount/exception
  commit point, openapi merge) plus `todos` core and full-ASGI integration
  tests. New `without-web/README.md`; `without-asgi`'s and `integration`'s
  READMEs cross-reference the package and the example.

## Rationale: a few decisions worth remembering

- **A router dispatches on the scope, never the body.** `make_asgi_app` types
  the router as `(T, HttpScope) -> HttpHandler`; it never sees the inbound
  stream, so matching is a total, pure function of `method` + `path`. Query,
  headers, and body may be *declared* on an endpoint (for typing and OpenAPI)
  but never change *which* handler is selected. Path params are the one thing
  the router must own, because only the pattern knows `/todos/42` binds `id=42`.
- **Single source of truth, recovered not restated.** Whichever layer parses a
  value owns its schema: the router owns method/path/path-param, the handler
  owns query/body/responses. `openapi()` *asks* each endpoint for its half via
  `describe()` rather than re-declaring it on the route, so the body shape is
  declared exactly where it is parsed (no denormalized state). This is the same
  "recover a description from declared structure" move `BIG_IDEA` promised for
  the DAG pillar, here realized in HTTP and recovered *from the handlers*.
- **The exception commit point is protocol-specific, not "the first event."**
  The status line is committed at `ResponseStart`, not at any first outbound
  event: informational events (`EarlyHint`, `ResponseDebug`) precede it and
  don't lock out recovery, so an exception after them can still be mapped. The
  same shape generalizes to WebSocket, where the equivalent commit point is
  `WebsocketAccept` (before accept a close still rejects the connection). One
  generic `_guarding` core takes a `commits` predicate and a `recover` function;
  `catching` and `catching_websocket` are thin wrappers.
- **The trie is the substrate; 405/404/mounting fall out of it.** Choosing a
  radix tree over regex-per-pattern was not about speed (irrelevant at this
  scale) but because precedence, the 405-vs-404 split, and mounting-as-graft are
  *recovered* from tree structure rather than authored by hand. The accepted
  cost is per-node resolution order plus backtracking when a converter rejects.
- **`Router` is a value with a compiled tree.** The route table is data; the
  tree is built once at construction and held immutably (control-plane work off
  the per-request path). `dispatch` is then a pure walk.
- **`todos` is deliberately read-mostly.** State is a single immutable
  `TodoList` held for the connection; `POST` validates and echoes the next id
  without persisting. This keeps the example about *routing* and leaves a shared
  mutable store (the actor-model question) out of scope, rather than smuggling
  it in through a CRUD demo.

## Status

Done and verified (mypy strict clean across 63 source files, the test suite
green at 225 passing, ruff lint + format clean):

- `without-web`: full package (converters, patterns, trie, router, responses,
  exceptions, openapi), wired into the workspace and `integration` deps,
  `py.typed`, README, `__init__` re-exports with `__all__`.
- `integration.todos`: `core` (pure `TodoList`, `NewTodo`, `TodoNotFound`) and
  `app` (HTTP `Router` + `WebsocketRouter`, one `stack` middleware, exception
  handlers, `todos_openapi()`); `__init__` re-exports.
- Tests: `without-web/tests/` (converters, patterns, trie, router, openapi) and
  `integration/tests/todos/` (core + full-ASGI app, incl. websocket and openapi).

## Open questions and next steps

Raised this checkpoint:

1. **`todos` persistence is stubbed.** `POST` echoes rather than persisting, to
   avoid a shared mutable store. Threading a live `Context[TodoList]` updated by
   a fold would make it a genuine CRUD service and would finally exercise the
   actor-model question (`ACTOR_MODEL.md`) in anger.
2. **Opaque-mount prefixes must be literal.** A parameterized mount prefix is
   rejected at build; trimming a parameterized prefix correctly (the path text
   differs from the pattern) is deferred. Grafted `without-web` mounts have no
   such limit because they re-parse patterns.
3. **No URL reversal / `url_for`.** The trie knows enough to render a path from
   a route + params, but reverse routing is unbuilt (called out as out of scope
   in `ROUTER_DESIGN.md`).

Carried from Checkpoint 13, still open:

4. **`request_digest`'s ordering assumption** (streaming-aware digest in a
   trailer would generalize the buffered fallback).
5. **`wrap`'s dual form** kept as a convenience, strictly two single-edge wraps.
6. **`Settings.max_bytes`** is core config only the shell reads (transport limit
   split out of domain config is a reasonable next move).

Carried from Checkpoint 11, still open:

7. **No real HTTP/WebSocket testing.** Everything is still hand-built `scope`
   dicts, scripted `receive`, capturing `send`. A few `httpx.ASGITransport`
   tests plus a `uvicorn` smoke run (now over `todos_app` too) would catch
   conformance gaps. Still the best first target; `without-web` raises the
   stakes because more routing logic now rides on scope fidelity.
8. **Extensions are modeled but never negotiated in anger** beyond
   `request_digest`.
9. **A non-ASGI shell** would make the portability promise concrete (nothing yet
   drives the same `Lifespan[T]` from a queue processor).
10. **Intra-workspace deps are unpinned**, a packaging gap for publishing
    (now one more package, `without-web`, in the same boat).
11. **Unconsumed or partially-read input streams.** `make_asgi_app` never
    `aclose()`s the inbound stream; `contextlib.aclosing(...)` would make
    cleanup deterministic.

Carried forward from earlier checkpoints (still open):

- **The actor-model question** (`ACTOR_MODEL.md`, open question #2), now with a
  concrete place it would land (open question 1 above).
- **Static `Context` ceremony**: the static-config-as-plain-value question is
  unchanged.
- A **dynamic-merge** connector as the single sanctioned funnel; whether a
  consumer parameter deserves a named `Leaf` type; factoring the connection-set +
  bounded-drain orchestration out of `kv`.
- Deferred deliberately: graph/DAG recovery and visualization on `graphlib`
  (though `without-web`'s `openapi()` is the first realized instance of the
  recover-a-description-from-structure idea); known-hard FRP problems (diamond
  glitches, feedback cycles, teardown order); a deterministic "await next
  update" signal to replace `testing.tick`.

Documentation debt (carried forward): `BIG_IDEA.md` and the early checkpoints
still call the model an "async reducer"; it is more accurately an **async scan**,
and the functional-core / imperative-shell, connection-as-stream, and
lifecycle-as-a-portable-value framings should be folded into `BIG_IDEA.md` when it
is next revised.
