# Checkpoint 18

A snapshot of where `without` stands, succeeding `CHECKPOINT_17.md`. For the prior
state see `CHECKPOINT_17.md`; for the original pitch see `BIG_IDEA.md`.

This checkpoint clears three of the open questions raised in Checkpoint 17, all in
`without-web` and its `integration` examples: response encoding left the core,
middleware can now read the connection state, and exception handlers register with
their precise type. It then added one feature on top: middleware can be scoped to a
single route or a mounted subtree, not just the whole router. Everything is green:
mypy clean (70 source files), 261 tests, full pre-commit clean.

## What changed since Checkpoint 17

- **Response encoding left the core (was open question 1).** `without-web` no
  longer ships `json_response`/`text_response`. `responses.py` keeps only the
  encoding-agnostic `buffered` adapter (it takes a `Response`; the `JSON_HEADERS`/
  `TEXT_HEADERS` constants went with the helpers). Encoding (serializer, key
  ordering, content type) is an application choice, so the router only needs to
  produce a `Response` (status, headers, bytes). This mirrors the
  schema-library-agnostic stance for OpenAPI (`schema_for` is injected).
  - The example apps own encoding through a shared `integration/responses.py`
    (`json.dumps(..., sort_keys=True)`, UTF-8). `todos` and `transform` both import
    from it; `transform` previously carried its own inline copy, now removed.
  - The `without-web` tests carry their own `tests/helpers.py` with a
    `json_response` (resolved via pytest's prepend import mode, no `__init__.py`),
    so they exercise the router with concrete `Response` values without the core
    shipping an encoder.
  - The `without-web` README example was updated: it builds a `Response` directly
    (and is now `async def`, matching the all-async handler rule), with a note that
    encoding is the app's choice.

- **Middleware can read the connection state (was open question 2).** The
  `Middleware` vocabulary was widened from `(H, S) -> H` to `(T, H, S) -> H`:

  ```python
  type Middleware[T, H, S] = Callable[[T, H, S], H]
  type HttpMiddleware[T] = Middleware[T, HttpHandler, HttpScope]
  type WebsocketMiddleware[T] = Middleware[T, WebsocketHandler, WebsocketScope]
  ```

  State leads so a cross-cutting middleware (auth, rate limiting, config-driven
  behavior) can read the same `T` the dispatched handler sees. The change threads
  through `stack` and both `dispatch` sites (`Router`/`WebsocketRouter` now pass
  `state` to `catching`/`catching_websocket` and `self.middleware`).
  - **`wrap` stays the scope-only end.** Its transformers see the scope but not the
    state, so the middleware it produces is typed `Middleware[object, ...]` and
    ignores `T` (contravariantly usable in a `stack` over any state). A middleware
    that needs the state is written directly as `(T, handler, scope) -> handler`,
    the same way `access_timing`/`request_digest` were already hand-written. This
    keeps the existing narrative: `wrap` for edge transforms, a plain processor for
    whole-handler concerns (now including reading state).
  - State-ignoring middleware (`catching`, `access_timing`, `request_digest`, all
    `wrap` products) take their state argument as `object` and ignore it; they slot
    into a `stack` over any concrete `T` by Callable contravariance. The
    `transform` example gained `advertise_limit`, a genuine state-reading
    middleware that stamps `settings.http.max_bytes` onto every response, mixed
    into the same `stack` as the state-ignoring ones to show the vocabulary threads
    `Settings` through.
  - The `transform.router` custom `Router` (built on `without-asgi`'s tools) widened
    its `middleware` field and `dispatch` the same way.

- **Exception handling dropped its registry entirely (was open question 3).**
  The first pass added typed `on(ExcType, handler)` builders over a
  `Mapping[type[Exception], handler]` registry held as an `exception_handlers`
  field on the routers. Pushing on "do we even need a registry?" showed we do not:
  the registry is what *forced* the typed-builder + `cast` apparatus (a
  heterogeneous mapping cannot be typed), and the genuinely framework-level
  concern is only the commit point. So the registry, the `on`/`on_websocket`
  builders, the `cast`, the `ExceptionHandler` mapping types, the MRO `_lookup`,
  and the `exception_handlers` router field were all removed.

  `catching(recover)` is now the whole surface: a middleware over `_guarding`
  whose `recover` is the app's policy, an ordinary
  `(Exception) -> Awaitable[Response | None]` function (`None` propagates).

  ```python
  async def recover(exc: Exception) -> Response | None:
      match exc:
          case TodoNotFound():     # exc narrows to TodoNotFound; reads exc.todo_id
              return json_response(404, {"error": str(exc), "id": exc.todo_id})
          case ValidationError():
              return json_response(422, {"error": "invalid todo body", "fields": exc.error_count()})
          case _:
              return None

  Router(routes=(...), middleware=stack(powered_by, catching(recover)))
  ```

  `match` narrows each case to its real type (no `assert isinstance`, no cast) and
  the app gets full control (re-raise, chain, async work). Exception handling is no
  longer a distinct router concern: it is middleware the app stacks where it wants,
  so both `dispatch` methods lost their special-case branch and field. The `todos`
  example registers it innermost (`stack(powered_by, catching(recover))`) so a
  mapped response still flows out through `powered_by`. `catching_websocket` is the
  sibling mapping to a `WebsocketClose`.

- **Scoped middleware: per-route and per-subtree (new feature).** The router-wide
  `middleware` field applied to every dispatch; there was no way to scope middleware
  to certain routes (e.g. auth on `/admin` only). Two composing additions, no new
  mechanism:
  - `with_middleware(endpoint, *middleware)`: a protocol-generic combinator that
    wraps one endpoint. An `Endpoint` builds the handler and a `Middleware` is
    `(T, H, S) -> H`, so this is composition; the result is a narrower `Endpoint`,
    usable per method (`get=with_middleware(h, auth)`) or on an opaque mount target.
  - A mounted sub-`Router`'s own `middleware` is now **honored on graft**. Before,
    `_flatten` grafted a mounted router's routes into the parent trie but silently
    dropped its `middleware` (a latent bug). Now `_behind` pushes the sub-router's
    middleware onto each grafted leaf (every method endpoint, or the delegate
    target), so middleware on a mounted router applies to its whole subtree and
    nowhere else. Parent middleware still nests outside it.

  `integration.todos` demonstrates both via `require_authorization`, the `admin`
  mount's own middleware: it reads the scope for an `Authorization` header and
  short-circuits with a `401` (returning a handler that never runs the endpoint),
  scoped to `/admin` while the public todo routes stay open. A middleware replacing
  the handler outright is the auth pattern the router-wide hook could not express
  selectively.

## Rationale: a few decisions worth remembering

- **The helpers moved, the principle stayed.** Dropping `json_response` from the
  core is the same move as injecting `schema_for`: the framework produces the typed
  value (`Response`, OpenAPI doc) and leaves the boundary encoding decision to the
  app. The `buffered` adapter survives in the core precisely because it is
  encoding-agnostic (it routes a `Response`, it does not build one).
- **`wrap` is deliberately state-blind.** Threading `T` everywhere was tempting,
  but `wrap`'s whole role is scope-aware *edge* transforms; giving its transformers
  state would have changed every `wrap` call site for a capability they do not use.
  Typing `wrap`'s product at `Middleware[object, ...]` lets the state-ignoring
  majority stay untouched while a hand-written middleware reads `T` when it must.
  Callable contravariance makes the two compose in one `stack`.
- **No registry beats a typed registry.** The `on()`/`cast` machinery existed only
  to make a heterogeneous `type -> handler` mapping type-check. Removing the mapping
  removes the problem: a `recover` function written as `match exc:` gets real types
  for free, and is strictly more expressive (re-raise, chain, fallthrough). The one
  thing worth keeping in the framework is the commit-point-aware guard (`_guarding`);
  the policy is the app's. This is the same "the app owns the boundary decision"
  move as dropping the response encoders, applied to exception-to-response mapping.
  Composition still works through the middleware `stack` (an outer `catching`
  handles what an inner one returns `None` for) rather than a mapping merge.

## Status

Green and verified (mypy clean, ruff check + format clean, tests passing):

- `without_asgi.routing`: `Middleware[T, H, S]`, `HttpMiddleware[T]`/
  `WebsocketMiddleware[T]`, `stack`, `wrap` (now `Middleware[object, ...]`).
- `without_web`: `responses` (encoders removed, `buffered` kept), `exceptions`
  (registry/`on`/`_lookup` removed; `catching(recover)`/`catching_websocket(recover)`
  over `_guarding`; new `ExceptionRecover`/`WebsocketExceptionRecover` aliases),
  `router` (both `dispatch` sites thread state and lost their `exception_handlers`
  field/branch; new `with_middleware` combinator and `_behind` helper; `_flatten`
  honors a grafted sub-router's `middleware`), `__init__` re-exports
  (`ExceptionRecover`/`WebsocketExceptionRecover`/`with_middleware` in;
  `on`/`on_websocket`/`ExceptionHandler`/`json_response`/`text_response` out).
- `integration`: new `responses.py`; `todos` and `transform` import from it;
  `transform` gained `advertise_limit` and widened middleware signatures; `todos`
  maps exceptions with a `match`-based `recover` stacked as `catching(...)`, dropped
  its `assert isinstance`, and gained `require_authorization` as the `admin` mount's
  scoped middleware; `transform.router` widened.
- Tests: `tests/helpers.py` (new); `test_router` gained
  `test_recover_narrows_the_exception_type_and_reads_typed_fields`,
  `test_middleware_reads_the_dispatched_state`, and three scoped-middleware tests
  (`with_middleware` on one route, a mounted router keeping its middleware, parent +
  mount both applying); its exception tests now stack `catching(recover)`; the three
  `without-web` test files import `json_response` from `helpers`. `todos`' app tests
  gained an admin-gate `401` case and a `headers=` arg on `_request`.
- Docs: `without-asgi` and `without-web` READMEs updated for the middleware
  signature, the dropped encoders, `catching(recover)`, and the new scoped-middleware
  section (`with_middleware` + per-subtree mount middleware).

## Open questions and next steps

Carried forward from Checkpoint 17, still open:

1. **No shared components / `$ref`** in the OpenAPI output; `schema_for` inlines
   every schema. Couples with the still-open `url_for` / reverse-routing item.
2. **`todos` persistence is stubbed** (`POST` echoes); a live `Context[TodoList]`
   updated by a fold would exercise the actor-model question.
3. **Opaque-mount prefixes** are literal-only.
4. **No real HTTP/WebSocket testing**: still hand-built `scope` dicts;
   `httpx.ASGITransport` + a `uvicorn` smoke run remain the best first target.
5. **Intra-workspace deps unpinned**; a packaging gap for publishing.
6. **Unconsumed input streams**: `make_asgi_app` never `aclose()`s the inbound
   stream at the boundary.
7. The **actor-model question** (`ACTOR_MODEL.md`); **static `Context` ceremony**;
   a **dynamic-merge** connector; graph/DAG recovery on `graphlib`; known-hard FRP
   problems.

Operational, carried: CI on the `proof-of-concept` branch (PR #1, draft) has two
**flaky concurrency tests** in `kv/test_shell.py`
(`test_shutdown_drains_inflight_requests`,
`test_a_client_reset_does_not_disturb_other_connections`) that pass locally.

Documentation debt (carried): `BIG_IDEA.md` still calls the model an "async
reducer" (it is an async *scan*); the functional-core / connection-as-stream /
parsing-as-a-value framings should be folded in when it is next revised.
