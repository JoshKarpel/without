# Checkpoint 17

A snapshot of where `without` stands, succeeding `CHECKPOINT_16.md`. For the prior
state see `CHECKPOINT_16.md`; for the original pitch see `BIG_IDEA.md`.

This checkpoint is mostly about *handler shape*. Two changes landed together
(handlers are now uniformly `async`, and websocket handlers became processors
rather than processor-factories), and a third thread (a `providing`/`deriving`
endpoint-wrapper for request-scoped dependency injection) was explored and
deliberately *dropped* once it became clear the first change had already made it
unnecessary. Everything is green: mypy clean (68 source files), 255 tests, full
pre-commit clean.

## What changed since Checkpoint 16

- **Handler color is fixed to `async`.** `Returned` lost its synchronous arm:

  ```python
  type Reply = Response | Stream[Outbound]
  type Returned = Awaitable[Reply] | Stream[Outbound]   # was: Reply | Awaitable[Reply]
  ```

  The motivating problem is function coloring: the point of a handler is to be
  able to `await` I/O, and a sync `def ... return Response` cannot. A coherent
  rule fell out that is subtler than "all async":
  - A sync `def ... return Response` is now a **type error** (a bare value with no
    place to `await`).
  - A sync `def ... return some_stream` is **still allowed**: the async work lives
    in the returned `Stream[Outbound]`, not in the builder. So the streaming-output
    tests did not change. The line is "no synchronous *value*", not "no synchronous
    *function*".

  `_emit` simplified accordingly (it now binds the awaited `Reply` into a fresh
  variable rather than reassigning, since `Returned` no longer admits a bare
  `Response`). Exception handlers were given the same async color
  (`ExceptionHandler = Callable[[Exception], Awaitable[Response]]`,
  `WebsocketExceptionHandler` likewise), awaited inside `_guarding`, so no
  sync-colored handler that cannot `await` is left anywhere. The lower-level
  `buffered` BYO adapter (`(state, match, body) -> Response`) was left synchronous
  on purpose: it is the explicit escape hatch, analogous to the endpoint protocol
  itself, not a decorator handler.

- **Websocket handlers are processors, not processor-factories.** `@ws` got the
  same treatment `@post.stream` already had: the handler *is* the frame processor.
  It takes the live inbound frames as a trailing `Stream[WebsocketInbound]`
  argument and yields `WebsocketOutbound` directly, instead of being a builder that
  returns a `WebsocketHandler`:

  ```python
  @ws("/todos/session")
  async def session(todos: TodoList, inputs: Stream[WebsocketInbound]) -> AsyncIterator[WebsocketOutbound]:
      working = todos
      async for event in inputs:
          ...   # yields WebsocketAccept/Send/Close directly, no inner function
  ```

  - The `ws` overload ladder now carries the trailing `Stream[WebsocketInbound]`
    and returns a new exported alias `WebsocketReturned = Stream[WebsocketOutbound]`
    (the websocket analog of `Returned`, with no buffered arm because a connection
    is inherently a stream). The alias also dodges a `cog` gotcha: the raw return
    type produced a literal triple close-bracket that collides with cog's
    end-of-generator marker, the same reason the HTTP ladders return the bare
    `Returned` rather than a raw stream type. The ladder was regenerated through
    `tools/regenerate.sh`.
  - The lower-level `WebsocketEndpoint` protocol `(T, Match) -> WebsocketHandler`
    is unchanged; only the `@ws` decorator sugar changed. The websocket *fallback*
    (`refuse` in the example) stays an endpoint builder, consistent with the HTTP
    fallback being a `handle(...)` endpoint.

- **Request-scoped dependency injection: explored, then dropped.** The session
  started by asking how `without-web` could offer FastAPI-style request-scoped
  dependencies. The first design was an endpoint wrapper, `providing(acquire,
  endpoint)` (async, `async with` teardown) plus a synchronous `deriving(derive,
  endpoint)`, that adapted an `Endpoint[U]` down to an `Endpoint[T]` so a
  `Router[T]` could hold a route whose handler saw a narrower `U`. It was built and
  then **removed**, because making handlers uniformly `async` (the change above)
  already solved the problem more simply: a handler is now itself an async scope,
  so a request-scoped resource is just an `async with` in the handler body, with
  structural teardown over exactly the handler's lifetime (buffered handlers
  release as the `return` unwinds; streaming handlers hold the resource across the
  whole stream). The idiomatic "swap the dependency in tests" is the pattern the
  project already uses: carry the factory in `T` (lifespan/app state) and read it
  in the handler. See the rationale below.

## Rationale: a few decisions worth remembering

- **Inline `async with` beats a `Depends`-style wrapper here.** FastAPI needs
  `Depends(yield)` because its handlers can be synchronous and the framework owns
  the request lifecycle. Once every handler is an `async` scope, the framework does
  not need to own acquisition/teardown at all: the handler writes `async with`
  itself. That is also more aligned with the project's own rules (explicit
  dependency injection, no framework magic, dependencies visible at the call site)
  than a registry of providers would be. The connection-scoped dependency is `T`;
  the request-scoped one is an `async with` inside the handler. Both scopes are
  covered with zero new framework surface.
- **The tell that the wrapper was at the wrong layer** was that `providing` had to
  re-forward the endpoint's `describe()` (a `_Carrying` shim mirroring
  `openapi._Described`), because wrapping the endpoint in a new callable otherwise
  silently stripped the route from the OpenAPI document. Inline `async with` keeps
  `describe()` intact for free, because the endpoint is still built normally by
  `handle`/`@get`. Needing machinery to preserve a property the simpler approach
  preserves automatically is a sign the abstraction is misplaced.
- **All-async is a coherence choice, not just symmetry.** It removes the one
  remaining function-coloring seam, so endpoint-level composition (and the dropped
  DI idea, had it been kept) never has to bridge a sync and an async handler.

## Status

Green and verified (mypy clean, ruff check + format clean, tests passing):

- `without_web`: `handlers` (`Returned` without its sync arm, `_emit`,
  `WebsocketReturned`, `@ws` as processor, regenerated `ws` ladder), `exceptions`
  (async exception handlers + `_guarding`), `__init__` re-exports
  (`WebsocketReturned` added). `tools/ladders.py` updated for the `ws` ladder.
- `integration.todos`: buffered handlers and exception handlers are `async`;
  `session` is the websocket processor directly.
- Tests: `test_handlers` (async handler stubs, the `ws` processor form, an `_ok`
  async helper), `test_router` (async exception handler), all updated and passing.

## Open questions and next steps

Raised this checkpoint:

1. **Response encoding belongs to the app (open).** `without-web` ships
   `json_response`/`text_response` in `responses.py`, which bake in encoding
   decisions (`json.dumps(..., sort_keys=True)`, UTF-8, content type). Encode /
   decode / dump is an application choice; the router only needs to produce a
   `Response` (status, headers, bytes). This mirrors the schema-library-agnostic
   stance already taken for OpenAPI (`schema_for` is injected). Options to weigh:
   drop these helpers from the core, relegate them to an example/convenience layer,
   or make the serializer injectable.

2. **Middleware seeing state (open, deferred).** Middleware (`Middleware[H, S] =
   (H, S) -> H`) runs downstream of state binding and cannot read `T`, so a
   cross-cutting middleware that needs config from the lifespan state has no way to
   reach it. Widening to `(T, H, S) -> H` is a small, mechanical change across the
   `without-asgi` composition vocabulary (`stack`, `wrap`, the aliases) and both
   `dispatch` sites. This was the *read* half of the state-threading discussion;
   the *write* half (the endpoint wrapper) was dropped in favor of inline `async
   with`, but the read half still has standalone value for cross-cutting concerns
   (auth, rate limiting, config-driven behavior) and remains a reasonable next
   step.

3. **Exception-handler registration loses the precise type (open).** Handlers are
   registered as `Mapping[type[Exception], Callable[[Exception], Awaitable[Response]]]`,
   so a handler receives the base `Exception` and must narrow it
   (`assert isinstance(exc, TodoNotFound)`) before touching its fields, even though
   the key already names the type. The registration cannot take the right type
   directly, because the mapping is heterogeneous (each key wants a handler typed
   to a *different* exception) and Python cannot express "the value's parameter
   type matches the key". Options to weigh:
   - **Typed registration helper.** An `on(TodoNotFound, handler)` builder where
     `handler` is typed `(TodoNotFound) -> Awaitable[Response]`, tying the key's
     type to the handler's parameter at the call site and erasing to the
     base-typed mapping internally. Keeps the declarative, mergeable registry (it
     is implemented as `catching` middleware over the trie) and removes the
     `assert isinstance`.
   - **Lifespan-like wrapper.** The app supplies a function the processor run
     happens *inside*, so it writes ordinary `try/except SpecificType as exc` and
     gets natural narrowing, real types, and full control (re-raise, chain, async
     work). More flexible, but it drops the declarative registry and the
     framework's commit-point handling (an exception can only be mapped before
     `ResponseStart` is on the wire, currently owned by `_guarding`); the user
     would have to express that or be handed it. Note this is the same shape
     `catching`/`_guarding` already implement internally, exposed to the app.
   - **Status quo.** Keep the base-typed handler with an `assert isinstance` inside,
     if the ergonomic cost is judged acceptable.

Carried forward from Checkpoint 16, still open:

4. **No shared components / `$ref`** in the OpenAPI output; `schema_for` inlines
   every schema. Couples with the still-open `url_for` / reverse-routing item.
5. **`todos` persistence is stubbed** (`POST` echoes); a live `Context[TodoList]`
   updated by a fold would exercise the actor-model question.
6. **Opaque-mount prefixes** are literal-only.
7. **No real HTTP/WebSocket testing**: still hand-built `scope` dicts;
   `httpx.ASGITransport` + a `uvicorn` smoke run remain the best first target.
8. **Intra-workspace deps unpinned**; a packaging gap for publishing.
9. **Unconsumed input streams**: `make_asgi_app` never `aclose()`s the inbound
   stream at the boundary.
10. The **actor-model question** (`ACTOR_MODEL.md`); **static `Context` ceremony**;
    a **dynamic-merge** connector; graph/DAG recovery on `graphlib`; known-hard FRP
    problems.

Operational, carried: CI on the `proof-of-concept` branch (PR #1, draft) has two
**flaky concurrency tests** in `kv/test_shell.py`
(`test_shutdown_drains_inflight_requests`,
`test_a_client_reset_does_not_disturb_other_connections`) that pass locally.

Documentation debt (carried): `BIG_IDEA.md` still calls the model an "async
reducer" (it is an async *scan*); the functional-core / connection-as-stream /
parsing-as-a-value framings should be folded in when it is next revised.
