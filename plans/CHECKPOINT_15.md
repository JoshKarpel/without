# Checkpoint 15

A snapshot of where `without` stands, succeeding `CHECKPOINT_14.md` and
succeeded by `CHECKPOINT_16.md`. It was written mid-stream (hence the
step-by-step "Open questions and next steps" below), but the work it describes
landed green: typed request extraction, the input/output 2×2, streaming-input
routes, and extractor-recovered OpenAPI are all in and tested (mypy clean, ruff
clean, tests passing). For the prior state see `CHECKPOINT_14.md`; for the
original pitch see `BIG_IDEA.md`.

## Headline: typed request extraction for `without-web`

Checkpoint 14 left handlers reading the request by hand: a path parameter came
back as `Mapping[str, object]` (every handler did `assert isinstance(id, int)`),
and a query parameter was parsed off the raw `query_string` with its OpenAPI
schema *separately* declared on `@describe` (denormalized: two places to keep in
sync). This checkpoint replaces that with a single idea, **parsing-as-a-value**,
and folds routing, typing, and OpenAPI onto it.

## What changed since Checkpoint 14

- **New `without_web.extractors`.** `Request` is the parsed-once context (scope +
  already-parsed path params + buffered body). `Extractor[V]` is a value pairing
  a pure `extract: Request -> V` with the OpenAPI fragment it contributes, so a
  handler's parameter list and request body are *recovered* from the extractors
  it declares, never restated: one declaration, two consumers (parse and
  describe). Constructors: `path_param`, `query_param`, `header_param`, `body`,
  `catch_all`, `http_scope`, `websocket_scope`. Extraction raising rejects the
  request (mapped to a 4xx by the router's existing exception handlers); it never
  decides which handler runs. `http_scope()`/`websocket_scope()` are the escape
  hatch that dissolves the "pass the scope down vs. parse parts of it" tension:
  the raw scope is just another extractor a handler can compose alongside parsed
  ones. `Request.scope` is `HttpScope | WebsocketScope`, so one
  `query_param`/`header_param`/`path_param` token serves both protocols (the
  whole-scope read is the one place that splits, since only it knows the concrete
  type).
- **New `without_web.handlers`.** `handle(*extractors, fn=...)` is a typed
  combinator: an overload ladder (0-10 extractors) ties each extractor's type to
  `fn`'s parameters, so a `path_param("id", INT)` paired with an `fn` expecting a
  `str` is a *mypy error at the `fn=` argument*, with no runtime introspection
  (this is the key difference from FastAPI, whose decorator is not statically
  checked). It buffers the request **input** so the `body` extractor can read it,
  but does **not** force buffered output: `fn` returns `Reply = Response |
  Stream[Outbound]`, so a streaming handler (`async def ... yield`) works
  directly.
- **Method decorators `get`/`post`/`put`/`patch`/`delete`/`head`/`options`.**
  `@get(pattern, *extractors)` co-locates the route with the handler, ties the
  extractor types to its parameters, and **returns a `Route` value** (it
  registers nothing: assembly stays the explicit, declarative
  `Router(routes=(...))`, with no import-order side effects). All seven share a
  single overload ladder via a `_Method` dataclass with an overloaded
  `__call__`. Each is single-method; the `Router` now **merges `Route`s that
  share a pattern** into one method map, so the 405-vs-404 split still falls out
  of the trie. `handle` remains the lower-level builder for endpoints that are
  not routes (the `fallback`).
- **Websocket decorator `ws`.** `@ws(pattern, *extractors)` is the websocket
  sibling of `@get`: it ties typed `path_param`/`query_param`/`header_param`
  tokens to a handler that returns a `WebsocketHandler` (the frame processor) and
  returns a `WebsocketRoute`. There is no body to buffer (a handshake has none,
  so a `body` extractor is rejected), so it is synchronous: build the body-less
  `Request`, run the extractors, call the handler. This closes the
  websocket-extraction gap: a handler reads typed tokens instead of narrowing
  `match.params["id"]` with an `assert isinstance` (the path-param tying is
  exercised in `test_handlers`).
- **Streaming-input HTTP routes (`handle_stream`, `@post.stream`/...) and the
  input/output 2×2.** The input-side dual of `ws`: where `handle`/`@post` buffer
  the body and call `fn` with a finished `Request`, `handle_stream(*extractors,
  fn=...)` leaves the inbound stream untouched and hands it to `fn` as a trailing
  `Stream[Inbound]` argument, so `fn` *is* the processor (no inner function),
  reading the live stream as it arrives (a streaming upload, a long poll, a loop
  driven by request chunks). The same overload ladder ties the extractor types,
  but extractors are scope-only; a `body` extractor is rejected (buffering the
  body is exactly what a streaming route avoids), the same constraint `ws` carries
  for the handshake. Each method decorator gains a `.stream` form
  (`@post.stream(pattern, *extractors)`) via a `_StreamMethod` reached as a
  property, so the top-level names stay the seven methods. The live stream is
  deliberately *not* modeled as an extractor: an `Extractor` reads the parsed-once
  `Request` *value*, and a consume-once stream is a *place*, so it is passed as an
  argument rather than smuggled into the frozen `Request`.
  - **Output is unified across both input modes.** Buffering is a 2×2 (input ×
    output). Input is the one build-time axis (`handle` vs `handle_stream`); output
    is always whatever the handler returns, relayed by a single `_emit` dispatch
    that accepts three runtime shapes: a `Response` (encoded once), an
    `Awaitable[Reply]` (an `async def` handler awaited, then relayed), or an
    `AsyncIterator[Outbound]` (an `async def ... yield` handler streamed event by
    event). This is value dispatch on the result, not signature introspection (the
    same move as the existing `isinstance(result, Response)` check). It makes all
    four combinations expressible and adds a previously-missing one: an `async def`
    buffered handler (`-> Response`) under `@post`, which the old non-awaiting
    `_reply` silently rejected. `Reply` stays the resolved output value; a new
    `Returned = Reply | Awaitable[Reply]` is what a handler may return.
  - `todos` gains `POST /todos/import`, folding a newline-delimited stream into
    the list as it arrives (reassembling lines across chunk boundaries, echoing
    each result while later chunks are still in flight, reporting a malformed line
    in its own record since the `200` is already committed).
- **Websocket `feed` reshaped into the folding `session`.** The same fold, kept
  open and bidirectional: the id-scoped title-prefix echo became
  `/todos/session`, a body-less `@ws` route that threads a working `TodoList`
  across its inbound frames (each text frame a `NewTodo`, folded in, the created
  todo and running total sent straight back), a *scan* over the connection. A
  malformed frame is answered in-band rather than closing (the handshake is
  already accepted, the websocket analog of the import's committed `200`). This
  drops the `{todo_id}` path-param and `TodoNotFound`-before-accept demos from
  the example (both still unit-tested), trading them for the more interesting
  stateful-fold story; the per-frame parse rejection replaces the handshake-time
  `TodoNotFound`, so the socket router no longer needs an exception handler.
- **Patterns are strings and t-strings; the name gap is closed.** A route pattern
  is `str | Template`: a plain string for a literal-only path (`@get("/todos")`)
  or a t-string (PEP 750, Python 3.14) interpolating path-param tokens
  (`@get(t"/todos/{todo_id}", todo_id, ...)`). The `"/todos/{id:int}"` string
  mini-language and `parse_pattern` are gone. One `path_param(...)` token is the
  *single source of truth*: interpolated into the t-string (the segment, matched
  and schemed through its converter) **and** passed in the positional extractor
  list (the typed read), since a `Template` erases its interpolation types. A
  brace in a *plain* string is a build error steering to the t-string form, not a
  route that silently never matches. This bumped the workspace floor to **Python
  3.14** (`requires-python = ">=3.14"`, `.python-version`, CI matrix). Because a
  pattern never *names* a converter (a token carries the value), the
  **converter-by-name registry indirection is also gone**: `Converter` is a typed
  value carrying its own `name`/`parse`/`schema`, tokens carry the converter
  *value* straight into the trie, and `build`/`walk`/`Router` no longer thread a
  registry. Extending = construct a `Converter` and use it; `DEFAULT_CONVERTERS`
  is gone (it had no remaining consumer), and the built-ins are exported as
  values (`STR`/`INT`/`FLOAT`/`UUID`/`PATH`).
- **OpenAPI recovered from the extractors.** Query/header/body schemas come from
  the very extractors that parse them (a `HeaderParam` type was added);
  path-param schemas come from the converter value on the segment. `@describe` is
  no longer needed in `todos` (its job is subsumed), though it and `buffered`
  remain for bring-your-own handlers.
- **`todos` rewritten** to decorators + t-string/string patterns. Every `assert
  isinstance` and the hand-rolled `done_filter`/`_path_id` are gone; handlers are
  plain functions of typed values.
- **Tests** updated/added: `test_extractors`, `test_handlers` (incl. a streamed
  response), and rewrites of `test_trie`/`test_patterns`/`test_router`/
  `test_openapi` to t-string/string patterns + converter values.

## Rationale: a few decisions worth remembering

- **Parsing is a value, so "raw scope" and "parsed part" compose instead of
  competing.** Once an extractor is a pure `Request -> V`, a handler declares
  exactly the pieces it wants (raw or parsed) and gets them typed. This is the
  same single-source-of-truth / recover-from-structure move as Checkpoint 14's
  `openapi()`, now extended to all four input locations (path, query, header,
  body).
- **The decorator annotates; it does not register.** This is the deliberate
  split from FastAPI: co-locating the route declaration with the handler (you
  can't understand the handler without its params) is good, but *which table the
  route lives in* is a separate, assembly-time concern that stays declarative.
  So `@get` returns a `Route` value and the one-shot immutable `Router` is
  preserved with no mutable builder.
- **Static tying without magic has a known ceiling.** The overload ladder ties
  types for 0-10 extractors; beyond that Python cannot infer a record type from a
  variadic list of tokens, so the fallback is a body/query *model* extractor (one
  extractor collapsing many fields), which keeps real arity small. There is no
  runtime signature introspection anywhere.
- **Input buffering is the one build-time axis; output is never forced.** A
  buffered-input endpoint (`handle`/`@post`) consumes the inbound stream cleanly
  and hands the handler typed values, never the raw body; a streamed-input
  endpoint (`handle_stream`/`@post.stream`) hands the live stream through instead.
  Output is orthogonal and never force-buffered: the handler returns a `Response`,
  awaits one, or streams `Outbound`, relayed by `_emit`. That is the full 2×2,
  with input the only mode chosen at build time.
- **Two covariances are stated explicitly.** `Converter[V]` and `Extractor[V]`
  use a legacy `TypeVar(covariant=True)` because PEP 695's inferred variance
  treats a frozen dataclass field as invariant; the variance is sound (`V` only
  in return position) and is what lets a heterogeneous mix collect as
  `Extractor[object]`. `Converter` equality/hash is by `name` alone (it is a trie
  key), so `parse`/`schema` are `compare=False`.

## Status

Green and verified (mypy clean, ruff check + format clean, tests passing):

- `without_web`: `extractors`, `handlers` (incl. the method decorators and the
  streaming-input `handle_stream`/`@post.stream` siblings), string/t-string
  `Pattern`, value-carrying `Converter`/trie, merge-by-pattern `Router`,
  extractor-recovered OpenAPI. Re-exports and `__all__` updated.
- `integration.todos`: rewritten to decorators + t-string/string patterns, plus a
  streaming `POST /todos/import` and a folding `/todos/session` websocket.

## Open questions and next steps

Raised this checkpoint (the in-progress edges):

1. **Websocket extraction (resolved this checkpoint).** `@ws` now ties typed
   `path_param`/`query_param`/`header_param` tokens to a websocket handler, so a
   handler reads them instead of narrowing `match.params["id"]` by hand
   (exercised in `test_handlers`, since the reshaped `todos` session is now
   body-less and path-param-free). One small residual:
   `http_scope()`/`websocket_scope()` recover the concrete scope with a runtime
   `assert` (the `Request.scope` union is not statically tied to the route's
   protocol); using the wrong one fails loud rather than at compile time.
2. **Live input streaming (resolved this checkpoint).** `handle_stream` and the
   `@get.stream`/`@post.stream`/... method decorators register a handler that
   reads the inbound HTTP stream and reacts as it arrives. The handler *is* the
   processor: it takes the state, the typed extractor values, and the live
   `Stream[Inbound]` as a trailing argument (no inner function), the input-side
   dual of `ws`. A `body` extractor is rejected (it would force the buffering a
   streaming route avoids). Output is free and unified across both input modes via
   `_emit` (see the 2×2 bullet above), so all four input/output buffering combos
   are expressible. `todos`' `POST /todos/import` exercises stream-in/stream-out
   end-to-end. One residual: the stream is not an extractor (it is a place, not a
   value), so the handler reaches it as a positional argument rather than through
   the typed extractor list, and a single route mixing buffered-body parsing with
   live streaming is still not expressible (by design: the two are distinct input
   contracts).
3. **Arity ladders cap at 10 extractors.** `handle`, the `@get`/`@post`/...
   decorators (`_Method.__call__`), and `into` each carry an overload ladder up
   to 10 (the ladders are generated, not hand-written). Past 10, the intended
   path is `into(make, *extractors)`: it combines several extractors into one
   typed value, tying each extractor's type to `make`'s constructor params,
   reusing the existing tokens, and merging their OpenAPI fragments. Dataclass/
   `NamedTuple` constructors work positionally; a pydantic model (keyword-only
   init + validators) is wrapped in a small factory lambda, so a rejecting
   validator raises for the exception handlers to map. `into` can also nest for
   the rare >10 case.
4. **`@describe` / `buffered` are now legacy-ish in `todos`.** They remain for
   BYO handlers but the example no longer uses them; decide whether they stay
   public or fold into the extractor world.
5. **No `url_for` / reverse routing** (carried from CP14): the t-string pattern now
   makes a token-based reversal more natural than the string form did.

Carried from Checkpoint 14, still open:

6. **`todos` persistence is stubbed** (`POST` echoes); threading a live
   `Context[TodoList]` updated by a fold would exercise the actor-model question.
7. **Opaque-mount prefixes** are literal-only (now via `split_path`, not the
   removed `parse_pattern`).
8. **No real HTTP/WebSocket testing**: everything is still hand-built `scope`
   dicts; `httpx.ASGITransport` + a `uvicorn` smoke run remain the best first
   target, more so now that more routing rides on t-string patterns.
9. **Intra-workspace deps unpinned**; a packaging gap for publishing.
10. **Unconsumed input streams**: `make_asgi_app` never `aclose()`s the inbound
    stream (`handle` does drain it, but the boundary cleanup is still open).

Carried forward from earlier checkpoints (still open):

- The **actor-model question** (`ACTOR_MODEL.md`), with open question 6 as its
  concrete landing spot.
- **Static `Context` ceremony**; a **dynamic-merge** connector; graph/DAG
  recovery on `graphlib`; known-hard FRP problems.

Operational, carried: CI on the `proof-of-concept` branch (PR #1, draft) has two
**flaky concurrency tests** in `kv/test_shell.py`
(`test_shutdown_drains_inflight_requests`,
`test_a_client_reset_does_not_disturb_other_connections`) that pass locally;
unrelated to this checkpoint's work but still red in CI.

Documentation debt (carried): `BIG_IDEA.md` still calls the model an "async
reducer" (it is an async *scan*); the functional-core / connection-as-stream /
lifecycle-as-a-portable-value framings, and now the parsing-as-a-value
extractor story, should be folded in when it is next revised.
