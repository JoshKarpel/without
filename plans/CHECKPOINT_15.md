# Checkpoint 15 (IN PROGRESS)

A snapshot of work **still underway**, succeeding `CHECKPOINT_14.md`. Unlike the
previous checkpoints, this one is written mid-stream: the design has settled and
the code is green (mypy clean, ruff clean, tests passing) at each step described
below, but the feature set is not finished. See "Open questions and next steps"
for what remains. For the prior state see `CHECKPOINT_14.md`; for the original
pitch see `BIG_IDEA.md`.

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
  websocket-extraction gap; `feed` no longer reads `match.params["id"]` with an
  `assert isinstance`.
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
- **Buffer the input, never force-buffer the output.** Input buffering is
  unconditional (we never hand the raw body onward, and it consumes the inbound
  stream cleanly), but the output may stream.
- **Two covariances are stated explicitly.** `Converter[V]` and `Extractor[V]`
  use a legacy `TypeVar(covariant=True)` because PEP 695's inferred variance
  treats a frozen dataclass field as invariant; the variance is sound (`V` only
  in return position) and is what lets a heterogeneous mix collect as
  `Extractor[object]`. `Converter` equality/hash is by `name` alone (it is a trie
  key), so `parse`/`schema` are `compare=False`.

## Status (NOT done)

Green and verified at the current step (mypy clean, ruff check + format clean,
tests passing):

- `without_web`: `extractors`, `handlers` (incl. the method decorators),
  string/t-string `Pattern`, value-carrying `Converter`/trie, merge-by-pattern
  `Router`, extractor-recovered OpenAPI. Re-exports and `__all__` updated.
- `integration.todos`: rewritten to decorators + t-string/string patterns.

## Open questions and next steps

Raised this checkpoint (the in-progress edges):

1. **Websocket extraction (resolved this checkpoint).** `@ws` now ties typed
   `path_param`/`query_param`/`header_param` tokens to a websocket handler, so
   `feed` no longer narrows `match.params["id"]` by hand. One small residual:
   `http_scope()`/`websocket_scope()` recover the concrete scope with a runtime
   `assert` (the `Request.scope` union is not statically tied to the route's
   protocol); using the wrong one fails loud rather than at compile time.
2. **No route reads the input stream live.** `handle` (and so every
   `@get`/`@post`) buffers the request body before the handler runs, and `fn`
   gets a finished `Request`. There is no decorator/extractor way to register a
   handler that *reads* the inbound HTTP stream and reacts to it as it arrives
   (a streaming upload, a long poll, a server-sent loop driven by request
   chunks); you must drop to a raw `route()` with a hand-written
   `(state, scope) -> handler` processor. The output side already streams
   (`Reply`); the input side does not. This is the dual of the websocket gap:
   both are about live input.
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
