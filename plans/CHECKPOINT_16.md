# Checkpoint 16

A snapshot of where `without` stands, succeeding `CHECKPOINT_15.md`. For the
prior state see `CHECKPOINT_15.md`; for the original pitch see `BIG_IDEA.md`.

Two threads landed this checkpoint, in the Kent Beck order ("make the change
easy, then make the easy change"): first a permanent code generator for
`without-web`'s `@overload` ladders (the *make it easy* step), then first-class
streaming/sequential bodies in the OpenAPI output (the *easy change*, which the
generator turned into a one-line spec edit). Everything is green: mypy clean (68
source files), 255 tests, full pre-commit clean including the new generator hook.

## What changed since Checkpoint 15

- **The overload ladders are generated, not hand-maintained.** `without-web`
  carries six near-identical `@overload` ladders (`handle`, `handle_stream`,
  `_Method.__call__`, `_StreamMethod.__call__`, `ws`, `into`), each ~11 stubs
  over arity 0-10. They are pure mechanical repetition, and every parameter
  change was an N×M hand-edit across ~66 blocks (the immediate trigger: adding
  `request_body` to the two streaming ladders). They are now generated:
  - **`tools/ladders.py`** holds the per-ladder spec (leading params, the `fn`
    shape, return type, keyword tail) and an `emit(name)` that builds the stubs.
    It owns the `A,B,C,D,E,F,G,H,J,K` TypeVar sequence (skipping `I`) and the
    `[T, ...]`/`[M, ...]` generics, so none of that is transcribed by hand. It is
    deliberately *outside* the shipped `without_web` package (run via `cog -I
    tools`), so codegen machinery never ships.
  - **[`cog`](https://cog.readthedocs.io/)** (Ned Batchelder's, dev-only dep
    `cogapp`) inlines the output between `# [[[cog ... ]]]` / `# [[[end]]]`
    markers in `handlers.py` and `extractors.py`. The generator spec sits in the
    marker comment; the output sits right below it.
  - **`tools/regenerate.sh`** runs `cog -r` *then* `ruff format` as one unit, and
    the pre-commit hook `generate-ladders` calls it. This is the key idempotency
    move: `cog` alone is not a formatter fixed point (it does not mirror ruff's
    blank-line rules), so a cog-only hook would rewrite on every run and never
    pass; running cog-then-ruff makes the hook's net output the *formatter's*
    fixed point. No `--check` flag is needed: pre-commit already fails CI if any
    hook modifies a file.
  - Replacing the hand-written ladders was a pure marker insertion in
    `handlers.py` (zero content change) and a one-time reformat of `into` in
    `extractors.py` (its short overloads expand to one-param-per-line, kept stable
    by a magic trailing comma). Overloads now have a blank line between them
    (owned by ruff: two at module level, one inside the decorator classes).
  - The arity cap is still 10 (past that, `into` collapses extractors into one
    model); bumping it is now a one-number change in `ladders.py`.

- **Streaming / sequential bodies are first-class in the OpenAPI output.** The
  spec types modeled content as "one document, one `schema`", which is a lie for
  a stream (an NDJSON/SSE body is not a single JSON value). Replaced with a sum
  type that picks the right OpenAPI 3.2 keyword:
  - **`Single(schema)` / `Sequence(item_schema)`** (alias `Shape`), carried by a
    **`Body(media_type, shape)`**. `Single` renders `schema`; `Sequence` renders
    OpenAPI 3.2's **`itemSchema`** (each item's shape, for a sequential media
    type). `RequestBodySpec` is gone, folded into `Body` (one type for request
    and response content); `ResponseSpec` is now `description` + `body: Body |
    None`. The document version is bumped `3.1.0` → `3.2.0`.
  - **`without-web` stays wire-agnostic.** It does not enumerate NDJSON vs SSE vs
    `json-seq`: the media type is an app-supplied string (as `body()` already
    took one), and the handler emits the bytes itself. `Single`/`Sequence` is
    **documentation only**: nothing on the runtime path (`_emit`, the stream
    processor) reads it; it only chooses which key the renderer emits. The danger
    to guard against is ever making `Sequence` insert NDJSON newlines or SSE
    `data:` prefixes: that is framing, which is the app's.
  - **Streaming routes can now describe their inbound stream.** `handle_stream`
    and `@post.stream`/... gained a description-only `request_body: Body | None`.
    It does *not* parse (the handler consumes the live stream by hand); a `body`
    extractor is still rejected. The asymmetry with a buffered route (whose body
    schema rides its `body` extractor) is inherent: a buffered body is
    parsed-as-a-value, a streamed body is consumed-as-a-place, so its description
    cannot ride on an extractor and is passed alongside `responses=` instead.
  - **`todos` exercises it end to end.** `POST /todos/import` now declares
    `request_body=Body("application/x-ndjson", Sequence(NewTodo))` and a `200`
    whose body is `Sequence(import_result_schema)`, the result item a `oneOf` of
    the success and error shapes (the same multi-variant payload an SSE/event
    stream documents with `itemSchema` + `oneOf`).

- **API surface.** `Body`, `Single`, `Sequence` are exported; `RequestBodySpec`
  is removed. `ResponseSpec(media_type=..., schema=...)` becomes
  `ResponseSpec(description=..., body=Body(...))`. Emitted documents are
  `3.2.0`, so rendering a stream needs `itemSchema`-aware tooling (description
  only, no runtime impact).

## Rationale: a few decisions worth remembering

- **Codegen via an in-file tool, not a bespoke region-rewriter.** `cog` keeps the
  generator spec co-located with its output and is a mature single-purpose
  dependency, which clears the supply-chain bar better than hand-rolled marker
  surgery. The generator lives in `tools/` so it never ships.
- **cog-then-ruff in one hook is what makes regeneration idempotent.** The
  generator never has to reproduce the formatter's whitespace rules; it emits a
  reasonable canonical form (magic trailing commas to prevent collapse) and lets
  ruff own spacing. The combined hook's output is the formatter fixed point.
- **`Single`/`Sequence` is documentation-only metadata.** It carries a
  `SchemaRef`, never a parser or formatter, and the runtime path never reads it.
  This keeps the wire mechanism (relay raw `Outbound`) and the wire description
  (`Body` → OpenAPI) fully decoupled; the app remains the single source of the
  bytes *and* the truth they are documented against.
- **A streamed body's description is passed, not recovered.** Consistent with
  CP15's "the stream is a place, not a value": an `Extractor` reads the
  parsed-once `Request` value, so a consume-once stream cannot be one, and its
  OpenAPI fragment is declared next to `responses=` rather than smuggled into an
  extractor.

## Status

Green and verified (mypy clean, ruff check + format clean, tests passing):

- `without_web`: `openapi` (`Single`/`Sequence`/`Body`, `itemSchema`, 3.2.0),
  `extractors` (`body()` builds `Body(Single(...))`), `handlers` (streaming
  `request_body`), generated overload ladders, `__init__` re-exports.
- `tools/ladders.py` + `tools/regenerate.sh` + the `generate-ladders` pre-commit
  hook; `cogapp` added to the dev group; `uv.lock` updated.
- `integration.todos`: `POST /todos/import` declares its NDJSON in/out streams.
- Tests: `test_openapi` gains an `itemSchema` case; `test_extractors`,
  `test_handlers`, and the `todos` integration test updated to `Body`/`Single`/
  `Sequence` and the new streaming description.

## Open questions and next steps

Raised or resolved this checkpoint:

1. **Inbound streaming description (resolved).** A `@post.stream` route can now
   document its inbound sequence via `request_body=Body(..., Sequence(...))`,
   closing CP15's gap where a streaming route described its output but never its
   input. Still by design: a single route cannot mix a buffered-body extractor
   with live streaming (distinct input contracts).
2. **OpenAPI 3.2 tooling maturity.** `itemSchema` is young (3.2 shipped Sep
   2025); generator/UI support is still catching up. Since the app owns the bytes
   and this is description-only, that is a documentation-tooling risk, not a
   runtime one. A 3.1 fallback (`schema` describing one item + a prose note) is
   deliberately *not* built until something needs it.
3. **No shared components / `$ref`.** `schema_for` inlines every schema, and the
   `import_result` `oneOf` is inline; there is no components section or `$ref`
   reuse yet. Couples with the still-open **`url_for` / reverse routing** item.
4. **`@describe` / `buffered` remain legacy-ish** (carried from CP15): kept for
   bring-your-own handlers, unused by the example.

Carried forward, still open:

5. **`todos` persistence is stubbed** (`POST` echoes); a live `Context[TodoList]`
   updated by a fold would exercise the actor-model question.
6. **Opaque-mount prefixes** are literal-only.
7. **No real HTTP/WebSocket testing**: still hand-built `scope` dicts;
   `httpx.ASGITransport` + a `uvicorn` smoke run remain the best first target.
8. **Intra-workspace deps unpinned**; a packaging gap for publishing.
9. **Unconsumed input streams**: `make_asgi_app` never `aclose()`s the inbound
   stream at the boundary.
10. The **actor-model question** (`ACTOR_MODEL.md`); **static `Context`
    ceremony**; a **dynamic-merge** connector; graph/DAG recovery on `graphlib`;
    known-hard FRP problems.

Operational, carried: CI on the `proof-of-concept` branch (PR #1, draft) has two
**flaky concurrency tests** in `kv/test_shell.py`
(`test_shutdown_drains_inflight_requests`,
`test_a_client_reset_does_not_disturb_other_connections`) that pass locally.

Documentation debt (carried): `BIG_IDEA.md` still calls the model an "async
reducer" (it is an async *scan*); the functional-core / connection-as-stream /
parsing-as-a-value framings should be folded in when it is next revised.
