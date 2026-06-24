# Checkpoint 9

A snapshot of where `without` stands, succeeding `CHECKPOINT_8.md`. For the
original pitch see `BIG_IDEA.md`; for the critical review and open design
questions see `REVIEW_BIG_IDEA.md`; for the actor-model digression see
`ACTOR_MODEL.md`.

## What changed since Checkpoint 8

This session was entirely about `without-asgi`, taking it from a thin
proof-of-concept boundary to a near-complete, spec-faithful ASGI adapter. The
work was driven conversationally, one design decision at a time, and the package
grew enough that it was reorganized into focused modules at the end.

- **A `narrow` parse helper replaced the hand-rolled `_as_str`/`_as_bytes`
  boilerplate (`narrow.py`).** `narrow(value, type)` does one strict
  `isinstance`-narrow-or-raise; `narrow_to_str` / `narrow_to_bytes` /
  `narrow_to_int` are the named common cases. Real functions, not
  `functools.partial`, because the partial lost its return type under mypy
  without an annotation crutch. This is the package's only generic parse
  primitive; everything else is a thin `_as_*` built on it. ASGI scopes are
  in-process dicts of already-decoded Python objects (bytes, not JSON), so the
  relevant operation is strict type-narrowing, not deserialization, which is
  also why `msgspec`/`pydantic` were considered and declined for the leaf
  parsing.
- **The raw ASGI surface is now named `Raw*` (`RawScope`, `RawMessage`).** This
  frees the clean names for the parsed types and makes the raw-versus-parsed
  boundary visible, which is the whole point of parse-don't-validate.
- **Scopes are a typed union, parsed once at the boundary.** `HttpScope`,
  `WebsocketScope`, and `LifespanScope` (plus the shared `Asgi` version value)
  now model *every* field the ASGI spec defines, with per-field docstrings
  quoting the spec and each class docstring linking to its spec section. The
  parsers enforce the spec's required-versus-optional split strictly (a missing
  required field raises). `type ConnectionScope = HttpScope | WebsocketScope`
  and `type Scope = ConnectionScope | LifespanScope`; `parse_scope` dispatches
  on the `type` discriminator. The earlier `scope_type(scope) -> str` helper was
  a parse-don't-validate smell (read a field, branch on the bare string, re-parse
  the same dict); it was folded into `parse_scope` and removed.
- **Every ASGI extension from the reference docs is modeled.** HTTP response
  extensions (`http.response.push`, `zerocopysend`, `pathsend`, `early_hint`,
  `trailers`, `debug`) join the `Outbound` union with `encode_outbound` cases;
  the generic `extensions` scope key is captured as a `Mapping`; the `tls`
  extension is read into a typed `Tls` by `parse_tls`; and the WebSocket denial
  response (`websocket.http.response.start`/`body`) is part of the WebSocket
  outbound union. The `trailers` flag was added to `ResponseStart` since the
  Trailers extension needs it.
- **WebSocket support is complete in both directions.** Inbound
  (`parse_websocket_inbound`: connect / receive / disconnect) and outbound
  (`encode_websocket_outbound`: accept / send / close / denial response), with
  `shell.py` gaining `websocket_inbound` / `websocket_outbound` stream and sink
  mirroring the HTTP pair. A websocket data frame is modeled as
  `WebsocketData = WebsocketText | WebsocketBinary`, so the spec's "exactly one
  of text/bytes" invariant is unrepresentable rather than runtime-checked.
- **`with_lifespan` was renamed `make_asgi_app(lifespan, handler)`.** It is the
  ASGI entrypoint, so the factory name reads better than the wrapper name, and
  its second parameter is now `handler` rather than `app` (the returned callable
  is the app). It also became the single place that parses each raw scope into a
  typed value and dispatches: a `LifespanScope` drives the lifespan protocol, a
  `ConnectionScope` is handed to `handler`, which therefore never sees the
  lifespan scope.
- **The original `core.py` was split into `types` / `scope` / `inbound` /
  `outbound`.** `types` keeps the lean shared base: the raw ASGI surface and the
  shared `WebsocketData` payload that both directions use. `scope` holds the
  connection/lifespan scopes (and `Tls`) with their parsing; `inbound` holds
  received-event types and their `parse_*`; `outbound` holds sent-event types and
  their `encode_*`. `scope`, `inbound`, and `outbound` all depend only on `types`
  (never each other). The test modules were split the same way (`test_asgi_scope`
  / `test_asgi_inbound` / `test_asgi_outbound`).
- **A package `CLAUDE.md` was added (`packages/without-asgi/CLAUDE.md`).** It
  links the ASGI spec, records that the dataclasses cover every field, and notes
  the `django/asgiref` GitHub `specs/` rST as a fallback when readthedocs
  rate-limits (which it did, repeatedly, this session).

## Rationale: a few decisions worth remembering

- **Defaults belong on app-constructed events, not parser-constructed ones.**
  Outbound events, websocket sends, lifespan replies, and `Response` are built by
  hand, so they carry the spec's optional-field defaults (`WebsocketClose()`,
  `ResponseStart(status=200)`, `ZeroCopySend(file=f)` all work). Scopes and
  inbound events are built only by a `parse_*` function that already supplies
  every field via `.get(default)`, so a field default there would be dead code
  and would mask a parser that forgot a field. The dividing line is *who
  constructs it*.
- **`scope["state"]` is deliberately not surfaced.** ASGI's `state` namespace is
  the standard way to pass lifespan-cycle data into each request. `without`
  already does this explicitly: `make_asgi_app` threads the lifespan value to
  `handler` through its internal `_Cell`. Surfacing `state` too would be a
  redundant, parallel state path, so the scopes omit it and say so in their
  docstrings.
- **Extensions negotiation stays app policy.** The adapter represents
  `scope.extensions` and provides the typed event + encoder; the app checks the
  advertised dict before emitting an extension event, the same way it owns which
  status codes it sends. The adapter does not gate it.

## Status

Done and verified (mypy strict clean across 40 source files, 140 tests passing,
ruff lint + format clean):

- `without-asgi`: full scope/event/extension coverage of the HTTP, WebSocket,
  and lifespan sub-specs plus the TLS extension, parsed and encoded at the
  boundary, organized into `types` / `scope` / `inbound` / `outbound` / `shell`
  / `lifespan` modules, with `narrow` underneath and a package `CLAUDE.md` above.
- `without_integration.flags`: unchanged behavior, now wired on `make_asgi_app`,
  receiving an already-parsed `ConnectionScope` and rejecting `WebsocketScope`
  with `NotImplementedError` (the app is HTTP-only by design).

## Open questions and next steps

Resolved this session:

- **Scope of `without-asgi` (Checkpoint 8 #3).** No longer "representable but
  unimplemented": HTTP, WebSocket, lifespan, and every reference extension are
  modeled and round-trip through `parse_*` / `encode_*`.

Still open, carried from earlier checkpoints:

1. **No real HTTP/WebSocket testing (Checkpoint 8 #1), now more pressing.**
   Everything is still driven by hand-built `scope` dicts, scripted `receive`,
   and capturing `send`. The surface is now large (dozens of event and extension
   encoders) and none of it has been exercised against a real server. A few
   `httpx.ASGITransport` tests and a `uvicorn` smoke run over `make_asgi_app`
   would catch conformance gaps the hand-written `send`-capture cannot (header
   casing, body framing, the handshake as a server drives it, extension
   message shapes).
2. **Routing fidelity (Checkpoint 8 #2).** `Router` still matches `(method,
   path)` exactly; path parameters and a decorator ergonomic remain deferred.
3. **WebSocket is modeled but unused.** `make_asgi_app` dispatches a
   `WebsocketScope` to the handler, the streams exist, but no example handles a
   websocket connection (flags rejects it). A worked websocket example (echo, or
   a flags-over-websocket feed) would turn the modeling into evidence and
   exercise `websocket_inbound`/`websocket_outbound` end to end.
4. **Extensions are modeled but never negotiated in anger.** No example reads
   `scope.extensions` and emits an extension event (e.g. zero-copy/path-send for
   a file response). A small `supports(scope, name)` ergonomic helper plus one
   worked use would validate the negotiation story.
5. **A non-ASGI shell would make the portability promise concrete (Checkpoint
   8).** `make_asgi_app` consumes a portable `Lifespan[T]`; nothing yet drives
   the same lifespan from a queue processor or CLI to prove the seam.
6. **Intra-workspace deps are unpinned, a packaging gap for publishing.**
   `without-env`, `without-configmap`, and `without-asgi` each declare a bare
   `"without"` dependency with no version constraint. The packages are published
   in lockstep (one shared version derived from the release tag, stamped onto
   each via `uv version` in `.github/workflows/publish.yml`), but that step only
   sets each package's *own* version and never constrains the cross-dependency.
   So a published `without-asgi X.Y.Z` ships `Requires-Dist: without` with no
   lower bound, letting pip pair it with a mismatched `without`. Before the
   first real release, either have the publish step also pin each intra-workspace
   dep to the shared version (`without==X.Y.Z`), or commit a lower bound in each
   `pyproject.toml` and bump it on release. The first keeps source at `0.0.0` and
   derives everything from the tag.

Carried forward from earlier checkpoints (still open):

- **The actor-model question** (`ACTOR_MODEL.md`, open question #2). Unchanged;
  `flags` is still stateless and reads a `Context` rather than messaging a shared
  fold. A stateful ASGI or websocket example would add pressure.
- **Static `Context` ceremony** (open question #3): the dynamic half remains
  demonstrated; the static-config-as-plain-value question is unchanged.
- A **dynamic-merge** connector as the single sanctioned funnel; whether
  `serve`'s consumer parameter deserves a named `Leaf` type; factoring the
  connection-set + bounded-drain orchestration out of `kv`.
- Deferred deliberately: graph/DAG recovery and visualization on `graphlib`;
  known-hard FRP problems (diamond glitches, feedback cycles, teardown order); a
  deterministic "await next update" signal to replace `testing.tick`.

Documentation debt (carried forward): `BIG_IDEA.md` and the early checkpoints
still call the model an "async reducer"; it is more accurately an **async scan**,
and the functional-core / imperative-shell, connection-as-stream, and
lifecycle-as-a-portable-value framings should be folded into `BIG_IDEA.md` when
it is next revised.
