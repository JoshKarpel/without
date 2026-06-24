# Checkpoint 13

A snapshot of where `without` stands, succeeding `CHECKPOINT_12.md`. For the
original pitch see `BIG_IDEA.md`; for the critical review and open design
questions see `REVIEW_BIG_IDEA.md`; for the actor-model digression see
`ACTOR_MODEL.md`.

## What changed since Checkpoint 12

One focused pass on the middleware story, prompted by disliking the hand-written
stream plumbing in the `transform` example's `access_log`. The plumbing was lifted
into a single core primitive plus one routing convenience, and the example grew
two stateful middleware that map out the whole design space.

- **Core gains `compose`, and `pipe` is gone (`without.wiring`).** `compose[A, B,
  C](first: Processor[A, B], second: Processor[B, C]) -> Processor[A, C]` composes
  two processors on the event edge and may change the stream type at the join. It
  is the processor→processor sibling of the old `pipe` (source→processor), which
  was just `processor(source)` and earned less than `compose` (which builds a
  reusable processor without needing a source). `pipe` had two call sites, both
  tests, now written as direct application; its test became two `compose` tests
  (order, and a type-changing join).
- **`without-asgi` routing gains `wrap` (`without_asgi.routing`).** `wrap[In, Out,
  S](*, inbound=None, outbound=None) -> Middleware[Processor[In, Out], S]` lifts
  scope-aware edge transformer(s) into a `Middleware`, saving the `(handler,
  scope) -> handler` wrapper and the inner `compose`. It is *type-preserving* by
  construction (each side is `Stream[X] -> Stream[X]`), so the result is an
  endomorphism, which is exactly what `stack` composes and what the ASGI boundary
  fixes (`Processor[Inbound, Outbound]` in, same out). The `Middleware[H, S]` name
  is unchanged; core owns `compose`, not a `Middleware` type, so there is no
  collision and no rename.
- **The example's middleware now span the full taxonomy (`transform.app`).** Three
  shapes, picked by what the middleware needs:
  - `access_log = wrap(inbound=log_request, outbound=log_response)`: two
    *independent* ends with no shared state, so one `wrap` bundles them.
    `with_header` and `socket_log` are the single-edge cases (`wrap(outbound=...)`
    / `wrap(inbound=...)`).
  - `access_timing`: needs one value (the start time) spanning the whole handling.
    That is a plain local in a single output-wrapping processor (the first output
    pull drives the handler, so the local is set as handling begins and read once
    the stream ends). No inbound transform, no second closure, no `nonlocal`.
  - `request_digest`: the genuine both-sides case. The inbound end feeds each body
    chunk into a `hashlib.sha256`; the outbound end reads the finished digest into
    an `x-request-digest` response header (the way S3 returns an `ETag` of an
    uploaded object). The shared state is the hasher, *mutated in place*, so still
    no `nonlocal`, and `compose` wraps both ends around the handler.
- **Tests and docs kept in sync.** New `test_compose_*` in `without`; new
  `test_access_timing_*` and `test_request_digest_*` in the `transform` example
  (the digest test is deterministic against `sha256(body)`). The `without-asgi`
  README documents `wrap`; `ACTOR_MODEL.md` and `REVIEW_BIG_IDEA.md` had their
  `pipe` references updated to `compose`.

## Rationale: a few decisions worth remembering

- **`compose` is the primitive; `wrap` is the sugar.** A stream transformer *is* a
  `Processor`, so wrapping a handler's edge is just composition. `compose` names
  that on the event edge; `wrap` binds the scope and composes for the common
  stateless case. Anything stateful drops to a plain `Middleware` and uses
  `compose` directly.
- **`compose` is binary on purpose.** A binary `compose[A, B, C]` is cleanly typed
  and may change the join type. A variadic compose would hit an un-typeable chain
  (each link a different arrow), the same reason `wrap` takes a single transform
  per side rather than a list. Nest for three or more stages.
- **Middleware that stacks must be type-preserving.** `stack` composes `H -> H`,
  and the ASGI boundary fixes `H = Processor[Inbound, Outbound]`, so a stackable
  middleware is necessarily an endomorphism. That is why `wrap` is type-preserving
  while `compose` (the general, type-changing arrow) lives in core.
- **`nonlocal` is avoidable, twice over.** Duration needs only a value spanning
  the handler, which is a local in one output-wrapping processor (no cross-end
  state at all). A genuine both-ends middleware shares state by *mutating a holder
  in place* (the hasher), never rebinding a name. `nonlocal` only appears if you
  insist on rebinding a closure variable from a second generator, which neither
  case requires.
- **`wrap`'s dual form is bundling, not interaction.** Two independent edges in
  one `wrap` is a convenience equivalent to two single-edge wraps ("might as well
  be separate calls"); it can never serve a stateful middleware, because the
  shared state is per-request and lives in a closure the plain `Middleware` form
  creates. Kept as a convenience; `access_log` uses it.
- **`Processor.__call__`'s parameter name is part of the contract.** A function
  passed where a `Processor` is expected must name its parameter `inputs` to match
  the protocol structurally; closures handed to `compose` follow that.

## Status

Done and verified (mypy strict clean across 43 source files, the test suite green
at 157 passing, ruff lint + format clean):

- `without.wiring`: `compose` added, `pipe` removed; `__init__` re-exports and the
  module-top edge comment updated.
- `without_asgi.routing`: `wrap` added (type-preserving, scope-binding, built on
  `compose`); `__all__` updated; `stack`'s loop variable renamed to avoid
  shadowing the new `wrap`. `Middleware` / `HttpMiddleware` / `WebsocketMiddleware`
  and `buffered` unchanged.
- `integration.transform.app`: `access_log` / `with_header` / `socket_log` via
  `wrap`; `access_timing` (single output-wrapping processor) and `request_digest`
  (both-edges, shared hasher, `compose`) added and wired into the HTTP stack.

## Open questions and next steps

Raised this checkpoint:

1. **`request_digest`'s ordering assumption.** It is correct only because the
   `@buffered` routes read the whole request before responding, so the digest is
   complete when `ResponseStart` flows out. A handler that streamed its response
   before draining the request would digest only the chunks seen by then. Fine for
   the example; a streaming-aware variant (digest in a trailer) would generalize.
2. **`wrap`'s dual form.** Kept as a convenience for two independent stateless
   edges, but it is strictly equivalent to two single-edge wraps. Worth revisiting
   if it reads as unjustified; making it single-edge would push `access_log` to
   two stack entries.

Carried from Checkpoint 12, still open:

3. **Generic `Router[T, S, H]` vs two concrete routers.** The example's router is a
   single protocol-generic type with a per-route match predicate; a real
   two-protocol app might prefer two small concrete routers. Left generic.
4. **`Settings.max_bytes` is core config only the shell reads.** Splitting the
   shell-only transport limit out of the domain config is a reasonable next move.

Carried from Checkpoint 11, still open:

5. **No real HTTP/WebSocket testing.** Everything is still driven by hand-built
   `scope` dicts, scripted `receive`, and capturing `send`. A few
   `httpx.ASGITransport` tests plus a `uvicorn` smoke run over `make_asgi_app`
   would catch conformance gaps. Still the best first target.
6. **Routing fidelity.** Dispatch still matches `(method, path)` or `path`
   exactly; path parameters and a route-registration decorator remain deferred.
7. **Extensions are modeled but never negotiated in anger.** A small
   `supports(scope, name)` helper plus one worked use would validate the story.
8. **A non-ASGI shell would make the portability promise concrete.** Nothing yet
   drives the same `Lifespan[T]` from a queue processor or CLI.
9. **Intra-workspace deps are unpinned**, a packaging gap for publishing.
10. **Unconsumed or partially-read input streams.** `make_asgi_app` never
    explicitly `aclose()`s the inbound stream; wrapping the handler call in
    `contextlib.aclosing(...)` would make cleanup deterministic.

Carried forward from earlier checkpoints (still open):

- **The actor-model question** (`ACTOR_MODEL.md`, open question #2). Unchanged;
  the example is still stateless and reads a snapshot rather than messaging a
  shared fold.
- **Static `Context` ceremony**: the static-config-as-plain-value question is
  unchanged.
- A **dynamic-merge** connector as the single sanctioned funnel; whether a
  consumer parameter deserves a named `Leaf` type; factoring the connection-set +
  bounded-drain orchestration out of `kv`.
- Deferred deliberately: graph/DAG recovery and visualization on `graphlib`;
  known-hard FRP problems (diamond glitches, feedback cycles, teardown order); a
  deterministic "await next update" signal to replace `testing.tick`.

Documentation debt (carried forward): `BIG_IDEA.md` and the early checkpoints
still call the model an "async reducer"; it is more accurately an **async scan**,
and the functional-core / imperative-shell, connection-as-stream, and
lifecycle-as-a-portable-value framings should be folded into `BIG_IDEA.md` when it
is next revised.
