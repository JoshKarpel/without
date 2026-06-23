# Checkpoint 8

A snapshot of where `without` stands, succeeding `CHECKPOINT_7.md`. For the
original pitch see `BIG_IDEA.md`; for the critical review and open design
questions see `REVIEW_BIG_IDEA.md`; for the actor-model digression see
`ACTOR_MODEL.md`.

## What changed since Checkpoint 7

This session did the cleanup Checkpoint 7 named as its next step, and in doing so
resolved its open question #2 (the hand-rolled `_Holder` place). The lifespan
dance, intercepting the ASGI lifespan protocol, holding the set-up state, and
dispatching every other scope, was lifted out of the `flags` example into a
reusable `without-asgi` primitive. The work was driven by a design conversation
that landed on two constraints the primitive had to honor: lifespan resources
that depend on each other, and the same lifespan running unchanged in a non-ASGI
shell (a queue processor, a CLI, a test).

- **New `without-asgi` primitive `with_lifespan` (`lifespan.py`).** It is the
  ASGI *driver* for a portable lifespan, nothing more:

  ```python
  type Lifespan[T]  = Callable[[], AbstractAsyncContextManager[T]]
  type ScopedApp[T] = Callable[[T, Scope, Receive, Send], Awaitable[None]]

  def with_lifespan[T](lifespan: Lifespan[T], app: ScopedApp[T]) -> ASGIApp
  ```

  On a `lifespan` scope it drives the startup/shutdown handshake; every other
  scope is passed to `app` with the set-up state threaded in. Setup and teardown
  failures are reported as `lifespan.startup.failed` / `lifespan.shutdown.failed`
  with the exception message, the loud-failure path that justifies the protocol's
  existence.
- **The shared place is now generic, internal, and tested once (`_Cell[T]`).**
  The ASGI process model (one long-lived lifespan call, many short request calls
  that must share the set-up state) still forces exactly one shared reference.
  That reference moved from a per-app `_Holder` into the library's `_Cell`, with
  a `require()` guard that turns the can't-happen "request before startup" case
  into a `RuntimeError` rather than a silent `None`.
- **`flags/app.py` shed `_Holder`, `_run_lifespan`, and six lifespan imports.**
  Its lifespan is now literally `lambda: sample(source)`, since `sample` is
  already an async context manager, and `make_app` is just a `serve` handler that
  receives the live `Context[Flags]` per request, wrapped by `with_lifespan`.
- **New tests (`test_asgi_lifespan.py`, five cases).** The handshake with
  enter-then-exit ordering, requests handed the state, a request before startup
  failing loud, and both setup-failure and teardown-failure reply paths. One
  existing test (`test_websocket_scope_is_unsupported`) now runs inside
  `_running(app)`: under a real server an unsupported scope only arrives after
  startup, so exercising it without startup would (correctly) trip the
  `require()` guard instead of the handler's `NotImplementedError`.

## Rationale: lifespan is a portable value, `with_lifespan` is the driver

Two structural insights shaped the primitive, both reinforcing Checkpoint 6's
"a connection's lifecycle is a stream's lifecycle" and the functional-core /
imperative-shell framing.

- **Interdependence argues for one composed context manager, not a list.** The
  wrapper takes a single `Lifespan[T]`, never a tuple of independent resources.
  Authors compose interdependent resources *inside* it with nested `async with`,
  which gives dependency ordering on the way up and reverse-order teardown on the
  way down for free. A flat list of resources would throw that ordering away, so
  the wrapper stays out of composition entirely.
- **`Lifespan[T]` names no ASGI types, so the value ports across shells.** This
  is the seam that delivers the project's key promise: the same `lifespan()`
  value drives any shell, and only the thing wrapping it changes. `with_lifespan`
  binds it to the ASGI lifespan protocol; a future queue-processor shell would
  `async with lifespan() as state: ...` around its consume loop; a CLI or test
  does the same directly. Lifecycle is a value the shell decides how to run, the
  same way the per-message processor (`state -> Processor[In, Out]`) already
  ports. The one thing that legitimately differs per shell is the per-message
  dispatch, because the transport shapes (ASGI's `scope/receive/send` vs a queue
  envelope) genuinely differ, and that dispatch is thin.

## Why this is a real primitive, not sugar

The `AsyncExitStack` inside `_drive` has to live across two separate server
messages: it is entered when `startup` arrives and closed when `shutdown`
arrives, with the wrapper suspended at `await receive()` for the shutdown event
in between. A plain `async with` cannot straddle that gap, which is the same
reason Checkpoint 7 gave for why lifespan "cannot be threaded through a per-event
fold," now made concrete. The enclosing `async with AsyncExitStack()` also
guarantees teardown if the server cancels the lifespan task before shutdown is
ever sent. This is exactly the work no app should reimplement, so it lives in the
adapter once.

## Status

Done and verified (mypy strict clean across 34 source files, 98 tests passing,
ruff lint + format clean):

- `without-asgi`: the `with_lifespan` driver, the `Lifespan` / `ScopedApp` type
  aliases exported from the package, and the internal `_Cell` holder and `_drive`
  protocol loop, tested at the handshake, dispatch, and both failure paths.
- `without_integration.flags`: unchanged behavior, now wired on `with_lifespan`
  with no example-local lifespan plumbing. All Checkpoint 7 `flags` tests still
  pass, including the dynamic-reload test that proves a request mid-lifetime sees
  a reloaded `Context`.

## Open questions and next steps

Resolved this session:

- **The shared `_Holder` place (Checkpoint 7 #2).** Lifted into the library as a
  generic `_Cell`, set once at startup and read per request, tested in one place.
  The remaining `require()` guard is the deliberate loud-failure for a
  spec-impossible ordering, not a smell.

Still open, carried from Checkpoint 7:

1. **No real HTTP testing (Checkpoint 7 #1).** Everything is still driven by
   constructing `scope`, a scripted `receive`, and a capturing `send`. A few
   `httpx.ASGITransport` tests and one `uvicorn` smoke run would catch
   conformance gaps (header casing, body framing, the handshake as a real server
   drives it) that the hand-written `send`-capture cannot. The `with_lifespan`
   handshake is now a prime candidate for such a test.
2. **Routing fidelity (Checkpoint 7 #3).** `Router` still matches `(method,
   path)` exactly; path parameters and a decorator ergonomic remain deferred.
3. **Scope of `without-asgi` (Checkpoint 7 #4), refined.** Lifespan *policy* is
   no longer purely the app's: `with_lifespan` now lives in the adapter. This is
   consistent with "the boundary, and only the boundary", driving the ASGI
   lifespan protocol is boundary work, and the portable `Lifespan[T]` it consumes
   names no ASGI types. Streaming response bodies and websockets remain
   representable but unimplemented.

New, prompted by this session:

- **A non-ASGI shell would make the portability promise concrete.** `with_lifespan`
  is built so the same `Lifespan[T]` value drives a queue processor unchanged,
  but nothing yet exercises that. A small `without`-side shell (or a
  `without-redis`-shaped one) consuming an identical lifespan would turn the
  promise from a claim into evidence, and would pair naturally with the still-open
  actor-model question (a stateful example behind a singular fold).

Carried forward from earlier checkpoints (still open):

- **The actor-model question** (`ACTOR_MODEL.md`, open question #2). Unchanged
  this session; `flags` is still stateless and reads a `Context` rather than
  messaging a shared fold. A stateful ASGI or queue example would add pressure.
- **Static `Context` ceremony** (open question #3): the dynamic half remains
  demonstrated; the static-config-as-plain-value question is unchanged.
- A **dynamic-merge** connector as the single sanctioned funnel; whether `serve`'s
  consumer parameter deserves a named `Leaf` type; factoring the connection-set +
  bounded-drain orchestration out of `kv`.
- Deferred deliberately: graph/DAG recovery and visualization on `graphlib`;
  known-hard FRP problems (diamond glitches, feedback cycles, teardown order);
  a deterministic "await next update" signal to replace `testing.tick`.

Documentation debt (carried forward): `BIG_IDEA.md` and the early checkpoints
still call the model an "async reducer"; it is more accurately an **async scan**,
and the functional-core / imperative-shell and connection-as-stream framings
(now joined by lifecycle-as-a-portable-value) should be folded into `BIG_IDEA.md`
when it is next revised.
