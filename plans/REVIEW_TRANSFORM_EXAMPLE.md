# Review: the `transform` example app

A review of `integration.transform` as of Checkpoint 13, read against the
project's original goals (`BIG_IDEA.md`) and discovered goals
(`REVIEW_BIG_IDEA.md`). The question asked: how are we doing?

## What the example is

A text-transform service over HTTP and WebSocket, built as an integration test
of the whole stack: `without-configmap` supplies a live `Settings` context,
`without-asgi` supplies the protocol boundary, and the app wires routes,
handlers, and middleware on top. It is the flagship "build a web server on it
and see what the user code looks like" deliverable that `REVIEW_BIG_IDEA.md`
named as the real test of the contract.

## Where it delivers against the goals

These are genuinely well demonstrated:

- **Functional core / imperative shell.** `core.py` is pure text domain: `Mode`,
  `apply_mode`, `transform`, `resolve_mode`, and a `TransformError` that "never
  names a status code, wire format, or byte count." The shell (`transform_text`)
  owns bytes, decode, status, and headers. The payoff shows up in the tests:
  `test_transform_core.py` is plain `assert`, no mocks, no scope dicts. This is
  the sans-IO testability lever working as `REVIEW_BIG_IDEA.md` predicted.
- **Lifespan-as-a-variable / connection-as-stream.** Handlers are
  `Stream[Inbound] -> Stream[Outbound]` processors with per-request lifespan;
  config is long-lived via `sample(source)`. The unification the pitch bet on
  (an HTTP request and a config watch are the same shape with different state
  lifespans) is visibly true here.
- **Context-updated-by-event-stream plus DI.** `watch_config -> sample ->
  config.current()`, and `text_transform_app(source: Stream[Settings])` takes
  the source as an argument. `test_handlers_pick_up_a_config_reload_mid_lifetime`
  proves the reload loop end to end with no patching. The connect-time snapshot
  (`config.current()` at dispatch) is a thoughtful state-placement decision.
- **The narrow-waist thesis, at one seam.** The strongest evidence the project
  holds: `without-configmap` and `without-asgi` snap together through nothing but
  the bare `Stream[Settings]` / `Context[Settings]` contract. Neither package
  knows about the other. That is precisely the success metric `REVIEW` set: two
  independently written pieces snap together without either knowing about the
  other.

## Where it falls short of the goals

1. **The DAG / declarative pillar is entirely absent, and the promised scaffold
   doesn't exist.** `BIG_IDEA.md` leads with "any workflow can be executed as a
   DAG if you declare inputs not order," and `REVIEW_BIG_IDEA.md` made a `@node`
   decorator plus a mermaid generator (`without.graph`) the *first-step*
   deliverable. There is no `graph` module, no `@node`, no `from_reducer`
   anywhere in the tree. The example is pure linear composition (`compose`,
   `stack`, `Router.dispatch`). The fan-out/fan-in primitives that exist in
   `wiring.py` (`broadcast`, `distribute`, `route`, `tee`, `merge`) are used by
   *nothing* in the flagship example. One of the two original headline pillars is
   both unbuilt and unexercised. Either it has been dropped (in which case the
   checkpoints and `BIG_IDEA` should say so explicitly) or the example owes it a
   demonstration.
2. **"Maximum concurrency" is delivered only at the granularity ASGI already
   gives you.** The first line of `BIG_IDEA` is concurrency. The example gets
   per-request concurrency for free from ASGI, but nothing demonstrates the
   *intra-request* DAG concurrency the pitch promised. The transform workload is
   trivially sequential, so it structurally cannot show it. The concurrency claim
   is unproven by the flagship example.
3. **The portability claim is asserted, not proven.** ~~A comment in
   `text_transform_app` says "the same value would drive a non-ASGI shell
   unchanged," which is plausible, but no second shell consumes the same
   `source`.~~ **Addressed (this pass).** `transform.cli` is now a second shell
   over the same `transform.core`: it reads stdin lines and prints them
   transformed, drawing config from a `without-env` `Context` instead of a
   ConfigMap. The core and its `TransformConfig` are byte-for-byte unchanged
   between the ASGI and CLI shells; only the edge I/O and the config source
   differ. That makes the narrow-waist interoperability claim concrete at a
   *second* seam (env -> CLI) rather than resting on the single configmap -> ASGI
   one. Closes checkpoint open item #8.
4. **The middleware pass may have drifted from "demonstrate the contract" to
   "demonstrate without-asgi's middleware library."** Checkpoint 13's own words
   are that the example "grew two stateful middleware that map out the whole
   design space." The result is five middlewares (`access_timing`, `access_log`,
   `request_digest`, `with_header`, `socket_log`) that mostly exist to populate a
   taxonomy, not to serve a transform service. The `compose`/`wrap` taxonomy is
   elegant, but it is a *without-asgi package* concern, not a narrow-waist
   *contract* concern. The flagship integration example is the place to prove the
   contract; middleware ergonomics could live in a smaller, asgi-package-local
   example.

## Smaller, concrete findings

- **`request_digest` is a didactic landmine.** ~~It carries a documented
  correctness caveat (the digest is complete only because `@buffered` drains the
  body before responding; a streaming handler would digest only what it had
  seen).~~ **Addressed (this pass).** It now negotiates the
  `http.response.trailers` extension via a new `without-asgi` `extension(scope,
  name)` helper (which returns the extension's options, parse-don't-validate, and
  which `parse_tls` now also routes through). When the server advertises trailers
  it returns the digest in a response trailer, emitted after the whole body, so an
  interleaved read/write streaming handler is digested correctly. When it does
  not, the header fallback drains the inbound stream to completion *before
  yielding anything*, so the header is correct for any handler, not just buffered
  ones. The "correct only for buffered routes" caveat is gone, and the example now
  doubles as the first worked use of extension negotiation (closes checkpoint open
  item #7).
- **`Settings.max_bytes` undercuts the functional-core boundary it is meant to
  showcase.** ~~The core's `Settings` carries `max_bytes`, but no core function
  reads it.~~ **Addressed (this pass).** The config now splits in two: the domain
  `TransformConfig` (in `core.py`, just the default `Mode`) and the shell-only
  `HttpConfig` (in `app.py`, the byte limit), composed by `Settings = {transform,
  http}`. The core only ever receives `settings.transform`, so it genuinely names
  no byte count; the CLI shell, which has no byte limit, simply omits the `http`
  half. Closes checkpoint open item #4.
- **`Router` lives in the example, so every real consumer re-implements
  dispatch.** Keeping the dispatcher out of without-asgi keeps the package
  unopinionated, but the `Router` here is about 50 lines, cleanly
  protocol-generic, and depends only on the public `Middleware` vocab plus the
  scopes. Every app will need one. This is the framework-vs-library tension worth
  deciding deliberately rather than deferring a fourth time (open #3): graduating
  it as an *optional* convenience would not violate the narrow waist.

  **Counterpoint (decided: keep it in the example for now).** A *general* router
  is not 50 lines. The moment it grows the features real apps expect (path
  parameters and the pattern matching/extraction they need, a registration
  decorator, route precedence, method-not-allowed vs not-found distinctions,
  mounting/sub-routers) it stops being unopinionated, and it is not clear that
  vocabulary belongs in `without-asgi` at all: the package's job is the ASGI
  boundary, not a routing DSL. The 50-line version reads small only because it
  matches `(method, path)` exactly and supports nothing else. Graduating the toy
  would either ship something too thin to be the router people reach for, or
  commit the package to growing a real one. So this stays in the example
  deliberately, not as deferral; if a router is ever extracted it should be its
  own package, not folded into the boundary adapter.
- **Test fidelity: the example validates its own understanding of ASGI, not
  ASGI.** Every test drives hand-built scope dicts and scripted `receive`/`send`.
  Given without-asgi's CLAUDE.md emphasis on spec-tracking, one
  `httpx.ASGITransport` round-trip would change "passes our tests" into
  "conforms." This is checkpoint #5 and I agree it is the single best next step.

## Bottom line

Against the *discovered* goals (narrow waist, sans-IO testability,
lifespan-as-a-variable, functional-core / shell), the example is strong and the
thesis looks alive. Against the *original* goals (DAG-from-declared-inputs,
visualization, maximum concurrency), the example is silent, and the
DAG/visualization scaffold that `REVIEW` called the first deliverable was never
built.

That is the gap to surface most loudly: the project has quietly become "a
narrow-waist contract for stream processors with pluggable I/O" (a good thing,
and it is working) and has set aside "a DAG executor that recovers parallelism
from declared inputs" (the other half of the original pitch). If that is an
intentional pivot, the docs should say so. If it is not, the next example should
declare inputs and fan out, not transform a string sequentially.

## Suggested next steps

- ~~**(a)** Draft a non-ASGI shell over the same `source` to prove portability.~~
  **Done (this pass):** `transform.cli`, an env-configured stdin/stdout shell over
  the same core. Closes open #8.
- **(b)** Prototype the missing `@node` / DAG-recovery piece, or formally record
  the pivot away from it. **Still the biggest open gap**, untouched by this pass.
- **(c)** Add one `httpx.ASGITransport` round-trip test to move from "passes our
  tests" to "conforms" (open #5). Still open.

## Addressed in the follow-up pass

After this review, a follow-up pass closed several of the smaller findings (see
the struck-through items above): the portability proof (`transform.cli`), the
`request_digest` correctness caveat (trailer negotiation plus a draining header
fallback, on a new `without-asgi` `extension` helper), and the
domain/shell config split (`TransformConfig` vs `HttpConfig`). The two *structural*
gaps remain open and unchanged: the absent DAG/declarative pillar (finding 1) and
the unexercised intra-request concurrency (finding 2). The middleware-drift
observation (finding 4) also stands.
