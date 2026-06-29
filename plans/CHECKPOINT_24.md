# Checkpoint 24

A snapshot of where `without` stands, succeeding `CHECKPOINT_23.md`. For the prior
state see `CHECKPOINT_23.md`; for the original pitch see `BIG_IDEA.md`.

This checkpoint is about the **shape of the HTTP client**, and specifically why it
feels less clean than the server even though both share the `stack` middleware
vocabulary. It captures a design discussion about client/server symmetry, then does
the first concrete cleanup it pointed to: folding response-body **release** (the
connection-pool cleanup hook) into the body stream's own lifecycle, so
`ClientResponse` stops carrying an out-of-band `_release` / `_fully_read` /
`_finalize` machine.

## Why the client felt less clean than the server

The server (`without-asgi`) came out clean because the message flow splits into two
phases: **routing** acts on the `scope` (the head, a value) and produces a
**handler** (`Stream[Inbound] -> Stream[Outbound]`, the body phase). Head is a value;
body is a stream. Middleware wrap the handler given the head.

The client has the *same* split, on the response: `ClientResponse` is `status` +
`headers` (the head, values, available the instant `await exchange(request)`
returns) plus the body as a live stream. The head must be a separate value here for
the same reason routing needs the scope: the client is the **consumer**, so it
branches on the head before the body (`follow_redirects` reads `status` +
`location`; a decompressor would read `content-encoding`), exactly the way routing
branches on the request scope. So "`ClientResponse` holds head *and* body" is not the
problem: that bundling *is* the two-phase split, head-value over body-stream.

### The data shape is a clean dual; the initiative is what flips

Both sides have a *request* (head + body-stream) and a *response* (head +
body-stream). All that changes is who produces and who consumes each:

| role        | request                                  | response                                 |
|-------------|------------------------------------------|------------------------------------------|
| **Server**  | framework produces (off wire), user reads | user produces, framework writes (to wire) |
| **Client**  | user produces, framework writes (to wire) | framework produces (off wire), user reads |

A perfect mirror. The shell does the same four jobs on both sides
(parse-wire-into-a-stream for the framework-produced one; serialize-a-stream-to-wire
for the user-produced one); only the assignment of which stream is
framework-produced flips. The data-plane plumbing is not intrinsically different,
it is the same machinery pointed the other way.

### Which end of the onion the user sits at

The composition machinery is *already literally identical*: the same `stack`. What
differs is where the user sits in it:

- **Server** (`dispatch`): `self.middleware(handler, state, scope)`, then the
  framework drives the result. The user's handler is at the **center** of the onion;
  middleware wrap *around* it; the framework calls inward.
- **Client**: `stack(self.middleware, middleware)(self.exchange)`, and then *the
  user* writes `await exchange(...)`. The user is at the **rim**; middleware wrap the
  framework's `exchange`; the user calls inward.

Same onion, same `stack`, opposite ends. This is the control inversion (Hollywood
principle), and it is irreducible for request/response, because the client user
**holds the continuation**: the code after `await response` and inside the
`async with` is theirs, and the framework can neither see nor drive it. The server
framework *surrounds* the user; the client framework *is surrounded by* the user.

That is why the client API is imperative and *should* be. Forcing it into a
server-style `Stream -> Stream` processor would take the continuation away from the
user to make the library look symmetric: optimizing the artifact's symmetry at the
cost of contorting the user's code, the wrong trade.

## How much of the shell can be shared

Layer by layer:

- **Wire cores (sans-IO):** already shared. `h11_wire` / `h2_wire` map events to and
  from the typed vocabulary in both directions; that is the parse/serialize half of
  the shell, transport-direction-agnostic by construction.
- **The connection pump:** mirror-symmetric, and the h2 case is the strongest
  candidate for *actual* shared code. The client's `_run` / `_handle`
  read-loop-plus-per-stream-queues is the dual of the server's
  `_serve_h2_connection`: both are "read bytes, feed shared `h2.Connection` under a
  lock, dispatch events to per-stream queues; drain per-stream send under flow
  control." A shared duplex-pump abstraction could absorb both, parameterized by
  initiator side, stream-id allocation, and the two flow-control directions. Caveat:
  the flow-control wakeup invariants differ subtly enough to merge carefully, not
  assume it is free.
- **The body-stream lifecycle:** can be made the same discipline (cleanup in the
  body generator's `finally`, natural end vs `GeneratorExit`). That is a property of
  "a body stream someone consumes," which both sides have. **This checkpoint does
  this part.**
- **Request/response framing and the user seat:** genuinely different, and should
  stay different. Server is `scope -> handler` (reactive processor at the center);
  client is `request -> await -> response` (imperative script at the rim).

The one place they are *fully* the same already is **full-duplex** (WebSockets):
with no request/response ordering to collapse, both sides are just
`Processor[Incoming, Outgoing]` over a connection-as-stream; only the driver
differs. Request/response is precisely the case that breaks symmetry, because it
imposes "whole request before response," which is what lets each side collapse its
duplex into a value on one end.

## The cleanup-ownership problem (what this checkpoint fixes)

On the server, the framework *produces* the response stream and always runs it to
exhaustion, so **end-of-stream is the one completion signal** and cleanup lives in
the producing generator's `finally`. One signal, automatic.

On the client, the *user* consumes the response body and may stop early, so there
are two completion signals: natural end-of-stream, or "user is done"
(context-manager exit). The previous design lifted the cleanup decision *out* of the
stream into a side channel on `ClientResponse` (`_release`, `_fully_read`,
`_released`, `_finalize`). That `_fully_read` boolean threaded alongside the body
*was* the end-of-stream signal, rebuilt by hand and out of band: exactly the queue /
end-of-stream trick the server gets for free, lost.

### The fix: fold release into the body stream's `finally`

A `_with_release(body, release)` generator folds the release decision into the
stream's own `finally`: reaching the end naturally releases with `fully_read=True`
(keep-alive eligible for h11; nothing to do for h2); being closed early runs the
`finally` with `False`, so the same hook closes the h11 socket or resets the h2
stream. The body generator owns *when* cleanup happens; the pool's `release` closure
owns *what* it does (return-to-pool vs close, or abort). `ClientResponse` shed its
lifecycle flags (no `_fully_read`, `_released`, `_finalize`): the generator's own
`CLOSED` state is the once-only latch. (The head/body split below then reshaped the
remaining `(status, headers, body)` further.)

Two invariants this rests on, both learned the hard way against the test suite:

- **The body must be entered before it can be closed.** `aclose()` on an async
  generator that was *never started* (state `AGEN_CREATED`) jumps straight to
  `AGEN_CLOSED` and **skips the `finally` entirely**. A response whose body is never
  read (a status-only check, an empty redirect hop, an empty `Set-Cookie` reply) would
  then never release, leaking the connection (surfaced as `StreamWriter.__del__`
  unraisable warnings). The fix: `_with_release` opens with a `yield b""` priming
  sentinel, and the `_releasing` builder consumes it at construction with `anext`, so
  the generator is always suspended *inside* the `try`. It costs no I/O, the sentinel
  is yielded before the body is ever pulled, and it never reaches the consumer.
- **Cleanup needs an explicit `aclose`.** An async generator's `finally` cannot be
  relied on under garbage collection (no loop to await in). The
  `async with pool.request(...)` block already calls `aclose()` on exit, so this
  holds, but it is the invariant the design sits on.

It also sets up `map_body` (deferred, below): wrapping a generator
(`async for chunk in inner: yield transform(chunk)`) propagates both natural
exhaustion and `GeneratorExit`/`aclose()` to the inner, so the inner's `finally`
(the release) still fires. Output-affecting client middleware and the cleanup
cleanliness are then the *same* mechanism.

## The response head/body split

With the body self-managing, `ClientResponse` was just a pairing, so `pool.request`
yields it directly (was `yield response.head, response.body`):

- `head` is a `ResponseHead` (status + headers), a value you branch on the instant the
  exchange returns.
- `body` is a `ResponseBody`, a once-consumable stream that releases its connection
  when it ends or is closed.

This is the consumer split that mirrors how the *server* consumes a request (a `scope`
value plus a body stream): pull the structured head out as a value, leave the body a
stream, so you decide before touching the body. `ClientResponse` is a `NamedTuple`, so a
caller takes it whole (`response.head`) or unpacks it (`head, body = response`) with each
field keeping its precise type, which a dataclass `__iter__` could not give (it collapses
both targets to `ResponseHead | ResponseBody`). It is the one place a `NamedTuple` earns
its keep over a frozen dataclass: the style default. The cost is that a `ClientExchange`
rebuilds it by construction rather than `dataclasses.replace`, trivial for two fields.
(`ClientRequest` stays a dataclass: middleware `replace` it constantly. Request is built,
response is consumed.) The request body parameter is `body=` to match, not `content=`:
without-http leaves encoding to the app, so there is no `data`/`json` to disambiguate
from the way httpx's `content=` does. The bare transport exchange is now `_exchange`
(private): `request` is the sole entrypoint, so a caller cannot reach the inner exchange
and accidentally bypass the pool's configured `middleware`. The pool's h2 flags were
renamed to say what they do: `allow_http2` (negotiate h2 over TLS, falling back to
HTTP/1.1) and `force_http2_cleartext` (assume h2c over cleartext by prior knowledge).
The allow/force split is a consequence of transport: TLS can negotiate (so h2 is
*allowed*), cleartext cannot (so h2 must be *forced* by assertion).

## The rule we discovered: default-value policy follows role, not shape

The sharp rule this work surfaced, worth stating on its own:

> A type's default-value policy is set by its **role**, not its fields. A type the
> *parser fills from the wire* (inbound) must have **no defaults**, so a field the
> parser forgot fails loudly instead of silently defaulting. A type the *user
> constructs* (outbound) carries defaults for ergonomic construction. So even when two
> types have identical fields, you do not reuse one across the boundary.

This is the without-asgi inbound/outbound rule, and without-asgi already lives it: a
body chunk is modeled *twice*, as `RequestBody(body, more_body)` (inbound, no defaults)
and `ResponseBody(body=b"", more_body=False)` (outbound, with defaults), the same
concept split because the roles differ. The seductive move here was to reuse
without-asgi's outbound `ResponseStart` as the client's response head, since the wire
round-trips the very value the server emitted: elegant identity of *fields*, wrong
identity of *purpose*. The client *parses* the head from the wire, so it is inbound,
and `ResponseStart`'s defaults (`headers=()`, `trailers=False`) would let a parser bug
pass silently. So without-http defines its own `ResponseHead(status, headers)` and
`ResponseTrailers(headers)`, no defaults, paralleling `RequestBody` vs `ResponseBody`.
Parse-don't-validate restated: the parsed type must *prove* every field was supplied;
a defaulted type cannot. Simple-vs-easy restated: reuse is easy, separate types are
simple. The send/receive symmetry survives where it matters (same fields), each
direction just owns the type with its own invariant.

## Trailers: in-band, opt-in, never a place

Real servers (gRPC over h2, `grpc-status`) send trailing headers after the body, so the
client must surface them, not silently drop data. The design that fell out:

- **Two channels, only one is value-clean.** Trailers can travel *beside* the body (a
  `body.trailers` slot, a field that is meaningless until the body drains: an
  illegal-intermediate-state place) or *in* the body stream (the terminal element). No
  third channel exists. In-band wins, and it matches the server's `Outbound`
  (`ResponseStart, ResponseBody*, ResponseTrailers?`): trailers are just the tail.
- **Default path drops, opt-in keeps.** `ResponseBody` is consumed once by one of a 2x2
  of methods: `__aiter__` / `read()` yield `bytes` and drop trailers (the common path
  pays nothing); `events()` / `read_with_trailers()` keep them, surfaced as
  `ResponseTrailers` after the bytes. Dropping still *drains* to the end (filters the
  terminal, never stops early) so the connection still releases as fully-read.
- **The decision is out-of-band, so it lives at the call site.** A client knows by
  contract whether an endpoint uses trailers, so *which method you call* encodes that.
  No runtime sniffing; the `Trailer` response header is at most an advisory nicety.
- **Never fail on presence.** Trailers arriving are valid HTTP, not malformed input, so
  ignoring them is a legitimate choice, not a swallowed error: a server adding a trailer
  must never crash a client that did not ask. Strictness belongs only at the demanding
  consumer: `read_with_trailers` returns *all* blocks (empty tuple if none), so a caller
  that *requires* trailers raises in its own terms.
- **Multiplicity.** The stream model carries 0/1/many `ResponseTrailers` blocks (a
  single HTTP response puts at most one on the wire: h2 requires the trailing HEADERS to
  carry END_STREAM, h11 chunked has one trailer section), so the body's many-block
  handling is pinned by unit tests over a hand-built stream, while h11 (`EndOfMessage`
  headers) and h2 (`TrailersReceived`) reception is tested end-to-end against raw
  servers.

## Client `wrap`, and why it stays per-package (not core alongside `stack`)

`without_http.wrap(request=, response=)` builds a `ClientMiddleware` from an
independent request transform and/or response transform, the easy path for simple
client middleware. `add_headers` is now a one-liner over it
(`wrap(request=lambda r: replace(r, headers=...))`); the stateful/looping ones
(`cookies`, `follow_redirects`) stay hand-written `ClientExchange` wrappers, exactly as
on the server (`wrap` for the simple case, direct middleware otherwise).

`wrap` is the client counterpart to without-asgi's `wrap`, but it does **not** fold into
core alongside `stack`, and the reason is the same kind discussed for the cog ladders:

- `stack` generalized because it **never invokes the handler**, it only composes
  `(handler, *ctx) -> handler` functions. Convention-agnostic, so one variadic generic
  serves the server (sync `Processor` + `(state, scope)`) and the client (async exchange
  + no context) together.
- `wrap` **reaches into the handler's invocation** (pre-process input, post-process
  output), so it is bound to the calling convention, and the two differ: the server
  handler is a sync `Processor` (`Stream -> Stream`) wrapped via `compose` threading
  `scope`; the client exchange is **async** value->value (`await inner(...)` then
  transform). A core `wrap` would need an `apply`-strategy parameter (sync vs async),
  more machinery than the two tiny helpers it replaces. Worse, a *sync* core `wrap`
  produces wrong code for the client: it would post-process the *coroutine*, not the
  awaited response.

So `wrap` lives per-package and parallel: `without_asgi.wrap` (inbound/outbound streams,
scope-aware, sync) and `without_http.wrap` (request/response, async). Rule of thumb to
add to the `stack` note: a helper that only *composes* middleware generalizes to core; a
helper that *invokes* the handler is convention-bound and stays where the handler lives.

Everything green: mypy clean (90 source files), full pytest suite (411 tests) passing,
pre-commit clean.

## Open questions and next steps

- **`map_body` ergonomic sugar.** Output-affecting client middleware now *work* via
  `wrap(response=lambda r: ClientResponse(r.head, ResponseBody(transform(r.body.events()))))`
  (a test exercises exactly this, uppercasing the body). What is missing is sugar: a
  `ResponseBody.map(transform)` so byte-counting / decompression read as one call
  instead of rebuilding a `ResponseBody` around `events()`. Pure ergonomics now, not a
  capability gap.
- **Shared h2 duplex pump:** sketch what one abstraction both `serve` and the pool's
  `_Http2Connection` would take (initiator side, stream-id allocation, the two
  flow-control directions) and decide whether they actually collapse or the invariant
  differences make it not worth it. The highest-value shell-sharing candidate.
- **Server-side trailer *sending*.** The client now *receives* trailers (h11 + h2); the
  without-http server still rejects them on the response path. Sending them (and the h2
  server push path) remains open, and would let a without-http-to-without-http round-trip
  exercise trailers end to end instead of via raw test servers.
- **Bound the connection pool (per host).** `ConnectionPool` is currently unbounded for
  HTTP/1.1 on two axes: *peak connections per origin* (`_request_h11` opens a fresh
  socket whenever no idle one is free, so N concurrent requests to one origin open N
  sockets) and *idle retention per origin* (`_idle_h11[origin]` grows without limit). h2
  is fine on connection count (one multiplexed connection per origin) but does not gate
  against the server's `SETTINGS_MAX_CONCURRENT_STREAMS`, so a large burst can over-issue
  streams on the one connection (the related h2 "stream limit", tied to the
  consumer-driven-duplex item). Two knobs, httpx-shaped, differing sharply in cost:
  - *Idle cap* (`max_keepalive_per_host`): cheap and pure, on return `aclose()` instead
    of pooling once the idle list is full. No new concurrency machinery; bounds fd/memory
    retention.
  - *Peak cap* (`max_connections_per_host`): the real feature, a per-origin
    `asyncio.Semaphore` acquired before checkout/open and released when the connection is
    returned or closed, so a request *waits* when the origin is saturated. The delicate
    part is bracketing the whole borrow so the permit is released exactly once even on
    partial-read abort or cancellation; this dovetails with the `_release` work, since
    release is already the single cleanup point. Optional acquire timeout and fairness are
    follow-ons.
  Build as one focused change (not folded into API polish); lean toward both knobs, since
  a client hitting an origin usually wants a peak cap, where the server deliberately
  leans on the listen backlog instead.
- Carried from Checkpoint 23 and still open: consumer-driven request/response duplex;
  WebSockets over HTTP/2 (RFC 8441); HTTP/2 server push;
  HTTP/3 over QUIC; a transport-level 503 for arbitrary hosted ASGI apps;
  OpenAPI shared components / `$ref`; `todos` persistence stubbed; intra-workspace
  deps unpinned; `make_asgi_app` never `aclose()`s the inbound stream (the server's
  symmetric request-body-abandonment case, punted where the client cannot punt);
  the actor-model question. Documentation debt: `BIG_IDEA.md` still calls the model an
  "async reducer" (it is an async *scan*); `plans/WITHOUT_HTTP.md` and older
  checkpoints still describe the client as a mandated aiohttp-style `Session`.
- Operational, carried: CI on `proof-of-concept` (PR #1, draft) has two flaky
  concurrency tests in `kv/test_shell.py` that pass locally.
