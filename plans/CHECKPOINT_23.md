# Checkpoint 23

A snapshot of where `without` stands, succeeding `CHECKPOINT_22.md`. For the prior
state see `CHECKPOINT_22.md`; for the original pitch see `BIG_IDEA.md`.

This checkpoint reworks the **middleware vocabulary** into a single generic `stack`
shared by client and server, and reshapes the HTTP **client** around it: the
`Session` becomes a `ConnectionPool`, cookies become composable middleware over a
caller-owned jar, and client middleware collapse to plain exchange endomorphisms.

Everything is green: mypy clean (89 source files), full pytest suite (400 tests)
passing, pre-commit clean.

## Guiding principle: push composition to the user

The design tenet that drove this work, worth stating on its own:

> Push *how the pieces are composed* to the user as much as possible. The user does
> the composition and hands the framework one already-combined value; the framework
> just applies it. It is fine, and expected, for the *user* to complect things in
> their own app: that is how they make the app do something useful. The simplicity
> that matters is the artifact's (the library's), not the user's app.

Concretely: the user calls `stack(...)` to combine middleware and passes the single
result; a router field holds *one* composed `Middleware`, never a list it folds
itself. This is why the first cut of this checkpoint, a framework-side
`apply(sequence, state, scope, inner)` that threaded and composed on the user's
behalf, was rejected in favour of a generic `stack` the user calls directly.

## One generic `stack` (handler-first, `TypeVarTuple`)

`stack` now lives in `without` core and is generic over any middleware shape:

```python
def stack[H, *Ctx](*middleware: Callable[[H, *Ctx], H]) -> Callable[[H, *Ctx], H]:
    def composed(handler: H, *context: *Ctx) -> H:
        for one in reversed(middleware):
            handler = one(handler, *context)
        return handler
    return composed
```

A *middleware* is `(handler, *context) -> handler`: it wraps a handler given some
fixed context, threaded unchanged into every middleware in the stack and chained
through them, first outermost. The context pack `*Ctx` is bound once per call, so
every middleware in one `stack(...)` must share a shape (mixing shapes is a type
error, verified). Two settings, one utility:

- **Server:** `Middleware[T, H, S] = Callable[[H, T, S], H]`, the context is
  `(state, scope)`. Reshaped **handler-first** (`(handler, state, scope)`, was
  `(state, handler, scope)`) so the chained arg sits at a fixed end for the variadic.
  The handler-first impl typechecks with no cast; handler-last forces a keyword-only
  parameter or a cast in the one core impl, so first won.
- **Client:** `ClientMiddleware = Endo[ClientExchange]`, the zero-context case
  (`(exchange) -> exchange`). The request *is* the value a client middleware
  transforms, so there is no scope to thread; the same `stack` composes them.

`stack` is re-exported from `without_asgi.routing` (server) and `without_http`
(client), so each side imports it from the package it already uses.

### Why not replace the cog ladders with `TypeVarTuple` too

`TypeVarTuple` carries a pack through **unchanged** (prepend/append fixed types,
concatenate). `stack` is the lucky case where the context is only ever threaded raw, so
one variadic generic covers every arity. The `handle` / `into` ladders in `without-web`
are the opposite: they take `Extractor[A], Extractor[B], …` and must **unwrap** each to
build `fn: Callable[[T, A, B, …], …]`. Two independent walls block expressing that with a
`TypeVarTuple`, either of which is fatal on its own (both confirmed against mypy strict):

- **No variadic map.** There is no `Map[F, *Ts]` in the spec, so you cannot relate the
  extractor pack `(Extractor[A], …)` to the value pack `(A, …)`. Write `*extractors: *Ts`
  with `fn: Callable[[T, *Ts], …]` and `*Ts` binds to the *extractor* types, so `fn` is
  typed to receive `Extractor[A], …` instead of `A, …`, the opposite of the point.
- **No bounds on a `TypeVarTuple`.** `def handle[T, *Ts: Extractor]` is rejected outright
  (`Cannot use bound with TypeVarTuple [syntax]`), so the pack can't even be constrained
  to extractors. (Per the spec: variance, constraints, and bounds are unsupported on a
  `TypeVarTuple`.)

The ladder exists precisely to name `A, B, C, …` as distinct `TypeVar`s so each
`Extractor[X] → X` unwrap is written out, so those ladders stay. Rule of thumb: if the
variadic types only ever appear raw, use `TypeVarTuple`; the moment one must appear
wrapped as `Something[T]` element-wise, you need the ladder.

## Caveat: middleware and the context threaded to them

`stack(f, g)(handler, state, scope)` is `f(g(handler, state, scope), state, scope)`:
the *same* `state`/`scope` is handed to every middleware, and a middleware returns a
handler, not a new context. So through `stack` a middleware cannot change what its
siblings or the handler receive in the context args. This is deliberate (the scope is
a value threaded identically, not a mutable place; without also does not surface a
mutable `scope["state"]`), and it is the same in the discarded `apply`.

The difference the user-composes principle makes: `apply` took a *sequence* and owned
the fold, so there was no seam to hand inner middleware a *modified* context. Now you
hand us one `(handler, state, scope) -> handler`; `stack` is just the default for the
fixed-context case. When a middleware must give an inner one an enriched context, you
write the composition yourself (a plain `HttpMiddleware` that calls the inner pieces
with the context it wants) and pass that single value. The default stays simple; the
escape hatch is the user's.

One layering note: in `dispatch` the handler is built (`_resolve(state, scope)`)
*before* the middleware wrap it, so a middleware can hand modified context to inner
*middleware* but the endpoint's handler was already built with the original scope. To
affect what the handler itself sees, do it the without way: transform its input stream
(`wrap(inbound=...)`) or work one layer up at the endpoint `(state, scope) -> handler`
and rebuild with the scope you want. Both are data transformation, not scope mutation.

## What moved on the server

- `without-asgi/routing.py`: `Middleware` reshaped handler-first; the old state-aware
  `stack` removed (now the generic core one, re-exported); `wrap` and
  `limit_concurrent_requests` produce handler-first middleware. No `apply` helper.
- `without-web/router.py`: `Router.middleware` / `WebsocketRouter.middleware` stay a
  *single* `HttpMiddleware` / `WebsocketMiddleware` (default `stack()`); `dispatch`
  calls `self.middleware(handler, state, scope)`. `with_middleware` and the mounted
  `_behind` compose with `stack` and apply handler-first. `catching` /
  `catching_websocket` reshaped handler-first.
- Integration apps compose with `stack(...)` again, the natural single-value form.

## HTTP client: `ConnectionPool`, cookies, exchange-endomorphism middleware

(Built across this session, on top of the streaming client from Checkpoint 22.)

- **`Session` is gone.** `ConnectionPool` is the entrypoint and its own async context
  manager (`async with ConnectionPool(...) as pool`); `open_session` removed. The
  router-shaped wrapper that "did nothing" is gone: the pool *is* the thing that does
  the work, so it owns `request(...)`, the default `middleware`, and per-request
  `middleware=`. `pool.exchange` is the bare inner exchange middleware wraps.
- **Cookies are composable middleware over a caller-owned value.** `CookieJar` is a
  mutable store *you* construct; `cookies(jar)` reads `Set-Cookie` into it and writes
  the matching `Cookie` header out. Cookie scope (application identity) is kept
  independent of connection reuse (transport): two requests share cookies exactly when
  they share a jar, not because they share a pool. The placement rule: keep pool-level
  middleware to *pure* decoration (headers, redirects, retry); anything carrying
  mutable, request-spanning identity is a value you own and pass per request. Jar
  matching covers host-only + `Domain` (subdomain), `Path`, `Secure`, and
  `Max-Age<=0` deletion; `Expires` date-based expiry is not yet honored.
- **Client middleware are `Endo[ClientExchange]`.** The old `(state, inner, request)`
  shape (both `state` and `request` were dead in every middleware) collapsed to
  `(exchange) -> exchange`. State a middleware must keep lives in a closure (the jar),
  as it does server-side.
- **aiohttp references dropped** from the live docs and code: the client is
  inspired-by, not look-alike.

## Open questions and next steps

- **Output-affecting client middleware.** Discussed, not built: a
  `ClientResponse.map_body(transform)` (the dual of the server's `wrap(outbound=...)`)
  that wraps the response body stream while preserving the release-once / partial-read
  lifecycle, so byte-counting / decompression middleware are clean two-liners instead
  of poking at `_body`/`_release`. The `Exchange -> Exchange` shape already *supports*
  reading and wrapping the response (the middleware holds the return value); `map_body`
  just makes the streaming-body case ergonomic.
- Carried from Checkpoint 22 and still open: consumer-driven request/response duplex
  and per-host pool limits; WebSockets over HTTP/2 (RFC 8441); HTTP/2 server push and
  trailers; HTTP/3 over QUIC; a transport-level 503 for arbitrary hosted ASGI apps;
  OpenAPI shared components / `$ref`; `todos` persistence stubbed; intra-workspace deps
  unpinned; `make_asgi_app` never `aclose()`s the inbound stream; the actor-model
  question. Documentation debt: `BIG_IDEA.md` still calls the model an "async reducer"
  (it is an async *scan*); `plans/WITHOUT_HTTP.md` and the older checkpoints still
  describe the client as a mandated aiohttp-style `Session`.
- Operational, carried: CI on `proof-of-concept` (PR #1, draft) has two flaky
  concurrency tests in `kv/test_shell.py` that pass locally.
