from __future__ import annotations

from collections.abc import AsyncIterator
from collections.abc import Callable
from dataclasses import dataclass

from without import Processor
from without import Stream
from without import compose

from without_asgi.app import HttpHandler
from without_asgi.app import HttpRouter
from without_asgi.app import WebsocketHandler
from without_asgi.inbound import Inbound
from without_asgi.outbound import Outbound
from without_asgi.outbound import Response
from without_asgi.outbound import encode_response
from without_asgi.scope import HttpScope
from without_asgi.scope import WebsocketScope
from without_asgi.shell import read_body

__all__ = [
    "HttpMiddleware",
    "Middleware",
    "WebsocketMiddleware",
    "buffered",
    "limit_concurrent_requests",
    "stack",
    "wrap",
]

# Tools for building a router, not a router. *Which* connection a route matches
# and how dispatch falls back are opinionated app choices, so without-asgi leaves
# the router to the app and ships only the unopinionated pieces: a `Middleware`
# vocabulary that composes, and `buffered` for the common request/response shape.
# The `make_asgi_app` seam still asks for nothing but an `HttpRouter[T]` function;
# these just make one easy to assemble.

# `Middleware` wraps a connection handler with the lifespan state and the parsed
# scope in hand: it is handed the connection state `T`, the inner handler to call,
# and the scope, and returns a replacement handler. State leads so a cross-cutting
# middleware (auth, rate limiting, config-driven behavior) can read the same `T`
# the dispatched handler sees; a middleware that does not need it ignores the
# argument. It is generic over `T`, the protocol's handler `H`, and scope `S`, so
# one vocabulary serves HTTP and WebSocket; the aliases name the two concrete
# instances and stay generic over `T`.
type Middleware[T, H, S] = Callable[[T, H, S], H]
type HttpMiddleware[T] = Middleware[T, HttpHandler, HttpScope]
type WebsocketMiddleware[T] = Middleware[T, WebsocketHandler, WebsocketScope]


def stack[T, H, S](*middleware: Middleware[T, H, S]) -> Middleware[T, H, S]:
    """Compose middleware into one, first argument outermost. A stack of
    middleware is itself a `Middleware`, so it nests, and `stack()` is identity."""

    def composed(state: T, handler: H, scope: S) -> H:
        for one in reversed(middleware):
            handler = one(state, handler, scope)
        return handler

    return composed


def wrap[In, Out, S](
    *,
    inbound: Callable[[Stream[In], S], Stream[In]] | None = None,
    outbound: Callable[[Stream[Out], S], Stream[Out]] | None = None,
) -> Middleware[object, Processor[In, Out], S]:
    """Build a (type-preserving) `Middleware` from scope-aware stream
    transformers. Each is given the parsed scope, so a no-config middleware is
    just `wrap(inbound=..., outbound=...)`; either omitted leaves that side
    untouched.

    A stream transformer is itself a `Processor`, so wrapping is composition:
    `inbound` runs before the handler, `outbound` after it. Keeping both sides
    same-type makes the result an endomorphism, which is what `stack` composes.

    `wrap` is the scope-only end of the vocabulary: its transformers see the
    scope but not the connection state, so the produced middleware ignores `T`
    (typed `object`, and contravariantly usable in a `stack` over any state).
    A middleware that must read the state is written directly as a
    `(T, handler, scope) -> handler` function.
    """

    def middleware(_state: object, handler: Processor[In, Out], scope: S) -> Processor[In, Out]:
        wrapped = handler
        if inbound is not None:
            before = inbound

            def on_inbound(inputs: Stream[In]) -> Stream[In]:
                return before(inputs, scope)

            wrapped = compose(on_inbound, wrapped)
        if outbound is not None:
            after = outbound

            def on_outbound(inputs: Stream[Out]) -> Stream[Out]:
                return after(inputs, scope)

            wrapped = compose(wrapped, on_outbound)
        return wrapped

    return middleware


@dataclass(slots=True)
class _RequestBudget:
    # A process-wide count of in-flight requests against a fixed ceiling. Mutation
    # is safe without a lock: `admit` checks and increments with no `await` between,
    # so on the single-threaded event loop it is atomic against other admits.
    limit: int
    in_flight: int = 0

    def admit(self) -> bool:
        if self.in_flight >= self.limit:
            return False
        self.in_flight += 1
        return True

    def release(self) -> None:
        self.in_flight -= 1


_OVERLOADED = Response(
    status=503,
    headers=(
        (b"content-type", b"text/plain; charset=utf-8"),
        (b"retry-after", b"1"),
    ),
    body=b"server overloaded\n",
)


def limit_concurrent_requests(limit: int, *, overloaded: Response = _OVERLOADED) -> HttpMiddleware[object]:
    """An `HttpMiddleware` that sheds load past `limit` concurrent in-flight requests.

    While `limit` requests are already running, further requests are answered
    immediately with the `overloaded` response (a `503 Service Unavailable` carrying
    `Retry-After: 1` by default), without invoking the inner handler or reading the
    request body. Pass your own `Response` to control the status, body, and headers
    (a JSON payload, a different `Retry-After`, a `429`); it is a fixed value rather
    than a per-request callback, since the point of shedding is to answer cheaply.

    This is request-level overload shedding, the complement to a transport's
    connection-admission cap: a connection cap bounds *pipes* (and under HTTP/1.1
    that nearly bounds requests too), but one HTTP/2 connection multiplexes many
    requests, so bounding in-flight *work* belongs here, where it wraps the app
    invocation and so applies under any transport. Mount it only on the HTTP router
    to leave long-lived WebSocket connections uncounted.

    Coverage is controlled by *where* you mount it, not a per-request flag, so it
    composes with route-scoped mounting (see `without-web`) to carve out exemptions.
    Kubernetes health probes are the motivating case, and the two kinds want
    opposite treatment: a **liveness** probe should bypass the limit, since failing
    it restarts a pod that is merely busy rather than broken (and the restart sheds
    that pod's in-flight work onto already-loaded siblings). A **readiness** probe is
    the opposite: letting it shed under load is often desirable, since a `503` there
    removes the pod from the Service endpoints until it recovers, steering new
    traffic away while the backlog drains.

    The ceiling is held in one budget built when the middleware is constructed and
    shared across every request through the closure; install it once at app
    assembly. An admitted request holds a slot for as long as the handler's output
    stream runs, releasing it when that stream finishes, fails, or is cancelled.
    """
    budget = _RequestBudget(limit)
    shed = tuple(encode_response(overloaded))

    async def gated(handler: HttpHandler, inputs: Stream[Inbound]) -> AsyncIterator[Outbound]:
        if not budget.admit():
            for event in shed:
                yield event
            return
        try:
            async for event in handler(inputs):
                yield event
        finally:
            budget.release()

    def middleware(_state: object, handler: HttpHandler, _scope: HttpScope) -> HttpHandler:
        def processor(inputs: Stream[Inbound]) -> Stream[Outbound]:
            return gated(handler, inputs)

        return processor

    return middleware


def buffered[T](make: Callable[[T, HttpScope, bytes], Response]) -> HttpRouter[T]:
    """Build an `HttpRouter` that reads the whole request body and emits one
    `Response`, for handlers that don't stream. Usable as a decorator on a
    `(state, scope, body) -> Response` function.

    The handler is built with the per-request state when the request arrives;
    `make` then runs once the body is in hand. Whether that state is a live
    holder or a snapshot is the caller's choice, in how it threads the state.
    """

    def build(state: T, head: HttpScope) -> HttpHandler:
        def processor(inputs: Stream[Inbound]) -> Stream[Outbound]:
            return respond(inputs, lambda body: make(state, head, body))

        return processor

    return build


async def respond(inputs: Stream[Inbound], make: Callable[[bytes], Response]) -> AsyncIterator[Outbound]:
    body = await read_body(inputs)
    for event in encode_response(make(body)):
        yield event
