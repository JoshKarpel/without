from __future__ import annotations

from collections.abc import AsyncIterator
from collections.abc import Callable

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
