from __future__ import annotations

from collections.abc import AsyncIterator
from collections.abc import Callable

from without import Stream

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
]

# Tools for building a router, not a router. *Which* connection a route matches
# and how dispatch falls back are opinionated app choices, so without-asgi leaves
# the router to the app and ships only the unopinionated pieces: a `Middleware`
# vocabulary that composes, and `buffered` for the common request/response shape.
# The `make_asgi_app` seam still asks for nothing but an `HttpRouter[T]` function;
# these just make one easy to assemble.

# `Middleware` wraps a connection handler with the parsed scope in hand: it is
# handed the inner handler to call and returns a replacement. It is generic over
# the protocol's handler `H` and scope `S` so one vocabulary serves HTTP and
# WebSocket; the aliases name the two concrete instances.
type Middleware[H, S] = Callable[[H, S], H]
type HttpMiddleware = Middleware[HttpHandler, HttpScope]
type WebsocketMiddleware = Middleware[WebsocketHandler, WebsocketScope]


def stack[H, S](*middleware: Middleware[H, S]) -> Middleware[H, S]:
    """Compose middleware into one, first argument outermost. A stack of
    middleware is itself a `Middleware`, so it nests, and `stack()` is identity."""

    def composed(handler: H, scope: S) -> H:
        for wrap in reversed(middleware):
            handler = wrap(handler, scope)
        return handler

    return composed


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
