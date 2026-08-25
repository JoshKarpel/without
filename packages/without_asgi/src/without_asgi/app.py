from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from contextlib import AsyncExitStack
from contextlib import aclosing
from dataclasses import dataclass
from typing import assert_never

from without import Processor
from without import Sink
from without import Stream
from without import close_stream
from without import stream_from_iterable

from without_asgi.inbound import Inbound
from without_asgi.inbound import Shutdown
from without_asgi.inbound import Startup
from without_asgi.inbound import WebsocketInbound
from without_asgi.outbound import Outbound
from without_asgi.outbound import Response
from without_asgi.outbound import ShutdownComplete
from without_asgi.outbound import ShutdownFailed
from without_asgi.outbound import StartupComplete
from without_asgi.outbound import StartupFailed
from without_asgi.outbound import WebsocketClose
from without_asgi.outbound import WebsocketOutbound
from without_asgi.outbound import encode_lifespan_reply
from without_asgi.outbound import encode_response
from without_asgi.scope import HttpScope
from without_asgi.scope import LifespanScope
from without_asgi.scope import WebsocketScope
from without_asgi.scope import parse_scope
from without_asgi.shell import http_inbound
from without_asgi.shell import http_outbound
from without_asgi.shell import lifespan_inbound
from without_asgi.shell import websocket_inbound
from without_asgi.shell import websocket_outbound
from without_asgi.types import ASGIApp
from without_asgi.types import RawScope
from without_asgi.types import Receive
from without_asgi.types import Send

# A `Lifespan` is a plain async context manager that sets up some state `T`,
# yields it for the server's lifetime, and tears it down. It names no ASGI types
# on purpose: the same value drives any shell (an ASGI server here, a queue
# processor or a test elsewhere). Interdependent resources compose *inside* it
# with nested `async with`, which also gives reverse-order teardown for free.
type Lifespan[T] = Callable[[], AbstractAsyncContextManager[T]]

# A `*Handler` is the `Processor` that serves one connection: it maps that
# connection's inbound event stream to its outbound one, the same `Processor`
# shape as every other `without` node, and is the only thing an app writes per
# connection. A `*Router` selects the handler for a connection from the lifespan
# state and the parsed scope: `make_asgi_app` calls it once per connection, then
# owns the receive/send wiring around the returned handler (parsing inbound,
# encoding outbound), so neither the router nor the handler touches the raw ASGI
# callables. "Router" is the role even when it always returns the same handler (a
# constant router that never dispatches on path is still one). The state is
# threaded in per call rather than captured, so it stays a value the router is
# handed, not a place it reaches into. HTTP and WebSocket have separate
# router/handler pairs because their event types differ, which keeps an HTTP
# handler from emitting a WebSocket event (and vice versa) by construction.
type HttpHandler = Processor[Inbound, Outbound]
type HttpRouter[T] = Callable[[T, HttpScope], HttpHandler]
type WebsocketHandler = Processor[WebsocketInbound, WebsocketOutbound]
type WebsocketRouter[T] = Callable[[T, WebsocketScope], WebsocketHandler]


class _Unset:
    pass


_UNSET = _Unset()


@dataclass(slots=True)
class _Cell[T]:
    # The one shared reference the ASGI process model forces: lifespan startup
    # and each request are separate `app()` calls, so the state set up by the
    # former must reach the latter through a place in the wrapper's closure. ASGI
    # guarantees startup completes before any request, so `require` is never
    # reached before `value` is set; the guard turns the can't-happen case into a
    # loud failure rather than a silent `None`.
    value: T | _Unset = _UNSET

    def require(self) -> T:
        if isinstance(self.value, _Unset):
            raise RuntimeError("lifespan startup has not completed")
        return self.value


async def _drive[T](lifespan: Lifespan[T], cell: _Cell[T], receive: Receive, send: Send) -> None:
    # The stack outlives the `startup` branch: it is entered when startup arrives
    # and closed when shutdown arrives, two separate server messages with the
    # `await receive()` for shutdown in between. A plain `async with` cannot
    # straddle that gap, which is exactly why the lifespan protocol exists. The
    # enclosing `async with` also guarantees teardown if the lifespan task is
    # cancelled before shutdown is ever sent.
    async with AsyncExitStack() as stack:
        async for event in lifespan_inbound(receive):
            match event:
                case Startup():
                    try:
                        cell.value = await stack.enter_async_context(lifespan())
                    except Exception as exc:  # noqa: BLE001 - ASGI lifespan reports any startup failure as StartupFailed
                        await send(encode_lifespan_reply(StartupFailed(message=str(exc))))
                        return
                    await send(encode_lifespan_reply(StartupComplete()))
                case Shutdown():
                    try:
                        await stack.aclose()
                    except Exception as exc:  # noqa: BLE001 - ASGI lifespan reports any shutdown failure as ShutdownFailed
                        await send(encode_lifespan_reply(ShutdownFailed(message=str(exc))))
                        return
                    await send(encode_lifespan_reply(ShutdownComplete()))
                case _ as unreachable:
                    assert_never(unreachable)


# What the default routers refuse a connection with. An `http` scope gets `501
# Not Implemented` (the HTTP status for "the server has no handler for this", as
# opposed to `500` for an unexpected failure); a `websocket` scope is closed
# before `accept`, which the ASGI server is required to turn into a `403`.
_HTTP_UNSUPPORTED = Response(
    status=501,
    headers=((b"content-type", b"text/plain; charset=utf-8"),),
    body=b"this application does not serve http\n",
)
_WEBSOCKET_UNSUPPORTED = WebsocketClose()


# The default routers each `make_asgi_app` protocol falls back to, so an app
# serves a protocol only by passing its own router to override the default. They
# ignore the threaded state, hence `object`, which keeps them assignable as the
# default for any `make_asgi_app[T]`.
def refuse_http(state: object, head: HttpScope) -> HttpHandler:
    """An `HttpRouter` that refuses every request with `501 Not Implemented`."""

    def handler(inputs: Stream[Inbound]) -> Stream[Outbound]:
        return stream_from_iterable(encode_response(_HTTP_UNSUPPORTED))

    return handler


def refuse_websocket(state: object, head: WebsocketScope) -> WebsocketHandler:
    """A `WebsocketRouter` that refuses every connection by closing before `accept` (a `403`)."""

    def handler(inputs: Stream[WebsocketInbound]) -> Stream[WebsocketOutbound]:
        return stream_from_iterable((_WEBSOCKET_UNSUPPORTED,))

    return handler


async def _closing[T](sink: Sink[T], events: Stream[T]) -> None:
    """
    Drive `events` into `sink`, closing the stream on the way out however that goes.

    The outbound counterpart of `aclosing` around the inbound stream, over `close_stream`
    because a handler returns a `Stream` rather than a generator specifically.
    """
    try:
        await sink(events)
    finally:
        await close_stream(events)


def make_asgi_app[T](
    lifespan: Lifespan[T],
    http: HttpRouter[T] = refuse_http,
    websocket: WebsocketRouter[T] = refuse_websocket,
) -> ASGIApp:
    """
    Build the ASGI app that drives `lifespan` and runs a per-connection
    `Processor` over each connection's event stream.

    This is the ASGI entrypoint: it parses each raw scope into its typed value
    and owns all the receive/send wiring. The `lifespan` scope is set up once on
    `startup` and torn down on `shutdown`, with boot failures reported as
    `lifespan.startup.failed` / `lifespan.shutdown.failed`. For a connection
    scope it calls the matching handler with the state threaded in, wraps
    `receive` into the inbound event stream, runs the returned `Processor`, and
    drains its outbound stream into `send`: the handler only ever sees streams.
    The inbound stream is closed when the handler exits, so a handler that
    abandons the request body early does not leave it dangling for GC.

    Each protocol's router defaults to one that refuses the connection, so an app
    serves a protocol only by passing its own router (an HTTP-only app passes
    `http`, a WebSocket-only app passes `websocket`). The default refusal never
    reaches app code: an HTTP scope gets a `501 Not Implemented` response, a
    WebSocket scope is closed before `accept` (which the server turns into a
    `403`). Drilling under this driver, e.g. to build a handler that needs the raw
    `receive`/`send`, is `parse_scope` plus the `http_inbound` / `http_outbound`
    (and websocket) shell functions this wires together.
    """
    cell: _Cell[T] = _Cell()

    async def app(scope: RawScope, receive: Receive, send: Send) -> None:
        match parse_scope(scope):
            case LifespanScope():
                await _drive(lifespan, cell, receive, send)
            case HttpScope() as head:
                # `aclosing` closes the inbound stream when the handler exits, so a
                # handler that abandons the request body early does not leave the
                # generator (and any resource its `finally` releases) dangling for GC.
                # `_closing` is the same guarantee on the way out, and it is the one a
                # long-lived response depends on: a client that goes away mid-stream ends
                # this at the `send` that fails, and the handler's own `finally` (an event
                # source, a heartbeat's pull task) has to run there rather than whenever
                # the collector reaches it.
                async with aclosing(http_inbound(receive)) as inbound:
                    await _closing(http_outbound(send), http(cell.require(), head)(inbound))
            case WebsocketScope() as head:
                async with aclosing(websocket_inbound(receive)) as inbound:
                    await _closing(websocket_outbound(send), websocket(cell.require(), head)(inbound))
            case _ as unreachable:
                assert_never(unreachable)

    return app
