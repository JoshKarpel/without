from __future__ import annotations

from collections.abc import AsyncIterator
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Mapping

from without import Processor
from without import Stream
from without_asgi import HttpHandler
from without_asgi import HttpScope
from without_asgi import Outbound
from without_asgi import Response
from without_asgi import ResponseStart
from without_asgi import WebsocketAccept
from without_asgi import WebsocketClose
from without_asgi import WebsocketHandler
from without_asgi import WebsocketOutbound
from without_asgi import WebsocketScope
from without_asgi import encode_response
from without_asgi.routing import HttpMiddleware
from without_asgi.routing import WebsocketMiddleware

type ExceptionHandler = Callable[[Exception], Awaitable[Response]]
type WebsocketExceptionHandler = Callable[[Exception], Awaitable[WebsocketClose]]


def catching(handlers: Mapping[type[Exception], ExceptionHandler]) -> HttpMiddleware:
    """Build middleware that maps declared exceptions to responses.

    Exception handling is not a new mechanism: it is a `Middleware` that wraps a
    handler and watches its outbound stream. An exception whose type (or a base
    of it) is in `handlers` becomes that handler's `Response`; anything else
    propagates unchanged.

    Honest limitation: the mapping applies only while the status line can still
    be set, i.e. until the first `ResponseStart` flows out. Informational events
    (`EarlyHint`, `ResponseDebug`) precede it and do not commit the status, so an
    exception after them can still be mapped; once `ResponseStart` is on the
    wire the exception re-raises, because the handler can abort but not re-status.
    """

    def middleware(handler: HttpHandler, scope: HttpScope) -> HttpHandler:
        async def recover(exc: Exception) -> tuple[Outbound, ...] | None:
            mapped = _lookup(handlers, type(exc))
            return None if mapped is None else encode_response(await mapped(exc))

        return _guarding(handler, _is_response_start, recover)

    return middleware


def catching_websocket(handlers: Mapping[type[Exception], WebsocketExceptionHandler]) -> WebsocketMiddleware:
    """The WebSocket sibling of `catching`, mapping exceptions to a close.

    The equivalent commit point is `WebsocketAccept`: before the handshake is
    accepted a close still rejects the connection (the server turns it into a
    `403`), so a mapped exception becomes that `WebsocketClose`. Once accepted
    the connection is established and the exception re-raises.
    """

    def middleware(handler: WebsocketHandler, scope: WebsocketScope) -> WebsocketHandler:
        async def recover(exc: Exception) -> tuple[WebsocketOutbound, ...] | None:
            mapped = _lookup(handlers, type(exc))
            return None if mapped is None else (await mapped(exc),)

        return _guarding(handler, _is_websocket_accept, recover)

    return middleware


def _guarding[In, Out](
    handler: Processor[In, Out],
    commits: Callable[[Out], bool],
    recover: Callable[[Exception], Awaitable[tuple[Out, ...] | None]],
) -> Processor[In, Out]:
    # The shared core: relay the handler's outbound stream, remembering whether
    # the protocol's commit event has gone out. An exception before the commit is
    # offered to `recover`; once committed (or unrecognized) it propagates.
    async def processor(inputs: Stream[In]) -> AsyncIterator[Out]:
        committed = False
        try:
            async for event in handler(inputs):
                committed = committed or commits(event)
                yield event
        except Exception as exc:
            replacement = None if committed else await recover(exc)
            if replacement is None:
                raise
            for event in replacement:
                yield event

    return processor


def _is_response_start(event: Outbound) -> bool:
    return isinstance(event, ResponseStart)


def _is_websocket_accept(event: WebsocketOutbound) -> bool:
    return isinstance(event, WebsocketAccept)


def _lookup[H](handlers: Mapping[type[Exception], H], raised: type[BaseException]) -> H | None:
    for klass in raised.__mro__:
        if (handler := handlers.get(klass)) is not None:
            return handler
    return None
