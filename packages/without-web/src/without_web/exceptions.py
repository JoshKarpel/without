from __future__ import annotations

from collections.abc import AsyncIterator
from collections.abc import Awaitable
from collections.abc import Callable

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

# An app's exception policy: given a raised exception, produce the reply to send
# instead, or `None` to let it propagate. Writing this as `match exc:` (or
# `try/except`) gives natural narrowing and full control, so there is no registry
# of types to handlers and no per-type erasure: the policy is just a function.
type ExceptionRecover = Callable[[Exception], Awaitable[Response | None]]
type WebsocketExceptionRecover = Callable[[Exception], Awaitable[WebsocketClose | None]]


def catching(recover: ExceptionRecover) -> HttpMiddleware[object]:
    """Build middleware that maps exceptions to a response, before the status commits.

    Exception handling is not a new mechanism: it is a `Middleware` that wraps a
    handler and watches its outbound stream. `recover` is the app's policy: it is
    handed a raised exception and returns the `Response` to send instead, or
    `None` to let the exception propagate. There is deliberately no registry of
    `type -> handler`: a `recover` written as `match exc:` narrows each case to
    its real type (no `assert isinstance`) and can re-raise, chain, or do async
    work, all of which a heterogeneous mapping could not express without a cast.

    Honest limitation: the mapping applies only while the status line can still
    be set, i.e. until the first `ResponseStart` flows out. Informational events
    (`EarlyHint`, `ResponseDebug`) precede it and do not commit the status, so an
    exception after them can still be mapped; once `ResponseStart` is on the
    wire the exception re-raises, because the handler can abort but not re-status.
    """

    def middleware(_state: object, handler: HttpHandler, scope: HttpScope) -> HttpHandler:
        async def recovering(exc: Exception) -> tuple[Outbound, ...] | None:
            response = await recover(exc)
            return None if response is None else encode_response(response)

        return _guarding(handler, _is_response_start, recovering)

    return middleware


def catching_websocket(recover: WebsocketExceptionRecover) -> WebsocketMiddleware[object]:
    """The WebSocket sibling of `catching`, mapping exceptions to a close.

    The equivalent commit point is `WebsocketAccept`: before the handshake is
    accepted a close still rejects the connection (the server turns it into a
    `403`), so a mapped exception becomes that `WebsocketClose`. Once accepted
    the connection is established and the exception re-raises. `recover` returns
    the `WebsocketClose` to send, or `None` to propagate.
    """

    def middleware(_state: object, handler: WebsocketHandler, scope: WebsocketScope) -> WebsocketHandler:
        async def recovering(exc: Exception) -> tuple[WebsocketOutbound, ...] | None:
            close = await recover(exc)
            return None if close is None else (close,)

        return _guarding(handler, _is_websocket_accept, recovering)

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
