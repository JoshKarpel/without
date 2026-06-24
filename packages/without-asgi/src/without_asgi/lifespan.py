from __future__ import annotations

from collections.abc import Awaitable
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from contextlib import AsyncExitStack
from dataclasses import dataclass

from without_asgi.inbound import Shutdown
from without_asgi.inbound import Startup
from without_asgi.outbound import ShutdownComplete
from without_asgi.outbound import ShutdownFailed
from without_asgi.outbound import StartupComplete
from without_asgi.outbound import StartupFailed
from without_asgi.outbound import encode_lifespan_reply
from without_asgi.scope import ConnectionScope
from without_asgi.scope import HttpScope
from without_asgi.scope import LifespanScope
from without_asgi.scope import WebsocketScope
from without_asgi.scope import parse_scope
from without_asgi.shell import lifespan_inbound
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

# A request handler that receives the lifespan state explicitly, alongside the
# parsed connection scope and the ASGI receive/send. It is handed a
# `ConnectionScope`, never the lifespan scope, which `make_asgi_app` consumes
# itself. The state is threaded in per call rather than captured, so it stays a
# value the handler is handed, not a place it reaches into.
type ScopedApp[T] = Callable[[T, ConnectionScope, Receive, Send], Awaitable[None]]


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
                    except Exception as exc:
                        await send(encode_lifespan_reply(StartupFailed(message=str(exc))))
                        return
                    await send(encode_lifespan_reply(StartupComplete()))
                case Shutdown():
                    try:
                        await stack.aclose()
                    except Exception as exc:
                        await send(encode_lifespan_reply(ShutdownFailed(message=str(exc))))
                        return
                    await send(encode_lifespan_reply(ShutdownComplete()))


def make_asgi_app[T](lifespan: Lifespan[T], handler: ScopedApp[T]) -> ASGIApp:
    """Build the ASGI app that drives `lifespan` and dispatches each connection
    scope to `handler` with the yielded state.

    This is the ASGI entrypoint: it parses each raw scope into its typed value.
    The `lifespan` scope is set up once on `startup` and torn down on `shutdown`,
    with boot failures reported as `lifespan.startup.failed` /
    `lifespan.shutdown.failed`. Each connection scope (`HttpScope`,
    `WebsocketScope`) is dispatched to `handler` with the state threaded in, so
    `handler` never has to handle the lifespan scope itself. Drilling under this
    driver, e.g. to build a different one, is `parse_scope` and the per-scope
    parsers.
    """
    cell: _Cell[T] = _Cell()

    async def app(scope: RawScope, receive: Receive, send: Send) -> None:
        match parse_scope(scope):
            case LifespanScope():
                await _drive(lifespan, cell, receive, send)
            case HttpScope() | WebsocketScope() as connection:
                await handler(cell.require(), connection, receive, send)

    return app
