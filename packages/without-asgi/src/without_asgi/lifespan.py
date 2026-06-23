from __future__ import annotations

from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager, AsyncExitStack
from dataclasses import dataclass

from without_asgi.core import (
    ASGIApp,
    Receive,
    Scope,
    Send,
    Shutdown,
    ShutdownComplete,
    ShutdownFailed,
    Startup,
    StartupComplete,
    StartupFailed,
    encode_lifespan_reply,
    scope_type,
)
from without_asgi.shell import lifespan_inbound

# A `Lifespan` is a plain async context manager that sets up some state `T`,
# yields it for the server's lifetime, and tears it down. It names no ASGI types
# on purpose: the same value drives any shell (an ASGI server here, a queue
# processor or a test elsewhere). Interdependent resources compose *inside* it
# with nested `async with`, which also gives reverse-order teardown for free.
type Lifespan[T] = Callable[[], AbstractAsyncContextManager[T]]

# A request handler that receives the lifespan state explicitly, alongside the
# ASGI triple. The state is threaded in per call rather than captured, so it
# stays a value the handler is handed, not a place it reaches into.
type ScopedApp[T] = Callable[[T, Scope, Receive, Send], Awaitable[None]]


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


def with_lifespan[T](lifespan: Lifespan[T], app: ScopedApp[T]) -> ASGIApp:
    """Drive a portable `lifespan` over the ASGI lifespan protocol, passing every
    other scope to `app` with the yielded state.

    The `lifespan` is set up once on `startup` and torn down on `shutdown`, with
    boot failures reported as `lifespan.startup.failed` / `lifespan.shutdown.failed`.
    Each non-lifespan scope is dispatched to `app` with the state threaded in.
    """
    cell: _Cell[T] = _Cell()

    async def wrapped(scope: Scope, receive: Receive, send: Send) -> None:
        if scope_type(scope) == "lifespan":
            await _drive(lifespan, cell, receive, send)
            return
        await app(cell.require(), scope, receive, send)

    return wrapped
