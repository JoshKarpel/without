from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from contextlib import suppress
from typing import assert_never

from without_asgi import Asgi
from without_asgi import ASGIApp
from without_asgi import LifespanScope
from without_asgi import RawMessage
from without_asgi import Shutdown
from without_asgi import ShutdownComplete
from without_asgi import ShutdownFailed
from without_asgi import Startup
from without_asgi import StartupComplete
from without_asgi import StartupFailed
from without_asgi import encode_lifespan_event
from without_asgi import encode_scope
from without_asgi import parse_lifespan_reply
from without_async import cancel_futures

# Lifespan uses its own spec version, distinct from the HTTP/WebSocket scopes.
_LIFESPAN = LifespanScope(asgi=Asgi(version="3.0", spec_version="2.0"))


class LifespanError(Exception):
    """The application reported a lifespan startup or shutdown failure."""


@asynccontextmanager
async def run_lifespan(app: ASGIApp) -> AsyncIterator[None]:
    """
    Drive the ASGI lifespan protocol around an app for the server's lifetime.

    Runs the app once with a `lifespan` scope as a background task: sends
    `lifespan.startup` on entry and waits for the app to ack, then `lifespan.shutdown`
    on exit. A startup/shutdown the app reports as failed raises `LifespanError`.

    An app that does not support lifespan signals so by raising before it acks
    startup; that is not an error, so the server continues without a lifespan
    cycle. This is the standard ASGI server fallback.
    """
    inbox: asyncio.Queue[RawMessage] = asyncio.Queue()
    started = asyncio.Event()
    finished = asyncio.Event()
    failure: list[str] = []

    async def receive() -> RawMessage:
        return await inbox.get()

    async def send(message: RawMessage) -> None:
        match parse_lifespan_reply(message):
            case StartupComplete():
                started.set()
            case StartupFailed(message=reason):
                failure.append(reason)
                started.set()
            case ShutdownComplete():
                finished.set()
            case ShutdownFailed(message=reason):
                failure.append(reason)
                finished.set()
            case _ as unreachable:
                assert_never(unreachable)

    async def drive() -> None:
        await app(encode_scope(_LIFESPAN), receive, send)

    task = asyncio.create_task(drive())
    await inbox.put(encode_lifespan_event(Startup()))

    if not await _wait_for(started, task):
        # The app finished before acking startup: lifespan is unsupported, which
        # the app signals by raising. Swallow that one signal and serve anyway.
        with suppress(Exception):
            await task
        yield
        return

    if failure:
        await task
        raise LifespanError(failure[0])

    try:
        yield
    finally:
        await inbox.put(encode_lifespan_event(Shutdown()))
        await _wait_for(finished, task)
        await task
        if failure:
            raise LifespanError(failure[0])


async def _wait_for(event: asyncio.Event, task: asyncio.Task[None]) -> bool:
    """Wait until `event` is set or `task` finishes; return whether the event won."""
    waiter = asyncio.create_task(event.wait())
    try:
        await asyncio.wait({waiter, task}, return_when=asyncio.FIRST_COMPLETED)
    finally:
        await cancel_futures([waiter])
    return event.is_set()
