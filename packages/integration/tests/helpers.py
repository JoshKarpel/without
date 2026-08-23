from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from integration.todos.core import Todo
from integration.todos.core import TodoList
from without_asgi import ASGIApp
from without_asgi import RawMessage


def a_todo_list() -> TodoList:
    """Two todos, one done and one not, so a filter has something to say either way."""
    return TodoList({1: Todo(id=1, title="write", done=False), 2: Todo(id=2, title="ship", done=True)})


@asynccontextmanager
async def running(app: ASGIApp) -> AsyncIterator[None]:
    """Drive an app's lifespan around the block, so its startup state is there to serve with."""
    inbox: asyncio.Queue[RawMessage] = asyncio.Queue()
    outbox: asyncio.Queue[RawMessage] = asyncio.Queue()

    async def receive() -> RawMessage:
        return await inbox.get()

    async def send(message: RawMessage) -> None:
        await outbox.put(message)

    async def drive() -> None:
        await app({"type": "lifespan", "asgi": {"version": "3.0"}}, receive, send)

    lifespan = asyncio.create_task(drive())
    await inbox.put({"type": "lifespan.startup"})
    assert await outbox.get() == {"type": "lifespan.startup.complete"}
    try:
        yield
    finally:
        await inbox.put({"type": "lifespan.shutdown"})
        assert await outbox.get() == {"type": "lifespan.shutdown.complete"}
        await lifespan


async def drive_websocket(app: ASGIApp, path: str, incoming: list[RawMessage]) -> list[RawMessage]:
    """Run one websocket connection to its end, returning everything the app sent."""
    scope: RawMessage = {"type": "websocket", "asgi": {"version": "3.0"}, "path": path, "headers": []}
    events = iter(incoming)

    async def receive() -> RawMessage:
        return next(events)

    sent: list[RawMessage] = []

    async def send(message: RawMessage) -> None:
        sent.append(message)

    await app(scope, receive, send)
    return sent
