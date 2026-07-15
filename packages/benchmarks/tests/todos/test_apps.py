from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from collections.abc import Callable
from contextlib import asynccontextmanager
from typing import cast

import pytest
from benchmarks.todos.apps import fastapi_todos
from benchmarks.todos.apps import without_todos
from without_asgi import ASGIApp
from without_asgi import RawMessage

# The two stacks are driven through the same in-process ASGI harness, so each test
# asserts *parity*: the without-web app and the FastAPI app, over the shared core,
# answer the happy paths with the identical rendered shape. (The 404 bodies differ
# by framework, so that case asserts only the status.)

# FastAPI is an ASGI app, but its `__call__` signature isn't structurally the
# `ASGIApp` alias, so the builder is cast at this boundary.
Stack = Callable[[], ASGIApp]
STACKS: list[Stack] = [without_todos, cast(Stack, fastapi_todos)]


@asynccontextmanager
async def _lifespan(app: ASGIApp) -> AsyncIterator[None]:
    inbox: asyncio.Queue[RawMessage] = asyncio.Queue()
    outbox: asyncio.Queue[RawMessage] = asyncio.Queue()

    async def receive() -> RawMessage:
        return await inbox.get()

    async def send(message: RawMessage) -> None:
        await outbox.put(message)

    async def drive() -> None:
        await app({"type": "lifespan", "asgi": {"version": "3.0"}}, receive, send)

    running: asyncio.Task[None] = asyncio.create_task(drive())
    await inbox.put({"type": "lifespan.startup"})
    assert await outbox.get() == {"type": "lifespan.startup.complete"}
    try:
        yield
    finally:
        await inbox.put({"type": "lifespan.shutdown"})
        assert await outbox.get() == {"type": "lifespan.shutdown.complete"}
        await running


async def _request(
    app: ASGIApp,
    method: str,
    path: str,
    *,
    body: bytes = b"",
    headers: list[tuple[bytes, bytes]] | None = None,
) -> tuple[int, object]:
    scope: RawMessage = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "scheme": "http",
        "method": method,
        "path": path,
        "raw_path": path.encode(),
        "query_string": b"",
        "headers": headers or [],
    }
    events = iter([{"type": "http.request", "body": body, "more_body": False}])

    async def receive() -> RawMessage:
        return next(events)

    sent: list[RawMessage] = []

    async def send(message: RawMessage) -> None:
        sent.append(message)

    await app(scope, receive, send)
    start = sent[0]
    status = start["status"]
    assert isinstance(status, int)
    payload = b"".join(
        message["body"]
        for message in sent
        if message["type"] == "http.response.body" and isinstance(message["body"], bytes)
    )
    return status, json.loads(payload)


@pytest.mark.parametrize("stack", STACKS)
async def test_list_renders_the_seeded_todos(stack: Stack) -> None:
    app = stack()
    async with _lifespan(app):
        status, payload = await _request(app, "GET", "/todos")
    assert status == 200
    assert payload == {
        "todos": [
            {"id": 1, "title": "write the paper", "done": False},
            {"id": 2, "title": "ship the release", "done": True},
            {"id": 3, "title": "water the plants", "done": False},
        ]
    }


@pytest.mark.parametrize("stack", STACKS)
async def test_show_returns_one_todo_by_id(stack: Stack) -> None:
    app = stack()
    async with _lifespan(app):
        status, payload = await _request(app, "GET", "/todos/2")
    assert status == 200
    assert payload == {"id": 2, "title": "ship the release", "done": True}


@pytest.mark.parametrize("stack", STACKS)
async def test_show_missing_todo_is_not_found(stack: Stack) -> None:
    app = stack()
    async with _lifespan(app):
        status, _payload = await _request(app, "GET", "/todos/404")
    assert status == 404


@pytest.mark.parametrize("stack", STACKS)
async def test_create_echoes_the_new_todo_with_the_next_id(stack: Stack) -> None:
    app = stack()
    body = json.dumps({"title": "review the draft", "done": True}).encode()
    async with _lifespan(app):
        status, payload = await _request(
            app,
            "POST",
            "/todos",
            body=body,
            headers=[
                (b"content-type", b"application/json"),
                (b"content-length", str(len(body)).encode()),
                (b"idempotency-key", b"bench-abc-123"),
            ],
        )
    assert status == 201
    assert payload == {
        "id": 4,
        "title": "review the draft",
        "done": True,
        "url": "/todos/4",
        "idempotency_key": "bench-abc-123",
    }
