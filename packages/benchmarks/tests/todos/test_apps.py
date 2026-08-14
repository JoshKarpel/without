from __future__ import annotations

import json
from collections.abc import Callable
from typing import cast

import pytest
from benchmarks.todos.apps import fastapi_todos
from benchmarks.todos.apps import without_todos
from without_asgi import ASGIApp
from without_asgi import Content
from without_asgi import RawHeaders
from without_asgi import json_content
from without_http import request
from without_http.testing import asgi_client

# The two stacks are driven through the same in-memory client, so each test asserts
# *parity*: the without-web app and the FastAPI app, over the shared core, answer the
# happy paths with the identical rendered shape. (The 404 bodies differ by framework, so
# that case asserts only the status.) `asgi_client` runs each app's lifespan and calls it
# directly, with no socket and no wire in between.

# FastAPI is an ASGI app, but its `__call__` signature isn't structurally the
# `ASGIApp` alias, so the builder is cast at this boundary.
Stack = Callable[[], ASGIApp]
STACKS: list[Stack] = [without_todos, cast(Stack, fastapi_todos)]


async def _request(
    app: ASGIApp,
    method: str,
    path: str,
    *,
    body: bytes | Content = b"",
    headers: RawHeaders = (),
) -> tuple[int, object]:
    async with asgi_client(app) as client:
        async with request(client, method, f"http://testserver{path}", headers=headers, body=body) as (head, payload):
            return head.status, json.loads(await payload.read())


@pytest.mark.parametrize("stack", STACKS)
async def test_list_renders_the_seeded_todos(stack: Stack) -> None:
    status, payload = await _request(stack(), "GET", "/todos")

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
    status, payload = await _request(stack(), "GET", "/todos/2")

    assert status == 200
    assert payload == {"id": 2, "title": "ship the release", "done": True}


@pytest.mark.parametrize("stack", STACKS)
async def test_show_missing_todo_is_not_found(stack: Stack) -> None:
    status, _payload = await _request(stack(), "GET", "/todos/404")

    assert status == 404


@pytest.mark.parametrize("stack", STACKS)
async def test_create_echoes_the_new_todo_with_the_next_id(stack: Stack) -> None:
    status, payload = await _request(
        stack(),
        "POST",
        "/todos",
        body=json_content({"title": "review the draft", "done": True}),
        headers=((b"idempotency-key", b"bench-abc-123"),),
    )

    assert status == 201
    assert payload == {
        "id": 4,
        "title": "review the draft",
        "done": True,
        "url": "/todos/4",
        "idempotency_key": "bench-abc-123",
    }
