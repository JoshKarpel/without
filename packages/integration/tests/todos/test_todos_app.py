from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from integration.todos.app import todos_app
from integration.todos.app import todos_openapi
from integration.todos.core import Todo
from integration.todos.core import TodoList
from without_asgi import ASGIApp
from without_asgi import RawMessage


def _todos() -> TodoList:
    return TodoList({1: Todo(id=1, title="write", done=False), 2: Todo(id=2, title="ship", done=True)})


@asynccontextmanager
async def _running(app: ASGIApp) -> AsyncIterator[None]:
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


async def _request(
    app: ASGIApp, method: str, path: str, *, query: bytes = b"", body: bytes = b""
) -> tuple[int, dict[str, list[bytes]], object]:
    scope: RawMessage = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": method,
        "path": path,
        "headers": [],
        "query_string": query,
    }
    request = iter([{"type": "http.request", "body": body, "more_body": False}])

    async def receive() -> RawMessage:
        return next(request)

    sent: list[RawMessage] = []

    async def send(message: RawMessage) -> None:
        sent.append(message)

    await app(scope, receive, send)
    start = sent[0]
    status = start["status"]
    assert isinstance(status, int)
    headers = start["headers"]
    assert isinstance(headers, list)
    collected: dict[str, list[bytes]] = {}
    for name, value in headers:
        collected.setdefault(name.decode(), []).append(value)
    payload = b"".join(m["body"] for m in sent if m["type"] == "http.response.body" and isinstance(m["body"], bytes))
    if not payload:
        return status, collected, None
    try:
        return status, collected, json.loads(payload)
    except json.JSONDecodeError:
        return status, collected, payload


async def test_lists_todos_with_the_powered_by_header() -> None:
    app = todos_app(_todos())
    async with _running(app):
        status, headers, body = await _request(app, "GET", "/todos")
    assert status == 200
    assert headers["x-powered-by"] == [b"without-web"]
    assert body == {"todos": [{"id": 1, "title": "write", "done": False}, {"id": 2, "title": "ship", "done": True}]}


async def test_filters_todos_by_the_done_query_parameter() -> None:
    app = todos_app(_todos())
    async with _running(app):
        _status, _headers, body = await _request(app, "GET", "/todos", query=b"done=true")
    assert body == {"todos": [{"id": 2, "title": "ship", "done": True}]}


async def test_shows_one_todo_by_typed_path_parameter() -> None:
    app = todos_app(_todos())
    async with _running(app):
        status, _headers, body = await _request(app, "GET", "/todos/1")
    assert status == 200
    assert body == {"id": 1, "title": "write", "done": False}


async def test_a_missing_todo_is_a_mapped_404() -> None:
    app = todos_app(_todos())
    async with _running(app):
        status, _headers, body = await _request(app, "GET", "/todos/99")
    assert status == 404
    assert body == {"error": "no todo with id 99", "id": 99}


async def test_creating_a_todo_echoes_the_next_id() -> None:
    app = todos_app(_todos())
    async with _running(app):
        status, _headers, body = await _request(app, "POST", "/todos", body=b'{"title": "deploy"}')
    assert status == 201
    assert body == {"id": 3, "title": "deploy", "done": False}


async def test_an_invalid_body_is_a_mapped_422() -> None:
    app = todos_app(_todos())
    async with _running(app):
        status, _headers, body = await _request(app, "POST", "/todos", body=b"{}")
    assert status == 422
    assert isinstance(body, dict) and body["error"] == "invalid todo body"


async def test_a_known_path_with_the_wrong_method_is_405_with_allow() -> None:
    app = todos_app(_todos())
    async with _running(app):
        status, headers, _body = await _request(app, "PUT", "/todos")
    assert status == 405
    assert headers["allow"] == [b"GET, POST"]


async def test_an_unknown_path_is_the_404_fallback() -> None:
    app = todos_app(_todos())
    async with _running(app):
        status, _headers, body = await _request(app, "GET", "/nope")
    assert status == 404
    assert body == {"error": "no route for GET /nope"}


async def test_the_admin_router_is_grafted_under_its_prefix() -> None:
    app = todos_app(_todos())
    async with _running(app):
        status, _headers, body = await _request(app, "GET", "/admin/stats")
    assert status == 200
    assert body == {"total": 2, "done": 1}


async def test_the_opaque_mount_sees_the_prefix_trimmed_scope() -> None:
    app = todos_app(_todos())
    async with _running(app):
        _status, _headers, body = await _request(app, "GET", "/legacy/ping")
    assert body == {"path": "/ping", "root_path": "/legacy"}


def _dig(value: object, *keys: str) -> object:
    for key in keys:
        assert isinstance(value, dict)
        value = value[key]
    return value


async def test_openapi_merges_router_and_handler_halves() -> None:
    spec = todos_openapi()
    paths = spec["paths"]
    assert isinstance(paths, dict)
    # Opaque mounts are black boxes: neither the prefix nor its catch-all appear.
    assert set(paths) == {"/todos", "/todos/{id}", "/admin/stats"}

    show_params = _dig(spec, "paths", "/todos/{id}", "get", "parameters")
    assert isinstance(show_params, list)
    assert {"name": "id", "in": "path", "required": True, "schema": {"type": "integer"}} in show_params

    properties = _dig(
        spec, "paths", "/todos", "post", "requestBody", "content", "application/json", "schema", "properties"
    )
    assert isinstance(properties, dict)
    assert "title" in properties

    list_params = _dig(spec, "paths", "/todos", "get", "parameters")
    assert isinstance(list_params, list)
    assert [param["in"] for param in list_params if isinstance(param, dict)] == ["query"]


async def _websocket(app: ASGIApp, path: str, incoming: list[RawMessage]) -> list[RawMessage]:
    scope: RawMessage = {"type": "websocket", "asgi": {"version": "3.0"}, "path": path, "headers": []}
    events = iter(incoming)

    async def receive() -> RawMessage:
        return next(events)

    sent: list[RawMessage] = []

    async def send(message: RawMessage) -> None:
        sent.append(message)

    await app(scope, receive, send)
    return sent


async def test_the_feed_prefixes_each_frame_with_the_todo_title() -> None:
    app = todos_app(_todos())
    async with _running(app):
        sent = await _websocket(
            app,
            "/todos/1/events",
            [
                {"type": "websocket.connect"},
                {"type": "websocket.receive", "text": "soon"},
                {"type": "websocket.disconnect", "code": 1000},
            ],
        )
    assert sent[0]["type"] == "websocket.accept"
    assert sent[1] == {"type": "websocket.send", "text": "write: soon"}


async def test_the_feed_for_an_unknown_todo_closes_before_accept() -> None:
    app = todos_app(_todos())
    sent: list[RawMessage] = []

    async def receive() -> RawMessage:
        raise AssertionError("an unknown todo is rejected without reading frames")

    async def send(message: RawMessage) -> None:
        sent.append(message)

    async with _running(app):
        await app(
            {"type": "websocket", "asgi": {"version": "3.0"}, "path": "/todos/99/events", "headers": []}, receive, send
        )

    assert sent == [{"type": "websocket.close", "code": 4404, "reason": "no todo with id 99"}]
