from __future__ import annotations

import asyncio
import json
from collections import deque
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from contextlib import suppress
from dataclasses import dataclass
from dataclasses import field

import httpx
from integration.todos.app import todos_app
from integration.todos.core import Todo
from integration.todos.core import TodoList
from without_http import ConnectionPool
from without_http import add_headers
from without_http import serving
from wsproto import ConnectionType
from wsproto import WSConnection
from wsproto.events import AcceptConnection
from wsproto.events import Event
from wsproto.events import Request
from wsproto.events import TextMessage

# This is the composition proof: a `without-web` router becomes an ASGI app via
# `without-asgi`'s `make_asgi_app`, that app is served by `without-http`, and it is
# driven by both `httpx` (any HTTP client) and `without-http`'s own client. The
# same app would run unchanged under uvicorn.


def _todos() -> TodoList:
    return TodoList({1: Todo(id=1, title="write", done=False), 2: Todo(id=2, title="ship", done=True)})


@dataclass(slots=True)
class _WebSocket:
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    conn: WSConnection
    pending: deque[Event] = field(default_factory=deque)

    async def next_event(self) -> Event:
        while not self.pending:
            data = await self.reader.read(65536)
            self.conn.receive_data(data)
            self.pending.extend(self.conn.events())
            if data == b"":  # pragma: no cover - defensive EOF guard; no test forces a vanished peer
                break
        return self.pending.popleft()

    async def send_text(self, text: str) -> None:
        self.writer.write(self.conn.send(TextMessage(data=text)))
        await self.writer.drain()


@asynccontextmanager
async def _websocket(host: str, port: int, path: str) -> AsyncIterator[_WebSocket]:
    reader, writer = await asyncio.open_connection(host, port)
    conn = WSConnection(ConnectionType.CLIENT)
    writer.write(conn.send(Request(host=host, target=path)))
    await writer.drain()
    try:
        yield _WebSocket(reader=reader, writer=writer, conn=conn)
    finally:
        writer.close()
        with suppress(OSError):
            await writer.wait_closed()


async def test_todos_router_served_over_without_http_is_reachable_by_httpx() -> None:
    async with serving(todos_app(_todos())) as server:
        async with httpx.AsyncClient(base_url=f"http://{server.host}:{server.port}") as client:
            response = await client.get("/todos")

    assert response.status_code == 200
    assert json.loads(response.text) == {
        "todos": [{"id": 1, "title": "write", "done": False}, {"id": 2, "title": "ship", "done": True}]
    }


async def test_without_http_client_gets_one_todo() -> None:
    async with (
        serving(todos_app(_todos())) as server,
        ConnectionPool() as pool,
        pool.request("GET", f"http://{server.host}:{server.port}/todos/1") as (head, body),
    ):
        assert head.status == 200
        assert json.loads(await body.read()) == {"id": 1, "title": "write", "done": False}


async def test_a_missing_todo_maps_to_404() -> None:
    async with (
        serving(todos_app(_todos())) as server,
        ConnectionPool() as pool,
        pool.request("GET", f"http://{server.host}:{server.port}/todos/999") as (head, _body),
    ):
        assert head.status == 404


async def _admin_status(pool: ConnectionPool, url: str) -> int:
    async with pool.request("GET", url) as (head, _body):
        return head.status


async def test_client_middleware_supplies_the_admin_authorization_header() -> None:
    async with serving(todos_app(_todos())) as server:
        url = f"http://{server.host}:{server.port}/admin/stats"
        async with ConnectionPool() as pool:
            unauthorized = await _admin_status(pool, url)
        async with ConnectionPool(middleware=add_headers((b"authorization", b"Bearer let-me-in"))) as authorized:
            authorized_status = await _admin_status(authorized, url)

    assert unauthorized == 401
    assert authorized_status == 200


async def test_websocket_session_route_over_without_http() -> None:
    async with serving(todos_app(_todos())) as server, _websocket(server.host, server.port, "/todos/session") as socket:
        accept = await socket.next_event()
        assert isinstance(accept, AcceptConnection)

        await socket.send_text(json.dumps({"title": "deploy"}))
        reply = await socket.next_event()
        assert isinstance(reply, TextMessage)
        payload = json.loads(reply.data)
        assert payload["ok"] is True
        assert payload["todo"]["title"] == "deploy"
