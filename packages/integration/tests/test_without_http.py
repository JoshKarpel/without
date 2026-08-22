from __future__ import annotations

import asyncio
import gzip
import json
from collections import deque
from collections.abc import AsyncIterator
from compression import zstd
from contextlib import asynccontextmanager
from contextlib import suppress
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path

import httpx
from integration.todos.app import todos_app
from integration.todos.core import Todo
from integration.todos.core import TodoList
from without import Stream
from without_asgi import ASGIApp
from without_asgi import HttpHandler
from without_asgi import HttpScope
from without_asgi import Inbound
from without_asgi import Outbound
from without_asgi import file_response
from without_asgi import headers
from without_asgi import make_asgi_app
from without_asgi.compression import DEFAULT_COMPRESSORS
from without_http import DEFAULT_DECOMPRESSORS
from without_http import Client
from without_http import add_headers
from without_http import decompress
from without_http import request
from without_http import serving
from without_http.testing import loopback_client
from wsproto import ConnectionType
from wsproto import WSConnection
from wsproto.events import AcceptConnection
from wsproto.events import Event
from wsproto.events import Request
from wsproto.events import TextMessage

from .helpers import a_todo_list

# This is the composition proof: a `without-web` router becomes an ASGI app via
# `without-asgi`'s `make_asgi_app`, that app is served by `without-http`, and it is
# driven by `without-http`'s own client. The same app would run unchanged under uvicorn.
#
# What each test needs decides how much transport it gets. Most run over
# `loopback_client`, which is the whole server and the whole wire with no socket, because
# the composition is what they assert and a bound port adds nothing to it. Two keep a real
# one: the test whose claim *is* that an ordinary third-party client can talk to this
# server over a network, and the websocket tests, since the in-memory clients speak HTTP
# only.


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


def _file_app(path: Path, drained: asyncio.Event) -> ASGIApp:
    """A minimal ASGI app that streams `path` for any request via `file_response`."""

    @asynccontextmanager
    async def lifespan() -> AsyncIterator[None]:
        yield None

    async def serve(inputs: Stream[Inbound]) -> AsyncIterator[Outbound]:
        async for event in await file_response(path, chunk_size=1024):
            yield event
        drained.set()

    def router(state: None, scope: HttpScope) -> HttpHandler:
        return serve

    return make_asgi_app(lifespan, http=router)


async def test_file_response_streams_a_file_over_without_http(tmp_path: Path) -> None:
    payload = b"%PDF-1.7\n" + bytes(range(256)) * 40  # ~10 KB, spans several 1 KB chunks
    path = tmp_path / "report.pdf"
    path.write_bytes(payload)
    drained = asyncio.Event()

    async with loopback_client(_file_app(path, drained)) as client:
        async with request(client, "GET", "http://testserver/download") as (head, body):
            assert head.status == 200
            assert headers.first(head.headers, b"content-type") == b"application/pdf"
            assert headers.first(head.headers, b"content-length") == str(len(payload)).encode()
            assert await body.read() == payload

        # The response is complete before the *handler* is: `file_response` reads each
        # chunk on a worker thread, so ending its stream costs one more thread hop after
        # the last chunk is on the wire. Waiting for the handler's own signal is what
        # makes that deterministic; a client fast enough to reach teardown first would
        # otherwise cancel the connection mid-read.
        await drained.wait()


async def test_todos_router_served_over_without_http_is_reachable_by_httpx() -> None:
    async with serving(todos_app(a_todo_list())) as server:
        async with httpx.AsyncClient(base_url=f"http://{server.host}:{server.port}") as client:
            response = await client.get("/todos")

    assert response.status_code == 200
    assert json.loads(response.text) == {
        "todos": [{"id": 1, "title": "write", "done": False}, {"id": 2, "title": "ship", "done": True}]
    }


async def test_without_http_client_gets_one_todo() -> None:
    async with (
        loopback_client(todos_app(a_todo_list())) as client,
        request(client, "GET", "http://testserver/todos/1") as (head, body),
    ):
        assert head.status == 200
        assert json.loads(await body.read()) == {"id": 1, "title": "write", "done": False}


async def test_a_missing_todo_maps_to_404() -> None:
    async with (
        loopback_client(todos_app(a_todo_list())) as client,
        request(client, "GET", "http://testserver/todos/999") as (head, _body),
    ):
        assert head.status == 404


async def _admin_status(client: Client, url: str) -> int:
    async with request(client, "GET", url) as (head, _body):
        return head.status


async def test_client_middleware_supplies_the_admin_authorization_header() -> None:
    url = "http://testserver/admin/stats"

    async with loopback_client(todos_app(a_todo_list())) as client:
        unauthorized = await _admin_status(client, url)
        authorized = add_headers((b"authorization", b"Bearer let-me-in"))(client)
        authorized_status = await _admin_status(authorized, url)

    assert unauthorized == 401
    assert authorized_status == 200


def _many_todos(count: int = 40) -> TodoList:
    """A list whose rendered JSON clears `compress`'s default size gate."""
    return TodoList(
        {index: Todo(id=index, title=f"todo number {index}", done=index % 2 == 0) for index in range(count)}
    )


def test_the_default_coding_tables_cover_the_same_codings() -> None:
    """
    The server's `DEFAULT_COMPRESSORS` and the client's `DEFAULT_DECOMPRESSORS` are
    deliberately two tables and not one: they share no factory, the encode side
    carries parameters (a level, a quality) the decode side has none of, and the two
    directions are free to diverge where that is right (`deflate` is worth decoding
    from peers that send it and not worth ever emitting).

    What must not diverge *by accident* is the coding set, since a coding added to
    one side alone means this stack no longer round-trips against itself. Nothing
    structural enforces that, so it is asserted here, from the only package that
    imports both.
    """
    assert set(DEFAULT_COMPRESSORS) == set(DEFAULT_DECOMPRESSORS)


async def test_the_server_compresses_a_body_the_client_negotiated_for() -> None:
    """
    The two halves of the content-coding story meeting over the wire: the server's
    `compress` middleware picks a coding out of the request's `accept-encoding`, and
    the client's `decompress` middleware is what put that header there and what
    decodes the answer. Neither knows about the other.
    """
    async with loopback_client(todos_app(_many_todos())) as plain:
        decoding = decompress()(plain)
        async with request(decoding, "GET", "http://testserver/todos") as (head, body):
            payload = json.loads(await body.read())
            decoded = head

    assert decoded.status == 200
    assert len(payload["todos"]) == 40
    # The decoded head no longer claims an encoding, since the bytes it described are
    # gone; `vary` survives because it describes the resource, not the encoding.
    assert headers.first(decoded.headers, b"content-encoding") is None
    assert headers.get_all(decoded.headers, b"vary") == (b"accept-encoding",)


async def test_the_compressed_body_really_is_compressed_on_the_wire() -> None:
    """The undecorated client sees what the server actually sent, which is the claim above."""
    async with loopback_client(todos_app(_many_todos())) as client:
        async with request(client, "GET", "http://testserver/todos", headers=((b"accept-encoding", b"gzip"),)) as (
            head,
            body,
        ):
            wire = await body.read()

    assert headers.first(head.headers, b"content-encoding") == b"gzip"
    unencoded = gzip.decompress(wire)
    assert len(wire) < len(unencoded)
    assert len(json.loads(unencoded)["todos"]) == 40
    # A body held whole is re-described exactly rather than falling back to chunked.
    assert headers.first(head.headers, b"content-length") == str(len(wire)).encode()


async def test_the_server_prefers_zstd_when_the_client_takes_either() -> None:
    async with loopback_client(todos_app(_many_todos())) as client:
        async with request(
            client, "GET", "http://testserver/todos", headers=((b"accept-encoding", b"gzip, zstd"),)
        ) as (head, body):
            wire = await body.read()

    assert headers.first(head.headers, b"content-encoding") == b"zstd"
    assert len(json.loads(zstd.decompress(wire))["todos"]) == 40


async def test_an_unnegotiated_response_goes_out_unencoded() -> None:
    async with loopback_client(todos_app(_many_todos())) as client:
        async with request(client, "GET", "http://testserver/todos") as (head, body):
            wire = await body.read()

    assert headers.first(head.headers, b"content-encoding") is None
    assert len(json.loads(wire)["todos"]) == 40
    # Still declared, so a shared cache keys on the header that would have changed it.
    assert headers.get_all(head.headers, b"vary") == (b"accept-encoding",)


async def test_a_streaming_response_is_encoded_as_it_streams() -> None:
    """
    `POST /todos/import` commits its `200` before the upload finishes, so the head
    goes out while the body is still being produced: the encoded path that cannot
    state a `content-length` and is framed by the transport instead.
    """
    upload = b"".join(json.dumps({"title": f"imported {index}"}).encode() + b"\n" for index in range(60))

    async with loopback_client(todos_app(a_todo_list())) as plain:
        # The undecorated client first, so the encoding is a fact about the wire and
        # not something the decoder could have papered over.
        async with request(
            plain, "POST", "http://testserver/todos/import", body=upload, headers=((b"accept-encoding", b"gzip"),)
        ) as (encoded_head, encoded_body):
            wire = await encoded_body.read()
        decoding = decompress()(plain)
        async with request(decoding, "POST", "http://testserver/todos/import", body=upload) as (head, body):
            lines = [line for line in (await body.read()).split(b"\n") if line]

    assert headers.first(encoded_head.headers, b"content-encoding") == b"gzip"
    # No `content-length` to state: the head committed before the body existed, so the
    # encoded stream is framed by the transport.
    assert headers.first(encoded_head.headers, b"content-length") is None
    assert len(wire) < len(gzip.decompress(wire))

    assert head.status == 200
    assert len(lines) == 60
    assert all(json.loads(line)["ok"] is True for line in lines)


async def test_httpx_decodes_what_the_server_encoded() -> None:
    """
    The interop claim: an ordinary third-party client, sending its own default
    `accept-encoding` over a real socket, gets a body it can read. httpx offers
    brotli when its bindings are importable, which is the server's first preference,
    so this exercises the whole default table rather than its fallback.
    """
    async with serving(todos_app(_many_todos())) as server:
        async with httpx.AsyncClient(base_url=f"http://{server.host}:{server.port}") as client:
            response = await client.get("/todos")

    assert response.status_code == 200
    assert response.headers["content-encoding"] == "br"
    assert response.headers["vary"] == "accept-encoding"
    assert len(response.json()["todos"]) == 40


async def test_websocket_session_route_over_without_http() -> None:
    async with (
        serving(todos_app(a_todo_list())) as server,
        _websocket(server.host, server.port, "/todos/session") as socket,
    ):
        accept = await socket.next_event()
        assert isinstance(accept, AcceptConnection)

        await socket.send_text(json.dumps({"title": "deploy"}))
        reply = await socket.next_event()
        assert isinstance(reply, TextMessage)
        payload = json.loads(reply.data)
        assert payload["ok"] is True
        assert payload["todo"]["title"] == "deploy"
