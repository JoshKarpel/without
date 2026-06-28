from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from contextlib import suppress
from dataclasses import dataclass
from dataclasses import field

from without_asgi import RawScope
from without_asgi import Receive
from without_asgi import Send
from without_http import serving
from wsproto import ConnectionType
from wsproto import WSConnection
from wsproto.events import AcceptConnection
from wsproto.events import CloseConnection
from wsproto.events import Event
from wsproto.events import RejectConnection
from wsproto.events import Request
from wsproto.events import TextMessage

_BUFFER = 65536


@dataclass(slots=True)
class WebSocketClient:
    """A minimal wsproto-backed WebSocket client for exercising the server."""

    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    conn: WSConnection
    pending: deque[Event] = field(default_factory=deque)

    @classmethod
    async def connect(cls, host: str, port: int, path: str, *, subprotocols: tuple[str, ...] = ()) -> WebSocketClient:
        reader, writer = await asyncio.open_connection(host, port)
        conn = WSConnection(ConnectionType.CLIENT)
        writer.write(conn.send(Request(host=host, target=path, subprotocols=list(subprotocols))))
        await writer.drain()
        return cls(reader=reader, writer=writer, conn=conn)

    async def next_event(self) -> Event:
        while not self.pending:
            data = await self.reader.read(_BUFFER)
            self.conn.receive_data(data)
            self.pending.extend(self.conn.events())
            if data == b"":
                break
        return self.pending.popleft()

    async def send_text(self, text: str) -> None:
        self.writer.write(self.conn.send(TextMessage(data=text)))
        await self.writer.drain()

    async def close(self, code: int = 1000) -> None:
        with suppress(OSError):
            self.writer.write(self.conn.send(CloseConnection(code=code, reason="")))
            await self.writer.drain()

    async def aclose(self) -> None:
        self.writer.close()
        with suppress(OSError):
            await self.writer.wait_closed()


@asynccontextmanager
async def ws_session(host: str, port: int, path: str) -> AsyncIterator[WebSocketClient]:
    client = await WebSocketClient.connect(host, port, path)
    try:
        yield client
    finally:
        await client.aclose()


# These are raw ASGI WebSocket apps, the contract `without-http` must serve for
# any app (uvicorn-style). The `make_asgi_app` + typed-handler path is exercised
# end-to-end in the `integration` package.
async def echo_ws_app(scope: RawScope, receive: Receive, send: Send) -> None:
    if scope["type"] != "websocket":
        raise RuntimeError("this app serves only websocket")
    while True:
        message = await receive()
        match message["type"]:
            case "websocket.connect":
                await send({"type": "websocket.accept"})
            case "websocket.receive":
                text = message.get("text")
                if isinstance(text, str):
                    await send({"type": "websocket.send", "text": f"echo:{text}"})
            case "websocket.disconnect":
                return


async def reject_ws_app(scope: RawScope, receive: Receive, send: Send) -> None:
    if scope["type"] != "websocket":
        raise RuntimeError("this app serves only websocket")
    while True:
        message = await receive()
        if message["type"] == "websocket.connect":
            await send({"type": "websocket.close", "code": 1000})
            return


async def test_accepts_and_echoes_a_text_message() -> None:
    async with serving(echo_ws_app) as (host, port), ws_session(host, port, "/live") as client:
        accept = await client.next_event()
        assert isinstance(accept, AcceptConnection)

        await client.send_text("ping")
        echo = await client.next_event()
        assert isinstance(echo, TextMessage)
        assert echo.data == "echo:ping"


async def test_a_close_before_accept_rejects_the_handshake() -> None:
    async with serving(reject_ws_app) as (host, port), ws_session(host, port, "/live") as client:
        event = await client.next_event()

    assert isinstance(event, RejectConnection)
    assert event.status_code == 403


async def test_a_client_close_reaches_the_app_as_a_disconnect() -> None:
    seen: list[str] = []

    async def recording_ws_app(scope: RawScope, receive: Receive, send: Send) -> None:
        if scope["type"] != "websocket":
            raise RuntimeError("this app serves only websocket")
        while True:
            message = await receive()
            match message["type"]:
                case "websocket.connect":
                    await send({"type": "websocket.accept"})
                case "websocket.disconnect":
                    seen.append("disconnect")
                    return

    async with serving(recording_ws_app) as (host, port), ws_session(host, port, "/live") as client:
        accept = await client.next_event()
        assert isinstance(accept, AcceptConnection)
        await client.close()
        closed = await client.next_event()
        assert isinstance(closed, CloseConnection)

    assert seen == ["disconnect"]
