from __future__ import annotations

import asyncio
import logging
import ssl
from collections import deque
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from contextlib import suppress
from dataclasses import dataclass
from dataclasses import field
from datetime import timedelta

import pytest
from without_asgi import ASGIApp
from without_asgi import RawScope
from without_asgi import Receive
from without_asgi import Send
from without_http import serving
from wsproto import ConnectionType
from wsproto import WSConnection
from wsproto.events import AcceptConnection
from wsproto.events import BytesMessage
from wsproto.events import CloseConnection
from wsproto.events import Event
from wsproto.events import Ping
from wsproto.events import Pong
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
    async def connect(
        cls,
        host: str,
        port: int,
        path: str,
        *,
        subprotocols: tuple[str, ...] = (),
        ssl_context: ssl.SSLContext | None = None,
    ) -> WebSocketClient:
        reader, writer = await asyncio.open_connection(host, port, ssl=ssl_context)
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
                break  # pragma: no cover - tests always read a frame before reaching EOF here
        return self.pending.popleft()

    async def send_text(self, text: str) -> None:
        self.writer.write(self.conn.send(TextMessage(data=text)))
        await self.writer.drain()

    async def send_fragmented_text(self, first: str, rest: str) -> None:
        self.writer.write(self.conn.send(TextMessage(data=first, message_finished=False)))
        self.writer.write(self.conn.send(TextMessage(data=rest, message_finished=True)))
        await self.writer.drain()

    async def send_bytes(self, data: bytes) -> None:
        self.writer.write(self.conn.send(BytesMessage(data=data)))
        await self.writer.drain()

    async def send_fragmented_bytes(self, first: bytes, rest: bytes) -> None:
        self.writer.write(self.conn.send(BytesMessage(data=first, message_finished=False)))
        self.writer.write(self.conn.send(BytesMessage(data=rest, message_finished=True)))
        await self.writer.drain()

    async def send_raw(self, raw: bytes) -> None:
        self.writer.write(raw)
        await self.writer.drain()

    async def send_close(self, code: int, reason: str) -> None:
        self.writer.write(self.conn.send(CloseConnection(code=code, reason=reason)))
        await self.writer.drain()

    async def send_ping(self) -> None:
        self.writer.write(self.conn.send(Ping()))
        await self.writer.drain()

    async def send_pong(self) -> None:
        self.writer.write(self.conn.send(Pong()))
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


# These are raw ASGI WebSocket apps, the interface `without-http` must serve for
# any app (uvicorn-style). The `make_asgi_app` + typed-handler path is exercised
# end-to-end in the `integration` package.
async def echo_ws_app(scope: RawScope, receive: Receive, send: Send) -> None:
    if scope["type"] != "websocket":
        raise RuntimeError("this app serves only websocket")
    while True:
        message = await receive()
        match message["type"]:  # pragma: no branch - tests drive only these message types
            case "websocket.connect":
                await send({"type": "websocket.accept"})
            case "websocket.receive":
                text = message.get("text")
                if isinstance(text, str):  # pragma: no branch - this app's tests only send text frames
                    await send({"type": "websocket.send", "text": f"echo:{text}"})
            case "websocket.disconnect":  # pragma: no cover - the connection is torn down before the disconnect lands
                return


async def reject_ws_app(scope: RawScope, receive: Receive, send: Send) -> None:
    if scope["type"] != "websocket":
        raise RuntimeError("this app serves only websocket")
    while True:
        message = await receive()
        if message["type"] == "websocket.connect":  # pragma: no branch - the first message is always connect
            await send({"type": "websocket.close", "code": 1000})
            return


async def echo_any_ws_app(scope: RawScope, receive: Receive, send: Send) -> None:
    """Accept, then echo back every text or binary message until the client disconnects."""
    if scope["type"] != "websocket":
        raise RuntimeError("this app serves only websocket")
    while True:
        message = await receive()
        match message["type"]:  # pragma: no branch - tests drive only these message types
            case "websocket.connect":
                await send({"type": "websocket.accept"})
            case "websocket.receive":
                text = message.get("text")
                data = message.get("bytes")
                if isinstance(text, str):
                    await send({"type": "websocket.send", "text": f"echo:{text}"})
                elif isinstance(data, bytes):  # pragma: no branch - a receive is always text or binary
                    await send({"type": "websocket.send", "bytes": data})
            case "websocket.disconnect":  # pragma: no cover - the connection is torn down before the disconnect lands
                return


@dataclass(slots=True)
class DisconnectRecorder:
    """Captures the `code` and `reason` an app sees on its `websocket.disconnect`."""

    seen: asyncio.Event = field(default_factory=asyncio.Event)
    code: object = None
    reason: object = None


def record_disconnect_app(recorder: DisconnectRecorder) -> ASGIApp:
    """Accept, then record the code/reason of the first disconnect."""

    async def app(scope: RawScope, receive: Receive, send: Send) -> None:
        if scope["type"] != "websocket":
            raise RuntimeError("this app serves only websocket")
        while True:
            message = await receive()
            match message["type"]:  # pragma: no branch - tests drive only these message types
                case "websocket.connect":
                    await send({"type": "websocket.accept"})
                case "websocket.disconnect":  # pragma: no branch - the app only sees connect then disconnect
                    recorder.code = message["code"]
                    recorder.reason = message["reason"]
                    recorder.seen.set()
                    return

    return app


def _client_close_frame(code: int, reason: str) -> bytes:
    """A client->server WebSocket close frame, masked with a zero key (payload unchanged)."""
    payload = code.to_bytes(2, "big") + reason.encode()
    return bytes([0x88, 0x80 | len(payload), 0x00, 0x00, 0x00, 0x00]) + payload


async def test_accepts_and_echoes_a_text_message() -> None:
    async with serving(echo_ws_app) as server, ws_session(server.host, server.port, "/live") as client:
        accept = await client.next_event()
        assert isinstance(accept, AcceptConnection)

        await client.send_text("ping")
        echo = await client.next_event()
        assert isinstance(echo, TextMessage)
        assert echo.data == "echo:ping"


async def test_reassembles_a_fragmented_text_message() -> None:
    async with serving(echo_any_ws_app) as server, ws_session(server.host, server.port, "/live") as client:
        assert isinstance(await client.next_event(), AcceptConnection)

        await client.send_fragmented_text("frag", "mented")
        echo = await client.next_event()
        assert isinstance(echo, TextMessage)
        assert echo.data == "echo:fragmented"


async def test_accepts_and_echoes_a_binary_message() -> None:
    async with serving(echo_any_ws_app) as server, ws_session(server.host, server.port, "/live") as client:
        assert isinstance(await client.next_event(), AcceptConnection)

        await client.send_bytes(b"\x00\x01\x02")
        echo = await client.next_event()
        assert isinstance(echo, BytesMessage)
        assert echo.data == b"\x00\x01\x02"


async def test_reassembles_a_fragmented_binary_message() -> None:
    async with serving(echo_any_ws_app) as server, ws_session(server.host, server.port, "/live") as client:
        assert isinstance(await client.next_event(), AcceptConnection)

        await client.send_fragmented_bytes(b"\x00\x01", b"\x02\x03")
        echo = await client.next_event()
        assert isinstance(echo, BytesMessage)
        assert echo.data == b"\x00\x01\x02\x03"


@pytest.mark.security("an oversized WebSocket message is rejected (1009), bounding reassembly memory")
@pytest.mark.parametrize("kind", ["text", "binary"])
async def test_a_websocket_message_over_the_cap_is_rejected(kind: str, caplog: pytest.LogCaptureFixture) -> None:
    recorder = DisconnectRecorder()
    with caplog.at_level(logging.WARNING, logger="without_http.server"):
        async with serving(record_disconnect_app(recorder), max_websocket_message_bytes=8) as server:
            async with ws_session(
                server.host, server.port, "/live"
            ) as client:  # pragma: no branch - the async-with exit arc is unobservable across parametrization
                assert isinstance(await client.next_event(), AcceptConnection)

                if kind == "text":
                    await client.send_text("x" * 20)
                else:
                    await client.send_bytes(b"x" * 20)

                event = await client.next_event()
                assert isinstance(event, CloseConnection)
                assert event.code == 1009
                assert event.reason == "message too big"

                async with asyncio.timeout(
                    5
                ):  # pragma: no branch - the async-with exit arc is unobservable across parametrization
                    await recorder.seen.wait()

    assert recorder.code == 1009
    assert recorder.reason == "message too big"
    assert "Closing WebSocket after a message exceeded 8 bytes" in caplog.text


@pytest.mark.security("the WebSocket message cap is scoped: a message within it is still delivered")
@pytest.mark.parametrize("kind", ["text", "binary"])
async def test_a_websocket_message_within_the_cap_is_delivered(kind: str) -> None:
    async with serving(echo_any_ws_app, max_websocket_message_bytes=64) as server:
        async with ws_session(server.host, server.port, "/live") as client:
            assert isinstance(await client.next_event(), AcceptConnection)

            if kind == "text":
                await client.send_text("hi")
                echo = await client.next_event()
                assert isinstance(echo, TextMessage)
                assert echo.data == "echo:hi"
            else:
                await client.send_bytes(b"hi")
                echo = await client.next_event()
                assert isinstance(echo, BytesMessage)
                assert echo.data == b"hi"


async def test_a_malformed_websocket_frame_disconnects_the_app() -> None:
    disconnected = asyncio.Event()

    async def recording_ws_app(scope: RawScope, receive: Receive, send: Send) -> None:
        if scope["type"] != "websocket":
            raise RuntimeError("this app serves only websocket")
        while True:
            message = await receive()
            match message["type"]:  # pragma: no branch - the recording app sees only these message types
                case "websocket.connect":
                    await send({"type": "websocket.accept"})
                case "websocket.disconnect":  # pragma: no branch - the app only sees connect then disconnect
                    disconnected.set()
                    return

    async with serving(recording_ws_app) as server, ws_session(server.host, server.port, "/live") as client:
        assert isinstance(await client.next_event(), AcceptConnection)
        await client.send_raw(b"\xff\xff\xff\xff\xff\xff\xff\xff")  # not a valid frame: wsproto raises
        async with asyncio.timeout(5):
            await disconnected.wait()

    assert disconnected.is_set()


async def test_responds_to_a_ping_with_a_pong() -> None:
    async with serving(echo_ws_app) as server, ws_session(server.host, server.port, "/live") as client:
        assert isinstance(await client.next_event(), AcceptConnection)

        await client.send_ping()
        assert isinstance(await client.next_event(), Pong)


async def test_ignores_an_unsolicited_pong_and_keeps_serving(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING, logger="without_http.server"):
        async with serving(echo_ws_app) as server, ws_session(server.host, server.port, "/live") as client:
            assert isinstance(await client.next_event(), AcceptConnection)

            await client.send_pong()
            await client.send_text("still-here")
            echo = await client.next_event()
            assert isinstance(echo, TextMessage)
            assert echo.data == "echo:still-here"

    assert not any("Discarding unexpected WebSocket event" in record.getMessage() for record in caplog.records)


async def test_receiving_after_a_disconnect_keeps_returning_a_disconnect() -> None:
    seen: list[object] = []

    async def draining_ws_app(scope: RawScope, receive: Receive, send: Send) -> None:
        if scope["type"] != "websocket":
            raise RuntimeError("this app serves only websocket")
        message = await receive()
        if message["type"] == "websocket.connect":  # pragma: no branch - the first message is always connect
            await send({"type": "websocket.accept"})
        # Keep receiving past the queued disconnects so a later call hits the empty-queue path.
        for _ in range(3):
            seen.append((await receive())["type"])  # noqa: PERF401 - each receive has side effects, not a pure map

    async with serving(draining_ws_app) as server, ws_session(server.host, server.port, "/live") as client:
        assert isinstance(await client.next_event(), AcceptConnection)
        await client.close()
        assert isinstance(await client.next_event(), CloseConnection)

    assert seen == ["websocket.disconnect", "websocket.disconnect", "websocket.disconnect"]


async def test_a_close_pipelined_with_the_handshake_is_read_from_trailing_data() -> None:
    disconnected = asyncio.Event()

    async def recording_ws_app(scope: RawScope, receive: Receive, send: Send) -> None:
        if scope["type"] != "websocket":
            raise RuntimeError("this app serves only websocket")
        while True:
            message = await receive()
            match message["type"]:  # pragma: no branch - the recording app sees only these message types
                case "websocket.connect":
                    await send({"type": "websocket.accept"})
                case "websocket.disconnect":  # pragma: no branch - the app only sees connect then disconnect
                    disconnected.set()
                    return

    handshake = WSConnection(ConnectionType.CLIENT).send(Request(host="127.0.0.1", target="/live"))
    # A client->server close frame (opcode 0x8) masked with a zero key, so the payload
    # is unchanged; sent in the same write as the handshake so it lands in h11's
    # trailing_data after the upgrade request is parsed.
    close_frame = b"\x88\x82\x00\x00\x00\x00\x03\xe8"

    async with serving(recording_ws_app) as server:
        _reader, writer = await asyncio.open_connection(server.host, server.port)
        writer.write(handshake + close_frame)
        await writer.drain()
        async with asyncio.timeout(5):
            await disconnected.wait()
        writer.close()
        with suppress(OSError):
            await writer.wait_closed()

    assert disconnected.is_set()


async def test_a_close_before_accept_rejects_the_handshake() -> None:
    async with serving(reject_ws_app) as server, ws_session(server.host, server.port, "/live") as client:
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
            match message["type"]:  # pragma: no branch - the recording app sees only these message types
                case "websocket.connect":
                    await send({"type": "websocket.accept"})
                case "websocket.disconnect":  # pragma: no branch - the app only sees connect then disconnect
                    seen.append("disconnect")
                    return

    async with serving(recording_ws_app) as server, ws_session(server.host, server.port, "/live") as client:
        accept = await client.next_event()
        assert isinstance(accept, AcceptConnection)
        await client.close()
        closed = await client.next_event()
        assert isinstance(closed, CloseConnection)

    assert seen == ["disconnect"]


async def test_the_handshake_scope_carries_the_socket_addresses() -> None:
    captured: dict[str, object] = {}

    async def capturing_ws_app(scope: RawScope, receive: Receive, send: Send) -> None:
        if scope["type"] != "websocket":
            raise RuntimeError("this app serves only websocket")
        captured["server"] = scope["server"]
        captured["client"] = scope["client"]
        message = await receive()
        if message["type"] == "websocket.connect":  # pragma: no branch - the first message is always connect
            await send({"type": "websocket.accept"})
        await receive()  # blocks until the connection is torn down

    async with serving(capturing_ws_app) as server, ws_session(server.host, server.port, "/live") as client:
        assert isinstance(await client.next_event(), AcceptConnection)

        assert captured["server"] == [server.host, server.port]
        client_address = captured["client"]
        assert isinstance(client_address, list)
        assert client_address[0] == server.host
        assert isinstance(client_address[1], int)
        assert client_address[1] != server.port


async def test_serves_multiple_sequential_messages() -> None:
    async with serving(echo_ws_app) as server, ws_session(server.host, server.port, "/live") as client:
        assert isinstance(await client.next_event(), AcceptConnection)

        await client.send_text("one")
        first = await client.next_event()
        assert isinstance(first, TextMessage)
        assert first.data == "echo:one"

        await client.send_text("two")
        second = await client.next_event()
        assert isinstance(second, TextMessage)
        assert second.data == "echo:two"


@pytest.mark.parametrize("kind", ["text", "binary"])
async def test_a_message_exactly_at_the_cap_is_delivered(kind: str) -> None:
    async with serving(echo_any_ws_app, max_websocket_message_bytes=8) as server:
        async with ws_session(server.host, server.port, "/live") as client:
            assert isinstance(await client.next_event(), AcceptConnection)

            if kind == "text":
                await client.send_text("12345678")
                echo = await client.next_event()
                assert isinstance(echo, TextMessage)
                assert echo.data == "echo:12345678"
            else:
                await client.send_bytes(b"12345678")
                echo = await client.next_event()
                assert isinstance(echo, BytesMessage)
                assert echo.data == b"12345678"


@pytest.mark.parametrize("kind", ["text", "binary"])
async def test_the_byte_tally_resets_between_messages(kind: str) -> None:
    async with serving(echo_any_ws_app, max_websocket_message_bytes=8) as server:
        async with ws_session(server.host, server.port, "/live") as client:
            assert isinstance(await client.next_event(), AcceptConnection)

            if kind == "text":
                await client.send_text("ab")
                assert isinstance(await client.next_event(), TextMessage)
                await client.send_text("12345678")
                second = await client.next_event()
                assert isinstance(second, TextMessage)
                assert second.data == "echo:12345678"
            else:
                await client.send_bytes(b"ab")
                assert isinstance(await client.next_event(), BytesMessage)
                await client.send_bytes(b"12345678")
                second = await client.next_event()
                assert isinstance(second, BytesMessage)
                assert second.data == b"12345678"


@pytest.mark.security("fragments count toward the WebSocket message cap, so fragmentation cannot bypass it")
@pytest.mark.parametrize("kind", ["text", "binary"])
async def test_fragments_accumulate_toward_the_cap(kind: str) -> None:
    async with serving(echo_any_ws_app, max_websocket_message_bytes=8) as server:
        async with ws_session(server.host, server.port, "/live") as client:
            assert isinstance(await client.next_event(), AcceptConnection)

            if kind == "text":
                await client.send_fragmented_text("aaaaa", "bbbbb")
            else:
                await client.send_fragmented_bytes(b"aaaaa", b"bbbbb")

            event = await client.next_event()
            assert isinstance(event, CloseConnection)
            assert event.code == 1009


@pytest.mark.parametrize(("code", "reason"), [(3001, "shutdown"), (3002, "")])
async def test_a_client_close_delivers_its_code_and_reason(code: int, reason: str) -> None:
    recorder = DisconnectRecorder()
    async with serving(record_disconnect_app(recorder)) as server:
        async with ws_session(server.host, server.port, "/live") as client:
            assert isinstance(await client.next_event(), AcceptConnection)
            await client.send_close(code, reason)
            async with asyncio.timeout(
                5
            ):  # pragma: no branch - the async-with exit arc is unobservable across parametrization
                await recorder.seen.wait()

    assert recorder.code == code
    assert recorder.reason == reason


async def test_a_close_pipelined_with_the_handshake_delivers_code_and_reason() -> None:
    recorder = DisconnectRecorder()
    handshake = WSConnection(ConnectionType.CLIENT).send(Request(host="127.0.0.1", target="/live"))
    close_frame = _client_close_frame(3006, "pipe")

    async with serving(record_disconnect_app(recorder)) as server:
        _reader, writer = await asyncio.open_connection(server.host, server.port)
        writer.write(handshake + close_frame)
        await writer.drain()
        async with asyncio.timeout(5):
            await recorder.seen.wait()
        writer.close()
        with suppress(OSError):
            await writer.wait_closed()

    assert recorder.code == 3006
    assert recorder.reason == "pipe"


async def test_an_abrupt_tcp_close_disconnects_the_app_with_1006() -> None:
    recorder = DisconnectRecorder()
    async with serving(record_disconnect_app(recorder)) as server:
        client = await WebSocketClient.connect(server.host, server.port, "/live")
        assert isinstance(await client.next_event(), AcceptConnection)
        await client.aclose()  # abrupt FIN, no WebSocket close frame
        async with asyncio.timeout(5):
            await recorder.seen.wait()

    assert recorder.code == 1006
    assert recorder.reason == ""


async def test_receiving_past_the_disconnect_yields_an_abnormal_closure() -> None:
    seen: list[tuple[object, object]] = []

    async def draining_ws_app(scope: RawScope, receive: Receive, send: Send) -> None:
        if scope["type"] != "websocket":
            raise RuntimeError("this app serves only websocket")
        message = await receive()
        if message["type"] == "websocket.connect":  # pragma: no branch - the first message is always connect
            await send({"type": "websocket.accept"})
        for _ in range(2):
            disconnect = await receive()
            seen.append((disconnect["code"], disconnect["reason"]))

    async with serving(draining_ws_app) as server, ws_session(server.host, server.port, "/live") as client:
        assert isinstance(await client.next_event(), AcceptConnection)
        await client.send_close(3010, "")
        assert isinstance(await client.next_event(), CloseConnection)

    assert seen[0] == (3010, "")
    assert seen[1] == (1006, "")


async def test_a_close_sent_after_accept_reaches_the_client_as_a_close_frame() -> None:
    async def closing_ws_app(scope: RawScope, receive: Receive, send: Send) -> None:
        if scope["type"] != "websocket":
            raise RuntimeError("this app serves only websocket")
        message = await receive()
        if message["type"] == "websocket.connect":  # pragma: no branch - the first message is always connect
            await send({"type": "websocket.accept"})
            await send({"type": "websocket.close", "code": 3333})

    async with serving(closing_ws_app) as server, ws_session(server.host, server.port, "/live") as client:
        assert isinstance(await client.next_event(), AcceptConnection)
        event = await client.next_event()

    assert isinstance(event, CloseConnection)
    assert event.code == 3333


async def test_an_idle_websocket_is_disconnected(caplog: pytest.LogCaptureFixture) -> None:
    recorder = DisconnectRecorder()
    idle_timeout = timedelta(milliseconds=100)
    with caplog.at_level(logging.WARNING, logger="without_http.server"):
        async with serving(record_disconnect_app(recorder), idle_timeout=idle_timeout) as server:
            client = await WebSocketClient.connect(server.host, server.port, "/live")
            assert isinstance(await client.next_event(), AcceptConnection)
            # Send nothing: the pump's bounded read should time out and disconnect the app.
            async with asyncio.timeout(5):
                await recorder.seen.wait()
            await client.aclose()

    assert recorder.code == 1006
    assert recorder.reason == ""
    assert any("WebSocket read pump ended on a connection error" in record.getMessage() for record in caplog.records)
