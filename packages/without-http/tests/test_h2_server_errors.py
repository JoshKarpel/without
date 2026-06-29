from __future__ import annotations

import asyncio
from contextlib import suppress

import h2.config
import h2.connection
import h2.errors
import h2.events
import h2.settings
from test_server import echo_app
from test_server import receive_after_done_app
from without_asgi import RawScope
from without_asgi import Receive
from without_asgi import Send
from without_http import serving

_ILLEGAL_FRAME = b"\x00\x00\x00\x00\x00\x00\x00\x00\x00"  # DATA frame on stream 0


def _client() -> h2.connection.H2Connection:
    return h2.connection.H2Connection(config=h2.config.H2Configuration(client_side=True, header_encoding=None))


def _headers(host: str, port: int, method: str, path: str) -> list[tuple[bytes, bytes]]:
    return [
        (b":method", method.encode()),
        (b":path", path.encode()),
        (b":scheme", b"http"),
        (b":authority", f"{host}:{port}".encode()),
    ]


async def chunked_app(scope: RawScope, receive: Receive, send: Send) -> None:
    """Stream the response body across several `more_body` chunks, then a final empty one."""
    if scope["type"] != "http":
        raise RuntimeError("this app serves only http")
    await send({"type": "http.response.start", "status": 200, "headers": [(b"content-type", b"text/plain")]})
    for part in (b"one", b"two", b"three"):
        await send({"type": "http.response.body", "body": part, "more_body": True})
    await send({"type": "http.response.body", "body": b"", "more_body": False})


async def early_hint_app(scope: RawScope, receive: Receive, send: Send) -> None:
    """Send a 103 early hint before the final response."""
    if scope["type"] != "http":
        raise RuntimeError("this app serves only http")
    await send({"type": "http.response.early_hint", "links": [b"</style.css>; rel=preload"]})
    await send({"type": "http.response.start", "status": 200, "headers": [(b"content-type", b"text/plain")]})
    await send({"type": "http.response.body", "body": b"hinted"})


async def start_only_app(scope: RawScope, receive: Receive, send: Send) -> None:
    """Send the response start, then return without finishing the body."""
    if scope["type"] != "http":
        raise RuntimeError("this app serves only http")
    await send({"type": "http.response.start", "status": 200, "headers": [(b"content-type", b"text/plain")]})


async def slow_chunked_app(scope: RawScope, receive: Receive, send: Send) -> None:
    """Send a first chunk, pause for the client to reset, then send again so the write fails."""
    if scope["type"] != "http":
        raise RuntimeError("this app serves only http")
    await send({"type": "http.response.start", "status": 200, "headers": [(b"content-type", b"text/plain")]})
    await send({"type": "http.response.body", "body": b"first", "more_body": True})
    await asyncio.sleep(0.15)  # let the client's reset reach the server before the next chunk
    await send({"type": "http.response.body", "body": b"second", "more_body": True})


async def never_responds_app(scope: RawScope, receive: Receive, send: Send) -> None:
    """Never send a response, so an in-flight request stays open until cancelled."""
    if scope["type"] != "http":
        raise RuntimeError("this app serves only http")
    await asyncio.Event().wait()


async def server_push_app(scope: RawScope, receive: Receive, send: Send) -> None:
    """Attempt a server push, which HTTP/2 in without-http does not support."""
    if scope["type"] != "http":
        raise RuntimeError("this app serves only http")
    await send({"type": "http.response.push", "path": "/pushed", "headers": []})


async def _drive(
    host: str,
    port: int,
    method: str,
    path: str,
    *,
    body: bytes = b"",
) -> tuple[int, bytes, bool]:
    """Drive one h2c request to a terminal event (end, reset, or EOF) and report the outcome."""
    reader, writer = await asyncio.open_connection(host, port)
    conn = _client()
    conn.initiate_connection()
    stream_id = conn.get_next_available_stream_id()
    conn.send_headers(stream_id, _headers(host, port, method, path), end_stream=not body)
    if body:
        conn.send_data(stream_id, body, end_stream=True)
    writer.write(conn.data_to_send())
    await writer.drain()

    status = 0
    chunks: list[bytes] = []
    reset = False
    finished = False
    try:
        while not finished:
            data = await reader.read(65536)
            if not data:  # pragma: no cover - a terminal stream event ends the drive before EOF
                break
            for event in conn.receive_data(data):
                match event:
                    case h2.events.ResponseReceived(headers=headers):
                        status = int(next(value for name, value in headers if name == b":status"))
                    case h2.events.DataReceived(stream_id=sid, data=chunk, flow_controlled_length=length):
                        chunks.append(chunk)
                        conn.acknowledge_received_data(length, sid)
                    case h2.events.StreamEnded():
                        finished = True
                    case h2.events.StreamReset():
                        reset = True
                        finished = True
                    case _:
                        pass
            writer.write(conn.data_to_send())
            await writer.drain()
    finally:
        writer.close()
        with suppress(OSError):
            await writer.wait_closed()
    return status, b"".join(chunks), reset


async def test_streams_a_chunked_response_over_h2() -> None:
    async with serving(chunked_app) as server:
        status, body, _reset = await _drive(server.host, server.port, "GET", "/c")

    assert status == 200
    assert body == b"onetwothree"


async def test_sends_an_early_hint_before_the_response_over_h2() -> None:
    async with serving(early_hint_app) as server:
        status, body, _reset = await _drive(server.host, server.port, "GET", "/h")

    assert status == 200
    assert body == b"hinted"


async def test_an_unfinished_response_resets_the_stream() -> None:
    async with serving(start_only_app) as server:
        status, _body, reset = await _drive(server.host, server.port, "GET", "/s")

    assert status == 200
    assert reset is True


async def test_receiving_after_the_request_body_yields_a_disconnect_over_h2() -> None:
    async with serving(receive_after_done_app) as server:
        status, body, _reset = await _drive(server.host, server.port, "POST", "/x", body=b"payload")

    assert status == 200
    assert body == b"http.disconnect"


async def test_resetting_a_stream_mid_response_is_contained() -> None:
    async with serving(slow_chunked_app) as server:
        reader, writer = await asyncio.open_connection(server.host, server.port)
        conn = _client()
        conn.initiate_connection()
        stream_id = conn.get_next_available_stream_id()
        conn.send_headers(stream_id, _headers(server.host, server.port, "GET", "/big"), end_stream=True)
        writer.write(conn.data_to_send())
        await writer.drain()
        got_first = False
        async with asyncio.timeout(5):
            while not got_first:
                for event in conn.receive_data(await reader.read(65536)):
                    if isinstance(event, h2.events.DataReceived):
                        got_first = True
                writer.write(conn.data_to_send())
                await writer.drain()
        # Reset the stream but keep the connection open, so the app's next chunk hits the
        # closed stream and the server contains the send_data failure.
        conn.reset_stream(stream_id, error_code=h2.errors.ErrorCodes.CANCEL)
        writer.write(conn.data_to_send())
        await writer.drain()
        await asyncio.sleep(0.3)
        writer.close()
        with suppress(OSError):
            await writer.wait_closed()


async def test_a_settings_update_wakes_an_active_stream() -> None:
    async with serving(echo_app) as server:
        reader, writer = await asyncio.open_connection(server.host, server.port)
        conn = _client()
        conn.initiate_connection()
        stream_id = conn.get_next_available_stream_id()
        conn.send_headers(stream_id, _headers(server.host, server.port, "POST", "/u"), end_stream=False)
        writer.write(conn.data_to_send())
        await writer.drain()
        await asyncio.sleep(0.05)  # let the server open the stream
        conn.update_settings({h2.settings.SettingCodes.INITIAL_WINDOW_SIZE: 12345})
        writer.write(conn.data_to_send())
        await writer.drain()
        await asyncio.sleep(0.05)
        conn.send_data(stream_id, b"hello", end_stream=True)
        writer.write(conn.data_to_send())
        await writer.drain()

        status = 0
        body = b""
        finished = False
        while not finished:
            data = await reader.read(65536)
            if not data:  # pragma: no cover - the response ends the stream before EOF
                break
            for event in conn.receive_data(data):
                if isinstance(event, h2.events.ResponseReceived):
                    status = int(next(value for name, value in event.headers if name == b":status"))
                elif isinstance(event, h2.events.DataReceived):
                    body += event.data
                    conn.acknowledge_received_data(event.flow_controlled_length, event.stream_id)
                elif isinstance(event, h2.events.StreamEnded):
                    finished = True
            writer.write(conn.data_to_send())
            await writer.drain()
        writer.close()
        with suppress(OSError):
            await writer.wait_closed()

    assert status == 200
    assert body == b"POST /u hello"


async def test_a_client_goaway_closes_the_connection() -> None:
    async with serving(echo_app) as server:
        reader, writer = await asyncio.open_connection(server.host, server.port)
        conn = _client()
        conn.initiate_connection()
        stream_id = conn.get_next_available_stream_id()
        conn.send_headers(stream_id, _headers(server.host, server.port, "GET", "/g"), end_stream=True)
        conn.close_connection()  # GOAWAY in the same first packet
        writer.write(conn.data_to_send())
        await writer.drain()
        with suppress(ConnectionResetError, OSError):
            while await reader.read(65536):
                pass
        writer.close()
        with suppress(OSError):
            await writer.wait_closed()


async def test_an_illegal_first_frame_closes_the_connection() -> None:
    async with serving(echo_app) as server:
        reader, writer = await asyncio.open_connection(server.host, server.port)
        conn = _client()
        conn.initiate_connection()
        writer.write(conn.data_to_send() + _ILLEGAL_FRAME)  # preface plus a DATA frame on stream 0
        await writer.drain()
        with suppress(ConnectionResetError, OSError):
            while await reader.read(65536):
                pass
        writer.close()
        with suppress(OSError):
            await writer.wait_closed()


async def test_an_unsupported_extension_over_h2_becomes_a_500() -> None:
    async with serving(server_push_app) as server:
        status, _body, _reset = await _drive(server.host, server.port, "GET", "/p")

    assert status == 500


async def test_resetting_an_in_flight_stream_disconnects_it() -> None:
    async with serving(never_responds_app) as server:
        _reader, writer = await asyncio.open_connection(server.host, server.port)
        conn = _client()
        conn.initiate_connection()
        stream_id = conn.get_next_available_stream_id()
        conn.send_headers(stream_id, _headers(server.host, server.port, "GET", "/r"), end_stream=True)
        writer.write(conn.data_to_send())
        await writer.drain()
        await asyncio.sleep(0.1)  # let the server open the stream
        conn.reset_stream(stream_id, error_code=h2.errors.ErrorCodes.CANCEL)
        writer.write(conn.data_to_send())
        await writer.drain()
        await asyncio.sleep(0.05)  # let the server observe the reset
        writer.close()
        with suppress(OSError):
            await writer.wait_closed()


async def test_an_in_flight_stream_is_cancelled_on_server_shutdown() -> None:
    async with serving(never_responds_app) as server:
        _reader, writer = await asyncio.open_connection(server.host, server.port)
        conn = _client()
        conn.initiate_connection()
        stream_id = conn.get_next_available_stream_id()
        conn.send_headers(stream_id, _headers(server.host, server.port, "GET", "/slow"), end_stream=True)
        writer.write(conn.data_to_send())
        await writer.drain()
        await asyncio.sleep(0.1)  # let the server start the never-finishing stream
    # leaving the serving block cancels the in-flight stream task
    writer.close()
    with suppress(OSError):
        await writer.wait_closed()
