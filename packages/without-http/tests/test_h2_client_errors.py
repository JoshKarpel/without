from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from contextlib import suppress

import h2.config
import h2.connection
import h2.errors
import h2.events
import h2.settings
import pytest
from without_http import ConnectionPool
from without_http import request
from without_http.client import _empty_body
from without_http.client import _Http2Connection
from without_http.client import _Stream


@asynccontextmanager
async def _idle_connection() -> AsyncIterator[_Http2Connection]:
    """A constructed `_Http2Connection` over a real socket, with no read loop running."""

    async def _hold(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            with suppress(Exception):
                await reader.read()
        finally:
            writer.close()
            with suppress(OSError):
                await writer.wait_closed()

    server = await asyncio.start_server(_hold, "127.0.0.1", 0)
    host, port = server.sockets[0].getsockname()[:2]
    reader, writer = await asyncio.open_connection(host, port)
    conn = h2.connection.H2Connection(config=h2.config.H2Configuration(client_side=True, header_encoding=None))
    connection = _Http2Connection(reader, writer, conn, asyncio.Lock(), {}, asyncio.Event())
    try:
        yield connection
    finally:
        writer.close()
        with suppress(OSError):
            await writer.wait_closed()
        server.close()
        await server.wait_closed()


def _stream() -> _Stream:
    return _Stream(window=asyncio.Event(), head=asyncio.Event(), chunks=asyncio.Queue())


async def test_handle_ignores_events_for_unknown_streams() -> None:
    async with _idle_connection() as connection:
        connection._handle(h2.events.ResponseReceived(stream_id=999, headers=[(b":status", b"200")]))
        connection._handle(h2.events.DataReceived(stream_id=999, data=b"x", flow_controlled_length=1))
        connection._handle(h2.events.TrailersReceived(stream_id=999, headers=[(b"x-end", b"1")]))
        connection._handle(h2.events.StreamEnded(stream_id=999))
        connection._handle(h2.events.StreamReset(stream_id=999))
        connection._handle(h2.events.WindowUpdated(stream_id=999, delta=10))

        assert connection._streams == {}


async def test_handle_stream_reset_errors_the_waiting_stream() -> None:
    async with _idle_connection() as connection:
        stream = _stream()
        connection._streams[7] = stream

        connection._handle(h2.events.StreamReset(stream_id=7))

        assert isinstance(stream.error, ConnectionError)
        assert stream.head.is_set()
        assert stream.window.is_set()
        assert await stream.chunks.get() is None
        assert 7 not in connection._streams


async def test_handle_connection_terminated_marks_the_connection_closed() -> None:
    async with _idle_connection() as connection:
        connection._handle(h2.events.ConnectionTerminated())

        assert connection._closed.is_set()


async def test_fail_pending_errors_a_headless_stream_but_not_one_with_a_head() -> None:
    async with _idle_connection() as connection:
        waiting = _stream()
        already_headed = _stream()
        already_headed.head.set()
        connection._streams[1] = waiting
        connection._streams[3] = already_headed

        connection._fail_pending()

        assert isinstance(waiting.error, ConnectionError)
        assert waiting.head.is_set()
        assert already_headed.error is None
        assert connection._streams == {}


async def test_abort_returns_early_for_an_already_finished_stream() -> None:
    async with _idle_connection() as connection:
        await connection.abort(999)  # not in _streams: nothing to reset


async def test_request_on_a_closed_connection_raises() -> None:
    async with _idle_connection() as connection:
        connection._closed.set()

        with pytest.raises(ConnectionError, match="closed before the request was sent"):
            await connection.request(
                method=b"GET", target=b"/", scheme="http", authority=b"h", headers=(), body=_empty_body()
            )


# Integration scenarios against deliberately misbehaving raw h2c servers.


@asynccontextmanager
async def _raw_h2_server(
    handle: object,
) -> AsyncIterator[tuple[str, int]]:
    server = await asyncio.start_server(handle, "127.0.0.1", 0)  # type: ignore[arg-type]
    host, port = server.sockets[0].getsockname()[:2]
    async with server:
        yield host, port


def _new_h2() -> h2.connection.H2Connection:
    return h2.connection.H2Connection(config=h2.config.H2Configuration(client_side=False, header_encoding=None))


async def test_request_raises_when_the_server_resets_the_stream() -> None:
    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        conn = _new_h2()
        conn.initiate_connection()
        writer.write(conn.data_to_send())
        await writer.drain()
        with suppress(ConnectionResetError, OSError):
            while chunk := await reader.read(65536):
                for event in conn.receive_data(chunk):
                    if isinstance(event, h2.events.RequestReceived):
                        conn.reset_stream(event.stream_id, error_code=h2.errors.ErrorCodes.INTERNAL_ERROR)
                writer.write(conn.data_to_send())
                await writer.drain()
        writer.close()

    async with _raw_h2_server(handle) as (host, port), ConnectionPool(force_http2_cleartext=True) as pool:
        with pytest.raises(ConnectionError, match="reset the HTTP/2 stream"):
            async with request(pool, "GET", f"http://{host}:{port}/") as _response:  # pragma: no branch
                pass  # pragma: no cover


async def test_request_raises_when_the_server_sends_goaway_without_responding() -> None:
    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        conn = _new_h2()
        conn.initiate_connection()
        writer.write(conn.data_to_send())
        await writer.drain()
        with suppress(ConnectionResetError, OSError):
            while chunk := await reader.read(65536):  # pragma: no branch
                for event in conn.receive_data(chunk):
                    if isinstance(event, h2.events.RequestReceived):
                        conn.close_connection()
                writer.write(conn.data_to_send())
                await writer.drain()
                if conn.state_machine.state.name == "CLOSED":  # pragma: no branch
                    break
        writer.close()

    async with _raw_h2_server(handle) as (host, port), ConnectionPool(force_http2_cleartext=True) as pool:
        with pytest.raises(ConnectionError):
            async with request(pool, "GET", f"http://{host}:{port}/") as _response:  # pragma: no branch
                pass  # pragma: no cover


# A DATA frame on stream 0 is a connection-level protocol error every h2 peer rejects.
_ILLEGAL_FRAME = b"\x00\x00\x00\x00\x00\x00\x00\x00\x00"


async def test_request_raises_when_the_server_violates_the_protocol() -> None:
    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        conn = _new_h2()
        conn.initiate_connection()
        writer.write(conn.data_to_send())
        await writer.drain()
        with suppress(ConnectionResetError, OSError):
            await reader.read(65536)
            writer.write(_ILLEGAL_FRAME)
            await writer.drain()
            await asyncio.sleep(0.05)
        writer.close()
        with suppress(OSError):
            await writer.wait_closed()

    async with _raw_h2_server(handle) as (host, port), ConnectionPool(force_http2_cleartext=True) as pool:
        with pytest.raises(ConnectionError):
            async with request(pool, "GET", f"http://{host}:{port}/") as _response:  # pragma: no branch
                pass  # pragma: no cover


async def _slow_body() -> AsyncIterator[bytes]:
    yield b"first"
    await asyncio.sleep(0.1)  # let the read loop observe the server's reset before the next chunk
    yield b"second"  # pragma: no cover - the reset now cancels this send before the second chunk


async def test_uploading_a_streaming_body_to_a_resetting_server_raises() -> None:
    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        conn = _new_h2()
        conn.initiate_connection()
        writer.write(conn.data_to_send())
        await writer.drain()
        with suppress(ConnectionResetError, OSError):
            while chunk := await reader.read(65536):
                for event in conn.receive_data(chunk):
                    if isinstance(event, h2.events.RequestReceived):
                        conn.reset_stream(event.stream_id, error_code=h2.errors.ErrorCodes.REFUSED_STREAM)
                writer.write(conn.data_to_send())
                await writer.drain()
        writer.close()
        with suppress(OSError):
            await writer.wait_closed()

    async with _raw_h2_server(handle) as (host, port), ConnectionPool(force_http2_cleartext=True) as pool:
        with pytest.raises(ConnectionError):
            async with request(
                pool, "POST", f"http://{host}:{port}/", body=_slow_body()
            ) as _response:  # pragma: no branch
                pass  # pragma: no cover


async def test_uploading_to_a_server_that_closes_mid_upload_raises() -> None:
    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        conn = _new_h2()
        conn.initiate_connection()
        writer.write(conn.data_to_send())
        await writer.drain()
        with suppress(ConnectionResetError, OSError):
            while chunk := await reader.read(65536):  # pragma: no branch
                events = conn.receive_data(chunk)
                if any(isinstance(event, h2.events.DataReceived) for event in events):
                    break
                writer.write(conn.data_to_send())  # pragma: no cover - the first read already carries the body
                await writer.drain()  # pragma: no cover
        writer.close()
        with suppress(OSError):
            await writer.wait_closed()

    async with _raw_h2_server(handle) as (host, port), ConnectionPool(force_http2_cleartext=True) as pool:
        with pytest.raises(ConnectionError):
            async with request(
                pool, "POST", f"http://{host}:{port}/", body=b"z" * 300_000
            ) as _response:  # pragma: no branch
                pass  # pragma: no cover


def _head_then_body_then(*, finish: str) -> object:
    """Send a head and one body chunk, then `reset` the stream or `goaway` the connection."""

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        conn = _new_h2()
        conn.initiate_connection()
        writer.write(conn.data_to_send())
        await writer.drain()
        with suppress(ConnectionResetError, OSError):
            while chunk := await reader.read(65536):
                for event in conn.receive_data(chunk):
                    if isinstance(event, h2.events.StreamEnded):
                        conn.send_headers(event.stream_id, [(b":status", b"200")])
                        conn.send_data(event.stream_id, b"partial-body", end_stream=False)
                        writer.write(conn.data_to_send())
                        await writer.drain()
                        await asyncio.sleep(0.05)  # let the client receive the head error-free first
                        if finish == "reset":
                            conn.reset_stream(event.stream_id, error_code=h2.errors.ErrorCodes.INTERNAL_ERROR)
                        else:
                            conn.close_connection()
                writer.write(conn.data_to_send())
                await writer.drain()
        writer.close()
        with suppress(OSError):
            await writer.wait_closed()

    return handle


async def test_a_response_reset_after_its_head_raises_when_the_body_is_read() -> None:
    async with (
        _raw_h2_server(_head_then_body_then(finish="reset")) as (host, port),
        ConnectionPool(force_http2_cleartext=True) as pool,
    ):
        with pytest.raises(ConnectionError, match="reset the HTTP/2 stream"):
            async with request(pool, "GET", f"http://{host}:{port}/") as (_head, body):  # pragma: no branch
                await body.read()


async def test_a_response_body_is_truncated_when_a_goaway_closes_the_connection() -> None:
    async with (
        _raw_h2_server(_head_then_body_then(finish="goaway")) as (host, port),
        ConnectionPool(force_http2_cleartext=True) as pool,
    ):
        async with request(pool, "GET", f"http://{host}:{port}/") as (head, body):
            assert head.status == 200
            await asyncio.sleep(0.15)  # let the GOAWAY mark the connection closed
            assert await body.read() == b"partial-body"


async def test_a_connection_aborted_mid_request_raises() -> None:
    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        conn = _new_h2()
        conn.initiate_connection()
        writer.write(conn.data_to_send())
        await writer.drain()
        with suppress(ConnectionResetError, OSError):
            await reader.read(65536)
            await asyncio.sleep(0.02)
        writer.transport.abort()

    async with _raw_h2_server(handle) as (host, port), ConnectionPool(force_http2_cleartext=True) as pool:
        with pytest.raises(ConnectionError):
            async with request(pool, "GET", f"http://{host}:{port}/") as _response:  # pragma: no branch
                pass  # pragma: no cover


async def test_run_loop_swallows_an_oserror_from_the_socket_and_fails_pending_streams() -> None:
    """
    A read raising `OSError` (an abrupt reset) ends the loop and fails in-flight streams.

    Reproduced deterministically rather than via a real socket: on Linux/macOS an abrupt
    close surfaces as `OSError` from the read, but on Windows it arrives as a clean EOF, so
    a socket-based test leaves the `except OSError` branch uncovered there.
    """
    async with _idle_connection() as connection:
        stream = _stream()
        connection._streams[5] = stream

        async def _raise(n: int = -1) -> bytes:
            raise OSError("connection reset")

        connection._reader.read = _raise  # type: ignore[method-assign]

        await connection._run()

        assert connection._closed.is_set()
        assert isinstance(stream.error, ConnectionError)


async def test_concurrent_first_requests_share_one_pooled_h2c_connection() -> None:
    async def fetch(pool: ConnectionPool, host: str, port: int, index: int) -> bytes:
        async with request(pool, "POST", f"http://{host}:{port}/", body=f"body{index}".encode()) as (_head, body):
            return await body.read()

    async with (
        _raw_h2_server(await _echo_h2_server()) as (host, port),
        ConnectionPool(force_http2_cleartext=True) as pool,
    ):
        results = await asyncio.gather(*(fetch(pool, host, port, index) for index in range(4)))
        assert sorted(results) == [b"body0", b"body1", b"body2", b"body3"]
        assert len(pool._h2) == 1


async def test_abandoning_an_open_h2_body_resets_the_stream() -> None:
    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        conn = _new_h2()
        conn.initiate_connection()
        writer.write(conn.data_to_send())
        await writer.drain()
        with suppress(ConnectionResetError, OSError):
            while chunk := await reader.read(65536):
                for event in conn.receive_data(chunk):
                    if isinstance(event, h2.events.RequestReceived):
                        conn.send_headers(event.stream_id, [(b":status", b"200")])
                        conn.send_data(event.stream_id, b"partial", end_stream=False)
                writer.write(conn.data_to_send())
                await writer.drain()
        writer.close()

    async with _raw_h2_server(handle) as (host, port), ConnectionPool(force_http2_cleartext=True) as pool:
        async with request(pool, "GET", f"http://{host}:{port}/") as (head, _body):
            assert head.status == 200
            # leave the block without reading the body: release aborts the stream


async def _echo_h2_server() -> object:
    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        conn = _new_h2()
        conn.initiate_connection()
        writer.write(conn.data_to_send())
        await writer.drain()
        bodies: dict[int, bytearray] = {}
        with suppress(ConnectionResetError, OSError):
            while chunk := await reader.read(65536):
                for event in conn.receive_data(chunk):
                    if isinstance(event, h2.events.RequestReceived):
                        bodies[event.stream_id] = bytearray()
                    elif isinstance(event, h2.events.DataReceived):
                        bodies[event.stream_id].extend(event.data)
                        conn.acknowledge_received_data(event.flow_controlled_length, event.stream_id)
                    elif isinstance(event, h2.events.StreamEnded):
                        conn.send_headers(event.stream_id, [(b":status", b"200")])
                        conn.send_data(event.stream_id, bytes(bodies[event.stream_id]), end_stream=True)
                writer.write(conn.data_to_send())
                await writer.drain()
        writer.close()
        with suppress(OSError):
            await writer.wait_closed()

    return handle


async def _empty_then(*parts: bytes) -> AsyncIterator[bytes]:
    for part in parts:
        yield part


async def test_h2_streams_a_request_body_skipping_empty_chunks() -> None:
    async with (
        _raw_h2_server(await _echo_h2_server()) as (host, port),
        ConnectionPool(force_http2_cleartext=True) as pool,
    ):
        upload = _empty_then(b"", b"ab", b"", b"cd")
        async with request(pool, "POST", f"http://{host}:{port}/", body=upload) as (head, body):
            assert head.status == 200
            assert await body.read() == b"abcd"


async def test_h2c_reuses_a_pooled_connection_for_a_second_request() -> None:
    async with (
        _raw_h2_server(await _echo_h2_server()) as (host, port),
        ConnectionPool(force_http2_cleartext=True) as pool,
    ):
        url = f"http://{host}:{port}/"
        async with request(pool, "POST", url, body=b"first") as (_head, body):
            assert await body.read() == b"first"
        async with request(pool, "POST", url, body=b"second") as (_head, body):
            assert await body.read() == b"second"
        assert len(pool._h2) == 1


async def _raising_body(first: bytes) -> AsyncIterator[bytes]:
    yield first
    raise ValueError("boom before head")


async def test_h2_a_request_body_error_before_the_head_surfaces_to_the_caller() -> None:
    # The echo server answers only on end-of-stream, so the head never arrives before the
    # body raises: this exercises failing a stream whose head has not yet been set.
    async with (
        _raw_h2_server(await _echo_h2_server()) as (host, port),
        ConnectionPool(force_http2_cleartext=True) as pool,
    ):
        with pytest.raises(ValueError, match="boom before head"):
            async with request(
                pool, "POST", f"http://{host}:{port}/", body=_raising_body(b"partial")
            ) as _r:  # pragma: no branch
                pass  # pragma: no cover


async def _big_body() -> AsyncIterator[bytes]:
    for _ in range(64):  # pragma: no branch - cancelled by the reset before the loop finishes
        yield b"z" * 100_000  # far past the flow-control window, so the send is still in flight on reset


async def test_h2_a_reset_during_a_large_upload_surfaces_and_does_not_strand() -> None:
    async def reset_on_request(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        conn = _new_h2()
        conn.initiate_connection()
        writer.write(conn.data_to_send())
        await writer.drain()
        with suppress(ConnectionResetError, OSError):
            while chunk := await reader.read(65536):
                for event in conn.receive_data(chunk):
                    if isinstance(event, h2.events.RequestReceived):
                        conn.reset_stream(event.stream_id, error_code=h2.errors.ErrorCodes.INTERNAL_ERROR)
                writer.write(conn.data_to_send())
                await writer.drain()
        writer.close()

    async with _raw_h2_server(reset_on_request) as (host, port), ConnectionPool(force_http2_cleartext=True) as pool:
        with pytest.raises(ConnectionError, match="reset the HTTP/2 stream"):
            async with request(pool, "POST", f"http://{host}:{port}/", body=_big_body()) as _r:  # pragma: no branch
                pass  # pragma: no cover


async def _head_status(pool: ConnectionPool, url: str) -> int:
    async with request(pool, "GET", url) as (head, _body):
        return head.status


async def test_h2_gates_new_streams_at_the_server_max_concurrent_streams() -> None:
    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        conn = _new_h2()
        conn.initiate_connection()
        conn.update_settings({h2.settings.SettingCodes.MAX_CONCURRENT_STREAMS: 1})
        writer.write(conn.data_to_send())
        await writer.drain()
        with suppress(ConnectionResetError, OSError):
            while chunk := await reader.read(65536):
                for event in conn.receive_data(chunk):
                    if isinstance(event, h2.events.RequestReceived):
                        # Head only, no END_STREAM: the stream stays open and keeps its slot.
                        conn.send_headers(event.stream_id, [(b":status", b"200")])
                writer.write(conn.data_to_send())
                await writer.drain()
        writer.close()

    async with _raw_h2_server(handle) as (host, port), ConnectionPool(force_http2_cleartext=True) as pool:
        url = f"http://{host}:{port}/"
        async with request(pool, "GET", url) as (head, _body):
            assert head.status == 200
            # The server advertised one stream, held open by this request, so a second
            # cannot be issued until this one's slot frees.
            second = asyncio.create_task(_head_status(pool, url))
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(asyncio.shield(second), timeout=0.3)
        # Leaving the block resets this stream, freeing the slot; the second proceeds.
        assert await asyncio.wait_for(second, timeout=5) == 200
