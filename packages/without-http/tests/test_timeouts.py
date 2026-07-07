from __future__ import annotations

import asyncio
import ssl
from collections.abc import AsyncIterator
from collections.abc import Awaitable
from collections.abc import Callable
from contextlib import asynccontextmanager
from contextlib import suppress

import h2.config
import h2.connection
import h2.settings
import pytest
from without import cancel_futures
from without_http import ConnectionPool
from without_http import ConnectTimeout
from without_http import HTTPTimeout
from without_http import PoolTimeout
from without_http import ReadTimeout
from without_http import Timeout
from without_http import WriteTimeout
from without_http import serving
from without_http.timeouts import phase

from .conftest import HOST
from .test_client import _large_upload
from .test_client import sized_echo_app

type Handler = Callable[[asyncio.StreamReader, asyncio.StreamWriter], Awaitable[None]]


@asynccontextmanager
async def _tcp_server(handle: Handler) -> AsyncIterator[tuple[str, int]]:
    """
    A raw TCP server whose deliberately-hung handlers are cancelled on exit.

    `asyncio.start_server` does not cancel active connection handlers when the server
    closes, so a handler parked forever (simulating a hung peer) would leak; tracking and
    cancelling them keeps each test self-contained.
    """
    handlers: set[asyncio.Task[None]] = set()

    async def run(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        with suppress(asyncio.CancelledError, OSError):
            await handle(reader, writer)
        writer.close()

    def spawn(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        handlers.add(asyncio.ensure_future(run(reader, writer)))

    server = await asyncio.start_server(spawn, HOST, 0)
    host, port = server.sockets[0].getsockname()[:2]
    try:
        yield host, port
    finally:
        # Cancel the (possibly parked) handlers first: `wait_closed` blocks on live
        # connections, so a handler still hanging would deadlock the teardown.
        await cancel_futures(handlers)
        server.close()
        await server.wait_closed()


async def _hang_after_reading_request(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    await reader.read(65536)
    await asyncio.Event().wait()  # read the request, then never respond


async def _never_read(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    await asyncio.Event().wait()  # accept, then never read the request body


async def test_connect_timeout_when_the_tls_handshake_never_completes(
    trusting_client_context_factory: Callable[[], ssl.SSLContext],
) -> None:
    async def swallow(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await asyncio.Event().wait()  # accept TCP, never speak TLS

    async with _tcp_server(swallow) as (host, port):
        async with ConnectionPool(
            allow_http2=False,
            ssl_context_factory=trusting_client_context_factory,
            timeout=Timeout(connect=0.2),
        ) as pool:
            with pytest.raises(ConnectTimeout):  # pragma: no branch
                async with pool.request("GET", f"https://{host}:{port}/") as _response:  # pragma: no branch
                    pass  # pragma: no cover


async def test_read_timeout_waiting_for_the_response_head() -> None:
    async with _tcp_server(_hang_after_reading_request) as (host, port):
        async with ConnectionPool(timeout=Timeout(read=0.2)) as pool:
            with pytest.raises(ReadTimeout):  # pragma: no branch
                async with pool.request("GET", f"http://{host}:{port}/") as _response:  # pragma: no branch
                    pass  # pragma: no cover


async def test_read_timeout_stalled_mid_response_body() -> None:
    async def head_then_stall(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await reader.read(65536)
        writer.write(b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n5\r\nhello\r\n")
        await writer.drain()
        await asyncio.Event().wait()  # never send the next chunk

    async def exchange(pool: ConnectionPool, url: str) -> None:
        async with pool.request("GET", url) as (head, body):
            assert head.status == 200
            await body.read()

    async with _tcp_server(head_then_stall) as (host, port):
        async with ConnectionPool(timeout=Timeout(read=0.2)) as pool:
            with pytest.raises(ReadTimeout):  # pragma: no branch
                await exchange(pool, f"http://{host}:{port}/")


async def test_write_timeout_when_the_peer_never_reads_the_request_body() -> None:
    async with _tcp_server(_never_read) as (host, port):
        async with ConnectionPool(timeout=Timeout(write=0.2)) as pool:
            with pytest.raises(WriteTimeout):  # pragma: no branch
                async with pool.request("POST", f"http://{host}:{port}/upload", body=_large_upload()) as (
                    _head,
                    body,
                ):  # pragma: no branch
                    await body.read()  # pragma: no cover


async def test_h2_write_timeout_when_the_flow_control_window_never_opens() -> None:
    async def zero_window(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        conn = h2.connection.H2Connection(config=h2.config.H2Configuration(client_side=False, header_encoding=None))
        conn.initiate_connection()
        conn.update_settings({h2.settings.SettingCodes.INITIAL_WINDOW_SIZE: 0})  # never grant the client a window
        writer.write(conn.data_to_send())
        await writer.drain()
        while chunk := await reader.read(65536):  # pragma: no branch - cancelled while looping
            conn.receive_data(chunk)
            writer.write(conn.data_to_send())
            await writer.drain()

    async with _tcp_server(zero_window) as (host, port):
        async with ConnectionPool(force_http2_cleartext=True, timeout=Timeout(write=0.2)) as pool:
            with pytest.raises(WriteTimeout):  # pragma: no branch
                # Larger than the default 65535 connection window, so it blocks regardless
                # of the timing of the zero-window SETTINGS.
                async with pool.request("POST", f"http://{host}:{port}/upload", body=b"x" * 100_000) as (
                    _head,
                    body,
                ):  # pragma: no branch
                    await body.read()  # pragma: no cover


async def test_write_timeout_after_the_head_surfaces_at_the_body_read() -> None:
    async def head_then_never_read(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await reader.readuntil(b"\r\n\r\n")  # read just the request headers, then respond
        writer.write(b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n")
        await writer.drain()
        await asyncio.Event().wait()  # never read the request body nor send the response body

    async def exchange(pool: ConnectionPool, url: str) -> None:
        async with pool.request("POST", url, body=_large_upload()) as (head, body):
            assert head.status == 200  # the head arrives before the upload stalls out
            await body.read()

    async with _tcp_server(head_then_never_read) as (host, port):
        async with ConnectionPool(timeout=Timeout(write=0.2)) as pool:
            with pytest.raises(WriteTimeout):  # pragma: no branch
                await exchange(pool, f"http://{host}:{port}/up")


async def test_pool_timeout_when_the_per_host_bound_is_saturated() -> None:
    async with (
        serving(sized_echo_app) as server,
        ConnectionPool(max_connections_per_host=1, timeout=Timeout(pool=0.2)) as pool,
    ):
        url = f"http://{server.host}:{server.port}/items"
        async with pool.request("GET", url) as (_head, first):  # holds the only permit (body unread)
            with pytest.raises(PoolTimeout):  # pragma: no branch
                async with pool.request("GET", url) as _second:  # pragma: no branch
                    pass  # pragma: no cover
            assert await first.read() == b"GET /items body="


async def test_a_request_inherits_the_pool_timeout_when_none_is_given() -> None:
    async with _tcp_server(_hang_after_reading_request) as (host, port):
        async with ConnectionPool(timeout=Timeout(read=0.2)) as pool:
            with pytest.raises(ReadTimeout):  # pragma: no branch - inherits the pool's read bound
                async with pool.request("GET", f"http://{host}:{port}/") as _response:  # pragma: no branch
                    pass  # pragma: no cover


async def test_a_per_request_timeout_replaces_an_inherited_bound() -> None:
    async def slow_then_answer(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await reader.read(65536)
        await asyncio.sleep(0.3)  # slower than the pool's read bound
        writer.write(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nhi")
        await writer.drain()

    async with _tcp_server(slow_then_answer) as (host, port):
        async with ConnectionPool(timeout=Timeout(read=0.05)) as pool:
            # The pool default would time out at 0.05s, but this request replaces the whole
            # Timeout with a disabled one, so the slow-but-completing response arrives.
            async with pool.request("GET", f"http://{host}:{port}/", timeout=Timeout()) as (
                head,
                body,
            ):  # pragma: no branch
                assert head.status == 200
                assert await body.read() == b"hi"


def test_typed_timeouts_are_catchable_broadly_and_specifically() -> None:
    error = ReadTimeout()
    assert isinstance(error, HTTPTimeout)
    assert isinstance(error, TimeoutError)
    assert not isinstance(WriteTimeout(), ReadTimeout)


async def test_phase_does_not_rewrap_an_inner_typed_timeout() -> None:
    # A read bracket nested inside a generous write bracket: the inner, more specific
    # classification must win rather than being re-wrapped as a WriteTimeout.
    async def nested() -> None:
        async with phase(1.0, WriteTimeout):  # pragma: no branch
            async with phase(0.05, ReadTimeout):  # pragma: no branch
                await asyncio.Event().wait()

    with pytest.raises(ReadTimeout):
        await nested()
