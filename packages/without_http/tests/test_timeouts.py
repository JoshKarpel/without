from __future__ import annotations

import asyncio
import ssl
from collections.abc import AsyncIterator
from collections.abc import Awaitable
from collections.abc import Callable
from contextlib import asynccontextmanager
from contextlib import suppress
from datetime import timedelta

import h2.config
import h2.connection
import h2.settings
import pytest
from without_async import cancel_futures
from without_http import Client
from without_http import ConnectionPool
from without_http import ConnectTimeout
from without_http import HTTPTimeout
from without_http import PoolTimeout
from without_http import ReadTimeout
from without_http import Timeout
from without_http import WriteTimeout
from without_http import deadline
from without_http import request
from without_http import serving

from .helpers import HOST
from .helpers import large_upload
from .helpers import sized_echo_app

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
        ) as pool:
            with pytest.raises(ConnectTimeout):  # pragma: no branch
                async with request(
                    pool, "GET", f"https://{host}:{port}/", timeout=Timeout(connect=timedelta(seconds=0.2))
                ) as _response:  # pragma: no branch
                    pass  # pragma: no cover


async def test_read_timeout_waiting_for_the_response_head() -> None:
    async with _tcp_server(_hang_after_reading_request) as (host, port):
        async with ConnectionPool() as pool:
            with pytest.raises(ReadTimeout):  # pragma: no branch
                async with request(
                    pool, "GET", f"http://{host}:{port}/", timeout=Timeout(read=timedelta(seconds=0.2))
                ) as _response:  # pragma: no branch
                    pass  # pragma: no cover


async def test_read_timeout_stalled_mid_response_body() -> None:
    async def head_then_stall(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await reader.read(65536)
        writer.write(b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n5\r\nhello\r\n")
        await writer.drain()
        await asyncio.Event().wait()  # never send the next chunk

    async def exchange(client: Client, url: str) -> None:
        async with request(client, "GET", url, timeout=Timeout(read=timedelta(seconds=0.2))) as (head, body):
            assert head.status == 200
            await body.read()

    async with _tcp_server(head_then_stall) as (host, port):
        async with ConnectionPool() as pool:
            with pytest.raises(ReadTimeout):  # pragma: no branch
                await exchange(pool, f"http://{host}:{port}/")


async def test_write_timeout_when_the_peer_never_reads_the_request_body() -> None:
    async with _tcp_server(_never_read) as (host, port):
        async with ConnectionPool() as pool:
            with pytest.raises(WriteTimeout):  # pragma: no branch
                async with request(
                    pool,
                    "POST",
                    f"http://{host}:{port}/upload",
                    body=large_upload(),
                    timeout=Timeout(write=timedelta(seconds=0.2)),
                ) as (
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
        async with ConnectionPool(force_http2_cleartext=True) as pool:
            with pytest.raises(WriteTimeout):  # pragma: no branch
                # Larger than the default 65535 connection window, so it blocks regardless
                # of the timing of the zero-window SETTINGS.
                async with request(
                    pool,
                    "POST",
                    f"http://{host}:{port}/upload",
                    body=b"x" * 100_000,
                    timeout=Timeout(write=timedelta(seconds=0.2)),
                ) as (
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

    async def exchange(client: Client, url: str) -> None:
        bound = Timeout(write=timedelta(seconds=0.2))
        async with request(client, "POST", url, body=large_upload(), timeout=bound) as (head, body):
            assert head.status == 200  # the head arrives before the upload stalls out
            await body.read()

    async with _tcp_server(head_then_never_read) as (host, port):
        async with ConnectionPool() as pool:
            with pytest.raises(WriteTimeout):  # pragma: no branch
                await exchange(pool, f"http://{host}:{port}/up")


async def test_pool_timeout_when_the_per_host_bound_is_saturated() -> None:
    async with serving(sized_echo_app) as server, ConnectionPool(max_connections_per_host=1) as pool:
        url = f"http://{server.host}:{server.port}/items"
        bound = Timeout(pool=timedelta(seconds=0.2))
        async with request(pool, "GET", url, timeout=bound) as (_head, first):  # holds the only permit
            with pytest.raises(PoolTimeout):  # pragma: no branch
                async with request(pool, "GET", url, timeout=bound) as _second:  # pragma: no branch
                    pass  # pragma: no cover
            assert await first.read() == b"GET /items body="


async def test_deadline_middleware_bounds_a_request_that_states_no_budget() -> None:
    async with _tcp_server(_hang_after_reading_request) as (host, port):
        async with ConnectionPool() as pool:
            client = deadline(Timeout(read=timedelta(seconds=0.2)))(pool)
            with pytest.raises(ReadTimeout):  # pragma: no branch - takes the middleware's read bound
                async with request(client, "GET", f"http://{host}:{port}/") as _response:  # pragma: no branch
                    pass  # pragma: no cover


async def test_a_requests_own_budget_survives_the_deadline_middleware() -> None:
    async def slow_then_answer(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await reader.read(65536)
        await asyncio.sleep(0.3)  # slower than the default bound, faster than this request's own
        writer.write(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nhi")
        await writer.drain()

    async with _tcp_server(slow_then_answer) as (host, port):
        async with ConnectionPool() as pool:
            client = deadline(Timeout(read=timedelta(seconds=0.05)))(pool)
            # The default would time out at 0.05s, but this request states a budget of its
            # own, which the middleware leaves alone, so the slow response arrives.
            async with request(client, "GET", f"http://{host}:{port}/", timeout=Timeout(read=timedelta(seconds=2))) as (
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


async def test_an_inner_bound_is_not_rewrapped_by_a_generous_outer_bound() -> None:
    # A read bound nested inside a generous write bound: the inner, more specific
    # classification must win rather than being re-wrapped as a WriteTimeout.
    timeout = Timeout(write=timedelta(seconds=1), read=timedelta(seconds=0.05))

    async def nested() -> None:
        async with timeout.writing():  # pragma: no branch
            async with timeout.reading():  # pragma: no branch
                await asyncio.Event().wait()

    with pytest.raises(ReadTimeout):
        await nested()
