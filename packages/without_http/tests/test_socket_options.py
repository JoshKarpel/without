from __future__ import annotations

import asyncio
import socket
from collections.abc import AsyncIterator
from collections.abc import Callable
from datetime import timedelta

import pytest
from without_asgi import RawMessage
from without_asgi import Receive
from without_asgi import Send
from without_async import Seconds
from without_http import ConnectionPool
from without_http import SocketOptions
from without_http import receive_buffer_size
from without_http import send_buffer_size
from without_http import serving
from without_http import tcp_keepalive
from without_http.client import _open
from without_http.socket_options import apply_socket_options

HOST = "127.0.0.1"


@pytest.fixture
async def listener() -> AsyncIterator[tuple[str, int]]:
    """A bare TCP listener that accepts and holds connections, yielding its address."""
    accepted: list[asyncio.StreamWriter] = []

    async def hold(_reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        accepted.append(writer)

    server = await asyncio.start_server(hold, HOST, 0)
    host, port = server.sockets[0].getsockname()[:2]
    try:
        yield host, port
    finally:
        for writer in accepted:
            writer.transport.abort()
        server.close()
        await server.wait_closed()


def _keepalive_enabled(writer: asyncio.StreamWriter) -> bool:
    sock = writer.get_extra_info("socket")
    return bool(sock.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE))


async def test_open_enables_keepalive_when_configured(listener: tuple[str, int]) -> None:
    host, port = listener
    _reader, writer, _ = await _open(host, port, ssl_context=None, socket_options=tcp_keepalive())
    try:
        assert _keepalive_enabled(writer)
    finally:
        writer.close()


async def test_open_leaves_keepalive_off_when_no_options_are_given(listener: tuple[str, int]) -> None:
    host, port = listener
    _reader, writer, _ = await _open(host, port, ssl_context=None, socket_options=())
    try:
        assert not _keepalive_enabled(writer)
    finally:
        writer.close()


# Every CI platform resolves all three probe knobs (the idle one via TCP_KEEPALIVE on
# macOS, TCP_KEEPIDLE elsewhere), so these assert against `tcp_keepalive()` rather than
# naming a platform-specific constant, and run everywhere instead of skipping (a skipped
# body would leave the test uncovered, and this repo measures test files at 100%).
def test_tcp_keepalive_enables_keepalive_then_the_configured_probe_values() -> None:
    options = tcp_keepalive(idle=Seconds(45), interval=Seconds(7), count=3)
    assert options[0] == (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    assert all(level == socket.IPPROTO_TCP for level, _option, _value in options[1:])
    assert [value for _level, _option, value in options[1:]] == [45, 7, 3]


async def test_open_applies_the_configured_probe_values_to_the_socket(listener: tuple[str, int]) -> None:
    host, port = listener
    options = tcp_keepalive(idle=Seconds(45), interval=Seconds(7), count=3)
    _reader, writer, _ = await _open(host, port, ssl_context=None, socket_options=options)
    sock = writer.get_extra_info("socket")
    try:
        for level, option, value in options:
            if level == socket.IPPROTO_TCP:
                assert sock.getsockopt(level, option) == value
    finally:
        writer.close()


def test_apply_is_a_noop_when_the_transport_has_no_socket() -> None:
    apply_socket_options(None, tcp_keepalive())


def test_pool_enables_keepalive_by_default() -> None:
    assert ConnectionPool().socket_options == tcp_keepalive()


def test_a_fractional_second_probe_duration_cannot_be_expressed_at_all() -> None:
    # The OS options carry integer seconds, and `Seconds` is what says so: there is no
    # way to hand `tcp_keepalive` a finer duration, so nothing here has to check for one.
    with pytest.raises(ValueError, match="a whole number of seconds cannot express"):
        tcp_keepalive(idle=Seconds.of(timedelta(milliseconds=1500)))


def test_a_platform_missing_every_probe_knob_enables_only_so_keepalive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # An old-Windows-like platform that exposes none of the per-probe constants: each knob is
    # dropped and only the portable SO_KEEPALIVE remains (a missing constant resolves to None
    # via getattr's default, never raising).
    for name in ("TCP_KEEPIDLE", "TCP_KEEPALIVE", "TCP_KEEPINTVL", "TCP_KEEPCNT"):
        monkeypatch.delattr(socket, name, raising=False)
    assert tcp_keepalive() == ((socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),)


def test_the_idle_knob_falls_back_to_the_macos_keepalive_name(monkeypatch: pytest.MonkeyPatch) -> None:
    # macOS has no TCP_KEEPIDLE; the idle knob is spelled TCP_KEEPALIVE and used the same way.
    monkeypatch.delattr(socket, "TCP_KEEPIDLE", raising=False)
    macos_idle_option = 0x1234
    monkeypatch.setattr(socket, "TCP_KEEPALIVE", macos_idle_option, raising=False)

    options = tcp_keepalive(idle=Seconds(45))

    assert options[1] == (socket.IPPROTO_TCP, macos_idle_option, 45)


@pytest.mark.parametrize(
    ("produce", "option"),
    [(send_buffer_size, socket.SO_SNDBUF), (receive_buffer_size, socket.SO_RCVBUF)],
    ids=["send", "receive"],
)
def test_a_buffer_size_produces_one_triple_for_its_own_option(
    produce: Callable[[int], SocketOptions], option: int
) -> None:
    assert produce(4096) == ((socket.SOL_SOCKET, option, 4096),)


def test_producers_concatenate_into_one_set_in_order() -> None:
    # The headers-like property the pool leans on: each producer describes one concern, so
    # combining them is plain concatenation rather than a merge that has to understand what
    # any of them mean.
    keepalive = tcp_keepalive()
    combined = keepalive + send_buffer_size(8192) + receive_buffer_size(4096)

    assert combined == (
        *keepalive,
        (socket.SOL_SOCKET, socket.SO_SNDBUF, 8192),
        (socket.SOL_SOCKET, socket.SO_RCVBUF, 4096),
    )


async def test_open_applies_a_pinned_send_buffer_to_the_socket(listener: tuple[str, int]) -> None:
    host, port = listener
    _reader, writer, _ = await _open(host, port, ssl_context=None, socket_options=send_buffer_size(8192))
    sock = writer.get_extra_info("socket")
    try:
        # `socket(7)`: the kernel doubles the requested size for its own bookkeeping and
        # returns the doubled value, so a pinned buffer is a bound to assert, not an equality.
        assert sock.getsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF) >= 8192
    finally:
        writer.close()


async def _never_reads_the_body(scope: RawMessage, receive: Receive, send: Send) -> None:
    if scope["type"] != "http":
        raise RuntimeError("this app serves only http")
    await asyncio.Event().wait()  # take the request, then never read the body nor respond


async def _upload(writer: asyncio.StreamWriter, megabytes: int) -> None:
    for _ in range(megabytes * 10):  # pragma: no branch - a pinned peer blocks long first
        writer.write(b"x" * 100_000)
        await writer.drain()


async def test_serving_pins_the_listening_sockets_receive_buffer_for_accepted_connections() -> None:
    # `serving` has no socket to hand back, so assert the option's *effect*: an accepted
    # connection inherits the listening socket's receive buffer, so a server that never
    # reads the body can only absorb about that much before the client's write blocks.
    # Pinning both ends leaves no autotuned buffer to swallow the whole 2 MB and make this
    # a race (which is exactly how this suite's upload tests once went flaky). Left
    # unpinned, the receive buffer alone would take megabytes more than is written here.
    async with serving(_never_reads_the_body, socket_options=receive_buffer_size(8192)) as server:
        _reader, writer = await asyncio.open_connection(server.host, server.port)
        writer.get_extra_info("socket").setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 8192)
        writer.write(b"POST /upload HTTP/1.1\r\nhost: h\r\ncontent-length: 100000000\r\n\r\n")

        with pytest.raises(TimeoutError):
            await asyncio.wait_for(_upload(writer, megabytes=2), timeout=1)

        writer.close()
