from __future__ import annotations

import asyncio
import ssl
from collections import deque
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from contextlib import suppress
from dataclasses import dataclass
from dataclasses import field

import h2.config
import h2.connection
from without_asgi import ASGIApp
from without_asgi import HttpScope
from without_asgi import RawScope
from without_asgi import Receive
from without_asgi import Response
from without_asgi import Send
from without_asgi import make_asgi_app
from without_asgi import parse_http_scope
from without_asgi.routing import buffered
from without_http.testing import AUTHORITY
from wsproto import ConnectionType
from wsproto import WSConnection
from wsproto.events import BytesMessage
from wsproto.events import CloseConnection
from wsproto.events import Event
from wsproto.events import Ping
from wsproto.events import Pong
from wsproto.events import Request
from wsproto.events import TextMessage

HOST = "127.0.0.1"

_BUFFER = 65536


async def read_body(receive: Receive) -> bytes:
    """Drain an ASGI request body into one `bytes`, as an app that buffers would."""
    body = b""
    more = True
    while more:
        message = await receive()
        if message["type"] == "http.disconnect":  # pragma: no cover - clients here never disconnect mid-body
            break
        chunk = message.get("body", b"")
        assert isinstance(chunk, bytes)
        body += chunk
        more = bool(message.get("more_body", False))
    return body


async def chunks(*parts: bytes) -> AsyncIterator[bytes]:
    """A request body that arrives in the given pieces, empty ones included."""
    for part in parts:
        yield part


async def large_upload() -> AsyncIterator[bytes]:
    """A body big enough that writing it blocks on a peer that never reads."""
    # To block, the upload must outrun the sender's send buffer *plus* the receiver's
    # receive buffer, since a peer that never reads still absorbs both. Linux autotunes
    # each up to `net.ipv4.tcp_wmem`/`tcp_rmem` (~10 MB combined on default kernels, and
    # more on a tuned host), so a cap sized against one buffer is a race the kernel wins:
    # the whole body lands in the buffers, nothing blocks, and the test hangs.
    #
    # The cap is generous rather than tuned because it costs nothing to raise: the
    # generator is lazy, so a consumer that blocks (or is cancelled by an early response)
    # only ever pays for the chunks it actually wrote.
    for _ in range(512):  # pragma: no branch - the early response cancels this before the loop finishes
        yield b"x" * 100_000


def h2_client() -> h2.connection.H2Connection:
    """A client-side h2 connection, for tests that drive the wire by hand."""
    return h2.connection.H2Connection(config=h2.config.H2Configuration(client_side=True, header_encoding=None))


def h2_request_headers(method: str, path: str) -> list[tuple[bytes, bytes]]:
    """The pseudo-header block one cleartext h2 request needs."""
    return [
        (b":method", method.encode()),
        (b":path", path.encode()),
        (b":scheme", b"http"),
        (b":authority", AUTHORITY),
    ]


async def echo_app(scope: RawScope, receive: Receive, send: Send) -> None:
    """A raw ASGI app that echoes the request line and body. Has no lifespan support."""
    if scope["type"] != "http":
        raise RuntimeError("this app serves only http")
    body = b""
    more = True
    while more:
        message = await receive()
        if message["type"] == "http.disconnect":  # pragma: no cover - clients here never disconnect mid-body
            return
        chunk = message.get("body", b"")
        assert isinstance(chunk, bytes)
        body += chunk
        more = bool(message.get("more_body", False))
    method = str(scope["method"])
    path = str(scope["path"])
    payload = f"{method} {path} {body.decode()}".encode()
    await send({"type": "http.response.start", "status": 200, "headers": [(b"content-type", b"text/plain")]})
    await send({"type": "http.response.body", "body": payload})


async def tagged_echo_app(scope: RawScope, receive: Receive, send: Send) -> None:
    """`echo_app` with the request's `x-test` header reflected, so a reply names its request."""
    if scope["type"] != "http":
        raise RuntimeError("this app serves only http")
    head = parse_http_scope(scope)
    body = await read_body(receive)
    marker = next((value for name, value in head.headers if name == b"x-test"), b"")
    payload = f"{head.method} {head.path} test={marker.decode()} body={body.decode()}".encode()
    await send({"type": "http.response.start", "status": 200, "headers": [(b"content-type", b"text/plain")]})
    await send({"type": "http.response.body", "body": payload})


async def sized_echo_app(scope: RawScope, receive: Receive, send: Send) -> None:
    """Like `echo_app` but sends a `content-length`, so the response is keep-alive framed."""
    if scope["type"] != "http":
        raise RuntimeError("this app serves only http")
    head = parse_http_scope(scope)
    body = await read_body(receive)
    payload = f"{head.method} {head.path} body={body.decode()}".encode()
    await send(
        {
            "type": "http.response.start",
            "status": 200,
            "headers": [(b"content-type", b"text/plain"), (b"content-length", str(len(payload)).encode())],
        }
    )
    await send({"type": "http.response.body", "body": payload})


async def receive_after_done_app(scope: RawScope, receive: Receive, send: Send) -> None:
    """Read the body to completion, then call `receive` once more to observe the disconnect."""
    if scope["type"] != "http":
        raise RuntimeError("this app serves only http")
    more = True
    while more:
        message = await receive()
        more = bool(message.get("more_body", False))
    trailing = await receive()
    trailing_type = trailing["type"]
    assert isinstance(trailing_type, str)
    body = trailing_type.encode()
    await send({"type": "http.response.start", "status": 200, "headers": [(b"content-type", b"text/plain")]})
    await send({"type": "http.response.body", "body": body})


def configured_app() -> ASGIApp:
    """An app whose lifespan state reaches its handler, so a pass proves the wiring."""

    @asynccontextmanager
    async def lifespan() -> AsyncIterator[str]:
        yield "configured-state"

    def handle(state: str, head: HttpScope, body: bytes) -> Response:
        return Response(status=200, headers=((b"content-type", b"text/plain"),), body=f"{state}:{head.path}".encode())

    return make_asgi_app(lifespan, http=buffered(handle))


def crash_app() -> ASGIApp:
    """An app whose handler raises, so a test sees what the server does with one that does."""

    @asynccontextmanager
    async def lifespan() -> AsyncIterator[None]:
        yield None

    def handle(state: None, head: HttpScope, body: bytes) -> Response:
        raise RuntimeError("handler exploded")

    return make_asgi_app(lifespan, http=buffered(handle))


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
