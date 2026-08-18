from __future__ import annotations

import asyncio
import logging
import socket
import ssl
import zlib
from base64 import b64encode
from collections.abc import AsyncGenerator
from collections.abc import AsyncIterator
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Mapping
from collections.abc import Sequence
from compression import zstd
from contextlib import asynccontextmanager
from contextlib import suppress
from dataclasses import dataclass
from dataclasses import field
from dataclasses import replace
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from email.utils import parsedate_to_datetime
from importlib import metadata
from typing import NamedTuple
from typing import Protocol
from typing import Self
from urllib.parse import SplitResult
from urllib.parse import urljoin
from urllib.parse import urlsplit

import brotli
import h2.config
import h2.connection
import h2.events
import h2.exceptions
import h11
from aiohappyeyeballs import AddrInfoType
from aiohappyeyeballs import start_connection
from without import Endo
from without import Stream
from without import cancel_futures
from without import stack
from without_asgi import Content
from without_asgi import RawHeaders
from without_asgi import StreamingContent
from without_asgi.headers import merge

from without_http.h2_wire import request_headers
from without_http.h2_wire import response_status_and_headers
from without_http.socket_options import SocketOptions
from without_http.socket_options import apply_socket_options
from without_http.socket_options import tcp_keepalive
from without_http.timeouts import Timeout
from without_http.timeouts import WriteTimeout

_BUFFER = 65536
_REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})
_NO_TIMEOUT = Timeout()  # a shared immutable "no timeouts" value, safe as a default
_DEFAULT_SOCKET_OPTIONS = tcp_keepalive()  # likewise: a shared immutable "keepalive on" default

# Wire codecs, named once. Request/response tokens are ASCII; Set-Cookie / Cookie header text
# is latin-1 (the byte-transparent superset h11 uses). Hoisting the codec names keeps mutmut's
# codec-name mutations on these two lines rather than every call site: an invalid `"XXasciiXX"`
# crashes and is killed, a case-swapped `"ASCII"` is an equivalent alias documented once (see
# docs/contributing/mutation-testing.md).
_ASCII = "ascii"
_LATIN1 = "latin-1"

logger = logging.getLogger(__name__)


async def _empty_body() -> AsyncIterator[bytes]:
    return
    yield  # unreachable: makes this an (empty) async generator


async def _single(chunk: bytes) -> AsyncIterator[bytes]:
    yield chunk


@dataclass(frozen=True, slots=True)
class Origin:
    """
    A request's destination: the key a connection is pooled and reused under.

    Frozen and hashable so it serves directly as a `dict` key in the pool. `secure`
    is derived from the scheme rather than stored, so the two can never disagree.
    """

    scheme: str
    host: str
    port: int

    @property
    def secure(self) -> bool:
        return self.scheme == "https"


@dataclass(frozen=True, slots=True)
class ClientRequest:
    """
    A client request as a value: the head, a streaming body, and its deadline.

    The body is a `Stream[bytes]` (an async iterable of chunks), so a request can be
    buffered (one chunk) or streamed (many), the upload half of the buffered/streaming
    matrix. Because the whole request is the value a `Client` transforms, middleware can
    rewrite it: add headers, change the URL, wrap the body, extend the deadline.

    `timeout` bounds each phase of *this* request (see `Timeout`), and defaults to no
    bounds at all. It rides on the request rather than on the transport because a
    deadline is the caller's policy, not the connection's: it is the caller's time
    budget that decides when slow progress is worse than failure. Carrying it here is
    what lets a `Client` be a plain one-argument function, and what lets middleware
    (`deadline`, or a retry that shortens each attempt) set it like any other field.
    """

    method: str
    url: str
    headers: RawHeaders = ()
    body: Stream[bytes] = field(default_factory=_empty_body)
    timeout: Timeout = _NO_TIMEOUT


@dataclass(frozen=True, slots=True)
class ResponseHead:
    """
    A response's head as the client parses it off the wire: status plus headers.

    Without-http's *inbound* counterpart to without-asgi's outbound `ResponseStart`.
    Same fields, deliberately *not* the same type: an outbound type carries defaults so
    an app constructs it ergonomically, but a parsed-from-the-wire type must have no
    defaults, so a field the parser forgot fails loudly instead of silently defaulting
    (the inbound/outbound rule, mirroring without-asgi's `RequestBody` vs `ResponseBody`).
    """

    status: int
    headers: RawHeaders


@dataclass(frozen=True, slots=True)
class ResponseTrailers:
    """
    A trailing header block, parsed off the wire after the response body.

    The inbound counterpart to without-asgi's outbound `ResponseTrailers`, with no
    defaults for the same reason as `ResponseHead`. A response is modeled as carrying
    zero or more such blocks at the tail of its body stream, so consumers see them as a
    sequence.
    """

    headers: RawHeaders


@dataclass(slots=True)
class ResponseBody:
    """
    A response body: a stream of `bytes` chunks, optionally ended by trailers.

    Consumed exactly once, by one of four methods spanning two axes, stream vs buffer
    and drop-trailers vs keep-trailers:

    - `async for chunk in body` / `await body.read()` yield `bytes`, dropping any
      trailers, so the common path pays nothing for a feature it does not use.
    - `body.events()` / `await body.read_with_trailers()` keep trailers, surfaced as
      `ResponseTrailers` after the byte chunks. Reach for these only when you know (out
      of band) the endpoint uses trailers; `read_with_trailers` returns *all* trailer
      blocks (an empty tuple if none), so a consumer that requires them enforces that.

    Dropping trailers still drains the stream to its end, so the connection releases as
    fully-read (see `_with_release`): it filters the terminal, it does not stop early.
    """

    _events: AsyncGenerator[bytes | ResponseTrailers]

    async def _chunks(self) -> AsyncIterator[bytes]:
        async for item in self._events:
            if isinstance(item, bytes):
                yield item

    def __aiter__(self) -> AsyncIterator[bytes]:
        return self._chunks()

    async def read(self) -> bytes:
        chunks = [chunk async for chunk in self]
        return b"".join(chunks)

    def events(self) -> AsyncIterator[bytes | ResponseTrailers]:
        return self._events

    async def read_with_trailers(self) -> tuple[bytes, tuple[ResponseTrailers, ...]]:
        chunks: list[bytes] = []
        trailers: list[ResponseTrailers] = []
        async for item in self._events:
            if isinstance(item, ResponseTrailers):
                trailers.append(item)
            else:
                chunks.append(item)
        return b"".join(chunks), tuple(trailers)

    async def aclose(self) -> None:
        await self._events.aclose()


class ClientResponse(NamedTuple):
    """
    A client response as a value: the head paired with the body.

    `head` is the parsed `ResponseHead` (status + headers), available the instant
    `await client(request)` returns. `body` is a `ResponseBody`, a once-consumable
    stream that releases its connection when it ends or is closed.

    A `NamedTuple` so a caller can take it whole (`response.head`, `response.body`) or
    unpack it (`head, body = response`) with each field keeping its precise type, which a
    `__iter__` on a dataclass could not give. The two halves are independent, the
    consumer split that mirrors how a server consumes a request (a `scope` value plus a
    body stream): branch on `head` without touching `body`. `request` yields this value
    and closes `body` on exit; it is also what a `ClientMiddleware` rewrites (by
    constructing a new one, since a `NamedTuple` has no `dataclasses.replace`).
    """

    head: ResponseHead
    body: ResponseBody


async def _with_release(
    body: AsyncGenerator[bytes | ResponseTrailers], release: Callable[[bool], Awaitable[None]]
) -> AsyncGenerator[bytes | ResponseTrailers]:
    """
    A response body that releases its connection when the body ends.

    Folds the release decision into the stream's own lifecycle, the client mirror of
    the server's end-of-stream cleanup: draining the body to the end runs `release`
    with `fully_read=True` (the h11 connection is keep-alive eligible), while closing
    it early (`GeneratorExit` from an abandoned read) runs the `finally` with `False`,
    so the same hook closes the socket or resets the stream. The body generator owns
    *when* cleanup happens; the pool's `release` owns *what* it does.

    The leading `yield b""` is a priming sentinel. `aclose()` on an async generator
    that was never entered skips its `finally` entirely, so a response whose body is
    never read (a status-only check, an empty redirect hop) would leak its connection.
    `_releasing` consumes the sentinel at construction, leaving the generator suspended
    *inside* the `try`, so `aclose` always runs the `finally`. It costs no I/O: the
    sentinel is yielded before the body is ever pulled.
    """
    fully_read = False
    try:
        yield b""
        async for chunk in body:
            yield chunk
        fully_read = True
    finally:
        if not fully_read:
            await body.aclose()
        await release(fully_read)


async def _releasing(
    body: AsyncGenerator[bytes | ResponseTrailers], release: Callable[[bool], Awaitable[None]]
) -> AsyncGenerator[bytes | ResponseTrailers]:
    """Build a release-on-end body and prime past its sentinel (see `_with_release`)."""
    armed = _with_release(body, release)
    await anext(armed)
    return armed


# A client *is* a function from a request to a response, the dual of a server handler:
# where a handler maps a request to a response over streams, a client maps a whole
# `ClientRequest` to a `ClientResponse`. Everything that answers a request is one, and
# they are interchangeable by construction: a `ConnectionPool` over the network, a
# canned response table in a test, an ASGI app driven in memory.
#
# A `ClientMiddleware` wraps a client into a client (`Endo`): it can rewrite the request
# before, or the response after, the inner client runs. This is the zero-context case of
# the shared `stack` vocabulary: a server middleware is `(handler, state, scope)`, a
# client one needs no context (the request is the value it transforms, not a fixed
# scope), so it is simply `(client) -> client`, and the same `stack` composes them.
# State a middleware must keep lives in a closure (see `cookies`), as it does
# server-side.
type Client = Callable[[ClientRequest], Awaitable[ClientResponse]]
type ClientMiddleware = Endo[Client]

_PASSTHROUGH: ClientMiddleware = stack()


def _has(headers: RawHeaders, name: bytes) -> bool:
    return any(existing.lower() == name for existing, _ in headers)


def _origin(parts: SplitResult) -> Origin:
    if parts.hostname is None:
        raise ValueError(f"client request URL must be absolute, got {parts.geturl()!r}")
    port = parts.port or (443 if parts.scheme == "https" else 80)
    return Origin(scheme=parts.scheme, host=parts.hostname, port=port)


def _target(parts: SplitResult) -> str:
    target = parts.path or "/"
    if parts.query:
        target = f"{target}?{parts.query}"
    return target


def _build_request(
    method: str,
    url: str,
    headers: RawHeaders,
    content: bytes | Stream[bytes] | Content | StreamingContent,
    timeout: Timeout,
) -> ClientRequest:
    """
    Assemble a `ClientRequest`, picking the body framing from `content`.

    Buffered `bytes` get a `content-length`; a streaming body whose length is unknown
    gets `transfer-encoding: chunked` (HTTP/1.1 frames it as chunks; over HTTP/2 the
    framing headers are dropped and the body rides DATA frames either way).

    A `Content` or `StreamingContent` is a body that already knows what it is, so its
    headers go *under* the caller's: an explicit `content-type` on the request wins
    over the one the encoding supplied, and everything after this point sees a plain
    body.
    """
    if isinstance(content, Content | StreamingContent):
        headers = merge(content.headers, headers)
        content = content.body
    if isinstance(content, bytes):
        if not content:
            return ClientRequest(
                method, url, headers, _empty_body(), timeout
            )  # pragma: no mutate - equals _empty_body()
        if not _has(headers, b"content-length"):
            headers = (*headers, (b"content-length", str(len(content)).encode(_ASCII)))
        return ClientRequest(method, url, headers, _single(content), timeout)
    if not _has(headers, b"content-length") and not _has(headers, b"transfer-encoding"):
        headers = (*headers, (b"transfer-encoding", b"chunked"))
    return ClientRequest(method, url, headers, content, timeout)


_ALPN_H2 = ("h2", "http/1.1")
_ALPN_HTTP11 = ("http/1.1",)
_HAPPY_EYEBALLS_DELAY = timedelta(milliseconds=250)  # RFC 8305 §5's recommended connection attempt delay


class Connect(Protocol):
    """
    How a pool reaches an origin: the one step that touches the network.

    Injected into `ConnectionPool` so the rest of it (reuse, bounds, protocol selection)
    stays independent of how a connection is made. `tcp_connect()` builds the default;
    a test dials an in-memory pipe, and a unix-socket or proxy connector would slot
    in the same way. The negotiated wire protocol comes back alongside the streams
    because only the connector can know it: ALPN is read off the finished handshake.
    """

    async def __call__(
        self,
        host: str,
        port: int,
        *,
        ssl_context: ssl.SSLContext | None,
        timeout: Timeout = ...,
        socket_options: SocketOptions = (),
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter, str]: ...


class Resolve(Protocol):
    """
    How a connector turns a name into candidate addresses: the resolution step alone.

    Injected into `tcp_connect`, defaulting to the operating system's resolver
    (`loop.getaddrinfo`), so resolution policy swaps without touching how the winning
    address is connected: a cache keyed however the caller likes, DNS over HTTPS, or a
    test's canned addresses. It returns `getaddrinfo`-shaped tuples (aiohappyeyeballs'
    `AddrInfoType`), which the connect race consumes directly. A cache lives in a
    wrapper here rather than in the pool, and its staleness bound is the caller's
    policy to choose, since `getaddrinfo` does not surface record TTLs.
    """

    async def __call__(self, host: str, port: int) -> Sequence[AddrInfoType]: ...


async def _getaddrinfo(host: str, port: int) -> Sequence[AddrInfoType]:
    return await asyncio.get_running_loop().getaddrinfo(host, port, type=socket.SOCK_STREAM)


def tcp_connect(
    *,
    resolve: Resolve = _getaddrinfo,
    happy_eyeballs_delay: timedelta | None = _HAPPY_EYEBALLS_DELAY,
) -> Connect:
    """
    A `Connect` over TCP: resolve the name, race the addresses, negotiate ALPN.

    This builds the default connect (`ConnectionPool()` behaves as
    `ConnectionPool(connect=tcp_connect())`), with its two steps injectable:

    - `resolve` turns the name into candidate addresses, defaulting to the OS
      resolver; see `Resolve` for what plugs in here.
    - [`aiohappyeyeballs`](https://github.com/aio-libs/aiohappyeyeballs) connects,
      racing address families per
      [RFC 8305](https://datatracker.ietf.org/doc/html/rfc8305) (Happy Eyeballs) with
      `happy_eyeballs_delay` between attempts (default 250 ms, the RFC's
      recommendation), so a dual-stack host with one black-holed family costs one
      delay rather than a full connect timeout; `None` tries the addresses strictly
      in turn. The race drives plain `loop.sock_connect`, so it behaves the same on
      any event loop, and it takes already-resolved addresses, which is what lets
      `resolve` be a separate step at all (asyncio's own racing is fused to its own
      resolution).

    The returned connector reports the negotiated wire protocol alongside the
    streams. `ssl_context` is `None` for cleartext, which has no negotiation and is
    always `http/1.1` (prior-knowledge h2c is opened directly by the pool instead),
    or a ready context whose ALPN offer the pool has already settled; over TLS the
    protocol is whatever ALPN selected. The `connect` timeout bound covers
    resolution, the connect race, and, over TLS, the handshake. `socket_options` is
    applied to the winning socket exactly as given.
    """
    seconds = None if happy_eyeballs_delay is None else happy_eyeballs_delay.total_seconds()

    async def connect(
        host: str,
        port: int,
        *,
        ssl_context: ssl.SSLContext | None,
        timeout: Timeout = _NO_TIMEOUT,
        socket_options: SocketOptions = (),
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter, str]:
        async with timeout.connecting():
            addr_infos = await resolve(host, port)
            sock = await start_connection(addr_infos, happy_eyeballs_delay=seconds)
            try:
                if ssl_context is None:
                    reader, writer = await asyncio.open_connection(sock=sock)
                else:
                    reader, writer = await asyncio.open_connection(sock=sock, ssl=ssl_context, server_hostname=host)
            except BaseException:
                # The winning socket is ours until a transport owns it; a failed or
                # cancelled handshake must not leak it.
                sock.close()
                raise
        apply_socket_options(writer.get_extra_info("socket"), socket_options)
        if ssl_context is None:
            return reader, writer, "http/1.1"
        ssl_object = writer.get_extra_info("ssl_object")
        negotiated = ssl_object.selected_alpn_protocol() if ssl_object is not None else None
        return reader, writer, "h2" if negotiated == "h2" else "http/1.1"

    return connect


_open = tcp_connect()  # the pool's default `Connect`


@dataclass(slots=True, eq=False)
class _Http11Connection:
    """
    One HTTP/1.1 connection, carrying one exchange at a time and kept for reuse.

    One exchange at a time, unlike the multiplexed `_Http2Connection`, but its two
    directions run concurrently: `send_head` writes the request line and headers, then
    a background task drives `send_body` while `read_head`/`iter_body` read the response
    on the same connection. This is what lets a server answer early (a `413` or redirect
    to a large upload) without the request-body write deadlocking the response read. It
    is safe without a lock because the two directions touch disjoint h11 state: only the
    send path calls `self._conn.send`, only the read path calls `next_event`/
    `receive_data`, and every such call is synchronous, so they never interleave
    mid-call. `finish` reports whether the completed cycle left the connection reusable
    (it does not, when the server signalled `Connection: close` or closed the socket, or
    the request body was not fully sent), so the pool knows whether to keep or drop it.
    """

    _reader: asyncio.StreamReader
    _writer: asyncio.StreamWriter
    _conn: h11.Connection
    send_error: BaseException | None = None

    @classmethod
    def new(cls, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> _Http11Connection:
        return cls(reader, writer, h11.Connection(our_role=h11.CLIENT))

    @property
    def usable(self) -> bool:
        """
        Whether an idle pooled connection still looks open before it is reused.

        Catches the common stale case where the server closed the kept-alive
        connection while it sat idle: asyncio surfaces the peer's `FIN` on the event
        loop, so the transport is closing or the reader is at EOF by checkout time.
        """
        return not self._writer.is_closing() and not self._reader.at_eof()

    async def send_head(self, request: ClientRequest, parts: SplitResult) -> None:
        """Write the request line and headers and flush them, so the server can respond."""
        headers = list(request.headers)
        if not _has(request.headers, b"host"):
            headers.insert(0, (b"host", parts.netloc.encode(_ASCII)))
        self._writer.write(self._conn.send(h11.Request(method=request.method, target=_target(parts), headers=headers)))
        await self._writer.drain()

    async def send_body(self, request: ClientRequest, timeout: Timeout = _NO_TIMEOUT) -> None:
        """
        Stream the request body, then end the message, as a background task.

        Draining per chunk gives ordinary write backpressure, and `timeout.writing()`
        bounds each drain: it bounds write *progress*, not the whole send, so a lazily-fed
        body that pauses between chunks does not trip it. An `OSError` means the peer
        stopped reading (the early-response half-close): the response is still valid, so
        swallow it and let `finish` decide the connection is not reusable. A
        `CancelledError` (the exchange abandoned the still-running send) propagates so the
        release path can tear down. A `WriteTimeout`, or the caller's body generator
        raising, is recorded and the connection closed to unblock a concurrent `read_head`
        on EOF, so `release` can surface it to the caller. It is recorded rather than
        re-raised so the teardown that awaits this task never has to catch it. `WriteTimeout`
        is handled before `OSError` because it is itself an `OSError` subclass.

        The send also stops the moment the peer half-closes (`reader.at_eof()`), the
        duplex-safe "connection is closing" signal: an early response or connection close
        means the server is going away, so there is no point streaming the rest of the
        body, and continuing to write would only trip the socket into a reset that could
        discard the response the read side is concurrently reading. An unfinished send
        just leaves the connection non-reusable (see `finish`); the response read owns
        surfacing the outcome. The arrival of the *response head* is deliberately not the
        signal: over a duplex exchange a response can begin while the request body is
        still legitimately in flight.
        """
        try:
            async for chunk in request.body:
                if self._reader.at_eof():
                    return
                if chunk:
                    self._writer.write(self._conn.send(h11.Data(data=chunk)))
                    async with timeout.writing():
                        await self._writer.drain()
            if self._reader.at_eof():
                return
            self._writer.write(self._conn.send(h11.EndOfMessage()))
            async with timeout.writing():
                await self._writer.drain()
        except asyncio.CancelledError:
            raise
        except WriteTimeout as exc:
            self.send_error = exc
            self.close()
        except OSError:  # pragma: no cover - defensive: the early-response half-close usually cancels this send first
            pass
        except Exception as exc:  # noqa: BLE001 - capture any caller body-generator error to surface it via the read side
            self.send_error = exc
            self.close()

    async def read_head(self, timeout: Timeout = _NO_TIMEOUT) -> tuple[int, RawHeaders]:
        while True:
            event = self._conn.next_event()
            if event is h11.NEED_DATA:
                async with timeout.reading():
                    data = await self._reader.read(_BUFFER)
                self._conn.receive_data(data)
                continue
            if isinstance(event, h11.InformationalResponse):
                continue
            if isinstance(event, h11.Response):
                return event.status_code, tuple((bytes(name), bytes(value)) for name, value in event.headers)
            # Unreachable: while a response is pending, h11 yields NEED_DATA, an
            # InformationalResponse, or a Response, and raises (rather than returning a
            # ConnectionClosed event) if the peer closes first. The guard stays as a
            # defensive backstop against an unexpected h11 event.
            raise ConnectionError("the server closed the connection before sending a response")  # pragma: no cover

    async def iter_body(self, timeout: Timeout = _NO_TIMEOUT) -> AsyncGenerator[bytes | ResponseTrailers]:
        while True:
            event = self._conn.next_event()
            if event is h11.NEED_DATA:
                async with timeout.reading():
                    data = await self._reader.read(_BUFFER)
                self._conn.receive_data(data)
                continue
            if isinstance(event, h11.Data):
                yield bytes(event.data)
            elif isinstance(event, h11.EndOfMessage):
                if event.headers:
                    yield ResponseTrailers(tuple((bytes(name), bytes(value)) for name, value in event.headers))
                return
            else:
                # Unreachable: the body phase yields only Data, EndOfMessage, or
                # NEED_DATA; any other state (a truncated length-framed body) makes h11
                # raise instead. The guard stays as a defensive backstop.
                logger.warning(f"Truncating response body on an unexpected h11 event: {event!r}")  # pragma: no cover
                return  # pragma: no cover

    def finish(self) -> bool:
        try:
            self._conn.start_next_cycle()
            return True
        except h11.LocalProtocolError:
            return False

    def close(self) -> None:
        # Abort rather than gracefully close: every close here discards the connection, and
        # a graceful close cannot flush a send buffer the peer has stopped reading (an
        # early-response or write-timeout half-close), so it would deadlock the concurrent
        # read waiting on the same transport. Aborting drops the buffer and EOFs the reader.
        self._writer.transport.abort()

    async def aclose(self) -> None:
        self._writer.transport.abort()
        with suppress(OSError):
            await self._writer.wait_closed()


@dataclass(slots=True)
class _Stream:
    """
    The per-request mutable state the read loop and a request coroutine share.

    `head` is set when the response start arrives; `chunks` carries the response body
    as `(data, flow_controlled_length)` pairs, ended by `None`, so the consumer
    acknowledges flow control only as it reads, keeping the window (and the buffer)
    bounded. `window` gates the *request* body sender on `WINDOW_UPDATE`. `trailers`
    accumulates any trailing header blocks, delivered after the body's last chunk.
    `send_task` is the background request-body sender, so the release path can cancel it
    when the response ends or is abandoned while the send is still in flight.
    """

    window: asyncio.Event
    head: asyncio.Event
    chunks: asyncio.Queue[tuple[bytes, int] | None]
    status: int = 0
    headers: RawHeaders = ()
    trailers: list[ResponseTrailers] = field(default_factory=list)
    error: BaseException | None = None
    send_task: asyncio.Task[None] | None = None


@dataclass(slots=True, eq=False)
class _Http2Connection:
    """
    One client-side `h2.Connection`, multiplexing many requests over one socket.

    The dual of the server's `_serve_h2_connection`: a read loop feeds wire bytes to
    the shared connection and dispatches its events, while each in-flight request
    coroutine writes its own stream out through that same connection. A single `lock`
    serializes all access to the connection object and the writer; `drain` happens
    outside it. The request-body flow-control invariant holds as on the server: a
    sender clears its wake event and waits *under the lock*, and the read loop sets it
    only after applying a `WINDOW_UPDATE` under that lock, so a growing window can
    never be a lost wakeup. Response-body flow control runs the other way: the read
    loop never acknowledges received data, the body consumer does, so an unread
    response cannot outrun the window.
    """

    _reader: asyncio.StreamReader
    _writer: asyncio.StreamWriter
    _conn: h2.connection.H2Connection
    _lock: asyncio.Lock
    _streams: dict[int, _Stream]
    _closed: asyncio.Event
    _stream_gate: asyncio.Event = field(default_factory=asyncio.Event)
    _task: asyncio.Task[None] | None = None

    @classmethod
    def start(cls, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> _Http2Connection:
        conn = h2.connection.H2Connection(config=h2.config.H2Configuration(client_side=True, header_encoding=None))
        connection = cls(reader, writer, conn, asyncio.Lock(), {}, asyncio.Event())
        conn.initiate_connection()
        # The preface sits in the writer's buffer; the first request's drain flushes
        # it. `start` runs synchronously to completion, so no other coroutine can write
        # before the preface is buffered first.
        writer.write(conn.data_to_send())
        connection._task = asyncio.create_task(connection._run())
        return connection

    @property
    def usable(self) -> bool:
        return not self._closed.is_set()

    async def request(
        self,
        *,
        method: bytes,
        target: bytes,
        scheme: str,
        authority: bytes,
        headers: RawHeaders,
        body: Stream[bytes],
        timeout: Timeout = _NO_TIMEOUT,
    ) -> tuple[int, RawHeaders, int, AsyncGenerator[bytes | ResponseTrailers], asyncio.Task[None] | None]:
        # Send the head immediately (`end_stream=False`) and stream the body as a background
        # task, so a slow-to-produce first chunk never delays the headers and the server can
        # respond, or speak first, while the body is still going out (the h2 mirror of h11's
        # duplex). The body task ends the stream: for a bodyless request it sends a lone empty
        # END_STREAM DATA frame, one extra frame in exchange for never blocking the head on the
        # body. This is what makes both client-speaks-first and server-speaks-first duplex work.
        stream = _Stream(window=asyncio.Event(), head=asyncio.Event(), chunks=asyncio.Queue())
        stream_id = await self._open_stream(
            stream,
            request_headers(method, target, scheme, authority, headers),
            end_stream=False,
            timeout=timeout,
        )
        # Once the stream is registered, any failure before the head is returned must
        # cancel the sender and reset the stream, so the slot and the connection are not
        # stranded (the h2 mirror of h11's pre-body guard).
        try:
            async with timeout.writing():
                await self._writer.drain()
            stream.send_task = asyncio.create_task(self._send_body(stream_id, stream, body, timeout))
            async with timeout.reading():
                await stream.head.wait()
            if stream.error is not None:
                raise stream.error
        except BaseException:
            await cancel_futures([stream.send_task])
            await self.abort(stream_id)
            raise
        return (
            stream.status,
            stream.headers,
            stream_id,
            self._iter_body(stream_id, stream, timeout),
            stream.send_task,
        )

    async def _open_stream(
        self, stream: _Stream, headers: list[tuple[bytes, bytes]], *, end_stream: bool, timeout: Timeout = _NO_TIMEOUT
    ) -> int:
        """
        Register and send a new stream's headers, waiting when at the server's stream limit.

        The gate is the h2 sibling of the h11 per-host pool bound: a burst of requests
        must not over-issue streams past `SETTINGS_MAX_CONCURRENT_STREAMS`. The limit is
        the server's (effectively unbounded until its SETTINGS arrive), read live from h2
        under `_lock` so the capacity check and the `send_headers` that consumes a slot
        are atomic. At capacity the gate is cleared under the lock and awaited outside it;
        the read loop sets it after a `StreamEnded`/`StreamReset`/settings change frees or
        raises a slot. Because capacity is re-checked under the lock on each pass, a lost
        wakeup only costs a re-check, never a stall, the same discipline as `window`. The
        `pool` bound (`timeout.pooling()`) covers the whole wait for a slot (a `PoolTimeout`).
        """
        async with timeout.pooling():
            while True:
                async with self._lock:
                    if self._closed.is_set():
                        raise ConnectionError("the HTTP/2 connection closed before the request was sent")
                    if self._conn.open_outbound_streams < self._conn.remote_settings.max_concurrent_streams:
                        stream_id = self._conn.get_next_available_stream_id()
                        self._streams[stream_id] = stream
                        self._conn.send_headers(stream_id, headers, end_stream=end_stream)
                        self._writer.write(self._conn.data_to_send())
                        return stream_id
                    self._stream_gate.clear()
                await self._stream_gate.wait()

    async def _send_body(
        self, stream_id: int, stream: _Stream, body: Stream[bytes], timeout: Timeout = _NO_TIMEOUT
    ) -> None:
        try:
            async for chunk in body:
                if chunk:
                    await self._send_data(stream_id, stream, chunk, end=False, timeout=timeout)
            await self._send_data(stream_id, stream, b"", end=True, timeout=timeout)
        except asyncio.CancelledError:
            raise
        except (
            ConnectionError
        ):  # pragma: no cover - defensive: the read loop surfaces the close first and cancels this send
            # The connection/stream went away mid-send; the read side already surfaces
            # the error to the consumer, so nothing more to do here.
            pass
        except Exception as exc:  # noqa: BLE001 - a WriteTimeout or the caller's body error, surfaced via the read side
            # Fail the stream so a waiting head or body read unblocks and surfaces `exc`,
            # rather than hanging for a body that will never finish. Recorded (not
            # re-raised) so the release path that awaits this task never has to catch it.
            await self._fail_stream(stream_id, stream, exc)

    async def _fail_stream(self, stream_id: int, stream: _Stream, exc: BaseException) -> None:
        stream.error = exc
        if not stream.head.is_set():
            stream.head.set()
        stream.chunks.put_nowait(None)
        stream.window.set()
        await self.abort(stream_id)

    async def _send_data(
        self, stream_id: int, stream: _Stream, data: bytes, *, end: bool, timeout: Timeout = _NO_TIMEOUT
    ) -> None:
        remaining = memoryview(data)
        while True:
            async with self._lock:
                if (
                    self._closed.is_set()
                ):  # pragma: no cover - defensive: a close mid-send is normally surfaced by the read loop first
                    raise ConnectionError("the HTTP/2 connection closed before the request body was sent")
                try:
                    window = self._conn.local_flow_control_window(stream_id)
                # Defensive: h2 returns a stale window for a reset stream rather than
                # raising here, so `send_data` below is what actually rejects a write to
                # a closed stream. This guard only fires for a fully purged stream.
                except h2.exceptions.StreamClosedError as exc:  # pragma: no cover
                    logger.warning(f"Aborting body send for closed HTTP/2 stream {stream_id}: {exc!r}")
                    return
                sendable = min(len(remaining), window, self._conn.max_outbound_frame_size)
                if sendable > 0 or len(remaining) == 0:
                    chunk, remaining = remaining[:sendable].tobytes(), remaining[sendable:]
                    last = end and len(remaining) == 0
                    try:
                        self._conn.send_data(stream_id, chunk, end_stream=last)
                    except (
                        h2.exceptions.ProtocolError,
                        h2.exceptions.StreamClosedError,
                    ):  # pragma: no cover - defensive: a reset mid-send is normally surfaced by the read loop first
                        return
                    self._writer.write(self._conn.data_to_send())
                    blocked = False
                else:
                    stream.window.clear()
                    blocked = True
            if blocked:
                async with timeout.writing():
                    await stream.window.wait()
                continue
            async with timeout.writing():
                await self._writer.drain()
            if len(remaining) == 0:
                return

    async def _iter_body(
        self, stream_id: int, stream: _Stream, timeout: Timeout = _NO_TIMEOUT
    ) -> AsyncGenerator[bytes | ResponseTrailers]:
        while True:
            async with timeout.reading():
                item = await stream.chunks.get()
            if item is None:
                break
            data, length = item
            yield data
            async with self._lock:
                if not self._closed.is_set():
                    self._conn.acknowledge_received_data(length, stream_id)
                    self._writer.write(self._conn.data_to_send())
            await self._writer.drain()
        if stream.error is not None:
            raise stream.error
        for trailer in stream.trailers:
            yield trailer

    async def abort(self, stream_id: int) -> None:
        """Reset a stream abandoned before its response ended, so the server stops sending."""
        if stream_id not in self._streams:
            return
        self._streams.pop(stream_id, None)
        with suppress(h2.exceptions.ProtocolError, h2.exceptions.StreamClosedError, OSError):
            async with self._lock:
                self._conn.reset_stream(stream_id)
                self._writer.write(self._conn.data_to_send())
            await self._writer.drain()
        # A client-side reset frees a stream slot with no server event to notice it, so
        # wake any request waiting on the stream gate here.
        self._stream_gate.set()

    async def _run(self) -> None:
        try:
            while not self._closed.is_set():
                data = await self._reader.read(_BUFFER)
                if not data:
                    break
                async with self._lock:
                    try:
                        events = self._conn.receive_data(data)
                    except h2.exceptions.ProtocolError:
                        self._writer.write(self._conn.data_to_send())
                        self._closed.set()
                        events = []
                    else:
                        self._writer.write(self._conn.data_to_send())
                await self._writer.drain()
                for event in events:
                    self._handle(event)
        except OSError:
            pass
        finally:
            self._fail_pending()

    def _fail_pending(self) -> None:
        self._closed.set()
        error = ConnectionError("the HTTP/2 connection closed before the response completed")
        for stream in self._streams.values():
            if not stream.head.is_set():
                stream.error = error
                stream.head.set()
            stream.chunks.put_nowait(None)
            stream.window.set()
        self._streams.clear()
        self._stream_gate.set()  # wake any request waiting on a stream slot so it sees the close

    def _handle(self, event: h2.events.Event) -> None:
        match event:
            case h2.events.ResponseReceived(stream_id=stream_id, headers=headers):
                if (stream := self._streams.get(stream_id)) is not None:
                    stream.status, stream.headers = response_status_and_headers(headers)
                    stream.head.set()
            case h2.events.DataReceived(stream_id=stream_id, data=data, flow_controlled_length=length):
                if (stream := self._streams.get(stream_id)) is not None:
                    stream.chunks.put_nowait((bytes(data), length))
            case h2.events.TrailersReceived(stream_id=stream_id, headers=headers):
                if (stream := self._streams.get(stream_id)) is not None:
                    stream.trailers.append(
                        ResponseTrailers(tuple((bytes(name), bytes(value)) for name, value in headers))
                    )
            case h2.events.StreamEnded(stream_id=stream_id):
                if (stream := self._streams.pop(stream_id, None)) is not None:
                    stream.chunks.put_nowait(None)
                self._stream_gate.set()  # a slot freed; wake a request waiting to issue one
            case h2.events.StreamReset(stream_id=stream_id):
                if (stream := self._streams.pop(stream_id, None)) is not None:
                    stream.error = ConnectionError("the server reset the HTTP/2 stream")
                    stream.head.set()
                    stream.chunks.put_nowait(None)
                    stream.window.set()
                self._stream_gate.set()  # a slot freed; wake a request waiting to issue one
            case h2.events.WindowUpdated(stream_id=stream_id):
                if stream_id == 0:
                    for stream in self._streams.values():
                        stream.window.set()
                elif (stream := self._streams.get(stream_id)) is not None:
                    stream.window.set()
            case h2.events.RemoteSettingsChanged():
                for stream in self._streams.values():
                    stream.window.set()
                self._stream_gate.set()  # the stream limit may have risen; re-check waiters
            case h2.events.ConnectionTerminated():
                self._closed.set()
            case _:
                pass

    async def aclose(self) -> None:
        self._closed.set()
        await cancel_futures([self._task])  # `_task` may be None before `start`; skipped
        # The GOAWAY is best-effort: a closed transport rejects the write with `RuntimeError` under
        # uvloop (stdlib drops it silently), so tolerate that alongside the h2/OS errors.
        with suppress(h2.exceptions.ProtocolError, OSError, RuntimeError):
            async with self._lock:
                self._conn.close_connection()
                self._writer.write(self._conn.data_to_send())
        self._writer.close()
        with suppress(OSError):
            await self._writer.wait_closed()


@dataclass(slots=True)
class _HostPool:
    """
    The per-origin HTTP/1.1 pool: the owner of checkout-or-open and checkin-or-close.

    A caller asks for a connection with `checkout` and unconditionally hands it back with
    `checkin`; the pool alone decides where each one comes from and goes to. It never
    opens sockets itself: `open_connection` is injected once at construction (bound to
    this origin's host, port, and TLS), so every connection the pool opens is opened the
    same way and this stays pure pool policy while the client keeps the TLS/connect
    concerns. Three pieces coordinate to bound concurrency *and* retention without
    conflating them, because a permit tracks a **checkout, not a socket**:

    - `checkout` takes a permit first (blocking, at the bound, until one is freed: the
      wait a `PoolTimeout` guards), then reuses an idle connection if one is available
      and calls `open_connection` only if none is. So a permit is held for the whole time
      a connection is in use, whether reused or freshly opened.
    - `checkin` frees the permit and, if the connection is still reusable, returns it to
      `idle` (closing it instead once `idle` is at `max_keepalive`); an idle connection
      therefore holds **no** permit.

    The `_semaphore` gates *concurrent use* (so a burst waits instead of opening unbounded
    sockets), and reuse-before-open under a held permit keeps the count of live sockets
    `<= max_connections`. `max_keepalive` bounds a different axis: how many idle sockets
    are *retained* once a burst subsides, so the pool ramps up under load and settles back
    down when quiet. Idle count never exceeds the high-water mark of concurrent checkouts,
    so `max_keepalive` above `max_connections` is simply never reached. When
    `max_connections` is `None` the pool is unbounded and permits are no-ops, so the
    `pool` timeout axis stays inert until a caller opts into a bound; when `max_keepalive`
    is `None` every reusable connection is retained.
    """

    open_connection: Callable[[Timeout], Awaitable[_Http11Connection]]
    max_keepalive: int | None = None
    idle: list[_Http11Connection] = field(default_factory=list)
    closed: bool = False
    _semaphore: asyncio.Semaphore | None = None

    @classmethod
    def new(
        cls,
        open_connection: Callable[[Timeout], Awaitable[_Http11Connection]],
        max_connections: int | None,
        max_keepalive: int | None,
    ) -> _HostPool:
        semaphore = asyncio.Semaphore(max_connections) if max_connections is not None else None
        return cls(open_connection=open_connection, max_keepalive=max_keepalive, _semaphore=semaphore)

    async def checkout(self, timeout: Timeout, *, preopened: _Http11Connection | None = None) -> _Http11Connection:
        """
        Take a permit (waiting at the bound, under the `pool` timeout) and return a
        connection to use: `preopened` if the caller already has one in hand (an h2 ALPN
        fallback), else a reusable idle one, else a freshly opened one (via the injected
        `open_connection`, under the `connect` timeout). The permit is held until the
        matching `checkin`; if anything past the permit fails, it is freed here.
        """
        try:
            async with timeout.pooling():
                if self._semaphore is not None:
                    await self._semaphore.acquire()
        except BaseException:
            # Freeing a preopened fallback here is all but unreachable: that origin's first
            # permit is essentially always free, so this only guards a cancel or a
            # concurrent-fallback race.
            if preopened is not None:  # pragma: no cover
                preopened.close()
            raise
        try:
            if preopened is not None:
                return preopened
            while self.idle:
                connection = self.idle.pop()
                if connection.usable:
                    return connection
                connection.close()
            return await self.open_connection(timeout)
        except BaseException:
            self._release()
            raise

    async def checkin(self, connection: _Http11Connection, *, reusable: bool) -> None:
        """
        Hand a checked-out connection back and free its permit. The pool retains it as
        idle only when the caller reports it reusable, the pool is open, and `idle` is
        below `max_keepalive`; otherwise it is closed. The caller reports connection
        health only, never pool policy.
        """
        try:
            under_keepalive = self.max_keepalive is None or len(self.idle) < self.max_keepalive
            if reusable and not self.closed and under_keepalive:
                self.idle.append(connection)
            else:
                await connection.aclose()
        finally:
            self._release()

    async def aclose(self) -> None:
        self.closed = True
        idle, self.idle = self.idle, []
        for connection in idle:
            await connection.aclose()

    def _release(self) -> None:
        if self._semaphore is not None:
            self._semaphore.release()


@dataclass(slots=True)
class ConnectionPool:
    """
    Connections keyed by origin: the `Client` that answers a request over the network.

    Calling it *is* the request (`await pool(request)`), so a pool is interchangeable
    with any other `Client` and composes with `ClientMiddleware` through `stack`. Most
    callers go through the `request` context manager rather than calling it directly,
    since that builds the `ClientRequest` and closes the response body for them.

    Open it as an async context manager (`async with ConnectionPool(...) as pool`) so
    its connections are closed on exit; a directly-constructed pool works for
    short-lived use but does not manage the long-lived connections keep-alive retains.

    HTTP/2 connections are kept and reused: many concurrent requests to one origin
    multiplex over a single connection, which is the point of h2. HTTP/1.1
    connections are kept too but used serially: an idle one is checked out for a
    request and returned once its response body is read (keep-alive), so a fresh one
    is opened only when none is idle. An h2 connection is negotiated over TLS by ALPN
    when `allow_http2` is set (the default; it is *allowed*, falling back to HTTP/1.1 if
    the server does not offer it), or over cleartext by *prior knowledge* when
    `force_http2_cleartext` is set (the caller asserting the server speaks h2c, since
    cleartext cannot negotiate); otherwise the origin speaks HTTP/1.1.

    `ssl_context_factory` produces the TLS client context (default
    `ssl.create_default_context`). The pool *calls* it to build the contexts it opens
    with and sets ALPN on them itself, holding one per distinct offer, so it never
    mutates (nor shares the ALPN of) a context the caller holds. Pass a factory, not a
    live context, precisely because ALPN can only be set context-wide: a shared context
    would be mutated out from under other pools or libraries using it.

    `connect` is how the pool reaches an origin, defaulting to a TCP connect that
    races dual-stack addresses per RFC 8305 (see `Connect` and `tcp_connect`, whose
    knobs also inject the resolver). It is the only part of the pool that touches the
    network, so replacing it points the same pooling, protocol selection, and
    keep-alive at somewhere else.

    Decoration (default headers, redirect following, cookies, a deadline) is *not* a
    pool concern: compose it around the pool with `stack`, which yields another `Client`.
    That keeps connection reuse (a transport concern) and application identity
    independent, rather than both hiding in the pool.

    `max_connections_per_host` bounds the number of concurrent HTTP/1.1 connections to
    one origin: at the bound, a checkout *waits* for one to be returned (the wait a
    `pool` timeout guards). It is unbounded by default, mirroring the server's choice
    to let OS backpressure cap connections rather than an in-process limit; opt into a
    bound when a caller wants explicit per-host backpressure.

    `max_keepalive_per_host` bounds a different axis: how many *idle* HTTP/1.1 connections
    are retained per origin once a burst subsides. At the cap, a returned connection is
    closed instead of pooled, so the pool ramps up to `max_connections_per_host` under
    concurrent load but settles back down to `max_keepalive_per_host` when quiet rather
    than holding every socket open. It is unbounded by default (every reusable connection
    is kept); a value above `max_connections_per_host` is never reached, since idle
    connections cannot outnumber concurrent checkouts. Both knobs, when set, MUST be `>=
    1`.

    Deadlines ride on the request (`ClientRequest.timeout`), not on the pool: the same
    pool serves callers with different time budgets, and a bound stored here would be a
    property of the connection rather than of the caller that wanted it.

    `socket_options` is applied to every socket the pool opens, as `(level, option, value)`
    triples. Build it by concatenating the pure producers in `without_http.socket_options`
    (`tcp_keepalive`, `send_buffer_size`, ...), the way headers concatenate: each describes
    one concern, and the pool hands the combined set to `setsockopt` in order. It defaults
    to `tcp_keepalive()`, so a pooled connection is probed for a peer that vanished
    silently; pass `()` for the kernel's defaults, or include `tcp_keepalive(...)` in a
    longer set to keep probing while setting more.
    """

    allow_http2: bool = True
    force_http2_cleartext: bool = False
    ssl_context_factory: Callable[[], ssl.SSLContext] = ssl.create_default_context
    connect: Connect = _open
    max_connections_per_host: int | None = None
    max_keepalive_per_host: int | None = None
    socket_options: SocketOptions = _DEFAULT_SOCKET_OPTIONS
    _h2: dict[Origin, _Http2Connection] = field(default_factory=dict)
    _h11: dict[Origin, _HostPool] = field(default_factory=dict)
    _h11_only: set[Origin] = field(default_factory=set)
    _contexts: dict[tuple[str, ...], ssl.SSLContext] = field(default_factory=dict)
    _origin_locks: dict[Origin, asyncio.Lock] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for name, value in (
            ("max_connections_per_host", self.max_connections_per_host),
            ("max_keepalive_per_host", self.max_keepalive_per_host),
        ):
            if value is not None and value < 1:
                raise ValueError(f"{name} must be >= 1 when set, got {value}")

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    async def __call__(self, request: ClientRequest) -> ClientResponse:
        """
        Answer one request over the network, which is what makes a pool a `Client`.

        Picks the origin's wire protocol, checks out (or opens) a connection, and returns
        the response as soon as its head arrives, with the body still streaming. The
        request's own `timeout` is threaded through as a value, never stored on the
        shared connection objects, so concurrent requests keep their own deadlines.
        """
        timeout = request.timeout
        parts = urlsplit(request.url)
        origin = _origin(parts)
        if origin.secure and self.allow_http2:
            return await self._request_secure(origin, request, parts, timeout)
        if not origin.secure and self.force_http2_cleartext:
            return await self._h2_response(await self._acquire_h2c(origin, timeout), request, parts, timeout)
        return await self._request_h11(origin, request, parts, timeout)

    def _context_for_connection(self, *, http2: bool) -> ssl.SSLContext:
        """
        The TLS context to open a secure connection with, advertising `h2` via ALPN only when `http2`.

        Produces one context per distinct ALPN offer from `ssl_context_factory` and caches it, so
        connections wanting the same offer share a pool-owned context while those wanting different
        offers never mutate a shared one. Construction is synchronous (no `await`), so concurrent
        opens can't race on the cache.
        """
        protocols = _ALPN_H2 if http2 else _ALPN_HTTP11
        context = self._contexts.get(protocols)
        if context is None:
            context = self.ssl_context_factory()
            context.set_alpn_protocols(list(protocols))
            self._contexts[protocols] = context
        return context

    async def _request_secure(
        self, origin: Origin, request: ClientRequest, parts: SplitResult, timeout: Timeout
    ) -> ClientResponse:
        connection = self._reusable_h2(origin)
        fallback: _Http11Connection | None = None
        if connection is None and origin not in self._h11_only:
            async with self._lock_for(origin):
                connection = self._reusable_h2(origin)
                if connection is None and origin not in self._h11_only:
                    reader, writer, protocol = await self.connect(
                        origin.host,
                        origin.port,
                        ssl_context=self._context_for_connection(http2=True),
                        timeout=timeout,
                        socket_options=self.socket_options,
                    )
                    if protocol == "h2":
                        connection = _Http2Connection.start(reader, writer)
                        self._h2[origin] = connection
                    else:
                        # ALPN fell back to HTTP/1.1; remember it so future requests
                        # skip the h2 handshake, and reuse this already-open connection.
                        self._h11_only.add(origin)
                        fallback = _Http11Connection.new(reader, writer)
        if connection is not None:
            return await self._h2_response(connection, request, parts, timeout)
        if fallback is not None:
            # The fallback connection was opened during the h2 handshake; hand it to
            # `checkout` as preopened so it takes a permit for this already-open socket
            # (closing it if the permit cannot be taken) and its checkin balances exactly.
            return await self._request_h11(origin, request, parts, timeout, preopened=fallback)
        return await self._request_h11(origin, request, parts, timeout)

    async def _acquire_h2c(self, origin: Origin, timeout: Timeout) -> _Http2Connection:
        connection = self._reusable_h2(origin)
        if connection is None:
            async with self._lock_for(origin):
                connection = self._reusable_h2(origin)
                if connection is None:
                    reader, writer, _ = await self.connect(
                        origin.host,
                        origin.port,
                        ssl_context=None,
                        timeout=timeout,
                        socket_options=self.socket_options,
                    )
                    connection = _Http2Connection.start(reader, writer)
                    self._h2[origin] = connection
        return connection

    async def _h2_response(
        self, connection: _Http2Connection, request: ClientRequest, parts: SplitResult, timeout: Timeout
    ) -> ClientResponse:
        status, headers, stream_id, body, send_task = await connection.request(
            method=request.method.encode(_ASCII),
            target=_target(parts).encode(_ASCII),
            scheme=parts.scheme,
            authority=parts.netloc.encode(_ASCII),
            headers=request.headers,
            body=request.body,
            timeout=timeout,
        )

        async def release(fully_read: bool) -> None:
            # Cancel the still-running body sender first: it may be parked indefinitely
            # pulling a lazy (queue-fed) body, so it will not end on its own.
            await cancel_futures([send_task])
            if not fully_read:
                await connection.abort(stream_id)

        return ClientResponse(ResponseHead(status, headers), ResponseBody(await _releasing(body, release)))

    async def _request_h11(
        self,
        origin: Origin,
        request: ClientRequest,
        parts: SplitResult,
        timeout: Timeout,
        preopened: _Http11Connection | None = None,
    ) -> ClientResponse:
        pool = self._host_pool(origin)
        connection = await pool.checkout(timeout, preopened=preopened)
        send_task: asyncio.Task[None] | None = None
        try:
            await connection.send_head(request, parts)
            send_task = asyncio.create_task(connection.send_body(request, timeout))
            status, headers = await connection.read_head(timeout)
        except BaseException:
            # The response body (which owns checkin) is not armed yet, so hand the
            # connection back here, unreusable, so the pool closes it and frees the permit
            # exactly once. A caller-body error (recorded on the connection) is preferred
            # over the read failure it caused.
            await cancel_futures([send_task])
            await pool.checkin(connection, reusable=False)
            if connection.send_error is not None:
                raise connection.send_error from None
            raise

        async def release(fully_read: bool) -> None:
            await cancel_futures([send_task])  # cancel-and-await the writer before deciding
            # This conjunction is the barrier against handing a desync'd connection back to the
            # pool: a half-sent request or half-read response cannot reach h11 DONE, so
            # `connection.finish()` fails and the connection is closed rather than reused. Do not
            # loosen it (e.g. trusting `fully_read` alone) — that reopens request smuggling.
            reusable = (
                fully_read
                and send_task is not None
                and not send_task.cancelled()
                and connection.finish()
                and connection.usable
            )
            await pool.checkin(connection, reusable=reusable)
            if connection.send_error is not None:
                raise connection.send_error

        return ClientResponse(
            ResponseHead(status, headers), ResponseBody(await _releasing(connection.iter_body(timeout), release))
        )

    def _host_pool(self, origin: Origin) -> _HostPool:
        pool = self._h11.get(origin)
        if pool is None:
            pool = _HostPool.new(
                lambda timeout: self._open_h11(origin, timeout),
                self.max_connections_per_host,
                self.max_keepalive_per_host,
            )
            self._h11[origin] = pool
        return pool

    async def _open_h11(self, origin: Origin, timeout: Timeout) -> _Http11Connection:
        ssl_context = self._context_for_connection(http2=False) if origin.secure else None
        reader, writer, _ = await self.connect(
            origin.host, origin.port, ssl_context=ssl_context, timeout=timeout, socket_options=self.socket_options
        )
        return _Http11Connection.new(reader, writer)

    def _reusable_h2(self, origin: Origin) -> _Http2Connection | None:
        connection = self._h2.get(origin)
        if connection is not None and not connection.usable:
            del self._h2[origin]
            return None
        return connection

    def _lock_for(self, origin: Origin) -> asyncio.Lock:
        lock = self._origin_locks.get(origin)
        if lock is None:
            lock = asyncio.Lock()
            self._origin_locks[origin] = lock
        return lock

    async def aclose(self) -> None:
        multiplexed = list(self._h2.values())
        host_pools = list(self._h11.values())
        self._h2.clear()
        self._h11.clear()
        for multiplexed_connection in multiplexed:
            await multiplexed_connection.aclose()
        for host_pool in host_pools:
            await host_pool.aclose()


@asynccontextmanager
async def request(
    client: Client,
    method: str,
    url: str,
    *,
    headers: RawHeaders = (),
    body: bytes | Stream[bytes] | Content | StreamingContent = b"",
    timeout: Timeout = _NO_TIMEOUT,
) -> AsyncIterator[ClientResponse]:
    """
    Send a request through `client` and yield its `ClientResponse` for the block.

    The one request surface, over *any* `Client`: a `ConnectionPool`, a pool wrapped in
    middleware (`stack(add_headers(...), cookies(jar))(pool)`), or an in-memory one from
    `without_http.testing`. It owns the two things a caller would otherwise repeat: the
    body framing (`bytes` gets a `content-length`, a `Stream[bytes]` gets
    `transfer-encoding: chunked`) and closing the response body on the way out, so a
    connection is never stranded by a body nobody read.

    `body` takes bytes, a `Stream[bytes]` to stream them, or a `Content` /
    `StreamingContent` when the caller holds an *encoding* rather than bare bytes:
    `body=json_content(order)` or `body=multipart_content(...)` sends the body and the
    `content-type` describing it together, since neither is any use without the other.

    The yielded `ClientResponse` can be taken whole (`response.head`, `response.body`) or
    unpacked (`head, body = ...`); read the response body with `async for chunk in body`
    / `await body.read()`, or `body.read_with_trailers()` when the endpoint carries
    trailers. On exit any unread body is drained or aborted.

    `timeout` bounds this request's phases (see `Timeout`), defaulting to no bounds. It
    lands on the `ClientRequest`, so middleware sees and can rewrite it like any other
    field; `deadline` sets the same field for every request through a client.
    """
    response = await client(_build_request(method, url, headers, body, timeout))
    try:
        yield response
    except BaseException:
        # An error is already in flight; still close the body to release the connection,
        # but suppress any close error (e.g. a surfaced request-body send failure) so it
        # cannot mask the original exception the caller is trying to debug.
        with suppress(Exception):
            await response.body.aclose()
        raise
    else:
        await response.body.aclose()


def wrap(
    *,
    request: Endo[ClientRequest] | None = None,
    response: Endo[ClientResponse] | None = None,
) -> ClientMiddleware:
    """
    Build a `ClientMiddleware` from a request and/or response transform.

    The client counterpart to without-asgi's `wrap`: where the server wraps a handler's
    inbound/outbound *streams*, the client wraps an exchange's *request* (before it is
    sent) and *response* (after it returns). `request` rewrites the outgoing
    `ClientRequest` (headers, URL, body); `response` transforms the returned
    `ClientResponse` (e.g. wrapping its body). Either omitted leaves that side untouched.

    This is the easy path for the *independent* before/after case (the dual of why
    `add_headers`, below, is a one-liner over it). A middleware whose two sides share
    state, like `cookies` needing the request URL when it stores the response, or that
    loops, like `follow_redirects`, is written directly as a `Client` wrapper.
    """

    def middleware(inner: Client) -> Client:
        async def exchange(outgoing: ClientRequest) -> ClientResponse:
            if request is not None:
                outgoing = request(outgoing)
            incoming = await inner(outgoing)
            if response is not None:
                incoming = response(incoming)
            return incoming

        return exchange

    return middleware


def add_headers(*headers: tuple[bytes, bytes]) -> ClientMiddleware:
    """
    Client middleware that adds headers to every request.

    The mirror of a server's request-decorating middleware: it sits in the same
    `stack` and rewrites the request before the inner client runs. This is how a
    caller sends the same headers on every request, or a single request adds its own.

    Every request gets a copy, whatever it already carries, which is what a field that
    may appear more than once (`accept`, `via`, a custom trace header) wants.
    `default_headers` is the counterpart for a field that may not.
    """
    extra: RawHeaders = tuple(headers)
    return wrap(request=lambda request: replace(request, headers=extra + request.headers))


def default_headers(*headers: tuple[bytes, bytes]) -> ClientMiddleware:
    """
    Client middleware that adds each of `headers` to a request not already carrying it.

    The shape a *default* takes for a field RFC 9110 allows only once
    (`authorization`, `user-agent`, an API key): `add_headers` would prepend a second
    copy, leaving the peer to resolve a duplicate the spec says cannot happen, so the
    per-request value the caller wrote to override the default is the one that silently
    loses. Each header is decided on its own, so a request stating one default and not
    another gets exactly the one it omitted.

    This is a default, not a policy: the request's own value wins. A caller that must
    not be overridden composes its own client rather than handing out one that can be,
    the same position `deadline` takes on a time budget.
    """
    defaults: RawHeaders = tuple(headers)

    def apply(request: ClientRequest) -> ClientRequest:
        missing = tuple((name, value) for name, value in defaults if not _has(request.headers, name))
        if not missing:
            return request
        return replace(request, headers=missing + request.headers)

    return wrap(request=apply)


def basic_auth(username: str, password: str) -> ClientMiddleware:
    """
    Client middleware that sends `Basic` authorization (RFC 7617) on every request.

    The credentials are fixed at composition, base64 of `username:password` encoded as
    UTF-8 (the charset RFC 7617 names and servers expect today). A colon in `username`
    is refused with `ValueError` rather than encoded: the receiver splits the decoded
    pair at the *first* colon, so the colon would silently move characters from the
    username into the password.

    This is a default rather than a policy: a request carrying its own `authorization`
    keeps it, so one call can authenticate as someone else without composing a second
    client.
    """
    if ":" in username:
        raise ValueError(f"a Basic auth username cannot contain a colon, got {username!r}")
    credentials = b64encode(f"{username}:{password}".encode())
    return default_headers((b"authorization", b"Basic " + credentials))


def bearer_auth(token: str, *, scheme: str = "Bearer") -> ClientMiddleware:
    """
    Client middleware that sends bearer-style authorization on every request.

    Sends `authorization: Bearer <token>` (RFC 6750) by default. The `scheme` prefix is
    the part real APIs disagree on, so it is injectable: pass the spelling the peer
    demands (`scheme="Token"`, `scheme="token"`), or `scheme=""` to send the bare token
    with no prefix at all. The header encodes as ASCII, which is the charset both the
    scheme and a bearer token are allowed, so a stray non-ASCII byte fails loudly here
    rather than on the wire.

    Like `basic_auth`, this is a default: a request carrying its own `authorization`
    keeps it.
    """
    value = f"{scheme} {token}" if scheme else token
    return default_headers((b"authorization", value.encode(_ASCII)))


# This library's own identity, `without-http/<version>`, read from the installed
# distribution so it cannot drift. `user_agent()` sends it when given no segments;
# pass it as a segment to join it with your own product token
# (`user_agent("myapp/1.0", USER_AGENT)`).
USER_AGENT = f"without-http/{metadata.version('without-http')}"


def user_agent(*segments: str) -> ClientMiddleware:
    """
    Client middleware that sends a `user-agent` header on every request.

    No user-agent is sent unless a caller composes this middleware (or writes the
    header itself): requests say exactly what the caller said. Some peers refuse an
    absent user-agent outright (the GitHub API 403s such requests), so this is the
    first header most callers reach for.

    `segments` are joined with single spaces, the separator RFC 9110 defines between
    product tokens, so `user_agent("myapp/1.0", USER_AGENT)` sends
    `myapp/1.0 without-http/<version>`. With no segments, the header is `USER_AGENT`
    alone: the same library-identity default httpx, requests, aiohttp, and niquests
    send, opted into rather than unbidden. The value encodes as ASCII, failing
    loudly here rather than on the wire. A request carrying its own `user-agent` keeps
    it: `user-agent` is a singleton field, so this is a default, not a policy.
    """
    value = " ".join(segments) if segments else USER_AGENT
    return default_headers((b"user-agent", value.encode(_ASCII)))


def deadline(timeout: Timeout) -> ClientMiddleware:
    """
    Client middleware that applies `timeout` to every request that bounds nothing itself.

    A default time budget for everything sent through the composed client, in the same
    `stack` as any other decoration. A request that bounds any phase of its own keeps its
    own `timeout` whole, so a caller with a tighter budget for one call is not overridden
    by the default; that is the difference between a default and a policy imposed from
    above. A request bounding *nothing* (the default `Timeout()`) reads as "no budget
    stated" rather than "no budget wanted", so it takes the default: a caller who wants
    one request exempt composes it against a client without this middleware.
    """

    def apply(request: ClientRequest) -> ClientRequest:
        if request.timeout != _NO_TIMEOUT:
            return request
        return replace(request, timeout=timeout)

    return wrap(request=apply)


_SENSITIVE_REDIRECT_HEADERS = frozenset({b"authorization", b"cookie", b"proxy-authorization"})
_BODY_FRAMING_HEADERS = frozenset({b"content-encoding", b"content-length", b"content-type", b"transfer-encoding"})


def _strip_headers(headers: RawHeaders, drop: frozenset[bytes]) -> RawHeaders:
    return tuple((name, value) for name, value in headers if name.lower() not in drop)


def follow_redirects(max_hops: int = 5) -> ClientMiddleware:
    """
    Client middleware that follows `3xx` redirects, up to `max_hops`.

    Each intermediate response is drained before the next hop, so its connection is
    released. The follow re-issues the same request body, so redirects with a
    one-shot streaming body are not replayable; in practice redirects follow bodyless
    requests.

    Credentials are not replayed across an origin boundary: when a hop's target has a
    different scheme, host, or port, `Authorization`, `Cookie`, and
    `Proxy-Authorization` are dropped before the request is re-issued. A hop that
    would downgrade `https` to `http` is refused outright (the `3xx` is returned
    unfollowed) so nothing is replayed over cleartext.
    """

    def middleware(inner: Client) -> Client:
        async def exchange(request: ClientRequest) -> ClientResponse:
            response = await inner(request)
            for _ in range(max_hops):
                if response.head.status not in _REDIRECT_STATUSES:
                    return response
                location = next((value for name, value in response.head.headers if name.lower() == b"location"), None)
                if location is None:
                    return response
                source = urlsplit(request.url)
                target = urlsplit(urljoin(request.url, location.decode(_ASCII)))
                if source.scheme == "https" and target.scheme == "http":
                    return response
                headers = request.headers
                if _origin(target) != _origin(source):
                    headers = _strip_headers(headers, _SENSITIVE_REDIRECT_HEADERS)
                method, body = request.method, request.body
                if response.head.status == 303 and request.method not in ("GET", "HEAD"):
                    # RFC 7231 §6.4.4: a 303 tells the client to fetch the target with GET,
                    # so the original method and body (and their framing headers) are dropped.
                    method, body = "GET", _empty_body()
                    headers = _strip_headers(headers, _BODY_FRAMING_HEADERS)
                await response.body.aclose()
                request = replace(request, url=target.geturl(), headers=headers, method=method, body=body)
                response = await inner(request)
            return response

        return exchange

    return middleware


_REQUEST_FRAMING_HEADERS = frozenset({b"content-length", b"transfer-encoding"})
_GZIP_CONTAINER = zlib.MAX_WBITS | 16  # the wbits offset that selects the gzip wrapper around DEFLATE


class Compressor(Protocol):
    """
    The incremental compressor shape `compressing` drives: feed chunks through
    `compress`, then `flush` ends the stream. `zlib.compressobj` and
    `zstd.ZstdCompressor` satisfy it as-is; a third-party codec plugs in with
    whatever thin adapter its own surface needs.
    """

    def compress(self, data: bytes, /) -> bytes: ...
    def flush(self) -> bytes: ...


async def _compressed(body: Stream[bytes], compressor: Compressor) -> AsyncIterator[bytes]:
    async for chunk in body:
        if compressed := compressor.compress(chunk):
            yield compressed
    yield compressor.flush()


def compressing(coding: bytes, make_compressor: Callable[[], Compressor]) -> ClientMiddleware:
    """
    A `ClientMiddleware` that encodes every request body with `coding`.

    The mechanism behind `gzip_compress` and `zstd_compress`, public so any coding
    those two do not ship arrives the same way: name the `content-encoding` token and
    supply a factory for a fresh `Compressor` per request
    (`compressing(b"br", make_brotli)`), and the rest is inherited rather than
    reimplemented.

    That rest: the lazy body `Stream[bytes]` is wrapped in the incremental compressor,
    so a streamed upload compresses chunk by chunk and nothing buffers; and the
    framing follows the rewrite, `content-length` no longer describes the body, so it
    is dropped and the compressed stream goes out `transfer-encoding: chunked` (over
    HTTP/2 the framing header is dropped with the other hop-by-hop headers and the
    body rides DATA frames as usual).

    Two kinds of request pass through untouched: one already carrying a
    `content-encoding` (the body is already encoded; re-compressing would corrupt it),
    and one with no body at all (neither `content-length` nor `transfer-encoding`,
    which is how `request` frames bodyless requests), so a plain `GET` does not grow
    an empty-payload compressed shell.
    """

    def apply(request: ClientRequest) -> ClientRequest:
        if _has(request.headers, b"content-encoding"):
            return request
        if not _has(request.headers, b"content-length") and not _has(request.headers, b"transfer-encoding"):
            return request
        headers = (
            *_strip_headers(request.headers, _REQUEST_FRAMING_HEADERS),
            (b"content-encoding", coding),
            (b"transfer-encoding", b"chunked"),
        )
        return replace(request, headers=headers, body=_compressed(request.body, make_compressor()))

    return wrap(request=apply)


def gzip_compress(level: int = zlib.Z_DEFAULT_COMPRESSION) -> ClientMiddleware:
    """
    Client middleware that gzips every request body sent through it.

    Requests have no `accept-encoding` negotiation, so this stays opt-in: compose it
    onto the clients whose upstreams are known to accept gzip requests, while the same
    pool backs uncompressed clients beside them. The scope is wherever the composition
    happens: decorate once at assembly for a whole client, or inline at one call site
    (`request(gzip_compress()(client), ...)`) for a single request, since decorating a
    client is a stateless function wrap.

    The body streams through an incremental compressor and the framing follows the
    rewrite (see `compressing` for both, and for which requests pass through
    untouched). `level` is zlib's compression level, defaulting to zlib's own
    default. `zstd_compress` and `brotli_compress` are the same middleware over their
    codings, and `compressing` is the shared mechanism for any coding beyond those;
    the response-side counterpart to all of them is `decompress`.
    """
    return compressing(b"gzip", lambda: zlib.compressobj(level, zlib.DEFLATED, _GZIP_CONTAINER))


def zstd_compress(level: int | None = None) -> ClientMiddleware:
    """
    Client middleware that zstd-compresses every request body sent through it.

    `gzip_compress`'s sibling: everything there (why it is opt-in, how the body
    streams, which requests pass through untouched) holds here, and only the coding
    differs. gzip is the coding everything decodes; reach for zstd where the upstream
    is known to decode it.

    `level` is zstd's compression level, defaulting to the library's own default.
    """
    return compressing(b"zstd", lambda: zstd.ZstdCompressor(level))


class _RawBrotliCompressor(Protocol):
    """The slice of `brotli.Compressor` the adapter drives (the bindings ship no types)."""

    def process(self, data: bytes, /) -> bytes: ...
    def finish(self) -> bytes: ...


@dataclass(slots=True, eq=False)
class _BrotliCompressor:
    """
    Adapt `brotli.Compressor` to the `Compressor` shape.

    Brotli's bindings spell the incremental surface `process`/`finish` (their `flush`
    is a mid-stream flush that keeps the stream open), so the adapter maps `compress`
    to `process` and `flush` to `finish`.
    """

    _raw: _RawBrotliCompressor

    def compress(self, data: bytes, /) -> bytes:
        return self._raw.process(data)

    def flush(self) -> bytes:
        return self._raw.finish()


def brotli_compress(quality: int = 11) -> ClientMiddleware:
    """
    Client middleware that brotli-compresses every request body sent through it.

    `gzip_compress`'s sibling over brotli ([Google's own bindings](https://github.com/google/brotli),
    a bundled dependency since the stdlib has no brotli): everything there holds here,
    and only the coding differs. `quality` is brotli's compression quality (0-11),
    defaulting to the library's own default (11, the maximum).
    """
    return compressing(b"br", lambda: _BrotliCompressor(brotli.Compressor(quality=quality)))


class Decompressor(Protocol):
    """
    The incremental decoder shape `decompress` drives: feed encoded chunks through
    `decompress`, with `eof` reporting whether a complete compressed stream has been
    seen (the truncation check relies on it) and `unused_data` holding the bytes that
    followed it (the next stream, for a coding that concatenates). `zlib.decompressobj`
    and `zstd.ZstdDecompressor` satisfy it as-is; a third-party codec plugs in with a
    thin adapter (e.g. mapping brotli's `is_finished()` to `eof`).
    """

    @property
    def eof(self) -> bool: ...
    @property
    def unused_data(self) -> bytes: ...
    def decompress(self, data: bytes, /) -> bytes: ...


def _gzip_decompressor() -> Decompressor:
    return zlib.decompressobj(wbits=_GZIP_CONTAINER)


class _RawBrotliDecompressor(Protocol):
    """The slice of `brotli.Decompressor` the adapter drives (the bindings ship no types)."""

    def process(self, data: bytes, /) -> bytes: ...
    def is_finished(self) -> bool: ...


@dataclass(slots=True, eq=False)
class _BrotliDecompressor:
    """Adapt `brotli.Decompressor` to the `Decompressor` shape (`process`/`is_finished` to `decompress`/`eof`)."""

    _raw: _RawBrotliDecompressor

    @property
    def eof(self) -> bool:
        return self._raw.is_finished()

    @property
    def unused_data(self) -> bytes:
        # `br` has no concatenation convention and the bindings hold nothing back: bytes
        # after a finished stream make `process` raise rather than accumulate.
        return b""

    def decompress(self, data: bytes, /) -> bytes:
        return self._raw.process(data)


def _brotli_decompressor() -> Decompressor:
    return _BrotliDecompressor(brotli.Decompressor())


# The codings decoded out of the box: gzip and zstd from the stdlib, brotli from the
# bundled bindings. `decompress`'s default table.
DEFAULT_DECOMPRESSORS: Mapping[bytes, Callable[[], Decompressor]] = {
    b"br": _brotli_decompressor,
    b"gzip": _gzip_decompressor,
    b"zstd": zstd.ZstdDecompressor,
}

_CONTENT_CODING_HEADERS = frozenset({b"content-encoding", b"content-length"})


async def _decompressed(
    events: AsyncIterator[bytes | ResponseTrailers], make_decompressor: Callable[[], Decompressor]
) -> AsyncGenerator[bytes | ResponseTrailers]:
    decompressor = make_decompressor()
    saw_compressed_bytes = False
    async for item in events:
        if not isinstance(item, bytes):
            yield item
            continue
        if item:
            saw_compressed_bytes = True
        pending = item
        while pending:
            if decoded := decompressor.decompress(pending):
                yield decoded
            # RFC 1952 §2.2: a gzip body is a *series* of members, and zstd frames
            # concatenate the same way, so a decoder that reaches the end of one holds
            # back whatever followed it. Those bytes begin the next stream, which a
            # fresh decoder takes; leaving them would drop the rest of the body. A
            # decoder still mid-stream holds nothing back, so the loop runs once.
            pending = decompressor.unused_data if decompressor.eof else b""
            if pending:
                decompressor = make_decompressor()
    # An empty body under a `content-encoding` head (a HEAD response, a 304) is fine;
    # a *partial* compressed stream inside a cleanly-ended body is not. Decompression
    # discards the transfer's own length signal, so this check is what stands in for it.
    if saw_compressed_bytes and not decompressor.eof:
        raise ConnectionError("the response body ended before its compressed stream did")


def decompress(
    decompressors: Mapping[bytes, Callable[[], Decompressor]] = DEFAULT_DECOMPRESSORS,
) -> ClientMiddleware:
    """
    Client middleware that negotiates and decodes compressed response bodies.

    Outbound it offers `accept-encoding` (a request already carrying its own keeps
    it); inbound it wraps the response body stream in an incremental decoder, so a
    streamed body decodes chunk by chunk and trailers pass through untouched.
    Middleware rather than pool behavior, so the transport never silently rewrites
    bytes: a caller that wants the wire encoding reads the undecorated client.

    `decompressors` maps each coding to a factory for a fresh `Decompressor`,
    defaulting to `DEFAULT_DECOMPRESSORS` (gzip and zstd via the stdlib, brotli via
    the bundled bindings). The offer is *derived from its keys*, so what is
    advertised and what is decoded cannot disagree; register a coding this package
    does not ship by extending the table
    (`decompress({**DEFAULT_DECOMPRESSORS, b"lzma": make_lzma})`) with any factory
    whose product satisfies `Decompressor`. The mapping is snapshotted, keys
    lowercased, when the middleware is built.

    The decoded response is self-consistent: `content-encoding` and `content-length`
    described the *encoded* body, so both are dropped from the head rather than left
    to contradict the bytes the stream now yields. A response whose `content-encoding`
    is not in the table (an unknown coding, or a stack of them) passes through whole,
    head and body untouched.

    A truncated compressed stream raises `ConnectionError` rather than passing off a
    prefix as the whole body, and corrupt bytes raise the codec's own error. A body
    that concatenates streams (multi-member gzip, back-to-back zstd frames, both of
    which the formats define and origins do serve) decodes whole: each decoder that
    ends hands its leftover bytes to a fresh one.
    """
    table = {name.lower(): make_decompressor for name, make_decompressor in decompressors.items()}
    offer = b", ".join(sorted(table))

    def middleware(inner: Client) -> Client:
        async def exchange(request: ClientRequest) -> ClientResponse:
            if not _has(request.headers, b"accept-encoding"):
                request = replace(request, headers=(*request.headers, (b"accept-encoding", offer)))
            response = await inner(request)
            encoding = next(
                (value for name, value in response.head.headers if name.lower() == b"content-encoding"), b""
            )
            make_decompressor = table.get(encoding.strip().lower())
            if make_decompressor is None:
                return response

            async def release(fully_read: bool) -> None:
                if not fully_read:
                    await response.body.aclose()

            return ClientResponse(
                ResponseHead(response.head.status, _strip_headers(response.head.headers, _CONTENT_CODING_HEADERS)),
                ResponseBody(await _releasing(_decompressed(response.body.events(), make_decompressor), release)),
            )

        return exchange

    return middleware


@dataclass(frozen=True, slots=True)
class _Cookie:
    """
    One stored cookie: its value plus the attributes that decide where it is sent.

    Identified by `(domain, path, name)`, the RFC 6265 cookie identity. `host_only`
    records whether the `Set-Cookie` carried a `Domain` attribute: without one a cookie
    is sent only to the exact host that set it, with one it is sent to that domain and
    its subdomains.
    """

    name: str
    value: str
    domain: str
    path: str
    secure: bool
    host_only: bool
    expires: datetime | None


def _default_path(request_path: str) -> str:
    """
    The RFC 6265 default-path for a cookie set without a `Path`: the request path up
    to (not including) its last `/`, or `/`.
    """
    if not request_path.startswith("/") or request_path == "/":  # pragma: no mutate - redundant "/" fast-path
        return "/"
    return request_path[: request_path.rindex("/")] or "/"


def _domain_matches(host: str, cookie: _Cookie) -> bool:
    if cookie.host_only:
        return host == cookie.domain
    return host == cookie.domain or host.endswith(f".{cookie.domain}")


def _path_matches(request_path: str, cookie: _Cookie) -> bool:
    if request_path == cookie.path:
        return True
    if not request_path.startswith(cookie.path):
        return False
    return cookie.path.endswith("/") or request_path[len(cookie.path)] == "/"


def _domain_acceptable(host: str, domain: str) -> bool:
    """
    Whether a response from `host` may set a cookie scoped to `Domain=domain`.

    Enforces RFC 6265 §5.3: the host must equal `domain` or be a subdomain of it, so
    `evil.example` cannot set a cookie for `victim.com`. A `domain` with no internal
    dot (a bare TLD like `com`, or `localhost`) is a stand-in for the public-suffix
    check and is rejected outright, blocking a domain-wide supercookie. Full
    public-suffix-list coverage (rejecting e.g. `co.uk`) is not yet implemented.
    """
    if "." not in domain:
        return False
    return host == domain or host.endswith(f".{domain}")


def _deletes(max_age: str | None) -> bool:
    if max_age is None:
        return False
    try:
        return int(max_age) <= 0
    except ValueError:
        return False


def _as_utc(when: datetime) -> datetime:
    """Normalize a datetime to an aware UTC value, assuming UTC for a naive input."""
    return when.replace(tzinfo=UTC) if when.tzinfo is None else when.astimezone(UTC)


def _parse_expires(value: str | None) -> datetime | None:
    """Parse a `Set-Cookie` `Expires` value into an aware UTC datetime, or `None`."""
    if value is None:
        return None
    try:
        parsed = parsedate_to_datetime(value)
    except ValueError, TypeError:
        return None
    return _as_utc(parsed)


def _parse_set_cookie(header: str, host: str, request_path: str) -> tuple[_Cookie, bool] | None:
    """
    Parse one `Set-Cookie` value into a `_Cookie` and whether it deletes one.

    `None` for a header with no `name=value`, or one whose `Domain` the response host is
    not allowed to set (see `_domain_acceptable`). A `Max-Age` of zero or less marks the
    cookie for deletion (the second tuple element); an `Expires` in the past is handled
    the same way by the jar. `Max-Age` takes precedence over `Expires`, so `Expires` is
    parsed onto the cookie only when no `Max-Age` is present. Whether a `Secure` cookie
    is admitted at all depends on the response transport, which the jar checks at store.
    """
    pair, _, rest = header.partition(";")
    name, sep, value = pair.partition("=")
    name, value = name.strip(), value.strip()
    if not sep or not name:
        return None
    attributes: dict[str, str] = {}
    for attribute in rest.split(";"):
        key, _, val = attribute.partition("=")
        key = key.strip().lower()
        if key:
            attributes[key] = val.strip()
    domain = attributes.get("domain", "").lstrip(".").lower()
    if domain and not _domain_acceptable(host, domain):
        return None
    max_age = attributes.get("max-age")
    absent_path = ""  # pragma: no mutate - absent Path falls to _default_path regardless of value
    path = attributes.get("path", absent_path)
    cookie = _Cookie(
        name=name,
        value=value,
        domain=domain or host,
        path=path if path.startswith("/") else _default_path(request_path),
        secure="secure" in attributes,
        host_only=not domain,
        expires=_parse_expires(attributes.get("expires")) if max_age is None else None,
    )
    return cookie, _deletes(max_age)


def _utcnow() -> datetime:
    return datetime.now(UTC)


@dataclass(slots=True)
class CookieJar:
    """
    A mutable cookie store you construct and hand to the `cookies` middleware.

    Deliberately *not* owned by a `ConnectionPool`: cookie scope (application identity)
    and connection reuse (transport) are independent, so binding them the way a single
    client object would is a needless coupling. Construct a jar, pass it to `cookies`,
    and *which requests share it* decides what shares cookies, one jar per logical user
    session regardless of how connections are pooled.

    Supports host-only and `Domain` (subdomain) matching, `Path` matching, the `Secure`
    attribute, and expiry via `Max-Age<=0` or a past `Expires`. A `Set-Cookie` whose
    `Domain` the response host is not allowed to set, or a `Secure` cookie offered over a
    cleartext response, is rejected at store time. `_now` supplies the clock for expiry
    (injectable for tests). Not yet: full public-suffix-list rejection of `Domain` values
    like `co.uk`, and forward expiry of a positive `Max-Age`.

    Two ways in: `store`, which parses `Set-Cookie` off an (untrusted) response and so
    applies the origin guards above; and `add`, which places a hand-written cookie the
    caller vouches for, skipping those guards.
    """

    _cookies: dict[tuple[str, str, str], _Cookie] = field(default_factory=dict)
    _now: Callable[[], datetime] = field(default=_utcnow)

    def add(
        self,
        name: str,
        value: str,
        *,
        domain: str,
        path: str = "/",
        secure: bool = False,
        subdomains: bool = False,
        expires: datetime | None = None,
    ) -> None:
        """
        Add a hand-written cookie directly, without a `Set-Cookie` response.

        The trusted counterpart to `store`: because the caller vouches for the cookie
        (a session token already in hand, a test fixture), the origin checks `store`
        applies to an untrusted response, the `Domain` scope check and the
        `Secure`-over-cleartext rejection, do not apply here. `subdomains=True` sends it
        to `domain` and its subdomains; the default sends it only to the exact `domain`
        host. An entry with the same `(domain, path, name)` identity is replaced.
        """
        cookie = _Cookie(
            name=name,
            value=value,
            domain=domain.lower(),
            path=path,
            secure=secure,
            host_only=not subdomains,
            expires=_as_utc(expires) if expires is not None else None,
        )
        self._cookies[(cookie.domain, cookie.path, cookie.name)] = cookie

    def store(self, url: str, headers: RawHeaders) -> None:
        """Fold every `Set-Cookie` in a response into the jar."""
        parts = urlsplit(url)
        host = (parts.hostname or "").lower()
        secure_request = parts.scheme == "https"
        now = self._now()
        self._prune_expired(now)
        for name, value in headers:
            if name.lower() != b"set-cookie":
                continue
            parsed = _parse_set_cookie(value.decode(_LATIN1), host, parts.path)
            if parsed is None:
                continue
            cookie, deletes = parsed
            if cookie.secure and not secure_request:
                continue  # a Secure cookie may only be set over a secure transport
            key = (cookie.domain, cookie.path, cookie.name)
            if deletes or (cookie.expires is not None and cookie.expires <= now):
                self._cookies.pop(key, None)
            else:
                self._cookies[key] = cookie

    def _prune_expired(self, now: datetime) -> None:
        """Drop cookies whose `Expires` has passed so a long-lived jar stays bounded."""
        expired = [key for key, cookie in self._cookies.items() if cookie.expires is not None and cookie.expires <= now]
        for key in expired:
            del self._cookies[key]

    def header_for(self, url: str) -> bytes | None:
        """The `Cookie` header value for a request to `url`, or `None` if none match."""
        parts = urlsplit(url)
        host = (parts.hostname or "").lower()
        path = parts.path or "/"
        secure = parts.scheme == "https"
        now = self._now()
        matched = [
            cookie
            for cookie in self._cookies.values()
            if _domain_matches(host, cookie)
            and _path_matches(path, cookie)
            and (secure or not cookie.secure)
            and (cookie.expires is None or cookie.expires > now)
        ]
        if not matched:
            return None
        matched.sort(key=lambda cookie: len(cookie.path), reverse=True)
        return "; ".join(f"{cookie.name}={cookie.value}" for cookie in matched).encode(_LATIN1)


def _with_cookie(headers: RawHeaders, value: bytes) -> RawHeaders:
    existing = next((current for name, current in headers if name.lower() == b"cookie"), None)
    merged = value if existing is None else existing + b"; " + value
    others = tuple((name, current) for name, current in headers if name.lower() != b"cookie")
    return (*others, (b"cookie", merged))


def cookies(jar: CookieJar) -> ClientMiddleware:
    """
    Client middleware that carries cookies through a `CookieJar` you own.

    Reads `Set-Cookie` off each response into `jar` and writes the matching `Cookie`
    header onto each outgoing request. This is the stateful counterpart to `add_headers`:
    its mutable jar is passed in explicitly rather than hidden in the pool, so two
    requests share cookies exactly when they share a jar.

    Place it *inside* `follow_redirects` in a `stack` (`stack(follow_redirects(),
    cookies(jar))`) so each redirect hop both sends the jar's cookies and collects any
    the hop sets.
    """

    def middleware(inner: Client) -> Client:
        async def exchange(request: ClientRequest) -> ClientResponse:
            header = jar.header_for(request.url)
            if header is not None:
                request = replace(request, headers=_with_cookie(request.headers, header))
            response = await inner(request)
            jar.store(request.url, response.head.headers)
            return response

        return exchange

    return middleware
