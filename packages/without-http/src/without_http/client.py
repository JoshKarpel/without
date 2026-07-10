from __future__ import annotations

import asyncio
import logging
import ssl
from collections.abc import AsyncGenerator
from collections.abc import AsyncIterator
from collections.abc import Awaitable
from collections.abc import Callable
from contextlib import asynccontextmanager
from contextlib import suppress
from dataclasses import dataclass
from dataclasses import field
from dataclasses import replace
from typing import NamedTuple
from typing import Self
from urllib.parse import SplitResult
from urllib.parse import urljoin
from urllib.parse import urlsplit

import h2.config
import h2.connection
import h2.events
import h2.exceptions
import h11
from without import Endo
from without import Stream
from without import cancel_futures
from without import stack
from without_asgi import RawHeaders

from without_http.h2_wire import request_headers
from without_http.h2_wire import response_status_and_headers
from without_http.timeouts import ConnectTimeout
from without_http.timeouts import PoolTimeout
from without_http.timeouts import ReadTimeout
from without_http.timeouts import Timeout
from without_http.timeouts import WriteTimeout
from without_http.timeouts import phase

_BUFFER = 65536
_REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})
_NO_TIMEOUT = Timeout()  # a shared immutable "no timeouts" value, safe as a default

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
    A client request as a value: the head plus a streaming body.

    The body is a `Stream[bytes]` (an async iterable of chunks), so a request can be
    buffered (one chunk) or streamed (many), the upload half of the buffered/streaming
    matrix. Because the whole request is the value a `ClientExchange` transforms,
    middleware can rewrite it: add headers, change the URL, wrap the body.
    """

    method: str
    url: str
    headers: RawHeaders = ()
    body: Stream[bytes] = field(default_factory=_empty_body)


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
    `await exchange(request)` returns. `body` is a `ResponseBody`, a once-consumable
    stream that releases its connection when it ends or is closed.

    A `NamedTuple` so a caller can take it whole (`response.head`, `response.body`) or
    unpack it (`head, body = response`) with each field keeping its precise type, which a
    `__iter__` on a dataclass could not give. The two halves are independent, the
    consumer split that mirrors how a server consumes a request (a `scope` value plus a
    body stream): branch on `head` without touching `body`. `ConnectionPool.request`
    yields this value and closes `body` on exit; it is also what a `ClientExchange`
    rewrites (by constructing a new one, since a `NamedTuple` has no `dataclasses.replace`).
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


# A client exchange is the dual of a server handler: where a handler maps a request to
# a response over streams, an exchange maps a whole `ClientRequest` to a
# `ClientResponse`. A `ClientMiddleware` wraps an exchange into an exchange (`Endo`):
# it can rewrite the request before, or the response after, the inner exchange runs.
# This is the zero-context case of the shared `stack` vocabulary: a server middleware
# is `(handler, state, scope)`, a client one needs no context (the request is the value
# it transforms, not a fixed scope), so it is simply `(exchange) -> exchange`, and the
# same `stack` composes them. State a middleware must keep lives in a closure (see
# `cookies`), as it does server-side.
type ClientExchange = Callable[[ClientRequest], Awaitable[ClientResponse]]
type ClientMiddleware = Endo[ClientExchange]

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


def _build_request(method: str, url: str, headers: RawHeaders, content: bytes | Stream[bytes]) -> ClientRequest:
    """
    Assemble a `ClientRequest`, picking the body framing from `content`.

    Buffered `bytes` get a `content-length`; a streaming body whose length is unknown
    gets `transfer-encoding: chunked` (HTTP/1.1 frames it as chunks; over HTTP/2 the
    framing headers are dropped and the body rides DATA frames either way).
    """
    if isinstance(content, bytes):
        if not content:
            return ClientRequest(method, url, headers, _empty_body())
        if not _has(headers, b"content-length"):
            headers = (*headers, (b"content-length", str(len(content)).encode("ascii")))
        return ClientRequest(method, url, headers, _single(content))
    if not _has(headers, b"content-length") and not _has(headers, b"transfer-encoding"):
        headers = (*headers, (b"transfer-encoding", b"chunked"))
    return ClientRequest(method, url, headers, content)


_ALPN_H2 = ("h2", "http/1.1")
_ALPN_HTTP11 = ("http/1.1",)


async def _open(
    host: str, port: int, *, ssl_context: ssl.SSLContext | None, connect: float | None = None
) -> tuple[asyncio.StreamReader, asyncio.StreamWriter, str]:
    """
    Open a connection and report the negotiated wire protocol.

    `ssl_context` is `None` for cleartext, which has no negotiation and is always
    `http/1.1` (prior-knowledge h2c is opened directly by the pool instead), or a ready
    context whose ALPN offer the pool has already settled. Over TLS the protocol is
    whatever ALPN selected: `h2` when the server takes the offer, else `http/1.1`. The
    `connect` deadline covers the TCP connect and, over TLS, the handshake, since both
    happen in the one `open_connection` await; the ALPN read after it is synchronous.
    """
    async with phase(connect, ConnectTimeout):
        if ssl_context is None:
            reader, writer = await asyncio.open_connection(host, port)
            return reader, writer, "http/1.1"
        reader, writer = await asyncio.open_connection(host, port, ssl=ssl_context)
    ssl_object = writer.get_extra_info("ssl_object")
    negotiated = ssl_object.selected_alpn_protocol() if ssl_object is not None else None
    return reader, writer, "h2" if negotiated == "h2" else "http/1.1"


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
            headers.insert(0, (b"host", parts.netloc.encode("ascii")))
        self._writer.write(self._conn.send(h11.Request(method=request.method, target=_target(parts), headers=headers)))
        await self._writer.drain()

    async def send_body(self, request: ClientRequest, write: float | None = None) -> None:
        """
        Stream the request body, then end the message, as a background task.

        Draining per chunk gives ordinary write backpressure, and the `write` deadline
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
                    async with phase(write, WriteTimeout):
                        await self._writer.drain()
            if self._reader.at_eof():
                return
            self._writer.write(self._conn.send(h11.EndOfMessage()))
            async with phase(write, WriteTimeout):
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

    async def read_head(self, read: float | None = None) -> tuple[int, RawHeaders]:
        while True:
            event = self._conn.next_event()
            if event is h11.NEED_DATA:
                async with phase(read, ReadTimeout):
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

    async def iter_body(self, read: float | None = None) -> AsyncGenerator[bytes | ResponseTrailers]:
        while True:
            event = self._conn.next_event()
            if event is h11.NEED_DATA:
                async with phase(read, ReadTimeout):
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
            pool=timeout.pool,
        )
        # Once the stream is registered, any failure before the head is returned must
        # cancel the sender and reset the stream, so the slot and the connection are not
        # stranded (the h2 mirror of h11's pre-body guard).
        try:
            async with phase(timeout.write, WriteTimeout):
                await self._writer.drain()
            stream.send_task = asyncio.create_task(self._send_body(stream_id, stream, body, timeout.write))
            async with phase(timeout.read, ReadTimeout):
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
            self._iter_body(stream_id, stream, timeout.read),
            stream.send_task,
        )

    async def _open_stream(
        self, stream: _Stream, headers: list[tuple[bytes, bytes]], *, end_stream: bool, pool: float | None = None
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
        `pool` deadline bounds the whole wait for a slot (the wait a `PoolTimeout` guards).
        """
        async with phase(pool, PoolTimeout):
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
        self, stream_id: int, stream: _Stream, body: Stream[bytes], write: float | None = None
    ) -> None:
        try:
            async for chunk in body:
                if chunk:
                    await self._send_data(stream_id, stream, chunk, end=False, write=write)
            await self._send_data(stream_id, stream, b"", end=True, write=write)
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
        self, stream_id: int, stream: _Stream, data: bytes, *, end: bool, write: float | None = None
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
                async with phase(write, WriteTimeout):
                    await stream.window.wait()
                continue
            async with phase(write, WriteTimeout):
                await self._writer.drain()
            if len(remaining) == 0:
                return

    async def _iter_body(
        self, stream_id: int, stream: _Stream, read: float | None = None
    ) -> AsyncGenerator[bytes | ResponseTrailers]:
        while True:
            async with phase(read, ReadTimeout):
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


@dataclass(frozen=True, slots=True)
class _HostPool:
    """
    The per-origin HTTP/1.1 pool: idle connections plus an optional capacity permit.

    Two pieces coordinate to bound concurrency without also bounding reuse. The
    `idle` list holds kept-alive connections not currently in use; the semaphore
    issues at most `max_connections` *permits*. They are deliberately separate
    because a permit tracks a **checkout, not a socket**:

    - A checkout takes a permit first (blocking, at the bound, until one is freed:
      the wait a `PoolTimeout` guards), then reuses an idle connection if one is
      available and opens a fresh socket only if none is. So a permit is held for
      the whole time a connection is in use, whether reused or freshly opened.
    - A release frees the permit and, if the connection is still reusable, returns
      it to `idle`; an idle connection therefore holds **no** permit.

    That split is what a single bounded queue of connections could not express: the
    permit gates *concurrent use* (so a burst waits instead of opening unbounded
    sockets), while reuse-before-open under a held permit keeps the count of live
    sockets `<= max`. Idle connections never exceed the high-water mark of concurrent
    checkouts, and are reused (not stacked on top of) the permitted ones, so the bound
    holds across both. When `max_connections` is `None` the pool is unbounded and
    `acquire`/`release` are no-ops, so the `pool` timeout axis stays inert until a
    caller opts into a bound.
    """

    idle: list[_Http11Connection] = field(default_factory=list)
    _semaphore: asyncio.Semaphore | None = None

    @classmethod
    def new(cls, max_connections: int | None) -> _HostPool:
        return cls(_semaphore=asyncio.Semaphore(max_connections) if max_connections is not None else None)

    async def acquire(self) -> None:
        if self._semaphore is not None:
            await self._semaphore.acquire()

    def release(self) -> None:
        if self._semaphore is not None:
            self._semaphore.release()


@dataclass(slots=True)
class ConnectionPool:
    """
    Connections keyed by origin, and the entrypoint for making requests.

    Open it as an async context manager (`async with ConnectionPool(...) as pool`) so
    its connections are closed on exit; a directly-constructed pool works for
    short-lived use but does not manage the long-lived connections keep-alive retains.
    Make requests through `async with pool.request(...) as response`.

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

    `middleware` decorates every request through the pool; per-request `middleware` on
    `request` composes inside it. Keep pool-level middleware to *pure* decoration
    (default headers, redirect following, retry): things that are values, not state.
    Anything carrying mutable, request-spanning identity (a `CookieJar`, an auth
    session) belongs in a value you own and pass per request, so that connection reuse
    (a transport concern) and application identity stay independent rather than both
    hiding in the pool.

    `max_connections_per_host` bounds the number of concurrent HTTP/1.1 connections to
    one origin: at the bound, a checkout *waits* for one to be returned (the wait a
    `pool` timeout guards). It is unbounded by default, mirroring the server's choice
    to let OS backpressure cap connections rather than an in-process limit; opt into a
    bound when a caller wants explicit per-host backpressure.

    `timeout` bounds each phase of a request (see `Timeout`); it defaults to no timeouts,
    since a deadline is the caller's policy, not the transport's. A per-request `timeout`
    on `request` replaces it wholesale for that call.
    """

    allow_http2: bool = True
    force_http2_cleartext: bool = False
    ssl_context_factory: Callable[[], ssl.SSLContext] = ssl.create_default_context
    middleware: ClientMiddleware = _PASSTHROUGH
    max_connections_per_host: int | None = None
    timeout: Timeout = _NO_TIMEOUT
    _h2: dict[Origin, _Http2Connection] = field(default_factory=dict)
    _h11: dict[Origin, _HostPool] = field(default_factory=dict)
    _h11_only: set[Origin] = field(default_factory=set)
    _contexts: dict[tuple[str, ...], ssl.SSLContext] = field(default_factory=dict)
    _origin_locks: dict[Origin, asyncio.Lock] = field(default_factory=dict)
    _closed: bool = False

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    @asynccontextmanager
    async def request(
        self,
        method: str,
        url: str,
        *,
        headers: RawHeaders = (),
        body: bytes | Stream[bytes] = b"",
        middleware: ClientMiddleware = _PASSTHROUGH,
        timeout: Timeout | None = None,
    ) -> AsyncIterator[ClientResponse]:
        """
        Send a request and yield its `ClientResponse` for the block, then release the connection.

        `body` is the request body: `bytes` to buffer it, or a `Stream[bytes]` to stream
        it. The yielded `ClientResponse` can be taken whole (`response.head`,
        `response.body`) or unpacked (`head, body = ...`); read the response body with
        `async for chunk in body` / `await body.read()`, or `body.read_with_trailers()`
        when the endpoint carries trailers. On exit any unread body is drained or aborted
        so the connection is never stranded.

        `middleware` is composed inside the pool's own `middleware`, so a single request
        can add decoration (a `CookieJar` via `cookies`, an extra header) on top of the
        pool-wide stack for this call alone.

        `timeout` replaces the pool's own `timeout` for this call (`None` inherits it).
        It is captured as a value in the transport exchange, so it does not compose through
        `middleware` the way decoration does; a deadline overrides, it does not layer.
        """
        outgoing = _build_request(method, url, headers, body)
        effective = self.timeout if timeout is None else timeout

        async def bound(outgoing: ClientRequest) -> ClientResponse:
            return await self._exchange(outgoing, effective)

        exchange = stack(self.middleware, middleware)(bound)
        response = await exchange(outgoing)
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

    async def _exchange(self, request: ClientRequest, timeout: Timeout) -> ClientResponse:
        # The bare transport exchange: the inner `ClientExchange` that `request` wraps
        # with `stack(self.middleware, ...)`. Private so callers go through `request`
        # and never accidentally bypass the pool's configured middleware. The effective
        # `timeout` is threaded in as a value (captured by `bound` above), never stored on
        # the shared connection objects, so concurrent requests keep their own deadlines.
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
                    reader, writer, protocol = await _open(
                        origin.host,
                        origin.port,
                        ssl_context=self._context_for_connection(http2=True),
                        connect=timeout.connect,
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
            # The fallback connection was opened during the h2 handshake, so it holds no
            # permit yet; take one for it before use so its release balances exactly.
            try:
                async with phase(timeout.pool, PoolTimeout):
                    await self._host_pool(origin).acquire()
            except BaseException:  # pragma: no cover - defensive: the fallback origin's first permit is always free, so this only guards a cancel
                fallback.close()
                raise
            return await self._request_h11(origin, request, parts, timeout, preopened=fallback)
        return await self._request_h11(origin, request, parts, timeout)

    async def _acquire_h2c(self, origin: Origin, timeout: Timeout) -> _Http2Connection:
        connection = self._reusable_h2(origin)
        if connection is None:
            async with self._lock_for(origin):
                connection = self._reusable_h2(origin)
                if connection is None:
                    reader, writer, _ = await _open(origin.host, origin.port, ssl_context=None, connect=timeout.connect)
                    connection = _Http2Connection.start(reader, writer)
                    self._h2[origin] = connection
        return connection

    async def _h2_response(
        self, connection: _Http2Connection, request: ClientRequest, parts: SplitResult, timeout: Timeout
    ) -> ClientResponse:
        status, headers, stream_id, body, send_task = await connection.request(
            method=request.method.encode("ascii"),
            target=_target(parts).encode("ascii"),
            scheme=parts.scheme,
            authority=parts.netloc.encode("ascii"),
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
        connection = preopened if preopened is not None else await self._acquire_h11(origin, timeout)
        send_task: asyncio.Task[None] | None = None
        try:
            await connection.send_head(request, parts)
            send_task = asyncio.create_task(connection.send_body(request, timeout.write))
            status, headers = await connection.read_head(timeout.read)
        except BaseException:
            # The response body (which owns release) is not armed yet, so tear the
            # connection down here and free the permit exactly once. A caller-body error
            # (recorded on the connection) is preferred over the read failure it caused.
            await cancel_futures([send_task])
            await connection.aclose()
            self._host_pool(origin).release()
            if connection.send_error is not None:
                raise connection.send_error from None
            raise

        async def release(fully_read: bool) -> None:
            try:
                await cancel_futures([send_task])  # cancel-and-await the writer before deciding
                reusable = (
                    fully_read
                    and send_task is not None
                    and not send_task.cancelled()
                    and connection.finish()
                    and connection.usable
                    and not self._closed
                )
                if reusable:
                    self._return_h11(origin, connection)
                else:
                    await connection.aclose()
            finally:
                self._host_pool(origin).release()
            if connection.send_error is not None:
                raise connection.send_error

        return ClientResponse(
            ResponseHead(status, headers), ResponseBody(await _releasing(connection.iter_body(timeout.read), release))
        )

    def _host_pool(self, origin: Origin) -> _HostPool:
        pool = self._h11.get(origin)
        if pool is None:
            pool = _HostPool.new(self.max_connections_per_host)
            self._h11[origin] = pool
        return pool

    async def _acquire_h11(self, origin: Origin, timeout: Timeout) -> _Http11Connection:
        pool = self._host_pool(origin)
        async with phase(timeout.pool, PoolTimeout):
            await pool.acquire()
        try:
            while pool.idle:
                connection = pool.idle.pop()
                if connection.usable:
                    return connection
                connection.close()
            ssl_context = self._context_for_connection(http2=False) if origin.secure else None
            reader, writer, _ = await _open(origin.host, origin.port, ssl_context=ssl_context, connect=timeout.connect)
            return _Http11Connection.new(reader, writer)
        except BaseException:
            pool.release()
            raise

    def _return_h11(self, origin: Origin, connection: _Http11Connection) -> None:
        if self._closed:
            connection.close()
            return
        self._host_pool(origin).idle.append(connection)

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
        self._closed = True
        multiplexed = list(self._h2.values())
        idle = [connection for pool in self._h11.values() for connection in pool.idle]
        self._h2.clear()
        self._h11.clear()
        for multiplexed_connection in multiplexed:
            await multiplexed_connection.aclose()
        for idle_connection in idle:
            await idle_connection.aclose()


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
    loops, like `follow_redirects`, is written directly as a `ClientExchange` wrapper.
    """

    def middleware(inner: ClientExchange) -> ClientExchange:
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
    `stack` and rewrites the request before the inner exchange runs. This is how a
    pool sends default headers (auth tokens, a user agent) on every request, or a
    single request adds its own.
    """
    extra: RawHeaders = tuple(headers)
    return wrap(request=lambda request: replace(request, headers=extra + request.headers))


def follow_redirects(max_hops: int = 5) -> ClientMiddleware:
    """
    Client middleware that follows `3xx` redirects, up to `max_hops`.

    Each intermediate response is drained before the next hop, so its connection is
    released. The follow re-issues the same request body, so redirects with a
    one-shot streaming body are not replayable; in practice redirects follow bodyless
    requests.
    """

    def middleware(inner: ClientExchange) -> ClientExchange:
        async def exchange(request: ClientRequest) -> ClientResponse:
            response = await inner(request)
            for _ in range(max_hops):
                if response.head.status not in _REDIRECT_STATUSES:
                    return response
                location = next((value for name, value in response.head.headers if name.lower() == b"location"), None)
                if location is None:
                    return response
                await response.body.aclose()
                request = replace(request, url=urljoin(request.url, location.decode("ascii")))
                response = await inner(request)
            return response

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


def _default_path(request_path: str) -> str:
    """
    The RFC 6265 default-path for a cookie set without a `Path`: the request path up
    to (not including) its last `/`, or `/`.
    """
    if not request_path.startswith("/") or request_path == "/":
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


def _deletes(max_age: str | None) -> bool:
    if max_age is None:
        return False
    try:
        return int(max_age) <= 0
    except ValueError:
        return False


def _parse_set_cookie(header: str, host: str, request_path: str) -> tuple[_Cookie, bool] | None:
    """
    Parse one `Set-Cookie` value into a `_Cookie` and whether it deletes one.

    `None` for a header with no `name=value`. A `Max-Age` of zero or less marks the
    cookie for deletion (the second tuple element); `Expires` is not parsed, so
    date-based expiry is not yet honored.
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
    path = attributes.get("path", "")
    cookie = _Cookie(
        name=name,
        value=value,
        domain=domain or host,
        path=path if path.startswith("/") else _default_path(request_path),
        secure="secure" in attributes,
        host_only=not domain,
    )
    return cookie, _deletes(attributes.get("max-age"))


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
    attribute, and deletion via `Max-Age<=0`. Not yet: `Expires` date-based expiry.
    """

    _cookies: dict[tuple[str, str, str], _Cookie] = field(default_factory=dict)

    def store(self, url: str, headers: RawHeaders) -> None:
        """Fold every `Set-Cookie` in a response into the jar."""
        parts = urlsplit(url)
        host = (parts.hostname or "").lower()
        for name, value in headers:
            if name.lower() != b"set-cookie":
                continue
            parsed = _parse_set_cookie(value.decode("latin-1"), host, parts.path)
            if parsed is None:
                continue
            cookie, deletes = parsed
            key = (cookie.domain, cookie.path, cookie.name)
            if deletes:
                self._cookies.pop(key, None)
            else:
                self._cookies[key] = cookie

    def header_for(self, url: str) -> bytes | None:
        """The `Cookie` header value for a request to `url`, or `None` if none match."""
        parts = urlsplit(url)
        host = (parts.hostname or "").lower()
        path = parts.path or "/"
        secure = parts.scheme == "https"
        matched = [
            cookie
            for cookie in self._cookies.values()
            if _domain_matches(host, cookie) and _path_matches(path, cookie) and (secure or not cookie.secure)
        ]
        if not matched:
            return None
        matched.sort(key=lambda cookie: len(cookie.path), reverse=True)
        return "; ".join(f"{cookie.name}={cookie.value}" for cookie in matched).encode("latin-1")


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

    def middleware(inner: ClientExchange) -> ClientExchange:
        async def exchange(request: ClientRequest) -> ClientResponse:
            header = jar.header_for(request.url)
            if header is not None:
                request = replace(request, headers=_with_cookie(request.headers, header))
            response = await inner(request)
            jar.store(request.url, response.head.headers)
            return response

        return exchange

    return middleware
