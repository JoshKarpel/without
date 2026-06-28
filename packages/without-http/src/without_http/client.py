from __future__ import annotations

import asyncio
import ssl
from collections.abc import AsyncIterator
from collections.abc import Awaitable
from collections.abc import Callable
from contextlib import asynccontextmanager
from contextlib import suppress
from dataclasses import dataclass
from dataclasses import replace
from urllib.parse import urljoin
from urllib.parse import urlsplit

import h11
from without_asgi import RawHeaders
from without_asgi.routing import Middleware
from without_asgi.routing import stack

_BUFFER = 65536
_REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})


@dataclass(frozen=True, slots=True)
class ClientRequest:
    """A whole client request as one value: the method, absolute URL, and body."""

    method: str
    url: str
    headers: RawHeaders = ()
    body: bytes = b""


@dataclass(frozen=True, slots=True)
class ClientResponse:
    """A whole client response as one value, the buffered counterpart of a request."""

    status: int
    headers: RawHeaders
    body: bytes


# A client exchange is the dual of a server handler: where a handler maps a
# request to a response over streams, an exchange maps a whole `ClientRequest` to
# a `ClientResponse`. Modeling it as a node lets the *same* `Middleware`
# vocabulary (`stack`) that wraps server handlers wrap client exchanges: a
# `ClientMiddleware` is `(state, inner_exchange, request) -> exchange`, the
# request playing the role the scope plays server-side.
type ClientExchange = Callable[[ClientRequest], Awaitable[ClientResponse]]
type ClientMiddleware = Middleware[object, ClientExchange, ClientRequest]

_PASSTHROUGH: ClientMiddleware = stack()


def _has(headers: RawHeaders, name: bytes) -> bool:
    return any(existing.lower() == name for existing, _ in headers)


async def _read_response(conn: h11.Connection, reader: asyncio.StreamReader) -> ClientResponse:
    status: int | None = None
    headers: RawHeaders = ()
    body = bytearray()
    while True:
        event = conn.next_event()
        if event is h11.NEED_DATA:
            conn.receive_data(await reader.read(_BUFFER))
            continue
        if isinstance(event, h11.Response):
            status = event.status_code
            headers = tuple((bytes(name), bytes(value)) for name, value in event.headers)
        elif isinstance(event, h11.Data):
            body.extend(event.data)
        elif not isinstance(event, h11.InformationalResponse):
            break
    if status is None:
        raise ConnectionError("the server closed the connection before sending a response")
    return ClientResponse(status=status, headers=headers, body=bytes(body))


async def _send(request: ClientRequest) -> ClientResponse:
    """The base client exchange: open one connection, send the request, read the response."""
    parts = urlsplit(request.url)
    if parts.hostname is None:
        raise ValueError(f"client request URL must be absolute, got {request.url!r}")
    secure = parts.scheme == "https"
    port = parts.port or (443 if secure else 80)
    target = parts.path or "/"
    if parts.query:
        target = f"{target}?{parts.query}"

    headers = list(request.headers)
    if not _has(request.headers, b"host"):
        headers.insert(0, (b"host", parts.netloc.encode("ascii")))
    if request.body and not _has(request.headers, b"content-length"):
        headers.append((b"content-length", str(len(request.body)).encode("ascii")))

    context = ssl.create_default_context() if secure else None
    reader, writer = await asyncio.open_connection(parts.hostname, port, ssl=context)
    try:
        conn = h11.Connection(our_role=h11.CLIENT)
        writer.write(conn.send(h11.Request(method=request.method, target=target, headers=headers)))
        if request.body:
            writer.write(conn.send(h11.Data(data=request.body)))
        writer.write(conn.send(h11.EndOfMessage()))
        await writer.drain()
        return await _read_response(conn, reader)
    finally:
        writer.close()
        with suppress(OSError):
            await writer.wait_closed()


@dataclass(frozen=True, slots=True)
class Session:
    """A reusable client session, opened once and shared, in the aiohttp style.

    A session is the mandated entrypoint rather than free `get`/`post`: it is the
    home for default headers, the middleware stack, and (later) the connection
    pool, so lifetime and limits stay explicit and injected. v1 opens a fresh
    connection per request; pooling is a contained follow-up behind this same
    surface. Open it with `open_session()` and make requests through
    `async with session.request(...) as response`.
    """

    default_headers: RawHeaders = ()
    middleware: ClientMiddleware = _PASSTHROUGH

    @asynccontextmanager
    async def request(
        self,
        method: str,
        url: str,
        *,
        headers: RawHeaders = (),
        body: bytes = b"",
    ) -> AsyncIterator[ClientResponse]:
        request = ClientRequest(method=method, url=url, headers=self.default_headers + headers, body=body)
        exchange = self.middleware(None, _send, request)
        yield await exchange(request)


@asynccontextmanager
async def open_session(
    *,
    headers: RawHeaders = (),
    middleware: ClientMiddleware = _PASSTHROUGH,
) -> AsyncIterator[Session]:
    """Open a `Session` for the duration of the `with` block."""
    yield Session(default_headers=headers, middleware=middleware)


def default_headers(*headers: tuple[bytes, bytes]) -> ClientMiddleware:
    """Client middleware that adds default headers to every request.

    The mirror of a server's request-decorating middleware: it sits in the same
    `stack` and rewrites the request before the inner exchange runs.
    """

    extra: RawHeaders = tuple(headers)

    def middleware(state: object, inner: ClientExchange, request: ClientRequest) -> ClientExchange:
        async def exchange(outgoing: ClientRequest) -> ClientResponse:
            return await inner(replace(outgoing, headers=extra + outgoing.headers))

        return exchange

    return middleware


def follow_redirects(max_hops: int = 5) -> ClientMiddleware:
    """Client middleware that follows `3xx` redirects, up to `max_hops`."""

    def middleware(state: object, inner: ClientExchange, request: ClientRequest) -> ClientExchange:
        async def exchange(outgoing: ClientRequest) -> ClientResponse:
            response = await inner(outgoing)
            for _ in range(max_hops):
                if response.status not in _REDIRECT_STATUSES:
                    return response
                location = next((value for name, value in response.headers if name.lower() == b"location"), None)
                if location is None:
                    return response
                outgoing = replace(outgoing, url=urljoin(outgoing.url, location.decode("ascii")))
                response = await inner(outgoing)
            return response

        return exchange

    return middleware
