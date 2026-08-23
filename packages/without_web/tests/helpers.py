from __future__ import annotations

import json
from collections.abc import AsyncIterator

from without_asgi import Asgi
from without_asgi import HttpHandler
from without_asgi import HttpScope
from without_asgi import Inbound
from without_asgi import RawHeaders
from without_asgi import RequestBody
from without_asgi import Response
from without_asgi import ResponseBody
from without_asgi import ResponseStart
from without_asgi import WebsocketScope
from without_asgi import json_content

# The connection facts every scope a router test builds shares. A test varies what its
# route matches on (the method, the path, the query, a header) and nothing else, so the
# rest is fixed here and each module states only its own sample values.
_ASGI = Asgi(version="3.0", spec_version="2.0")
_HTTP_VERSION = "1.1"


def a_scope(
    *,
    method: str,
    path: str,
    query: bytes = b"",
    root_path: str = "",
    headers: RawHeaders = (),
) -> HttpScope:
    """One HTTP scope, with everything a router does not route on already filled in."""
    return HttpScope(
        asgi=_ASGI,
        http_version=_HTTP_VERSION,
        method=method,
        scheme="http",
        path=path,
        raw_path=None,
        query_string=query,
        root_path=root_path,
        headers=headers,
        client=None,
        server=None,
        extensions=None,
    )


def a_websocket_scope(*, path: str, query: bytes = b"") -> WebsocketScope:
    """The websocket half of `a_scope`, for the routes that take a handshake."""
    return WebsocketScope(
        asgi=_ASGI,
        http_version=_HTTP_VERSION,
        scheme="ws",
        path=path,
        raw_path=None,
        query_string=query,
        root_path="",
        headers=(),
        client=None,
        server=None,
        subprotocols=(),
        extensions=None,
    )


async def a_request_body(payload: bytes = b"") -> AsyncIterator[Inbound]:
    """A request whose body arrives whole, in one event."""
    yield RequestBody(body=payload, more_body=False)


async def drive(handler: HttpHandler, payload: bytes = b"") -> tuple[ResponseStart, bytes]:
    """Run a handler over one request and return its response start and joined body."""
    events = [event async for event in handler(a_request_body(payload))]
    start = events[0]
    assert isinstance(start, ResponseStart)
    body = b"".join(event.body for event in events if isinstance(event, ResponseBody))
    return start, body


async def drive_json(handler: HttpHandler, payload: bytes = b"") -> tuple[int, object]:
    """`drive` for a handler that answers JSON: its status and the decoded body."""
    start, body = await drive(handler, payload)
    return start.status, json.loads(body)


def json_response(status: int, payload: object) -> Response:
    """
    Build a JSON `Response` for tests, with a deterministic (sorted-key) body.

    `without-web` decides nothing about encoding: the shape and the stdlib default live
    one layer down in `without-asgi` (`json_content`, `Response.from_content`), and the
    serializer stays a choice, which these tests exercise by passing their own.
    """
    return Response.from_content(status, json_content(payload, dumps=lambda value: json.dumps(value, sort_keys=True)))
