from __future__ import annotations

import json

from without_asgi import RawHeaders
from without_asgi import Response
from without_asgi import json_content

TEXT_HEADERS: RawHeaders = ((b"content-type", b"text/plain; charset=utf-8"),)


def _sorted(payload: object) -> str:
    """Deterministic JSON: two equal payloads built by different routes render the same bytes."""
    return json.dumps(payload, sort_keys=True, allow_nan=False)


def json_response(status: int, payload: object) -> Response:
    """
    A JSON response with a deterministic (sorted-key) body.

    `json_content` pairs the bytes with the `content-type` describing them; the
    *serializer* stays the application's choice, which is what the injected `dumps`
    is for. Sorting is this app's policy rather than the library's default, since it
    is a cost every response pays for a property only some callers want.
    """
    return Response.from_content(status, json_content(payload, dumps=_sorted))


def text_response(text: str, status: int = 200) -> Response:
    """A `text/plain; charset=utf-8` response."""
    return Response(status=status, headers=TEXT_HEADERS, body=text.encode())
