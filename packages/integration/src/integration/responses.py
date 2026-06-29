from __future__ import annotations

import json

from without_asgi import RawHeaders
from without_asgi import Response

JSON_HEADERS: RawHeaders = ((b"content-type", b"application/json"),)
TEXT_HEADERS: RawHeaders = ((b"content-type", b"text/plain; charset=utf-8"),)


def json_response(status: int, payload: object) -> Response:
    """
    A JSON response with a deterministic (sorted-key) body.

    Encoding (the serializer, its options, the content type) is an application
    choice, so it lives here in the app layer rather than in `without-web`: the
    router only needs the resulting `Response`.
    """
    return Response(status=status, headers=JSON_HEADERS, body=json.dumps(payload, sort_keys=True).encode())


def text_response(text: str, status: int = 200) -> Response:
    """A `text/plain; charset=utf-8` response."""
    return Response(status=status, headers=TEXT_HEADERS, body=text.encode())
