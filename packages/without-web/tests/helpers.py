from __future__ import annotations

import json

from without_asgi import Response


def json_response(status: int, payload: object) -> Response:
    """
    Build a JSON `Response` for tests.

    `without-web` no longer ships an encoding helper (encoding is the app's
    choice); the tests carry their own to exercise the router with concrete
    `Response` values.
    """
    return Response(
        status=status,
        headers=((b"content-type", b"application/json"),),
        body=json.dumps(payload, sort_keys=True).encode(),
    )
