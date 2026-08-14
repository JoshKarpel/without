from __future__ import annotations

import json

from without_asgi import Response
from without_asgi import json_content


def json_response(status: int, payload: object) -> Response:
    """
    Build a JSON `Response` for tests, with a deterministic (sorted-key) body.

    `without-web` decides nothing about encoding: the shape and the stdlib default live
    one layer down in `without-asgi` (`json_content`, `Response.from_content`), and the
    serializer stays a choice, which these tests exercise by passing their own.
    """
    return Response.from_content(status, json_content(payload, dumps=lambda value: json.dumps(value, sort_keys=True)))
