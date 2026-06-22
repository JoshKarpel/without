from __future__ import annotations

import json
from urllib.parse import parse_qs

from pydantic import BaseModel
from without_asgi import Response


class Flags(BaseModel):
    """The feature-flag set, validated from the ConfigMap YAML at the boundary."""

    flags: dict[str, bool] = {}


_JSON_HEADERS: tuple[tuple[bytes, bytes], ...] = ((b"content-type", b"application/json"),)


def _json_response(status: int, payload: object) -> Response:
    body = json.dumps(payload, sort_keys=True).encode()
    return Response(status=status, headers=_JSON_HEADERS, body=body)


def render_all(flags: Flags) -> Response:
    return _json_response(200, {"flags": flags.flags})


def render_one(flags: Flags, name: str) -> Response:
    try:
        enabled = flags.flags[name]
    except KeyError:
        return _json_response(404, {"error": f"unknown flag: {name}"})
    return _json_response(200, {"name": name, "enabled": enabled})


def bad_request(message: str) -> Response:
    return _json_response(400, {"error": message})


def route_not_found(method: str, path: str) -> Response:
    return _json_response(404, {"error": f"no route for {method} {path}"})


def flag_name(query_string: bytes) -> str | None:
    """The `name` query parameter, or `None` when it is absent."""
    values = parse_qs(query_string.decode()).get("name")
    return values[0] if values else None
