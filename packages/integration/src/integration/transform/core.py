from __future__ import annotations

import json
from enum import Enum
from typing import assert_never
from urllib.parse import parse_qs

from pydantic import BaseModel
from without_asgi import Response


class Mode(Enum):
    UPPER = "upper"
    LOWER = "lower"
    TITLE = "title"


class Settings(BaseModel):
    """The transform settings, validated from the ConfigMap YAML at the boundary."""

    default_mode: Mode = Mode.UPPER
    max_bytes: int = 1024


_JSON_HEADERS: tuple[tuple[bytes, bytes], ...] = ((b"content-type", b"application/json"),)
_TEXT_HEADERS: tuple[tuple[bytes, bytes], ...] = ((b"content-type", b"text/plain; charset=utf-8"),)


def _json_response(status: int, payload: object) -> Response:
    body = json.dumps(payload, sort_keys=True).encode()
    return Response(status=status, headers=_JSON_HEADERS, body=body)


def apply_mode(mode: Mode, text: str) -> str:
    match mode:
        case Mode.UPPER:
            return text.upper()
        case Mode.LOWER:
            return text.lower()
        case Mode.TITLE:
            return text.title()
        case _ as unreachable:
            assert_never(unreachable)


def transform(settings: Settings, requested_mode: str | None, body: bytes) -> Response:
    """Transform `body` and render the reply, applying the live `settings`.

    The query `mode` overrides `settings.default_mode`; `settings.max_bytes`
    caps the request size. Both come from the config `Context`, so a reload
    changes them for in-flight requests.
    """
    if len(body) > settings.max_bytes:
        return _json_response(413, {"error": f"body exceeds {settings.max_bytes} bytes"})
    mode = settings.default_mode if requested_mode is None else _parse_mode(requested_mode)
    if mode is None:
        return _json_response(400, {"error": f"unknown mode: {requested_mode}"})
    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError:
        return _json_response(400, {"error": "body is not valid UTF-8"})
    return Response(status=200, headers=_TEXT_HEADERS, body=apply_mode(mode, text).encode())


def render_modes(settings: Settings) -> Response:
    return _json_response(200, {"modes": [mode.value for mode in Mode], "default": settings.default_mode.value})


def route_not_found(method: str, path: str) -> Response:
    return _json_response(404, {"error": f"no route for {method} {path}"})


def _parse_mode(value: str) -> Mode | None:
    try:
        return Mode(value)
    except ValueError:
        return None


def mode_param(query_string: bytes) -> str | None:
    """The `mode` query parameter, or `None` when it is absent."""
    values = parse_qs(query_string.decode()).get("mode")
    return values[0] if values else None
