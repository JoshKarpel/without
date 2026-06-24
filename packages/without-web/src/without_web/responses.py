from __future__ import annotations

import json
from collections.abc import AsyncIterator
from collections.abc import Callable

from without import Stream
from without_asgi import HttpHandler
from without_asgi import HttpScope
from without_asgi import Inbound
from without_asgi import Outbound
from without_asgi import RawHeaders
from without_asgi import Response
from without_asgi import encode_response
from without_asgi import read_body

from without_web.router import Endpoint
from without_web.router import Match

JSON_HEADERS: RawHeaders = ((b"content-type", b"application/json"),)
TEXT_HEADERS: RawHeaders = ((b"content-type", b"text/plain; charset=utf-8"),)


def json_response(status: int, payload: object) -> Response:
    """A JSON response with a deterministic (sorted-key) body."""
    return Response(status=status, headers=JSON_HEADERS, body=json.dumps(payload, sort_keys=True).encode())


def text_response(text: str, status: int = 200) -> Response:
    """A `text/plain; charset=utf-8` response."""
    return Response(status=status, headers=TEXT_HEADERS, body=text.encode())


def buffered[T](make: Callable[[T, Match[HttpScope], bytes], Response]) -> Endpoint[T, HttpScope, HttpHandler]:
    """Adapt a body-reading `(state, match, body) -> Response` into an `Endpoint`.

    The web-flavored sibling of `without_asgi.routing.buffered`: it hands the
    handler the `Match` (the scope plus the router's already-parsed path
    parameters) rather than the bare scope, so a handler reads `match.params`
    and `match.scope` without re-parsing the target. Reads the whole request
    body, then runs `make` once and emits the single `Response`. Usable as a
    decorator.
    """

    def build(state: T, match: Match[HttpScope]) -> HttpHandler:
        def processor(inputs: Stream[Inbound]) -> Stream[Outbound]:
            return _respond(inputs, lambda body: make(state, match, body))

        return processor

    return build


async def _respond(inputs: Stream[Inbound], make: Callable[[bytes], Response]) -> AsyncIterator[Outbound]:
    body = await read_body(inputs)
    for event in encode_response(make(body)):
        yield event
