from __future__ import annotations

import json
from collections.abc import AsyncIterator

from without_asgi import Asgi
from without_asgi import HttpHandler
from without_asgi import HttpScope
from without_asgi import Inbound
from without_asgi import RequestBody
from without_asgi import Response
from without_asgi import ResponseBody
from without_asgi import ResponseStart
from without_web import Match
from without_web import buffered

from .helpers import json_response


def _scope() -> HttpScope:
    return HttpScope(
        asgi=Asgi(version="3.0", spec_version="2.0"),
        http_version="1.1",
        method="POST",
        scheme="http",
        path="/things/42",
        raw_path=None,
        query_string=b"",
        root_path="",
        headers=(),
        client=None,
        server=None,
        extensions=None,
    )


async def _inbound(payload: bytes) -> AsyncIterator[Inbound]:
    yield RequestBody(body=payload, more_body=False)


async def _run(handler: HttpHandler, payload: bytes) -> tuple[int, object]:
    events = [event async for event in handler(_inbound(payload))]
    start = events[0]
    assert isinstance(start, ResponseStart)
    raw = b"".join(event.body for event in events if isinstance(event, ResponseBody))
    return start.status, json.loads(raw)


async def test_buffered_passes_state_match_and_read_body_to_make() -> None:
    seen: dict[str, object] = {}
    match = Match(_scope(), {"id": 42})

    def make(state: str, got_match: Match[HttpScope], body: bytes) -> Response:
        seen.update(state=state, match=got_match, body=body)
        return json_response(201, {"ok": True})

    handler = buffered(make)("tenant", match)
    status, response = await _run(handler, b"alpha-body")

    assert status == 201
    assert response == {"ok": True}
    assert seen == {"state": "tenant", "match": match, "body": b"alpha-body"}
