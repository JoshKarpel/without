from __future__ import annotations

from without_asgi import HttpScope
from without_asgi import Response
from without_web import Match
from without_web import buffered

from .helpers import a_scope
from .helpers import drive_json
from .helpers import json_response


def _scope() -> HttpScope:
    return a_scope(method="POST", path="/things/42")


async def test_buffered_passes_state_match_and_read_body_to_make() -> None:
    seen: dict[str, object] = {}
    match = Match(_scope(), {"id": 42})

    def make(state: str, got_match: Match[HttpScope], body: bytes) -> Response:
        seen.update(state=state, match=got_match, body=body)
        return json_response(201, {"ok": True})

    handler = buffered(make)("tenant", match)
    status, response = await drive_json(handler, b"alpha-body")

    assert status == 201
    assert response == {"ok": True}
    assert seen == {"state": "tenant", "match": match, "body": b"alpha-body"}
