from __future__ import annotations

import uuid

import pytest
from without import Stream
from without import stream_from_iterable
from without_asgi import HttpScope
from without_asgi import Response
from without_asgi import WebsocketInbound
from without_asgi import WebsocketOutbound
from without_web import INT
from without_web import STR
from without_web import UUID
from without_web import Match
from without_web import buffered
from without_web import catch_all
from without_web import mount
from without_web import path_param
from without_web import route
from without_web import url_for
from without_web import ws


@buffered
def _ok(state: object, match: Match[HttpScope], body: bytes) -> Response:  # pragma: no cover - never dispatched
    return Response(status=200, headers=(), body=b"")


def _feed(
    state: object, room: str, inbound: Stream[WebsocketInbound]
) -> Stream[WebsocketOutbound]:  # pragma: no cover - never dispatched
    frames: list[WebsocketOutbound] = []
    return stream_from_iterable(frames)


uid = path_param("id", INT)
show_user = route(t"/users/{uid}", get=_ok)
tail = catch_all("rest")
serve_file = route(t"/files/{tail}", get=_ok)


def test_url_for_renders_a_typed_path_parameter() -> None:
    assert url_for(show_user, {"id": 42}) == "/users/42"


def test_url_for_renders_a_literal_route_without_values() -> None:
    assert url_for(route("/health", get=_ok)) == "/health"


def test_url_for_includes_a_baked_mount_prefix() -> None:
    # `mount` bakes the prefix into the route value, so it is part of the route's
    # own path: reversing needs no router and cannot miss the prefix.
    assert url_for(mount("/api")(show_user), {"id": 7}) == "/api/users/7"


def test_url_for_includes_nested_mount_prefixes() -> None:
    assert url_for(mount("/api")(mount("/v1")(show_user)), {"id": 9}) == "/api/v1/users/9"


def test_url_for_renders_a_uuid_parameter() -> None:
    token = path_param("token", UUID)
    session = route(t"/sessions/{token}", get=_ok)
    value = uuid.UUID("12345678-1234-5678-1234-567812345678")
    assert url_for(session, {"token": value}) == f"/sessions/{value}"


def test_url_for_allows_slashes_in_a_catch_all() -> None:
    assert url_for(serve_file, {"rest": "a/b/c.txt"}) == "/files/a/b/c.txt"


def test_url_for_reverses_a_websocket_route() -> None:
    room = path_param("room", STR)
    feed = ws(t"/feed/{room}", room)(_feed)
    assert url_for(feed, {"room": "lobby"}) == "/feed/lobby"


def test_url_for_reverses_a_route_value_with_no_router() -> None:
    # The point of baking: an HTTP route reverses the same however it is reached
    # (an HTTP handler, a websocket handler, a template), because it is a pure
    # function of the self-contained route value.
    assert url_for(mount("/api")(show_user), {"id": 3}) == "/api/users/3"


def test_url_for_rejects_a_value_the_converter_would_not_parse() -> None:
    with pytest.raises(ValueError, match="not a valid int"):
        url_for(show_user, {"id": "not-a-number"})


def test_url_for_rejects_a_value_that_does_not_round_trip() -> None:
    # str "42" renders "42" which INT parses to 42, which is not the str we gave.
    with pytest.raises(ValueError, match="does not round-trip"):
        url_for(show_user, {"id": "42"})


def test_url_for_rejects_a_single_segment_value_that_spans_segments() -> None:
    slug = path_param("slug", STR)
    page = route(t"/pages/{slug}", get=_ok)
    with pytest.raises(ValueError, match="spans multiple path segments"):
        url_for(page, {"slug": "a/b"})


def test_url_for_rejects_a_missing_path_parameter() -> None:
    with pytest.raises(ValueError, match="missing values for path parameter"):
        url_for(show_user, {})


def test_url_for_rejects_an_unknown_path_parameter() -> None:
    with pytest.raises(ValueError, match="unknown path parameter"):
        url_for(show_user, {"id": 1, "typo": 2})
