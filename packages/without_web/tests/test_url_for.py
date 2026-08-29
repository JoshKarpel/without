from __future__ import annotations

import re
import uuid
from enum import Enum

import pytest
from without_asgi import HttpScope
from without_asgi import Response
from without_asgi import WebsocketInbound
from without_asgi import WebsocketOutbound
from without_streams import Stream
from without_streams import stream_from_iterable
from without_web import INT
from without_web import STR
from without_web import UUID
from without_web import Match
from without_web import buffered
from without_web import catch_all
from without_web import choice
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


def test_url_for_renders_an_enum_member_as_the_value_it_routes_on() -> None:
    # A plain `Enum` is the case `str` gets wrong: `str(Region.WEST)` is
    # "Region.WEST", while the segment `choice` matches on is "west".
    class Region(Enum):
        WEST = "west"
        EAST = "east"

    where = path_param("region", choice(Region))
    listing = route(t"/regions/{where}", get=_ok)
    assert url_for(listing, {"region": Region.WEST}) == "/regions/west"


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
    with pytest.raises(ValueError, match=r"^value 'not-a-number' for path parameter 'id' is not a valid int$"):
        url_for(show_user, {"id": "not-a-number"})


def test_url_for_rejects_a_value_that_does_not_round_trip() -> None:
    # str "42" renders "42" which INT parses to 42, which is not the str we gave.
    message = "value '42' for path parameter 'id' does not round-trip through the int converter"
    with pytest.raises(ValueError, match=rf"^{re.escape(message)}$"):
        url_for(show_user, {"id": "42"})


def test_url_for_names_the_catch_all_parameter_that_does_not_round_trip() -> None:
    # The catch-all's PATH converter renders 123 to "123" and parses it back to the
    # str "123", which is not the int given, so the error names the "rest" parameter.
    message = "value 123 for path parameter 'rest' does not round-trip through the path converter"
    with pytest.raises(ValueError, match=rf"^{re.escape(message)}$"):
        url_for(serve_file, {"rest": 123})


def test_url_for_rejects_a_single_segment_value_that_spans_segments() -> None:
    slug = path_param("slug", STR)
    page = route(t"/pages/{slug}", get=_ok)
    with pytest.raises(ValueError, match=r"^value 'a/b' for path parameter 'slug' spans multiple path segments$"):
        url_for(page, {"slug": "a/b"})


def test_url_for_rejects_a_missing_path_parameter() -> None:
    with pytest.raises(ValueError, match="missing values for path parameter"):
        url_for(show_user, {})


def test_url_for_lists_every_missing_path_parameter() -> None:
    alpha = path_param("alpha", INT)
    beta = path_param("beta", INT)
    pair = route(t"/pair/{alpha}/{beta}", get=_ok)
    message = "url_for is missing values for path parameter(s): alpha, beta"
    with pytest.raises(ValueError, match=rf"^{re.escape(message)}$"):
        url_for(pair, {})


def test_url_for_rejects_an_unknown_path_parameter() -> None:
    with pytest.raises(ValueError, match="unknown path parameter"):
        url_for(show_user, {"id": 1, "typo": 2})


def test_url_for_lists_every_unknown_path_parameter() -> None:
    message = "url_for got values for unknown path parameter(s): typo1, typo2"
    with pytest.raises(ValueError, match=rf"^{re.escape(message)}$"):
        url_for(show_user, {"id": 1, "typo1": 2, "typo2": 3})


def test_url_for_renders_a_trailing_literal_after_a_parameter() -> None:
    edit = route(t"/todos/{uid}/edit", get=_ok)
    assert url_for(edit, {"id": 8}) == "/todos/8/edit"
