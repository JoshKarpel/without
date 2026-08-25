from __future__ import annotations

import gzip
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
from without_asgi import HttpHandler
from without_asgi import HttpScope
from without_asgi import Inbound
from without_asgi import Inventory
from without_asgi import Outbound
from without_asgi import RawHeaders
from without_asgi import Response
from without_asgi import ResponseStart
from without_asgi import encode_response
from without_asgi import inventory
from without_asgi.headers import first
from without_streams import Stream
from without_web import Match
from without_web import Route
from without_web import Router
from without_web import mount
from without_web import static_files
from without_web import url_for

from .helpers import a_scope
from .helpers import drive

_STYLESHEET = ("body { color: rebeccapurple; }\n" * 40).encode()
_SCRIPT = b"console.log('hi');\n"
# A status no route produces, so a test that reaches the fallback says so unambiguously
# rather than blending into a 404 the static route could also have returned.
_FELL_THROUGH = 599


@pytest.fixture
def assets(tmp_path: Path) -> Inventory:
    (tmp_path / "css").mkdir()
    (tmp_path / "css" / "app.css").write_bytes(_STYLESHEET)
    (tmp_path / "app.js").write_bytes(_SCRIPT)
    return inventory(tmp_path, encodings={})


def _fallback(state: object, match: Match[HttpScope]) -> HttpHandler:
    async def handler(inputs: Stream[Inbound]) -> AsyncIterator[Outbound]:
        for event in encode_response(Response(status=_FELL_THROUGH, body=b"fallback\n")):
            yield event

    return handler


async def _request(
    route: Route[object],
    path: str,
    *,
    method: str = "GET",
    headers: RawHeaders = (),
) -> tuple[ResponseStart, bytes]:
    router: Router[object] = Router(routes=(route,), fallback=_fallback)
    return await drive(router.dispatch(object(), a_scope(method=method, path=path, headers=headers)))


class TestRouting:
    async def test_the_remainder_is_the_inventory_key(self, assets: Inventory) -> None:
        start, body = await _request(static_files("/static", assets), "/static/css/app.css")

        assert start.status == 200
        assert body == _STYLESHEET

    async def test_a_key_the_inventory_does_not_hold_is_a_404(self, assets: Inventory) -> None:
        start, _body = await _request(static_files("/static", assets), "/static/nothing.css")

        assert start.status == 404

    async def test_head_is_routed_as_well_as_get(self, assets: Inventory) -> None:
        start, _body = await _request(static_files("/static", assets), "/static/app.js", method="HEAD")

        assert start.status == 200

    async def test_a_method_the_route_does_not_serve_is_a_405(self, assets: Inventory) -> None:
        start, _body = await _request(static_files("/static", assets), "/static/app.js", method="POST")

        assert start.status == 405

    async def test_the_bare_prefix_does_not_match(self, assets: Inventory) -> None:
        # A catch-all needs at least one segment, and a request for the directory itself
        # is a listing request, which an inventory never answers.
        start, _body = await _request(static_files("/static", assets), "/static/")

        assert start.status == _FELL_THROUGH

    async def test_a_multi_segment_prefix_works(self, assets: Inventory) -> None:
        start, body = await _request(static_files("/a/b", assets), "/a/b/app.js")

        assert (start.status, body) == (200, _SCRIPT)

    @pytest.mark.security("a traversal key reaches the route and finds no entry")
    async def test_a_traversal_key_simply_misses(self, assets: Inventory) -> None:
        start, _body = await _request(static_files("/static", assets), "/static/../../etc/passwd")

        assert start.status == 404

    async def test_a_custom_not_found_is_used(self, assets: Inventory) -> None:
        teapot = Response(status=418, body=b"nope\n")

        start, body = await _request(static_files("/static", assets, not_found=teapot), "/static/nothing.css")

        assert (start.status, body) == (418, b"nope\n")


class TestReverseRouting:
    def test_the_route_reverses_to_an_asset_url(self, assets: Inventory) -> None:
        assert url_for(static_files("/static", assets), {"rest": "css/app.css"}) == "/static/css/app.css"

    def test_the_parameter_name_can_be_chosen(self, assets: Inventory) -> None:
        route = static_files("/static", assets, parameter="asset")

        assert url_for(route, {"asset": "app.js"}) == "/static/app.js"

    def test_a_mount_prefix_is_baked_into_the_reversed_url(self, assets: Inventory) -> None:
        behind = mount("/v2")(static_files("/static", assets))

        assert url_for(behind, {"rest": "app.js"}) == "/v2/static/app.js"

    async def test_a_mounted_route_still_serves(self, assets: Inventory) -> None:
        behind = mount("/v2")(static_files("/static", assets))

        start, body = await _request(behind, "/v2/static/app.js")

        assert (start.status, body) == (200, _SCRIPT)


class TestNegotiationThroughTheRoute:
    async def test_a_client_offering_a_coding_gets_the_pre_compressed_variant(self, tmp_path: Path) -> None:
        (tmp_path / "app.css").write_bytes(_STYLESHEET)
        route = static_files("/static", inventory(tmp_path))

        start, body = await _request(route, "/static/app.css", headers=((b"accept-encoding", b"gzip"),))

        assert first(start.headers, b"content-encoding") == b"gzip"
        assert gzip.decompress(body) == _STYLESHEET
