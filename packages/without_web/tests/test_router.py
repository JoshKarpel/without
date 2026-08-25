from __future__ import annotations

import json
import re
from collections.abc import AsyncIterator

import pytest
from without_asgi import HttpHandler
from without_asgi import HttpScope
from without_asgi import Inbound
from without_asgi import Outbound
from without_asgi import Response
from without_asgi import ResponseStart
from without_asgi import WebsocketHandler
from without_asgi import WebsocketInbound
from without_asgi import WebsocketOutbound
from without_asgi import WebsocketScope
from without_asgi import WebsocketSend
from without_asgi import WebsocketText
from without_asgi import encode_response
from without_asgi.routing import HttpMiddleware
from without_asgi.routing import WebsocketMiddleware
from without_streams import Stream
from without_streams import stream_from_iterable
from without_web import INT
from without_web import Match
from without_web import Router
from without_web import WebsocketEndpoint
from without_web import WebsocketRouter
from without_web import buffered
from without_web import catch_all
from without_web import catching
from without_web import delegate
from without_web import mount
from without_web import path_param
from without_web import route
from without_web import with_middleware
from without_web import ws_delegate
from without_web import ws_mount
from without_web import ws_route

from .helpers import a_scope
from .helpers import a_websocket_scope
from .helpers import drive
from .helpers import json_response


class DomainError(Exception):
    pass


async def _to_400(exc: Exception) -> Response | None:
    match exc:
        case DomainError():
            return json_response(400, {"error": str(exc)})
        case _:
            return None


def _scope(method: str, path: str, *, root_path: str = "") -> HttpScope:
    return a_scope(method=method, path=path, root_path=root_path)


@buffered
def _ok(state: object, match: Match[HttpScope], body: bytes) -> Response:
    return json_response(200, {"who": "ok"})


@buffered
def _created(state: object, match: Match[HttpScope], body: bytes) -> Response:
    return json_response(201, {"who": "created"})


@buffered
def _show(state: object, match: Match[HttpScope], body: bytes) -> Response:
    return json_response(200, {"id": match.params["id"]})


@buffered
def _fallback(state: object, match: Match[HttpScope], body: bytes) -> Response:
    return json_response(404, {"error": f"no route for {match.scope.method} {match.scope.path}"})


def test_route_with_no_methods_is_a_build_error() -> None:
    with pytest.raises(ValueError, match="declares no methods"):
        route("/empty")


async def test_dispatch_selects_the_endpoint_for_the_method() -> None:
    router: Router[object] = Router(routes=(route("/todos", get=_ok, post=_created),), fallback=_fallback)
    start, body = await drive(router.dispatch(object(), _scope("POST", "/todos")))
    assert start.status == 201
    assert json.loads(body) == {"who": "created"}


async def test_dispatch_binds_typed_path_parameters() -> None:
    router: Router[object] = Router(routes=(route(t"/todos/{path_param('id', INT)}", get=_show),), fallback=_fallback)
    _start, body = await drive(router.dispatch(object(), _scope("GET", "/todos/42")))
    assert json.loads(body) == {"id": 42}


def test_a_partial_segment_template_is_a_build_error() -> None:
    with pytest.raises(ValueError, match=r"^a path parameter must occupy a whole segment$"):
        Router(routes=(route(t"/file.{path_param('ext', INT)}", get=_show),), fallback=_fallback)


def test_a_parameter_followed_by_text_in_the_same_segment_is_a_build_error() -> None:
    with pytest.raises(ValueError, match=r"^a path parameter must occupy a whole segment$"):
        Router(routes=(route(t"/todos/{path_param('id', INT)}x", get=_show),), fallback=_fallback)


def test_a_catch_all_before_another_segment_is_a_build_error() -> None:
    with pytest.raises(ValueError, match=r"^a catch-all parameter must be the last segment$"):
        Router(routes=(route(t"/x/{catch_all('rest')}/y", get=_show),), fallback=_fallback)


async def test_a_template_without_a_leading_slash_still_binds_its_parameter() -> None:
    router: Router[object] = Router(routes=(route(t"todos/{path_param('id', INT)}", get=_show),), fallback=_fallback)
    _start, body = await drive(router.dispatch(object(), _scope("GET", "/todos/42")))
    assert json.loads(body) == {"id": 42}


def test_a_non_path_param_interpolation_is_a_build_error() -> None:
    message = "an interpolation in a route pattern must be a path_param(...) or catch_all(...)"
    with pytest.raises(ValueError, match=rf"^{re.escape(message)}$"):
        Router(routes=(route(t"/x/{42}", get=_show),), fallback=_fallback)


async def test_a_known_path_with_an_unbound_method_is_405_with_an_allow_header() -> None:
    router: Router[object] = Router(routes=(route("/todos", get=_ok, post=_created),), fallback=_fallback)
    start, _body = await drive(router.dispatch(object(), _scope("DELETE", "/todos")))
    assert start.status == 405
    assert (b"allow", b"GET, POST") in start.headers


@pytest.mark.parametrize("method", ["HEAD", "PUT", "PATCH", "DELETE", "OPTIONS"])
async def test_dispatch_routes_each_declared_method_to_its_endpoint(method: str) -> None:
    router: Router[object] = Router(routes=(route("/r", **{method.lower(): _ok}),), fallback=_fallback)
    start, body = await drive(router.dispatch(object(), _scope(method, "/r")))
    assert start.status == 200
    assert json.loads(body) == {"who": "ok"}


async def test_method_not_allowed_carries_content_type_allow_and_body() -> None:
    router: Router[object] = Router(routes=(route("/todos", get=_ok, post=_created),), fallback=_fallback)
    start, body = await drive(router.dispatch(object(), _scope("DELETE", "/todos")))
    assert start.status == 405
    assert (b"content-type", b"text/plain; charset=utf-8") in start.headers
    assert (b"allow", b"GET, POST") in start.headers
    assert body == b"method not allowed\n"


@buffered
def _show_rest(state: object, match: Match[HttpScope], body: bytes) -> Response:
    return json_response(200, {"rest": match.params["rest"]})


async def test_dispatch_binds_a_catch_all_parameter() -> None:
    router: Router[object] = Router(routes=(route(t"/files/{catch_all('rest')}", get=_show_rest),), fallback=_fallback)
    _start, body = await drive(router.dispatch(object(), _scope("GET", "/files/a/b/c.txt")))
    assert json.loads(body) == {"rest": "a/b/c.txt"}


async def test_an_unknown_path_falls_back() -> None:
    router: Router[object] = Router(routes=(route("/todos", get=_ok),), fallback=_fallback)
    start, body = await drive(router.dispatch(object(), _scope("GET", "/nope")))
    assert start.status == 404
    assert json.loads(body) == {"error": "no route for GET /nope"}


async def test_a_string_pattern_is_a_literal_only_convenience() -> None:
    router: Router[object] = Router(routes=(route("/todos", get=_ok),), fallback=_fallback)
    start, body = await drive(router.dispatch(object(), _scope("GET", "/todos")))
    assert start.status == 200
    assert json.loads(body) == {"who": "ok"}


async def test_mount_bakes_its_prefix_into_a_route() -> None:
    outer: Router[object] = Router(routes=(mount("/admin")(route("/stats", get=_ok)),), fallback=_fallback)
    start, body = await drive(outer.dispatch(object(), _scope("GET", "/admin/stats")))
    assert start.status == 200
    assert json.loads(body) == {"who": "ok"}


async def test_mount_applied_to_many_routes_returns_a_tuple() -> None:
    api = mount("/api")
    outer: Router[object] = Router(routes=api(route("/a", get=_ok), route("/b", get=_created)), fallback=_fallback)
    a_start, _ = await drive(outer.dispatch(object(), _scope("GET", "/api/a")))
    b_start, _ = await drive(outer.dispatch(object(), _scope("GET", "/api/b")))
    assert a_start.status == 200
    assert b_start.status == 201


def _echo(state: object, head: HttpScope) -> HttpHandler:
    def handler(inputs: Stream[Inbound]) -> Stream[Outbound]:
        return stream_from_iterable(
            encode_response(json_response(200, {"path": head.path, "root_path": head.root_path}))
        )

    return handler


async def test_an_opaque_delegate_receives_the_prefix_trimmed_scope() -> None:
    outer: Router[object] = Router(routes=(delegate("/legacy", _echo),), fallback=_fallback)
    _start, body = await drive(outer.dispatch(object(), _scope("GET", "/legacy/ping")))
    assert json.loads(body) == {"path": "/ping", "root_path": "/legacy"}


async def test_mount_rebases_a_nested_delegate_to_the_full_prefix() -> None:
    app: Router[object] = Router(routes=(mount("/admin")(delegate("/legacy", _echo)),), fallback=_fallback)
    # `mount` prepends its prefix to the delegate, so the opaque app sits at
    # `/admin/legacy` and its scope is trimmed by that full prefix.
    _start, body = await drive(app.dispatch(object(), _scope("GET", "/admin/legacy/ping")))
    assert json.loads(body) == {"path": "/ping", "root_path": "/admin/legacy"}


@pytest.mark.security("a delegate trims the mount prefix by matched segment count, not byte length")
async def test_a_delegate_trims_by_segment_not_string_length() -> None:
    # A leading double slash still matches segment-wise; trimming must be by matched
    # segment count, so the sub-app sees `/ping`, not a byte-sliced `y/ping`.
    outer: Router[object] = Router(routes=(delegate("/legacy", _echo),), fallback=_fallback)
    _start, body = await drive(outer.dispatch(object(), _scope("GET", "//legacy/ping")))
    assert json.loads(body) == {"path": "/ping", "root_path": "/legacy"}


async def test_an_exception_before_response_start_is_mapped_to_a_response() -> None:
    @buffered
    def boom(state: object, match: Match[HttpScope], body: bytes) -> Response:
        raise DomainError("nope")

    router: Router[object] = Router(
        routes=(route("/boom", get=boom),),
        fallback=_fallback,
        middleware=catching(_to_400),
    )
    start, body = await drive(router.dispatch(object(), _scope("GET", "/boom")))
    assert start.status == 400
    assert json.loads(body) == {"error": "nope"}


async def test_catching_propagates_an_exception_when_recover_declines() -> None:
    @buffered
    def boom(state: object, match: Match[HttpScope], body: bytes) -> Response:
        raise ValueError("unmapped")

    router: Router[object] = Router(
        routes=(route("/boom", get=boom),),
        fallback=_fallback,
        middleware=catching(_to_400),
    )
    with pytest.raises(ValueError, match="unmapped"):
        await drive(router.dispatch(object(), _scope("GET", "/boom")))


class Forbidden(Exception):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


async def test_recover_narrows_the_exception_type_and_reads_typed_fields() -> None:
    async def recover(exc: Exception) -> Response | None:
        match exc:
            case Forbidden():
                return json_response(403, {"reason": exc.reason})
            case _:  # pragma: no cover - decline path tested elsewhere
                return None

    @buffered
    def deny(state: object, match: Match[HttpScope], body: bytes) -> Response:
        raise Forbidden("tenant-locked")

    router: Router[object] = Router(
        routes=(route("/deny", get=deny),),
        fallback=_fallback,
        middleware=catching(recover),
    )
    start, body = await drive(router.dispatch(object(), _scope("GET", "/deny")))
    assert start.status == 403
    assert json.loads(body) == {"reason": "tenant-locked"}


async def test_middleware_reads_the_dispatched_state() -> None:
    def stamp_state(handler: HttpHandler, state: str, scope: HttpScope) -> HttpHandler:
        async def processor(inputs: Stream[Inbound]) -> AsyncIterator[Outbound]:
            async for event in handler(inputs):
                if isinstance(event, ResponseStart):
                    yield ResponseStart(status=event.status, headers=(*event.headers, (b"x-state", state.encode())))
                else:
                    yield event

        return processor

    router: Router[str] = Router(routes=(route("/who", get=_ok),), fallback=_fallback, middleware=stamp_state)
    start, _body = await drive(router.dispatch("tenant-7", _scope("GET", "/who")))
    assert (b"x-state", b"tenant-7") in start.headers


async def test_an_exception_after_response_start_propagates() -> None:
    def boom_after(state: object, match: Match[HttpScope]) -> HttpHandler:
        async def processor(inputs: Stream[Inbound]) -> AsyncIterator[Outbound]:
            yield ResponseStart(status=201)
            raise DomainError("too late")

        return processor

    router: Router[object] = Router(
        routes=(route("/boom", get=boom_after),),
        fallback=_fallback,
        middleware=catching(_to_400),
    )
    with pytest.raises(DomainError):
        await drive(router.dispatch(object(), _scope("GET", "/boom")))


def _mark(name: bytes, value: bytes) -> HttpMiddleware[object]:
    def middleware(handler: HttpHandler, _state: object, scope: HttpScope) -> HttpHandler:
        async def processor(inputs: Stream[Inbound]) -> AsyncIterator[Outbound]:
            async for event in handler(inputs):
                if isinstance(event, ResponseStart):
                    yield ResponseStart(status=event.status, headers=(*event.headers, (name, value)))
                else:
                    yield event

        return processor

    return middleware


async def test_with_middleware_scopes_to_one_route() -> None:
    router: Router[object] = Router(
        routes=(route("/guarded", get=with_middleware(_ok, _mark(b"x-auth", b"yes"))), route("/open", get=_created)),
        fallback=_fallback,
    )
    guarded, _ = await drive(router.dispatch(object(), _scope("GET", "/guarded")))
    open_route, _ = await drive(router.dispatch(object(), _scope("GET", "/open")))
    assert (b"x-auth", b"yes") in guarded.headers
    assert not any(name == b"x-auth" for name, _ in open_route.headers)


async def test_mount_bakes_its_middleware_onto_the_routes_under_it() -> None:
    admin = mount("/admin", _mark(b"x-zone", b"admin"))
    app: Router[object] = Router(
        routes=(route("/health", get=_created), admin(route("/users", get=_ok))), fallback=_fallback
    )
    inside, _ = await drive(app.dispatch(object(), _scope("GET", "/admin/users")))
    outside, _ = await drive(app.dispatch(object(), _scope("GET", "/health")))
    assert (b"x-zone", b"admin") in inside.headers
    assert not any(name == b"x-zone" for name, _ in outside.headers)


async def test_mount_wraps_a_nested_delegate_with_its_middleware() -> None:
    def opaque(state: object, head: HttpScope) -> HttpHandler:
        def handler(inputs: Stream[Inbound]) -> Stream[Outbound]:
            return stream_from_iterable(encode_response(json_response(200, {"who": "opaque"})))

        return handler

    admin = mount("/admin", _mark(b"x-zone", b"admin"))
    app: Router[object] = Router(routes=(admin(delegate("/legacy", opaque)),), fallback=_fallback)
    start, body = await drive(app.dispatch(object(), _scope("GET", "/admin/legacy/ping")))
    assert start.status == 200
    assert json.loads(body) == {"who": "opaque"}
    assert (b"x-zone", b"admin") in start.headers


async def test_router_wide_and_mount_baked_middleware_both_apply() -> None:
    admin = mount("/admin", _mark(b"x-inner", b"section"))
    app: Router[object] = Router(
        routes=(admin(route("/users", get=_ok)),), fallback=_fallback, middleware=_mark(b"x-outer", b"app")
    )
    start, _ = await drive(app.dispatch(object(), _scope("GET", "/admin/users")))
    assert (b"x-outer", b"app") in start.headers
    assert (b"x-inner", b"section") in start.headers


@buffered
def _reflect_state_and_path(state: object, match: Match[HttpScope], body: bytes) -> Response:
    return json_response(200, {"state": state, "path": match.scope.path})


def _stamp_state_and_path(handler: HttpHandler, state: object, scope: HttpScope) -> HttpHandler:
    async def processor(inputs: Stream[Inbound]) -> AsyncIterator[Outbound]:
        async for event in handler(inputs):
            if isinstance(event, ResponseStart):
                stamped = (*event.headers, (b"x-mw-state", str(state).encode()), (b"x-mw-path", scope.path.encode()))
                yield ResponseStart(status=event.status, headers=stamped)
            else:
                yield event

    return processor


async def test_with_middleware_hands_the_state_and_scope_to_endpoint_and_middleware() -> None:
    guarded = with_middleware(_reflect_state_and_path, _stamp_state_and_path)
    router: Router[str] = Router(routes=(route("/w", get=guarded),), fallback=_fallback)
    start, body = await drive(router.dispatch("tenant-9", _scope("GET", "/w")))
    assert json.loads(body) == {"state": "tenant-9", "path": "/w"}
    assert (b"x-mw-state", b"tenant-9") in start.headers
    assert (b"x-mw-path", b"/w") in start.headers


def _opaque_reflect(state: object, head: HttpScope) -> HttpHandler:
    def handler(inputs: Stream[Inbound]) -> Stream[Outbound]:
        return stream_from_iterable(encode_response(json_response(200, {"state": state, "path": head.path})))

    return handler


async def test_mount_middleware_hands_the_state_and_scope_to_a_delegate_and_middleware() -> None:
    admin = mount("/admin", _stamp_state_and_path)
    app: Router[str] = Router(routes=(admin(delegate("/legacy", _opaque_reflect)),), fallback=_fallback)
    start, body = await drive(app.dispatch("tenant-5", _scope("GET", "/admin/legacy/ping")))
    assert json.loads(body) == {"state": "tenant-5", "path": "/ping"}
    assert (b"x-mw-state", b"tenant-5") in start.headers
    assert (b"x-mw-path", b"/ping") in start.headers


async def test_a_delegate_at_the_exact_prefix_sees_a_root_only_path() -> None:
    outer: Router[object] = Router(routes=(delegate("/legacy", _echo),), fallback=_fallback)
    _start, body = await drive(outer.dispatch(object(), _scope("GET", "/legacy")))
    assert json.loads(body) == {"path": "/", "root_path": "/legacy"}


async def test_a_delegate_trims_a_multi_segment_remainder() -> None:
    outer: Router[object] = Router(routes=(delegate("/legacy", _echo),), fallback=_fallback)
    _start, body = await drive(outer.dispatch(object(), _scope("GET", "/legacy/a/b")))
    assert json.loads(body) == {"path": "/a/b", "root_path": "/legacy"}


def test_declaring_one_method_twice_for_a_path_is_a_build_error() -> None:
    with pytest.raises(ValueError, match=r"^duplicate route: method GET declared twice for one path$"):
        Router(routes=(route("/todos", get=_ok), route("/todos", get=_created)), fallback=_fallback)


def _ws_scope(path: str) -> WebsocketScope:
    return a_websocket_scope(path=path)


def _ws_says(label: str) -> WebsocketEndpoint[object]:
    def endpoint(state: object, match: Match[WebsocketScope]) -> WebsocketHandler:
        def processor(inputs: Stream[WebsocketInbound]) -> Stream[WebsocketOutbound]:
            return stream_from_iterable((WebsocketSend(WebsocketText(text=label)),))

        return processor

    return endpoint


def _ws_echo(state: object, scope: WebsocketScope) -> WebsocketHandler:
    def processor(inputs: Stream[WebsocketInbound]) -> Stream[WebsocketOutbound]:
        return stream_from_iterable((WebsocketSend(WebsocketText(text=f"{scope.path}|{scope.root_path}")),))

    return processor


def _ws_prepend(label: str) -> WebsocketMiddleware[object]:
    def middleware(handler: WebsocketHandler, _state: object, scope: WebsocketScope) -> WebsocketHandler:
        async def processor(inputs: Stream[WebsocketInbound]) -> AsyncIterator[WebsocketOutbound]:
            yield WebsocketSend(WebsocketText(text=label))
            async for event in handler(inputs):
                yield event

        return processor

    return middleware


async def _ws_texts(handler: WebsocketHandler) -> list[str]:
    events = [event async for event in handler(stream_from_iterable(()))]
    return [
        event.data.text
        for event in events
        if isinstance(event, WebsocketSend) and isinstance(event.data, WebsocketText)
    ]


async def test_ws_mount_bakes_its_prefix_into_a_route() -> None:
    outer: WebsocketRouter[object] = WebsocketRouter(
        routes=(ws_mount("/admin")(ws_route("/stats", _ws_says("stats"))),), fallback=_ws_says("fallback")
    )
    assert await _ws_texts(outer.dispatch(object(), _ws_scope("/admin/stats"))) == ["stats"]


async def test_ws_mount_bakes_its_middleware_onto_the_routes_under_it() -> None:
    admin = ws_mount("/admin", _ws_prepend("zone"))
    app: WebsocketRouter[object] = WebsocketRouter(
        routes=(admin(ws_route("/feed", _ws_says("feed"))),), fallback=_ws_says("fallback")
    )
    inside = await _ws_texts(app.dispatch(object(), _ws_scope("/admin/feed")))
    outside = await _ws_texts(app.dispatch(object(), _ws_scope("/nowhere")))
    assert inside == ["zone", "feed"]
    assert outside == ["fallback"]


async def test_an_opaque_ws_delegate_receives_the_prefix_trimmed_scope() -> None:
    app: WebsocketRouter[object] = WebsocketRouter(
        routes=(ws_delegate("/legacy", _ws_echo),), fallback=_ws_says("fallback")
    )
    assert await _ws_texts(app.dispatch(object(), _ws_scope("/legacy/ping"))) == ["/ping|/legacy"]


@pytest.mark.security("a WebSocket delegate trims the mount prefix by matched segment count, not byte length")
async def test_a_ws_delegate_trims_by_segment_not_string_length() -> None:
    app: WebsocketRouter[object] = WebsocketRouter(
        routes=(ws_delegate("/legacy", _ws_echo),), fallback=_ws_says("fallback")
    )
    assert await _ws_texts(app.dispatch(object(), _ws_scope("//legacy/ping"))) == ["/ping|/legacy"]


async def test_ws_mount_wraps_a_nested_delegate_with_its_middleware() -> None:
    admin = ws_mount("/admin", _ws_prepend("zone"))
    app: WebsocketRouter[object] = WebsocketRouter(
        routes=(admin(ws_delegate("/legacy", _ws_echo)),), fallback=_ws_says("fallback")
    )
    # `ws_mount` prepends its prefix to the delegate, so the opaque app sits at
    # `/admin/legacy` and its scope is trimmed by that full prefix.
    assert await _ws_texts(app.dispatch(object(), _ws_scope("/admin/legacy/ping"))) == ["zone", "/ping|/admin/legacy"]


def _ws_reflect_state(state: object, scope: WebsocketScope) -> WebsocketHandler:
    def processor(inputs: Stream[WebsocketInbound]) -> Stream[WebsocketOutbound]:
        return stream_from_iterable((WebsocketSend(WebsocketText(text=f"{state}|{scope.path}")),))

    return processor


def _ws_stamp_state_and_path(handler: WebsocketHandler, state: object, scope: WebsocketScope) -> WebsocketHandler:
    async def processor(inputs: Stream[WebsocketInbound]) -> AsyncIterator[WebsocketOutbound]:
        yield WebsocketSend(WebsocketText(text=f"mw:{state}:{scope.path}"))
        async for event in handler(inputs):
            yield event

    return processor


async def test_ws_mount_middleware_hands_state_and_scope_to_a_delegate_and_middleware() -> None:
    admin = ws_mount("/admin", _ws_stamp_state_and_path)
    app: WebsocketRouter[str] = WebsocketRouter(
        routes=(admin(ws_delegate("/legacy", _ws_reflect_state)),), fallback=_ws_says("fallback")
    )
    texts = await _ws_texts(app.dispatch("tenant-3", _ws_scope("/admin/legacy/ping")))
    assert texts == ["mw:tenant-3:/ping", "tenant-3|/ping"]
