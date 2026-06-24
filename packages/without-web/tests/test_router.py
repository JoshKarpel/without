from __future__ import annotations

import json
from collections.abc import AsyncIterator

import pytest
from without import Stream
from without import stream
from without_asgi import Asgi
from without_asgi import HttpHandler
from without_asgi import HttpScope
from without_asgi import Inbound
from without_asgi import Outbound
from without_asgi import RequestBody
from without_asgi import Response
from without_asgi import ResponseBody
from without_asgi import ResponseStart
from without_asgi import encode_response
from without_web import Match
from without_web import Mount
from without_web import Router
from without_web import buffered
from without_web import json_response
from without_web import route


class DomainError(Exception):
    pass


def _scope(method: str, path: str, *, root_path: str = "") -> HttpScope:
    return HttpScope(
        asgi=Asgi(version="3.0", spec_version="2.0"),
        http_version="1.1",
        method=method,
        scheme="http",
        path=path,
        raw_path=None,
        query_string=b"",
        root_path=root_path,
        headers=(),
        client=None,
        server=None,
        extensions=None,
    )


async def _inbound(body: bytes = b"") -> AsyncIterator[Inbound]:
    yield RequestBody(body=body, more_body=False)


async def _run(handler: HttpHandler, body: bytes = b"") -> tuple[ResponseStart, bytes]:
    events = [event async for event in handler(_inbound(body))]
    start = events[0]
    assert isinstance(start, ResponseStart)
    payload = b"".join(event.body for event in events if isinstance(event, ResponseBody))
    return start, payload


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


async def test_dispatch_selects_the_endpoint_for_the_method() -> None:
    router: Router[object] = Router(routes=(route("/todos", get=_ok, post=_created),), fallback=_fallback)
    start, body = await _run(router.dispatch(object(), _scope("POST", "/todos")))
    assert start.status == 201
    assert json.loads(body) == {"who": "created"}


async def test_dispatch_binds_typed_path_parameters() -> None:
    router: Router[object] = Router(routes=(route("/todos/{id:int}", get=_show),), fallback=_fallback)
    _start, body = await _run(router.dispatch(object(), _scope("GET", "/todos/42")))
    assert json.loads(body) == {"id": 42}


async def test_a_known_path_with_an_unbound_method_is_405_with_an_allow_header() -> None:
    router: Router[object] = Router(routes=(route("/todos", get=_ok, post=_created),), fallback=_fallback)
    start, _body = await _run(router.dispatch(object(), _scope("DELETE", "/todos")))
    assert start.status == 405
    assert (b"allow", b"GET, POST") in start.headers


async def test_an_unknown_path_falls_back() -> None:
    router: Router[object] = Router(routes=(route("/todos", get=_ok),), fallback=_fallback)
    start, body = await _run(router.dispatch(object(), _scope("GET", "/nope")))
    assert start.status == 404
    assert json.loads(body) == {"error": "no route for GET /nope"}


async def test_a_mounted_router_is_grafted_at_the_prefix() -> None:
    inner: Router[object] = Router(routes=(route("/stats", get=_ok),), fallback=_fallback)
    outer: Router[object] = Router(routes=(Mount("/admin", inner),), fallback=_fallback)
    start, body = await _run(outer.dispatch(object(), _scope("GET", "/admin/stats")))
    assert start.status == 200
    assert json.loads(body) == {"who": "ok"}


async def test_an_opaque_mount_receives_the_prefix_trimmed_scope() -> None:
    def echo(state: object, head: HttpScope) -> HttpHandler:
        def handler(inputs: Stream[Inbound]) -> Stream[Outbound]:
            return stream(encode_response(json_response(200, {"path": head.path, "root_path": head.root_path})))

        return handler

    outer: Router[object] = Router(routes=(Mount("/legacy", echo),), fallback=_fallback)
    _start, body = await _run(outer.dispatch(object(), _scope("GET", "/legacy/ping")))
    assert json.loads(body) == {"path": "/ping", "root_path": "/legacy"}


async def test_an_exception_before_response_start_is_mapped_to_a_response() -> None:
    @buffered
    def boom(state: object, match: Match[HttpScope], body: bytes) -> Response:
        raise DomainError("nope")

    router: Router[object] = Router(
        routes=(route("/boom", get=boom),),
        fallback=_fallback,
        exception_handlers={DomainError: lambda exc: json_response(400, {"error": str(exc)})},
    )
    start, body = await _run(router.dispatch(object(), _scope("GET", "/boom")))
    assert start.status == 400
    assert json.loads(body) == {"error": "nope"}


async def test_an_exception_after_response_start_propagates() -> None:
    def boom_after(state: object, match: Match[HttpScope]) -> HttpHandler:
        async def processor(inputs: Stream[Inbound]) -> AsyncIterator[Outbound]:
            yield ResponseStart(status=201)
            raise DomainError("too late")

        return processor

    router: Router[object] = Router(
        routes=(route("/boom", get=boom_after),),
        fallback=_fallback,
        exception_handlers={DomainError: lambda exc: json_response(400, {"error": str(exc)})},
    )
    with pytest.raises(DomainError):
        await _run(router.dispatch(object(), _scope("GET", "/boom")))
