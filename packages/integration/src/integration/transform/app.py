from __future__ import annotations

import json
from collections.abc import AsyncIterator
from typing import assert_never
from urllib.parse import parse_qs

from without import Stream
from without import sample
from without import stream
from without_asgi import ASGIApp
from without_asgi import HttpHandler
from without_asgi import HttpScope
from without_asgi import Inbound
from without_asgi import Outbound
from without_asgi import RawHeaders
from without_asgi import Response
from without_asgi import ResponseStart
from without_asgi import WebsocketAccept
from without_asgi import WebsocketBinary
from without_asgi import WebsocketClose
from without_asgi import WebsocketConnect
from without_asgi import WebsocketDisconnect
from without_asgi import WebsocketHandler
from without_asgi import WebsocketInbound
from without_asgi import WebsocketOutbound
from without_asgi import WebsocketReceive
from without_asgi import WebsocketScope
from without_asgi import WebsocketSend
from without_asgi import WebsocketText
from without_asgi import make_asgi_app
from without_asgi.routing import HttpMiddleware
from without_asgi.routing import buffered
from without_asgi.routing import stack

from integration.transform.core import Mode
from integration.transform.core import Settings
from integration.transform.core import TransformError
from integration.transform.core import apply_mode
from integration.transform.core import transform
from integration.transform.router import Router
from integration.transform.router import http_route
from integration.transform.router import ws_route

JSON_HEADERS: RawHeaders = ((b"content-type", b"application/json"),)
TEXT_HEADERS: RawHeaders = ((b"content-type", b"text/plain; charset=utf-8"),)


def json_response(status: int, payload: object) -> Response:
    return Response(status=status, headers=JSON_HEADERS, body=json.dumps(payload, sort_keys=True).encode())


def text_response(text: str) -> Response:
    return Response(status=200, headers=TEXT_HEADERS, body=text.encode())


def mode_param(query_string: bytes) -> str | None:
    """The `mode` query parameter, or `None` when it is absent."""
    values = parse_qs(query_string.decode()).get("mode")
    return values[0] if values else None


# `buffered` adapts a body-reading `(state, scope, body) -> Response` into the
# `HttpRouter` shape, so it reads as a decorator: each handler stays a plain pure
# function of the request. The handlers are the shell: they own the bytes (the
# size limit and the decode), hand decoded text to the core, and render its result
# or its raised `TransformError` as a `Response`.


@buffered
def transform_text(settings: Settings, head: HttpScope, body: bytes) -> Response:
    if len(body) > settings.max_bytes:
        return json_response(413, {"error": f"body exceeds {settings.max_bytes} bytes"})
    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError:
        return json_response(400, {"error": "body is not valid UTF-8"})
    try:
        transformed = transform(settings, mode_param(head.query_string), text)
    except TransformError as error:
        return json_response(400, {"error": str(error)})
    return text_response(transformed)


@buffered
def modes(settings: Settings, head: HttpScope, body: bytes) -> Response:
    return json_response(200, {"modes": [mode.value for mode in Mode], "default": settings.default_mode.value})


@buffered
def not_found(settings: Settings, head: HttpScope, body: bytes) -> Response:
    return json_response(404, {"error": f"no route for {head.method} {head.path}"})


def transform_socket(settings: Settings, head: WebsocketScope) -> WebsocketHandler:
    """A websocket that transforms each text frame with the connect-time default mode."""

    async def processor(inputs: Stream[WebsocketInbound]) -> AsyncIterator[WebsocketOutbound]:
        async for event in inputs:
            match event:
                case WebsocketConnect():
                    yield WebsocketAccept()
                case WebsocketReceive(data):
                    match data:
                        case WebsocketText(text):
                            yield WebsocketSend(WebsocketText(text=apply_mode(settings.default_mode, text)))
                        case WebsocketBinary():
                            yield WebsocketClose(code=1003, reason="text frames only")
                        case _ as unreachable:
                            assert_never(unreachable)
                case WebsocketDisconnect():
                    return
                case _ as unreachable:
                    assert_never(unreachable)

    return processor


def refuse_socket(settings: Settings, head: WebsocketScope) -> WebsocketHandler:
    """The websocket fallback: close an unrouted path without reading any frames."""

    def handler(inputs: Stream[WebsocketInbound]) -> Stream[WebsocketOutbound]:
        return stream((WebsocketClose(),))

    return handler


def with_header(name: bytes, value: bytes) -> HttpMiddleware:
    """Build middleware that appends a header to every response."""

    def add_header(inner: HttpHandler, _head: HttpScope) -> HttpHandler:
        def processor(inputs: Stream[Inbound]) -> Stream[Outbound]:
            return inject_header(inner(inputs), name, value)

        return processor

    return add_header


async def inject_header(outputs: Stream[Outbound], name: bytes, value: bytes) -> AsyncIterator[Outbound]:
    async for event in outputs:
        if isinstance(event, ResponseStart):
            yield ResponseStart(status=event.status, headers=(*event.headers, (name, value)))
        else:
            yield event


# `access_log` and `socket_log` need no configuration, so they are `Middleware`
# directly, where `with_header` is a factory that builds one. A real service would
# emit structured records; printing just shows where the seam is, and that the same
# `Middleware` vocabulary composes for both protocols.
def access_log(inner: HttpHandler, head: HttpScope) -> HttpHandler:
    def processor(inputs: Stream[Inbound]) -> Stream[Outbound]:
        # Wraps both ends: the inbound stream to log the request as it arrives,
        # the outbound stream to log the status the handler finally produced.
        return log_response(inner(log_request(inputs, head)), head)

    return processor


async def log_request(inputs: Stream[Inbound], head: HttpScope) -> AsyncIterator[Inbound]:
    print(f"--> {head.method} {head.path}")
    async for event in inputs:
        yield event


async def log_response(outputs: Stream[Outbound], head: HttpScope) -> AsyncIterator[Outbound]:
    async for event in outputs:
        if isinstance(event, ResponseStart):
            print(f"<-- {head.method} {head.path} {event.status}")
        yield event


def socket_log(inner: WebsocketHandler, head: WebsocketScope) -> WebsocketHandler:
    def processor(inputs: Stream[WebsocketInbound]) -> Stream[WebsocketOutbound]:
        print(f"=== WS {head.path}")
        return inner(inputs)

    return processor


def text_transform_app(source: Stream[Settings]) -> ASGIApp:
    """A text-transform service whose handlers read a live config `Context`, over HTTP and WebSocket."""
    # The lifespan is just the config watch: `sample(source)` is already an async
    # context manager, so the same value would drive a non-ASGI shell unchanged.
    # `make_asgi_app` owns the protocol and the receive/send wiring; the routes,
    # handlers, and middleware are this app's, dispatched by its own `Router` built
    # on `without_asgi.routing`'s tools.
    requests: Router[Settings, HttpScope, HttpHandler] = Router(
        routes=(
            http_route("POST", "/transform", transform_text),
            http_route("GET", "/modes", modes),
        ),
        fallback=not_found,
        middleware=stack(
            # First is outermost, so it sees the status every other middleware produced.
            access_log,
            # https://www.gnuterrypratchett.com: keep his name moving on the overhead.
            with_header(b"x-clacks-overhead", b"GNU Terry Pratchett"),
        ),
    )
    sockets: Router[Settings, WebsocketScope, WebsocketHandler] = Router(
        routes=(ws_route("/stream", transform_socket),),
        fallback=refuse_socket,
        middleware=stack(socket_log),
    )
    # Lock the config at connection start: snapshot the live `Context` the moment a
    # request or websocket arrives, so a reload can't change it underfoot for the
    # rest of that connection. The next connection picks up the reload.
    return make_asgi_app(
        lambda: sample(source),
        http=lambda config, head: requests.dispatch(config.current(), head),
        websocket=lambda config, head: sockets.dispatch(config.current(), head),
    )
