from __future__ import annotations

import hashlib
import json
import time
from collections.abc import AsyncIterator
from typing import assert_never
from urllib.parse import parse_qs

from without import Stream
from without import compose
from without import sample
from without import stream
from without_asgi import ASGIApp
from without_asgi import HttpHandler
from without_asgi import HttpScope
from without_asgi import Inbound
from without_asgi import Outbound
from without_asgi import RawHeaders
from without_asgi import RequestBody
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
from without_asgi.routing import WebsocketMiddleware
from without_asgi.routing import buffered
from without_asgi.routing import stack
from without_asgi.routing import wrap

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


# How you write a `Middleware` depends on what it needs. `wrap` lifts scope-aware
# edge transformer(s) into a middleware, saving the `(handler, scope) -> handler`
# wrapper and the inner `compose`: reach for it whenever each edge is transformed
# on its own (logging, headers). When a middleware instead needs a value that spans
# the whole handling, that value is just a local in a plain processor (see
# `access_timing`); `wrap`'s separate transformers have no shared scope to hold it.
# A real service would emit structured records; printing just shows where the seam
# is, and that the same shape serves both protocols.


async def inject_header(outputs: Stream[Outbound], name: bytes, value: bytes) -> AsyncIterator[Outbound]:
    async for event in outputs:
        if isinstance(event, ResponseStart):
            yield ResponseStart(status=event.status, headers=(*event.headers, (name, value)))
        else:
            yield event


def with_header(name: bytes, value: bytes) -> HttpMiddleware:
    """Build middleware that appends a header to every response."""

    def add_header(outputs: Stream[Outbound], _head: HttpScope) -> Stream[Outbound]:
        return inject_header(outputs, name, value)

    return wrap(outbound=add_header)


async def log_request(inputs: Stream[Inbound], head: HttpScope) -> AsyncIterator[Inbound]:
    print(f"--> {head.method} {head.path}")
    async for event in inputs:
        yield event


async def log_response(outputs: Stream[Outbound], head: HttpScope) -> AsyncIterator[Outbound]:
    async for event in outputs:
        if isinstance(event, ResponseStart):
            print(f"<-- {head.method} {head.path} {event.status}")
        yield event


# Two independent ends that never share state, so one `wrap` bundles them: the
# inbound end logs the request as it arrives, the outbound end logs the status.
access_log: HttpMiddleware = wrap(inbound=log_request, outbound=log_response)


# The contrast: `access_timing` needs one value, the start time, to span the whole
# handling, from before the handler produces anything to after its last output.
# That is just a local in a single output-wrapping processor: the first pull drives
# the handler, so the local is set right as handling begins and read once the
# stream ends. No inbound transform and no second closure, so no shared mutable
# state and no `nonlocal`. `wrap` can't express this; its two transformers have no
# shared scope to hold the value.
def access_timing(handler: HttpHandler, head: HttpScope) -> HttpHandler:
    async def processor(inputs: Stream[Inbound]) -> AsyncIterator[Outbound]:
        started = time.monotonic()
        async for event in handler(inputs):
            yield event
        print(f"<-> {head.method} {head.path} {(time.monotonic() - started) * 1000:.1f} ms")

    return processor


# The genuine both-sides case: data observed on the inbound stream is carried into
# the outbound stream. The inbound end feeds each body chunk into a hasher; the
# outbound end reads the finished digest into a response header (the way S3 returns
# an `ETag` of an uploaded object). The shared state is the hasher, mutated in
# place, so there is still no `nonlocal`, and `compose` wraps both ends around the
# handler. This relies on the handler reading the whole request before it responds
# (true for the buffered routes here): a handler that streamed its response first
# would digest only the chunks seen by then.
def request_digest(handler: HttpHandler, head: HttpScope) -> HttpHandler:
    digest = hashlib.sha256()

    async def absorb(inputs: Stream[Inbound]) -> AsyncIterator[Inbound]:
        async for event in inputs:
            if isinstance(event, RequestBody):
                digest.update(event.body)
            yield event

    async def stamp(inputs: Stream[Outbound]) -> AsyncIterator[Outbound]:
        async for event in inputs:
            if isinstance(event, ResponseStart):
                tag = (b"x-request-digest", b"sha-256=" + digest.hexdigest().encode())
                yield ResponseStart(status=event.status, headers=(*event.headers, tag))
            else:
                yield event

    return compose(absorb, compose(handler, stamp))


async def log_connect(inputs: Stream[WebsocketInbound], head: WebsocketScope) -> AsyncIterator[WebsocketInbound]:
    print(f"=== WS {head.path}")
    async for event in inputs:
        yield event


socket_log: WebsocketMiddleware = wrap(inbound=log_connect)


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
            # First is outermost, so it times everything inside and sees the status
            # every other middleware produced.
            access_timing,
            access_log,
            request_digest,
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
