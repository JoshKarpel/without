from __future__ import annotations

import hashlib
import time
from collections.abc import AsyncIterator
from typing import assert_never
from urllib.parse import parse_qs

from pydantic import BaseModel
from without import Stream
from without import compose
from without import sample
from without import stream_from_iterable
from without_asgi import ASGIApp
from without_asgi import HttpHandler
from without_asgi import HttpScope
from without_asgi import Inbound
from without_asgi import Outbound
from without_asgi import RequestBody
from without_asgi import Response
from without_asgi import ResponseStart
from without_asgi import ResponseTrailers
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
from without_asgi import extension
from without_asgi import make_asgi_app
from without_asgi.routing import HttpMiddleware
from without_asgi.routing import WebsocketMiddleware
from without_asgi.routing import buffered
from without_asgi.routing import stack
from without_asgi.routing import wrap

from integration.responses import json_response
from integration.responses import text_response
from integration.transform.core import Mode
from integration.transform.core import TransformConfig
from integration.transform.core import TransformError
from integration.transform.core import apply_mode
from integration.transform.core import transform
from integration.transform.router import Router
from integration.transform.router import http_route
from integration.transform.router import ws_route


class HttpConfig(BaseModel):
    """
    The HTTP shell's own config: the request-body size limit it enforces.

    A transport concern, not a domain one, so it lives with the shell and never
    reaches the core (`transform.core` works in decoded text and names no byte
    count). The other shell, the CLI, has no equivalent.
    """

    max_bytes: int = 1024


class Settings(BaseModel):
    """
    The whole ASGI app's config, validated from the ConfigMap YAML at the boundary.

    Two sub-configs rather than one flat bag: `transform` is the domain config the
    core consumes, `http` is this shell's transport config. Splitting them keeps
    the core's `TransformConfig` free of shell concerns, so a handler hands the
    core `settings.transform` and reads its own limit from `settings.http`.
    """

    transform: TransformConfig = TransformConfig()
    http: HttpConfig = HttpConfig()


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
    if len(body) > settings.http.max_bytes:
        return json_response(413, {"error": f"body exceeds {settings.http.max_bytes} bytes"})
    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError:
        return json_response(400, {"error": "body is not valid UTF-8"})
    try:
        transformed = transform(settings.transform, mode_param(head.query_string), text)
    except TransformError as error:
        return json_response(400, {"error": str(error)})
    return text_response(transformed)


@buffered
def modes(settings: Settings, head: HttpScope, body: bytes) -> Response:
    return json_response(
        200, {"modes": [mode.value for mode in Mode], "default": settings.transform.default_mode.value}
    )


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
                            yield WebsocketSend(WebsocketText(text=apply_mode(settings.transform.default_mode, text)))
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
        return stream_from_iterable((WebsocketClose(),))

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


def with_header(name: bytes, value: bytes) -> HttpMiddleware[object]:
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
access_log: HttpMiddleware[object] = wrap(inbound=log_request, outbound=log_response)


# The contrast: `access_timing` needs one value, the start time, to span the whole
# handling, from before the handler produces anything to after its last output.
# That is just a local in a single output-wrapping processor: the first pull drives
# the handler, so the local is set right as handling begins and read once the
# stream ends. No inbound transform and no second closure, so no shared mutable
# state and no `nonlocal`. `wrap` can't express this; its two transformers have no
# shared scope to hold the value.
def access_timing(handler: HttpHandler, _state: object, head: HttpScope) -> HttpHandler:
    async def processor(inputs: Stream[Inbound]) -> AsyncIterator[Outbound]:
        started = time.monotonic()
        async for event in handler(inputs):
            yield event
        print(f"<-> {head.method} {head.path} {(time.monotonic() - started) * 1000:.1f} ms")

    return processor


def request_digest(handler: HttpHandler, _state: object, head: HttpScope) -> HttpHandler:
    """
    Middleware reporting a SHA-256 of the request body back on the response.

    The genuine both-sides case: data observed on the inbound stream (each body
    chunk, fed into a hasher) is carried into the outbound stream (the finished
    digest), the way S3 returns an `ETag` of an uploaded object. The shared state
    is the hasher, mutated in place, so there is no `nonlocal`, and `compose`
    wraps both ends around the handler.

    A digest is only correct once the whole request body has been hashed, so where
    it rides, and when the body is read, depend on what the server offers. A
    *trailer* is emitted after the final body chunk, the latest possible moment,
    so it captures the full digest even when a handler streams its response
    interleaved with reading the request: the general answer, and worth keeping the
    request streaming for. It needs the `http.response.trailers` extension, which
    the server advertises per connection, so this negotiates: `extension(...) is
    not None` picks the trailer.

    Without it we fall back to a response *header*, which must carry the complete
    digest at `ResponseStart`. Rather than trust the handler to read the whole
    request before responding, the fallback `absorb` drains the inbound stream to
    completion (hashing every chunk) *before yielding anything*, so the hasher is
    final before the handler runs and any `ResponseStart` it emits sees a complete
    digest. The cost is buffering the request, which the buffered routes pay anyway
    and which the HTTP inbound stream (finite) always terminates; the trade is that
    the header path cannot stream the request, which is exactly why the trailer
    path exists. Either way an empty-body route like `/modes` still gets a
    well-formed (empty-input) digest.
    """
    digest = hashlib.sha256()
    supports_trailers = extension(head.extensions, "http.response.trailers") is not None

    def tag() -> tuple[bytes, bytes]:
        return b"x-request-digest", b"sha-256=" + digest.hexdigest().encode()

    async def absorb(inputs: Stream[Inbound]) -> AsyncIterator[Inbound]:
        if supports_trailers:
            async for event in inputs:
                if isinstance(event, RequestBody):
                    digest.update(event.body)
                yield event
            return
        drained: list[Inbound] = []
        async for event in inputs:
            if isinstance(event, RequestBody):
                digest.update(event.body)
            drained.append(event)
        for event in drained:
            yield event

    async def stamp(inputs: Stream[Outbound]) -> AsyncIterator[Outbound]:
        async for event in inputs:
            if isinstance(event, ResponseStart) and supports_trailers:
                yield ResponseStart(status=event.status, headers=event.headers, trailers=True)
            elif isinstance(event, ResponseStart):
                yield ResponseStart(status=event.status, headers=(*event.headers, tag()))
            else:
                yield event
        if supports_trailers:
            yield ResponseTrailers(headers=(tag(),))

    return compose(absorb, compose(handler, stamp))


# The third kind of middleware: one that reads the *connection state*. `wrap`'s
# transformers see only the scope, and `access_timing`/`request_digest` carry their
# own local state, but a cross-cutting concern often needs the same config the
# handlers do (here the body-size limit, elsewhere an auth secret or a rate-limit
# budget). The `Middleware` vocabulary threads the lifespan `T` as the leading
# argument for exactly this: `advertise_limit` reads `settings.http.max_bytes` and
# surfaces it as a response header, without re-plumbing the config. Because the app
# snapshots `Settings` per connection, a reload changes the value the next
# connection advertises.
def advertise_limit(handler: HttpHandler, settings: Settings, _head: HttpScope) -> HttpHandler:
    """Stamp the configured request-body limit onto every response, read from state."""
    limit = str(settings.http.max_bytes).encode()

    async def processor(inputs: Stream[Inbound]) -> AsyncIterator[Outbound]:
        async for event in handler(inputs):
            if isinstance(event, ResponseStart):
                yield ResponseStart(status=event.status, headers=(*event.headers, (b"x-max-bytes", limit)))
            else:
                yield event

    return processor


async def log_connect(inputs: Stream[WebsocketInbound], head: WebsocketScope) -> AsyncIterator[WebsocketInbound]:
    print(f"=== WS {head.path}")
    async for event in inputs:
        yield event


socket_log: WebsocketMiddleware[object] = wrap(inbound=log_connect)


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
            # Reads the connection state to surface a config value; mixing it with
            # the state-ignoring middleware shows the vocabulary threads `Settings`
            # through the whole stack.
            advertise_limit,
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
