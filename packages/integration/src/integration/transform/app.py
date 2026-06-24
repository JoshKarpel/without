from __future__ import annotations

from collections.abc import AsyncIterator
from collections.abc import Callable
from dataclasses import dataclass

from without import Context
from without import Processor
from without import Stream
from without import sample
from without_asgi import ASGIApp
from without_asgi import HttpScope
from without_asgi import Inbound
from without_asgi import Outbound
from without_asgi import Response
from without_asgi import ResponseStart
from without_asgi import encode_response
from without_asgi import make_asgi_app
from without_asgi import read_body

from integration.transform.core import Settings
from integration.transform.core import mode_param
from integration.transform.core import render_modes
from integration.transform.core import route_not_found
from integration.transform.core import transform

# A handler is built per request from the connection facts and the live config
# Context, then run over the request's inbound event stream. This is the ASGI
# analogue of kv's per-connection `make_session`: routing picks one of these.
type Handler = Callable[[HttpScope, Context[Settings]], Processor[Inbound, Outbound]]

# Middleware wraps a handler in another handler, the same shape in and out, so a
# stack of them composes by plain function application. The router applies its
# stack to every route, so cross-cutting concerns live in one place.
type Middleware = Callable[[Handler], Handler]


async def _respond(events: Stream[Inbound], make: Callable[[bytes], Response]) -> AsyncIterator[Outbound]:
    body = await read_body(events)
    for event in encode_response(make(body)):
        yield event


def _handler(make: Callable[[HttpScope, Context[Settings], bytes], Response]) -> Handler:
    def build(head: HttpScope, settings: Context[Settings]) -> Processor[Inbound, Outbound]:
        def processor(inputs: Stream[Inbound]) -> Stream[Outbound]:
            # `make` reads `settings.current()` here, at request time, so each
            # request sees the latest reloaded config rather than a value
            # captured once at startup.
            return _respond(inputs, lambda body: make(head, settings, body))

        return processor

    return build


def transform_text(head: HttpScope, settings: Context[Settings], body: bytes) -> Response:
    return transform(settings.current(), mode_param(head.query_string), body)


def modes(head: HttpScope, settings: Context[Settings], body: bytes) -> Response:
    return render_modes(settings.current())


def not_found(head: HttpScope, settings: Context[Settings], body: bytes) -> Response:
    return route_not_found(head.method, head.path)


# A layer is the real middleware: it wraps the handler's `Processor` with the
# connection's `HttpScope` in hand. Because it sees the whole processor, one layer
# can transform the inbound stream (wrap `inputs` before handing it to `inner`),
# the outbound stream (wrap `inner`'s result), or both. `around` lifts a layer
# into router `Middleware`, supplying the `Handler -> Handler` plumbing that
# threads `head` and the config `Context` through, which every middleware would
# otherwise repeat.
type Layer = Callable[[Processor[Inbound, Outbound], HttpScope], Processor[Inbound, Outbound]]


def around(layer: Layer) -> Middleware:
    def wrap(inner: Handler) -> Handler:
        def build(head: HttpScope, settings: Context[Settings]) -> Processor[Inbound, Outbound]:
            return layer(inner(head, settings), head)

        return build

    return wrap


def with_header(name: bytes, value: bytes) -> Middleware:
    """Middleware that appends a header to every response."""
    return around(lambda inner, _head: _add_header(inner, name, value))


def _add_header(inner: Processor[Inbound, Outbound], name: bytes, value: bytes) -> Processor[Inbound, Outbound]:
    def processor(inputs: Stream[Inbound]) -> Stream[Outbound]:
        return _inject_header(inner(inputs), name, value)

    return processor


async def _inject_header(outputs: Stream[Outbound], name: bytes, value: bytes) -> AsyncIterator[Outbound]:
    async for event in outputs:
        if isinstance(event, ResponseStart):
            yield ResponseStart(status=event.status, headers=(*event.headers, (name, value)))
        else:
            yield event


def _log(inner: Processor[Inbound, Outbound], head: HttpScope) -> Processor[Inbound, Outbound]:
    def processor(inputs: Stream[Inbound]) -> Stream[Outbound]:
        # Wraps both ends: the inbound stream to log the request as it arrives,
        # the outbound stream to log the status the handler finally produced.
        return _log_response(inner(_log_request(inputs, head)), head)

    return processor


async def _log_request(inputs: Stream[Inbound], head: HttpScope) -> AsyncIterator[Inbound]:
    print(f"--> {head.method} {head.path}")
    async for event in inputs:
        yield event


async def _log_response(outputs: Stream[Outbound], head: HttpScope) -> AsyncIterator[Outbound]:
    async for event in outputs:
        if isinstance(event, ResponseStart):
            print(f"<-- {head.method} {head.path} {event.status}")
        yield event


# `access_log` needs no configuration, so it is a `Middleware` value directly,
# where `with_header` is a factory that builds one. A real service would emit
# structured log records in `_log_request`/`_log_response`; printing just shows
# where the seam is.
access_log: Middleware = around(_log)


@dataclass(frozen=True, slots=True)
class Route:
    method: str
    path: str
    handler: Handler


@dataclass(frozen=True, slots=True)
class Router:
    routes: tuple[Route, ...]
    not_found: Handler
    middleware: tuple[Middleware, ...] = ()

    def select(self, method: str, path: str) -> Handler:
        for route in self.routes:
            if route.method == method and route.path == path:
                return route.handler
        return self.not_found

    def route(self, settings: Context[Settings], head: HttpScope) -> Processor[Inbound, Outbound]:
        # The `HttpRouter` shape: handed the live `Context` and the parsed scope
        # per connection, it selects the handler that serves the connection, wraps
        # it in the middleware stack, then runs it (reading `.current()` at request
        # time). The stack wraps every route, `not_found` included, with the first
        # entry outermost.
        handler = self.select(head.method, head.path)
        for wrap in reversed(self.middleware):
            handler = wrap(handler)
        return handler(head, settings)


def text_transform_app(source: Stream[Settings]) -> ASGIApp:
    """A text-transform service whose handlers read the request body and a live config `Context`."""
    # The lifespan is just the config watch: `sample(source)` is already an async
    # context manager, so the same value would drive a non-ASGI shell unchanged.
    # `make_asgi_app` owns the protocol, the receive/send wiring, and the one
    # shared reference. The app is HTTP-only by passing no websocket router.
    router = Router(
        routes=(
            Route("POST", "/transform", _handler(transform_text)),
            Route("GET", "/modes", _handler(modes)),
        ),
        not_found=_handler(not_found),
        middleware=(
            # Outermost, so it sees the status every other layer finally produced.
            access_log,
            # https://www.gnuterrypratchett.com: keep his name moving on the overhead.
            with_header(b"x-clacks-overhead", b"GNU Terry Pratchett"),
        ),
    )
    return make_asgi_app(lambda: sample(source), router.route)
