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


def with_header(name: bytes, value: bytes) -> Middleware:
    """Middleware that appends a header to every response, as plain processor composition."""

    def wrap(inner: Handler) -> Handler:
        def build(head: HttpScope, settings: Context[Settings]) -> Processor[Inbound, Outbound]:
            return _inject(inner(head, settings), name, value)

        return build

    return wrap


def _inject(inner: Processor[Inbound, Outbound], name: bytes, value: bytes) -> Processor[Inbound, Outbound]:
    def processor(inputs: Stream[Inbound]) -> Stream[Outbound]:
        return _inject_header(inner(inputs), name, value)

    return processor


async def _inject_header(outputs: Stream[Outbound], name: bytes, value: bytes) -> AsyncIterator[Outbound]:
    async for event in outputs:
        if isinstance(event, ResponseStart):
            yield ResponseStart(status=event.status, headers=(*event.headers, (name, value)))
        else:
            yield event


def access_log(inner: Handler) -> Handler:
    """Middleware that prints one access line per response, observing (not changing) the stream.

    Takes no configuration, so it *is* a `Middleware` rather than a factory like
    `with_header`. A real service would emit a structured log record here; this
    just prints to show where the seam goes.
    """

    def build(head: HttpScope, settings: Context[Settings]) -> Processor[Inbound, Outbound]:
        return _log(inner(head, settings), head)

    return build


def _log(inner: Processor[Inbound, Outbound], head: HttpScope) -> Processor[Inbound, Outbound]:
    def processor(inputs: Stream[Inbound]) -> Stream[Outbound]:
        return _log_status(inner(inputs), head)

    return processor


async def _log_status(outputs: Stream[Outbound], head: HttpScope) -> AsyncIterator[Outbound]:
    async for event in outputs:
        if isinstance(event, ResponseStart):
            print(f"{head.method} {head.path} -> {event.status}")
        yield event


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
