from __future__ import annotations

from collections.abc import AsyncIterator
from collections.abc import Callable
from dataclasses import dataclass

from without import Context
from without import Processor
from without import Stream
from without import sample
from without_asgi import ASGIApp
from without_asgi import ConnectionScope
from without_asgi import HttpScope
from without_asgi import Inbound
from without_asgi import Outbound
from without_asgi import Receive
from without_asgi import Response
from without_asgi import ResponseStart
from without_asgi import Send
from without_asgi import WebsocketScope
from without_asgi import encode_response
from without_asgi import http_inbound
from without_asgi import http_outbound
from without_asgi import make_asgi_app

from without_integration.flags.core import Flags
from without_integration.flags.core import bad_request
from without_integration.flags.core import flag_name
from without_integration.flags.core import render_all
from without_integration.flags.core import render_one
from without_integration.flags.core import route_not_found

# A handler is built per request from the connection facts and the live config
# Context, then run over the request's inbound event stream. This is the ASGI
# analogue of kv's per-connection `make_session`: routing picks one of these.
type Handler = Callable[[HttpScope, Context[Flags]], Processor[Inbound, Outbound]]


async def _respond(events: Stream[Inbound], make: Callable[[], Response]) -> AsyncIterator[Outbound]:
    async for _event in events:  # consume the whole request before replying
        pass
    for event in encode_response(make()):
        yield event


def _handler(make: Callable[[HttpScope, Context[Flags]], Response]) -> Handler:
    def build(head: HttpScope, flags: Context[Flags]) -> Processor[Inbound, Outbound]:
        def processor(inputs: Stream[Inbound]) -> Stream[Outbound]:
            # `make` reads `flags.current()` here, at request time, so each
            # request sees the latest reloaded config rather than a value
            # captured once at startup.
            return _respond(inputs, lambda: make(head, flags))

        return processor

    return build


def all_flags(head: HttpScope, flags: Context[Flags]) -> Response:
    return render_all(flags.current())


def one_flag(head: HttpScope, flags: Context[Flags]) -> Response:
    name = flag_name(head.query_string)
    if name is None:
        return bad_request("missing 'name' query parameter")
    return render_one(flags.current(), name)


def not_found(head: HttpScope, flags: Context[Flags]) -> Response:
    return route_not_found(head.method, head.path)


def with_header(name: bytes, value: bytes) -> Callable[[Handler], Handler]:
    """Middleware: append a header to every response, as plain processor composition."""

    def wrap(inner: Handler) -> Handler:
        def build(head: HttpScope, flags: Context[Flags]) -> Processor[Inbound, Outbound]:
            return _inject(inner(head, flags), name, value)

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


@dataclass(frozen=True, slots=True)
class Route:
    method: str
    path: str
    handler: Handler


@dataclass(frozen=True, slots=True)
class Router:
    routes: tuple[Route, ...]
    not_found: Handler

    def select(self, method: str, path: str) -> Handler:
        for route in self.routes:
            if route.method == method and route.path == path:
                return route.handler
        return self.not_found


def make_app(source: Stream[Flags], router: Router) -> ASGIApp:
    # The lifespan is just the config watch: `sample(source)` is already an async
    # context manager, so the same value would drive a non-ASGI shell unchanged.
    # `make_asgi_app` owns the protocol and the one shared reference; handlers are
    # handed the live `Context` per request and read `.current()` at request time.
    async def serve(flags: Context[Flags], scope: ConnectionScope, receive: Receive, send: Send) -> None:
        match scope:
            case HttpScope() as head:
                handler = router.select(head.method, head.path)
                processor = handler(head, flags)
                inbound = http_inbound(receive)
                outbound = processor(inbound)
                await http_outbound(send)(outbound)
            case WebsocketScope():
                raise NotImplementedError("websocket scopes are not supported")

    return make_asgi_app(lambda: sample(source), serve)


def feature_flags_app(source: Stream[Flags]) -> ASGIApp:
    """A read-only feature-flag service whose handlers read a live config `Context`."""
    router = Router(
        routes=(
            Route("GET", "/flags", with_header(b"x-flags-source", b"configmap")(_handler(all_flags))),
            Route("GET", "/flag", _handler(one_flag)),
        ),
        not_found=_handler(not_found),
    )
    return make_app(source, router)
