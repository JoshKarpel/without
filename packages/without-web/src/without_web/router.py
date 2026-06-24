from __future__ import annotations

from collections.abc import Callable
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field
from dataclasses import replace

from without import Stream
from without import stream
from without_asgi import HttpHandler
from without_asgi import HttpRouter
from without_asgi import HttpScope
from without_asgi import Inbound
from without_asgi import Outbound
from without_asgi import Response
from without_asgi import WebsocketHandler
from without_asgi import WebsocketScope
from without_asgi import encode_response
from without_asgi.routing import HttpMiddleware
from without_asgi.routing import WebsocketMiddleware
from without_asgi.routing import stack

from without_web.converters import DEFAULT_CONVERTERS
from without_web.converters import Converter
from without_web.exceptions import ExceptionHandler
from without_web.exceptions import WebsocketExceptionHandler
from without_web.exceptions import catching
from without_web.exceptions import catching_websocket
from without_web.patterns import CatchAll
from without_web.patterns import Literal
from without_web.patterns import Segment
from without_web.patterns import parse_pattern
from without_web.patterns import split_path
from without_web.trie import Node
from without_web.trie import build
from without_web.trie import walk


@dataclass(frozen=True, slots=True)
class Match[S]:
    """What the router hands a handler: the scope plus already-parsed path params.

    `make_asgi_app`'s `HttpRouter` type is unchanged: `Router.dispatch` still
    presents as `(T, HttpScope) -> HttpHandler`. The richer `Match` is the
    router's *internal* endpoint protocol, the one place a handler reads the
    path parameters the route pattern bound.
    """

    scope: S
    params: Mapping[str, object]


# An endpoint builds the connection handler from the lifespan state `T` and the
# `Match`. It is the role the bare `(state, scope)` callable played before, now
# given the parsed params alongside the scope.
type Endpoint[T, S, H] = Callable[[T, Match[S]], H]
type HttpEndpoint[T] = Endpoint[T, HttpScope, HttpHandler]
type WebsocketEndpoint[T] = Endpoint[T, WebsocketScope, WebsocketHandler]

# Standard HTTP methods, the only keys `route` accepts, so a typo is a build
# error rather than a route that silently never matches.
_METHODS = ("GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS")


@dataclass(frozen=True, slots=True)
class Route[T]:
    """A path pattern bound to one endpoint per HTTP method."""

    pattern: str
    methods: Mapping[str, HttpEndpoint[T]]


@dataclass(frozen=True, slots=True)
class Mount[T]:
    """A sub-application mounted at a prefix.

    `target` is either a `without-web` `Router` (whose routes are grafted into
    this router's trie, so matching and OpenAPI see straight through with the
    prefix prepended) or an opaque `HttpRouter` (a BYO router or another app),
    which is handed the prefix-trimmed scope and treated as a black box.
    """

    prefix: str
    target: Router[T] | HttpRouter[T]


def route[T](
    pattern: str,
    *,
    get: HttpEndpoint[T] | None = None,
    head: HttpEndpoint[T] | None = None,
    post: HttpEndpoint[T] | None = None,
    put: HttpEndpoint[T] | None = None,
    patch: HttpEndpoint[T] | None = None,
    delete: HttpEndpoint[T] | None = None,
    options: HttpEndpoint[T] | None = None,
) -> Route[T]:
    """Build a `Route`, one endpoint per method keyword."""
    methods = {
        name: endpoint
        for name, endpoint in (
            ("GET", get),
            ("HEAD", head),
            ("POST", post),
            ("PUT", put),
            ("PATCH", patch),
            ("DELETE", delete),
            ("OPTIONS", options),
        )
        if endpoint is not None
    }
    if not methods:
        raise ValueError(f"route {pattern!r} declares no methods")
    return Route(pattern, methods)


@dataclass(frozen=True, slots=True)
class _Methods[T]:
    """A terminal carrying a `Route`'s method map (a real endpoint, or a 405)."""

    methods: Mapping[str, HttpEndpoint[T]]


@dataclass(frozen=True, slots=True)
class _Delegate[T]:
    """A terminal that hands the prefix-trimmed scope to an opaque sub-router."""

    prefix: str
    target: HttpRouter[T]


type _HttpLeaf[T] = _Methods[T] | _Delegate[T]


def _flatten[T](routes: tuple[Route[T] | Mount[T], ...]) -> list[tuple[tuple[Segment, ...], _HttpLeaf[T]]]:
    table: list[tuple[tuple[Segment, ...], _HttpLeaf[T]]] = []
    for entry in routes:
        match entry:
            case Route(pattern, methods):
                table.append((parse_pattern(pattern), _Methods(methods)))
            case Mount(prefix, target):
                head = parse_pattern(prefix)
                if isinstance(target, Router):
                    table.extend((head + tail, leaf) for tail, leaf in _flatten(target.routes))
                    continue
                if not all(isinstance(segment, Literal) for segment in head):
                    raise ValueError(f"an opaque mount prefix must be literal segments, got {prefix!r}")
                leaf = _Delegate(prefix=prefix, target=target)
                # The exact prefix and any deeper path both delegate: the
                # catch-all matches the deeper case, the bare prefix the exact one.
                table.append((head, leaf))
                table.append(((*head, CatchAll("__mount__")), leaf))
    return table


def _method_not_allowed[T](methods: Mapping[str, HttpEndpoint[T]]) -> HttpHandler:
    allow = ", ".join(sorted(methods)).encode()
    response = Response(
        status=405,
        headers=((b"content-type", b"text/plain; charset=utf-8"), (b"allow", allow)),
        body=b"method not allowed\n",
    )

    def handler(inputs: Stream[Inbound]) -> Stream[Outbound]:
        return stream(encode_response(response))

    return handler


def _remount(scope: HttpScope, prefix: str) -> HttpScope:
    # ASGI root_path semantics: the sub-app sees the path with the mount prefix
    # stripped and the prefix folded into root_path.
    trimmed = scope.path[len(prefix) :] or "/"
    return replace(scope, path=trimmed, root_path=scope.root_path + prefix)


_PASSTHROUGH_HTTP: HttpMiddleware = stack()
_PASSTHROUGH_WEBSOCKET: WebsocketMiddleware = stack()


@dataclass(frozen=True, slots=True)
class Router[T]:
    """An opinionated HTTP router whose `dispatch` is an `HttpRouter[T]`.

    The whole integration surface with `without-asgi` is that one type: pass
    `router.dispatch` as `make_asgi_app(http=...)` and bring-your-own (or no
    router at all) stays first-class. The route table is compiled to an
    immutable trie once at construction; `dispatch` is then a pure walk that
    recovers route precedence, 405-vs-404, and mounting from the tree's shape.
    """

    routes: tuple[Route[T] | Mount[T], ...]
    fallback: HttpEndpoint[T]
    converters: Mapping[str, Converter] = DEFAULT_CONVERTERS
    middleware: HttpMiddleware = _PASSTHROUGH_HTTP
    exception_handlers: Mapping[type[Exception], ExceptionHandler] = field(default_factory=dict)
    tree: Node[_HttpLeaf[T]] = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "tree", build(_flatten(self.routes), self.converters))

    def dispatch(self, state: T, scope: HttpScope) -> HttpHandler:
        handler = self._resolve(state, scope)
        if self.exception_handlers:
            handler = catching(self.exception_handlers)(handler, scope)
        return self.middleware(handler, scope)

    def _resolve(self, state: T, scope: HttpScope) -> HttpHandler:
        found = walk(self.tree, split_path(scope.path), self.converters)
        if found is None:
            return self.fallback(state, Match(scope, {}))
        match found.leaf:
            case _Methods(methods):
                endpoint = methods.get(scope.method)
                if endpoint is None:
                    return _method_not_allowed(methods)
                return endpoint(state, Match(scope, found.params))
            case _Delegate(prefix, target):
                return target(state, _remount(scope, prefix))


@dataclass(frozen=True, slots=True)
class WebsocketRoute[T]:
    """A path pattern bound to one WebSocket endpoint."""

    pattern: str
    endpoint: WebsocketEndpoint[T]


def ws_route[T](pattern: str, endpoint: WebsocketEndpoint[T]) -> WebsocketRoute[T]:
    """Build a `WebsocketRoute`."""
    return WebsocketRoute(pattern, endpoint)


@dataclass(frozen=True, slots=True)
class WebsocketRouter[T]:
    """The WebSocket sibling of `Router`, reusing the same trie machinery.

    There is no method layer, so no 405: a connection either matches a path or
    falls to the `fallback`. `dispatch` is a `WebsocketRouter[T]` for
    `make_asgi_app(websocket=...)`.
    """

    routes: tuple[WebsocketRoute[T], ...]
    fallback: WebsocketEndpoint[T]
    converters: Mapping[str, Converter] = DEFAULT_CONVERTERS
    middleware: WebsocketMiddleware = _PASSTHROUGH_WEBSOCKET
    exception_handlers: Mapping[type[Exception], WebsocketExceptionHandler] = field(default_factory=dict)
    tree: Node[WebsocketEndpoint[T]] = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        table = [(parse_pattern(r.pattern), r.endpoint) for r in self.routes]
        object.__setattr__(self, "tree", build(table, self.converters))

    def dispatch(self, state: T, scope: WebsocketScope) -> WebsocketHandler:
        found = walk(self.tree, split_path(scope.path), self.converters)
        endpoint = self.fallback if found is None else found.leaf
        params = {} if found is None else found.params
        handler = endpoint(state, Match(scope, params))
        if self.exception_handlers:
            handler = catching_websocket(self.exception_handlers)(handler, scope)
        return self.middleware(handler, scope)
