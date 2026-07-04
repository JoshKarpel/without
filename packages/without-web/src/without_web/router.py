from __future__ import annotations

from collections.abc import Callable
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field
from dataclasses import replace
from string.templatelib import Template
from typing import assert_never

from without import Stream
from without import stream_from_iterable
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
from without_asgi.routing import Middleware
from without_asgi.routing import WebsocketMiddleware
from without_asgi.routing import stack

from without_web.converters import PATH
from without_web.patterns import CatchAll
from without_web.patterns import Literal
from without_web.patterns import Param
from without_web.patterns import PathSpec
from without_web.patterns import Segment
from without_web.patterns import split_path
from without_web.trie import Node
from without_web.trie import build
from without_web.trie import walk


@dataclass(frozen=True, slots=True)
class Match[S]:
    """
    What the router hands a handler: the scope plus already-parsed path params.

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


# A pattern is either a plain string (a literal path, `@get("/todos")`) or a
# t-string interpolating path-param tokens (`t"/todos/{todo_id}"`, where
# `todo_id` is a `path_param(...)`/`catch_all(...)` value). The t-string carries
# the path *structure* for matching; the token's type is recovered separately
# from the handler's positional extractor list, since a `Template` erases its
# interpolation types. A plain string is taken verbatim, so parameters require the
# t-string form.
type Pattern = str | Template


def _segments(pattern: Pattern) -> tuple[Segment, ...]:
    if isinstance(pattern, str):
        return tuple(Literal(segment) for segment in split_path(pattern))
    return _template_segments(pattern)


def _template_segments(template: Template) -> tuple[Segment, ...]:
    # `Template.strings` interleaves with `.interpolations`
    # (strings[0], interp[0], strings[1], ...). A parameter must occupy a whole
    # segment, so the text around each interpolation must break on `/`.
    segments: list[Segment] = []
    interpolations = list(template.interpolations)
    pending = ""
    after_param = False
    for index, static in enumerate(template.strings):
        chunks = static.split("/")
        if after_param and chunks[0] != "":
            raise ValueError("a path parameter must occupy a whole segment")
        pending += chunks[0]
        for chunk in chunks[1:]:
            if pending:
                segments.append(Literal(pending))
            pending = chunk
        after_param = False
        if index < len(interpolations):
            if pending != "":
                raise ValueError("a path parameter must occupy a whole segment")
            segments.append(_token_segment(interpolations[index].value))
            after_param = True
    if pending:
        segments.append(Literal(pending))
    for segment in segments[:-1]:
        if isinstance(segment, CatchAll):
            raise ValueError("a catch-all parameter must be the last segment")
    return tuple(segments)


def _token_segment(value: object) -> Segment:
    spec = getattr(value, "path", None)
    if not isinstance(spec, PathSpec):
        raise ValueError("an interpolation in a route pattern must be a path_param(...) or catch_all(...)")
    return CatchAll(spec.name, spec.converter) if spec.catch_all else Param(spec.name, spec.converter)


@dataclass(frozen=True, slots=True)
class Route[T]:
    """
    A path pattern bound to one endpoint per HTTP method.

    The method-decorator form (`@get(...)`) produces a single-method `Route`; the
    `Router` merges `Route`s that share a pattern into one method map, so the
    405-vs-404 split still falls out of the trie.
    """

    pattern: Pattern
    methods: Mapping[str, HttpEndpoint[T]]


@dataclass(frozen=True, slots=True)
class Mount[T]:
    """
    A sub-application mounted at a literal-string prefix.

    `target` is either a `without-web` `Router` (whose routes are grafted into
    this router's trie, so matching and OpenAPI see straight through with the
    prefix prepended) or an opaque `HttpRouter` (a BYO router or another app),
    which is handed the prefix-trimmed scope and treated as a black box.

    The prefix is a plain `str`, hence a literal path: it cannot carry a path
    parameter, since that would need a `Template` (as route patterns use) and the
    type does not allow one here. The opaque-mount trim strips a fixed string
    (`root_path` semantics), so there is nowhere for a parameter to bind anyway.
    """

    prefix: str
    target: Router[T] | HttpRouter[T]


def route[T](
    pattern: Pattern,
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


def with_middleware[T, S, H](endpoint: Endpoint[T, S, H], *middleware: Middleware[T, H, S]) -> Endpoint[T, S, H]:
    """
    Scope middleware to one endpoint instead of the whole router.

    The router-wide `middleware` runs on every dispatch; this applies the same
    `Middleware` vocabulary to a single route (or an opaque mount target). An
    `Endpoint` builds the handler and a `Middleware` is `(handler, T, S) -> handler`,
    so this is just composition: build the handler, then run the middleware over it
    with the request's scope. First argument is outermost, matching `stack`. Use it per
    method, e.g. `route("/admin", get=with_middleware(list_admins, require_auth))`;
    for a whole mounted subtree, give the mounted `Router` its own `middleware`.
    """

    def built(state: T, match: Match[S]) -> H:
        return stack(*middleware)(endpoint(state, match), state, match.scope)

    return built


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


_PASSTHROUGH_HTTP: HttpMiddleware[object] = stack()
_PASSTHROUGH_WEBSOCKET: WebsocketMiddleware[object] = stack()


def _behind[T](leaf: _HttpLeaf[T], middleware: HttpMiddleware[T]) -> _HttpLeaf[T]:
    # Push a mounted sub-router's own `middleware` onto each of its grafted leaves,
    # so the subtree keeps its middleware once flattened into the parent trie. For a
    # method map every endpoint is wrapped; for an opaque delegate the middleware
    # wraps the handler the target builds from the remounted scope.
    match leaf:
        case _Methods(methods):
            return _Methods({method: with_middleware(endpoint, middleware) for method, endpoint in methods.items()})
        case _Delegate(prefix, target):

            def behind_target(state: T, scope: HttpScope) -> HttpHandler:
                return middleware(target(state, scope), state, scope)

            return _Delegate(prefix, behind_target)
        case _ as unreachable:
            assert_never(unreachable)


def _flatten[T](routes: tuple[Route[T] | Mount[T], ...]) -> list[tuple[tuple[Segment, ...], _HttpLeaf[T]]]:
    # Routes are merged by their parsed segments so that `@get` and `@post` on the
    # same path combine into one method map (the 405-vs-404 split needs them
    # together); mounts pass through. A method declared twice for one path is a
    # build-time fault.
    methods_by_path: dict[tuple[Segment, ...], dict[str, HttpEndpoint[T]]] = {}
    order: list[tuple[Segment, ...]] = []
    mounts: list[tuple[tuple[Segment, ...], _HttpLeaf[T]]] = []
    for entry in routes:
        match entry:
            case Route(pattern, methods):
                segments = _segments(pattern)
                if segments not in methods_by_path:
                    methods_by_path[segments] = {}
                    order.append(segments)
                bucket = methods_by_path[segments]
                for method, endpoint in methods.items():
                    if method in bucket:
                        raise ValueError(f"duplicate route: method {method} declared twice for one path")
                    bucket[method] = endpoint
            case Mount(prefix, target):
                head: tuple[Segment, ...] = tuple(Literal(segment) for segment in split_path(prefix))
                if isinstance(target, Router):
                    grafted = _flatten(target.routes)
                    if target.middleware is not _PASSTHROUGH_HTTP:
                        grafted = [(tail, _behind(leaf, target.middleware)) for tail, leaf in grafted]
                    mounts.extend((head + tail, leaf) for tail, leaf in grafted)
                    continue
                leaf: _HttpLeaf[T] = _Delegate(prefix=prefix, target=target)
                # The exact prefix and any deeper path both delegate: the
                # catch-all matches the deeper case, the bare prefix the exact one.
                mounts.append((head, leaf))
                mounts.append(((*head, CatchAll("__mount__", PATH)), leaf))
    routes_table: list[tuple[tuple[Segment, ...], _HttpLeaf[T]]] = [
        (segments, _Methods(methods_by_path[segments])) for segments in order
    ]
    return routes_table + mounts


def _method_not_allowed[T](methods: Mapping[str, HttpEndpoint[T]]) -> HttpHandler:
    allow = ", ".join(sorted(methods)).encode()
    response = Response(
        status=405,
        headers=((b"content-type", b"text/plain; charset=utf-8"), (b"allow", allow)),
        body=b"method not allowed\n",
    )

    def handler(inputs: Stream[Inbound]) -> Stream[Outbound]:
        return stream_from_iterable(encode_response(response))

    return handler


def _remount(scope: HttpScope, prefix: str) -> HttpScope:
    # ASGI root_path semantics: the sub-app sees the path with the mount prefix
    # stripped and the prefix folded into root_path.
    trimmed = scope.path[len(prefix) :] or "/"
    return replace(scope, path=trimmed, root_path=scope.root_path + prefix)


@dataclass(frozen=True, slots=True)
class Router[T]:
    """
    An opinionated HTTP router whose `dispatch` is an `HttpRouter[T]`.

    The whole integration surface with `without-asgi` is that one type: pass
    `router.dispatch` as `make_asgi_app(http=...)` and bring-your-own (or no
    router at all) stays first-class. The route table is compiled to an
    immutable trie once at construction; `dispatch` is then a pure walk that
    recovers route precedence, 405-vs-404, and mounting from the tree's shape.
    """

    routes: tuple[Route[T] | Mount[T], ...]
    fallback: HttpEndpoint[T]
    middleware: HttpMiddleware[T] = _PASSTHROUGH_HTTP
    tree: Node[_HttpLeaf[T]] = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "tree", build(_flatten(self.routes)))

    def dispatch(self, state: T, scope: HttpScope) -> HttpHandler:
        return self.middleware(self._resolve(state, scope), state, scope)

    def _resolve(self, state: T, scope: HttpScope) -> HttpHandler:
        found = walk(self.tree, split_path(scope.path))
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
            case _ as unreachable:
                assert_never(unreachable)


@dataclass(frozen=True, slots=True)
class WebsocketRoute[T]:
    """A path pattern bound to one WebSocket endpoint."""

    pattern: Pattern
    endpoint: WebsocketEndpoint[T]


def ws_route[T](pattern: Pattern, endpoint: WebsocketEndpoint[T]) -> WebsocketRoute[T]:
    """Build a `WebsocketRoute`."""
    return WebsocketRoute(pattern, endpoint)


@dataclass(frozen=True, slots=True)
class WebsocketRouter[T]:
    """
    The WebSocket sibling of `Router`, reusing the same trie machinery.

    There is no method layer, so no 405: a connection either matches a path or
    falls to the `fallback`. `dispatch` is a `WebsocketRouter[T]` for
    `make_asgi_app(websocket=...)`.
    """

    routes: tuple[WebsocketRoute[T], ...]
    fallback: WebsocketEndpoint[T]
    middleware: WebsocketMiddleware[T] = _PASSTHROUGH_WEBSOCKET
    tree: Node[WebsocketEndpoint[T]] = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        table = [(_segments(r.pattern), r.endpoint) for r in self.routes]
        object.__setattr__(self, "tree", build(table))

    def dispatch(self, state: T, scope: WebsocketScope) -> WebsocketHandler:
        found = walk(self.tree, split_path(scope.path))
        endpoint = self.fallback if found is None else found.leaf
        params = {} if found is None else found.params
        return self.middleware(endpoint(state, Match(scope, params)), state, scope)
