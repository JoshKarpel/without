from __future__ import annotations

from collections.abc import Callable
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field
from dataclasses import replace
from string.templatelib import Template
from types import MappingProxyType
from typing import Protocol
from typing import assert_never
from typing import overload

from without import Stream
from without import stream_from_iterable
from without_asgi import HttpHandler
from without_asgi import HttpRouter
from without_asgi import HttpScope
from without_asgi import Inbound
from without_asgi import Outbound
from without_asgi import Response
from without_asgi import WebsocketHandler
from without_asgi import WebsocketRouter as WebsocketDispatch
from without_asgi import WebsocketScope
from without_asgi import encode_response
from without_asgi.routing import HttpMiddleware
from without_asgi.routing import Middleware
from without_asgi.routing import WebsocketMiddleware
from without_asgi.routing import stack

from without_web.converters import PATH
from without_web.converters import Converter
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
# t-string form. A pattern is parsed into `Segment`s once, when the route is built.
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
        pending += chunks[0]  # pragma: no mutate - pending is always empty here (a param owns a whole segment)
        for chunk in chunks[1:]:
            if pending:
                segments.append(Literal(pending))
            pending = chunk
        after_param = False  # pragma: no mutate - reset is overwritten below before the next iteration reads it
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


class Reversible(Protocol):
    """
    Anything reverse routing can render: a value carrying parsed path `segments`.

    Both `Route` and `WebsocketRoute` satisfy it structurally, so `url_for` takes
    either without naming the lifespan-state type they are generic over (which
    would otherwise fight variance).
    """

    @property
    def segments(self) -> tuple[Segment, ...]: ...


@dataclass(frozen=True, slots=True)
class Route[T]:
    """
    A route: parsed path `segments` bound to one endpoint per HTTP method.

    The method-decorator form (`@get(...)`) produces a single-method `Route`; the
    `Router` merges `Route`s that share a path into one method map, so the
    405-vs-404 split still falls out of the trie. `segments` is the *complete*
    path, mount prefixes already baked in (see `mount`), so a `Route` is a
    self-contained value: it reverses (`url_for`) with no router, and its meaning
    does not depend on where it is placed.
    """

    segments: tuple[Segment, ...]
    methods: Mapping[str, HttpEndpoint[T]]


@dataclass(frozen=True, slots=True)
class Delegate[T]:
    """
    An opaque HTTP sub-application delegated to at a literal-string prefix.

    The bring-your-own-app escape hatch: `target` (another `HttpRouter`, a legacy
    app) is handed the prefix-trimmed scope (ASGI `root_path` semantics) and
    treated as a black box, since its routes cannot be seen, baked, or reversed.
    Transparent sub-apps whose routes you own use `mount(...)` instead, which
    bakes the prefix into the routes so they stay first-class values. The prefix
    is a plain `str`, hence a literal path with no parameter to bind.
    """

    prefix: str
    target: HttpRouter[T]


def delegate[T](prefix: str, target: HttpRouter[T]) -> Delegate[T]:
    """Mount an opaque HTTP app at `prefix` (see `Delegate`)."""
    return Delegate(prefix, target)


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
    return Route(_segments(pattern), methods)


def with_middleware[T, S, H](endpoint: Endpoint[T, S, H], *middleware: Middleware[T, H, S]) -> Endpoint[T, S, H]:
    """
    Scope middleware to one endpoint instead of the whole router.

    The router-wide `middleware` runs on every dispatch; this applies the same
    `Middleware` vocabulary to a single route (or an opaque delegate target). An
    `Endpoint` builds the handler and a `Middleware` is `(handler, T, S) -> handler`,
    so this is just composition: build the handler, then run the middleware over it
    with the request's scope. First argument is outermost, matching `stack`. Use it per
    method, e.g. `route("/admin", get=with_middleware(list_admins, require_auth))`;
    for a whole prefix, hand the middleware to `mount(...)`.
    """

    def built(state: T, match: Match[S]) -> H:
        return stack(*middleware)(endpoint(state, match), state, match.scope)

    return built


def _join(outer: str, inner: str) -> str:
    return "/" + "/".join((*split_path(outer), *split_path(inner)))


def _behind_http[T](target: HttpRouter[T], middleware: tuple[HttpMiddleware[object], ...]) -> HttpRouter[T]:
    if not middleware:
        return target

    def behind(state: T, scope: HttpScope) -> HttpHandler:
        return stack(*middleware)(target(state, scope), state, scope)

    return behind


@dataclass(frozen=True, slots=True)
class _Mount:
    """
    The reusable transform `mount(...)` returns: rebase routes under a prefix.

    Holds the literal prefix and the per-route middleware; applying it prepends the
    prefix to each route's `segments` and wraps each endpoint with the middleware,
    returning plain (now fully-pathed) routes. One route in returns one out (so it
    reads as a decorator); several return a tuple. The state type is bound by the
    routes it is applied to, not the mount, so the middleware is state-agnostic
    (`HttpMiddleware[object]`), which the cross-cutting concerns a prefix carries
    (auth, logging) already are; state-specific middleware goes on a route directly
    with `with_middleware`.
    """

    prefix: str
    middleware: tuple[HttpMiddleware[object], ...]

    @overload
    def __call__[T](self, route: Route[T], /) -> Route[T]: ...
    @overload
    def __call__[T](self, route: Delegate[T], /) -> Delegate[T]: ...
    @overload
    def __call__[T](
        self, first: Route[T] | Delegate[T], second: Route[T] | Delegate[T], /, *rest: Route[T] | Delegate[T]
    ) -> tuple[Route[T] | Delegate[T], ...]: ...
    def __call__[T](
        self, *routes: Route[T] | Delegate[T]
    ) -> Route[T] | Delegate[T] | tuple[Route[T] | Delegate[T], ...]:
        rebased = tuple(self._rebase(entry) for entry in routes)
        return rebased[0] if len(rebased) == 1 else rebased

    def _rebase[T](self, entry: Route[T] | Delegate[T]) -> Route[T] | Delegate[T]:
        head = tuple(Literal(segment) for segment in split_path(self.prefix))
        match entry:
            case Route(segments, methods):
                behind = {method: with_middleware(endpoint, *self.middleware) for method, endpoint in methods.items()}
                return Route(head + segments, behind)
            case Delegate(prefix, target):
                return Delegate(_join(self.prefix, prefix), _behind_http(target, self.middleware))
            case _ as unreachable:
                assert_never(unreachable)


def mount(prefix: str, *middleware: HttpMiddleware[object]) -> _Mount:
    """
    Bake a literal `prefix` (and optional per-route `middleware`) into HTTP routes.

    Returns a transform that rebases each `Route`/`Delegate` you hand it: the prefix
    is prepended to the path and the middleware wrapped onto each endpoint. The
    result is a plain route whose *segments already include the prefix*, so there is
    no `Mount` wrapper in the router, matching and OpenAPI see the full path, and
    reverse routing (`url_for`) needs no router. Store and reuse it, apply it to
    many routes at once, or use it as a decorator on one:

    ```python
    api = mount("/api", require_auth)        # a reusable mount point
    routes = api(list_users, create_user)    # -> a tuple of rebased routes

    @mount("/api")                           # or as a decorator on one route
    @get(t"/users/{uid}", uid)
    async def show_user(...): ...
    ```

    Nesting composes (`mount("/api")(mount("/v1")(r))` -> `/api/v1/...`). For a
    sub-app whose routes you cannot see, use `delegate(...)` instead: an opaque app
    cannot have a prefix baked in, so it stays a black box handed the trimmed scope.
    """
    return _Mount(prefix, middleware)


@dataclass(frozen=True, slots=True)
class _Methods[T]:
    """A terminal carrying a `Route`'s method map (a real endpoint, or a 405)."""

    methods: Mapping[str, HttpEndpoint[T]]


type _HttpLeaf[T] = _Methods[T] | Delegate[T]


_PASSTHROUGH_HTTP: HttpMiddleware[object] = stack()
_PASSTHROUGH_WEBSOCKET: WebsocketMiddleware[object] = stack()


def _flatten[T](routes: tuple[Route[T] | Delegate[T], ...]) -> list[tuple[tuple[Segment, ...], _HttpLeaf[T]]]:
    # Routes are merged by their (already-mounted) segments so that `@get` and
    # `@post` on the same path combine into one method map (the 405-vs-404 split
    # needs them together). A `Delegate` becomes a leaf at its prefix and a
    # catch-all just below it. A method declared twice for one path is a build fault.
    methods_by_path: dict[tuple[Segment, ...], dict[str, HttpEndpoint[T]]] = {}
    order: list[tuple[Segment, ...]] = []
    delegates: list[tuple[tuple[Segment, ...], _HttpLeaf[T]]] = []
    for entry in routes:
        match entry:
            case Route(segments, methods):
                if segments not in methods_by_path:
                    methods_by_path[segments] = {}
                    order.append(segments)
                bucket = methods_by_path[segments]
                for method, endpoint in methods.items():
                    if method in bucket:
                        raise ValueError(f"duplicate route: method {method} declared twice for one path")
                    bucket[method] = endpoint
            case Delegate(prefix, _target) as leaf:
                head: tuple[Segment, ...] = tuple(Literal(segment) for segment in split_path(prefix))
                # The exact prefix and any deeper path both delegate: the catch-all
                # matches the deeper case, the bare prefix the exact one.
                delegates.append((head, leaf))
                delegates.append(((*head, CatchAll("__mount__", PATH)), leaf))  # pragma: no mutate - name unread
            case _ as unreachable:
                assert_never(unreachable)
    routes_table: list[tuple[tuple[Segment, ...], _HttpLeaf[T]]] = [
        (segments, _Methods(methods_by_path[segments])) for segments in order
    ]
    return routes_table + delegates


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


def _strip_mount(path: str, prefix: str) -> str:
    # The sub-path an app mounted at `prefix` sees: `path` with the matched prefix
    # segments removed. Trimming by *segment count* rather than raw string length keeps
    # this consistent with the segment-wise, slash-insensitive matching: a request like
    # `//legacy/foo` matches `delegate("/legacy")`, and a byte-length slice would leave a
    # corrupted `y/foo` rather than the correct `/foo`.
    remainder = split_path(path)[len(split_path(prefix)) :]
    return "/" + "/".join(remainder) if remainder else "/"


def _remount(scope: HttpScope, prefix: str) -> HttpScope:
    # ASGI root_path semantics: the sub-app sees the path with the mount prefix
    # stripped and the prefix folded into root_path.
    return replace(scope, path=_strip_mount(scope.path, prefix), root_path=scope.root_path + prefix)


_NO_VALUES: Mapping[str, object] = MappingProxyType({})


def url_for(route: Reversible, values: Mapping[str, object] = _NO_VALUES) -> str:
    """
    Reverse a route to a concrete path: the inverse of the trie walk, as a pure function.

    `url_for(route, values)` fills the route's path parameters from `values` and
    renders the path it would match at, so a handler or template links to a route by
    its *value* rather than hand-assembling a string that drifts when the path
    changes. Because `mount` bakes any prefix into the route, the route's `segments`
    are its full path: reversing needs no router, holds no hidden prefix for another
    router to be ignorant of, and works the same whether the route came from
    `@get`, `mount(...)`, or a third-party package. A handler links by referencing
    the route *value* (immutable), never the assembled router.

    Each value is rendered and fed back through its segment's converter to prove it
    would parse straight back (parse, don't validate, in reverse): a value the
    converter would reject, one that does not round-trip, or a single-segment value
    containing `/` raises, as does a missing or unknown parameter. A `catch_all`
    segment is the one place `/` is allowed.
    """
    return _render_path(route.segments, values)


def _render_path(segments: tuple[Segment, ...], values: Mapping[str, object]) -> str:
    expected = frozenset(segment.name for segment in segments if isinstance(segment, Param | CatchAll))
    supplied = frozenset(values)
    if missing := expected - supplied:
        raise ValueError(f"url_for is missing values for path parameter(s): {', '.join(sorted(missing))}")
    if unexpected := supplied - expected:
        raise ValueError(f"url_for got values for unknown path parameter(s): {', '.join(sorted(unexpected))}")
    return "/" + "/".join(_render_segment(segment, values) for segment in segments)


def _render_segment(segment: Segment, values: Mapping[str, object]) -> str:
    match segment:
        case Literal(text):
            return text
        case Param(name, converter):
            return _render_value(name, converter, values[name], multi_segment=False)
        case CatchAll(name, converter):
            return _render_value(name, converter, values[name], multi_segment=True)
        case _ as unreachable:
            assert_never(unreachable)


def _render_value(name: str, converter: Converter[object], value: object, *, multi_segment: bool) -> str:
    # The inverse of the trie walk's `converter.parse`: render the value and prove
    # the converter would parse it straight back, so a generated path routes to the
    # route it came from (parse, don't validate, in reverse). A single-segment
    # param may not contain `/`, which would silently span segments.
    rendered = str(value)
    if not multi_segment and "/" in rendered:
        raise ValueError(f"value {value!r} for path parameter {name!r} spans multiple path segments")
    try:
        reparsed = converter.parse(rendered)
    except ValueError as error:
        raise ValueError(f"value {value!r} for path parameter {name!r} is not a valid {converter.name}") from error
    if reparsed != value:
        raise ValueError(
            f"value {value!r} for path parameter {name!r} does not round-trip through the {converter.name} converter"
        )
    return rendered


@dataclass(frozen=True, slots=True)
class Router[T]:
    """
    An opinionated HTTP router whose `dispatch` is an `HttpRouter[T]`.

    The whole integration surface with `without-asgi` is that one type: pass
    `router.dispatch` as `make_asgi_app(http=...)` and bring-your-own (or no
    router at all) stays first-class. The route table is compiled to an
    immutable trie once at construction; `dispatch` is then a pure walk that
    recovers route precedence, 405-vs-404, and delegation from the tree's shape.
    Routes are flat, self-contained values (mount prefixes are baked in by
    `mount`); only opaque `Delegate`s stay as wrappers, since a black box cannot
    be flattened.
    """

    routes: tuple[Route[T] | Delegate[T], ...]
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
            case Delegate(prefix, target):
                return target(state, _remount(scope, prefix))
            case _ as unreachable:
                assert_never(unreachable)


@dataclass(frozen=True, slots=True)
class WebsocketRoute[T]:
    """Parsed path `segments` bound to one WebSocket endpoint (the sibling of `Route`)."""

    segments: tuple[Segment, ...]
    endpoint: WebsocketEndpoint[T]


@dataclass(frozen=True, slots=True)
class WebsocketDelegate[T]:
    """
    The WebSocket sibling of `Delegate`: an opaque WebSocket app at a prefix.

    Handed the prefix-trimmed scope and treated as a black box; transparent
    WebSocket sub-apps use `ws_mount(...)` to bake the prefix into their routes.
    """

    prefix: str
    target: WebsocketDispatch[T]


def ws_route[T](pattern: Pattern, endpoint: WebsocketEndpoint[T]) -> WebsocketRoute[T]:
    """Build a `WebsocketRoute`."""
    return WebsocketRoute(_segments(pattern), endpoint)


def ws_delegate[T](prefix: str, target: WebsocketDispatch[T]) -> WebsocketDelegate[T]:
    """Mount an opaque WebSocket app at `prefix` (see `WebsocketDelegate`)."""
    return WebsocketDelegate(prefix, target)


@dataclass(frozen=True, slots=True)
class _WsMount:
    """The transform `ws_mount(...)` returns: rebase WebSocket routes under a prefix."""

    prefix: str
    middleware: tuple[WebsocketMiddleware[object], ...]

    @overload
    def __call__[T](self, route: WebsocketRoute[T], /) -> WebsocketRoute[T]: ...
    @overload
    def __call__[T](self, route: WebsocketDelegate[T], /) -> WebsocketDelegate[T]: ...
    @overload
    def __call__[T](
        self,
        first: WebsocketRoute[T] | WebsocketDelegate[T],
        second: WebsocketRoute[T] | WebsocketDelegate[T],
        /,
        *rest: WebsocketRoute[T] | WebsocketDelegate[T],
    ) -> tuple[WebsocketRoute[T] | WebsocketDelegate[T], ...]: ...
    def __call__[T](
        self, *routes: WebsocketRoute[T] | WebsocketDelegate[T]
    ) -> WebsocketRoute[T] | WebsocketDelegate[T] | tuple[WebsocketRoute[T] | WebsocketDelegate[T], ...]:
        rebased = tuple(self._rebase(entry) for entry in routes)
        return rebased[0] if len(rebased) == 1 else rebased

    def _rebase[T](self, entry: WebsocketRoute[T] | WebsocketDelegate[T]) -> WebsocketRoute[T] | WebsocketDelegate[T]:
        head = tuple(Literal(segment) for segment in split_path(self.prefix))
        match entry:
            case WebsocketRoute(segments, endpoint):
                return WebsocketRoute(head + segments, with_middleware(endpoint, *self.middleware))
            case WebsocketDelegate(prefix, target):
                return WebsocketDelegate(_join(self.prefix, prefix), _behind_ws(target, self.middleware))
            case _ as unreachable:
                assert_never(unreachable)


def ws_mount(prefix: str, *middleware: WebsocketMiddleware[object]) -> _WsMount:
    """The WebSocket sibling of `mount`: bake a prefix (and middleware) into WebSocket routes."""
    return _WsMount(prefix, middleware)


def _behind_ws[T](
    target: WebsocketDispatch[T], middleware: tuple[WebsocketMiddleware[object], ...]
) -> WebsocketDispatch[T]:
    if not middleware:
        return target

    def behind(state: T, scope: WebsocketScope) -> WebsocketHandler:
        return stack(*middleware)(target(state, scope), state, scope)

    return behind


@dataclass(frozen=True, slots=True)
class _Single[T]:
    """A terminal carrying one WebSocket endpoint (the sibling of `_Methods`)."""

    endpoint: WebsocketEndpoint[T]


type _WsLeaf[T] = _Single[T] | WebsocketDelegate[T]


def _flatten_ws[T](
    routes: tuple[WebsocketRoute[T] | WebsocketDelegate[T], ...],
) -> list[tuple[tuple[Segment, ...], _WsLeaf[T]]]:
    # The websocket sibling of `_flatten`: each route is a single-endpoint leaf, each
    # delegate a black box at its prefix plus a catch-all. There is no method map to
    # merge, so two endpoints at one path collide in `build` rather than here.
    table: list[tuple[tuple[Segment, ...], _WsLeaf[T]]] = []
    for entry in routes:
        match entry:
            case WebsocketRoute(segments, endpoint):
                table.append((segments, _Single(endpoint)))
            case WebsocketDelegate(prefix, _target) as leaf:
                head: tuple[Segment, ...] = tuple(Literal(segment) for segment in split_path(prefix))
                table.append((head, leaf))
                table.append(((*head, CatchAll("__mount__", PATH)), leaf))  # pragma: no mutate - name unread
            case _ as unreachable:
                assert_never(unreachable)
    return table


def _remount_ws(scope: WebsocketScope, prefix: str) -> WebsocketScope:
    # The websocket sibling of `_remount`: the sub-app sees the path with the mount
    # prefix stripped (by segment count, see `_strip_mount`) and the prefix folded
    # into root_path.
    return replace(scope, path=_strip_mount(scope.path, prefix), root_path=scope.root_path + prefix)


@dataclass(frozen=True, slots=True)
class WebsocketRouter[T]:
    """
    The WebSocket sibling of `Router`, reusing the same trie machinery.

    There is no method layer, so no 405: a connection either matches a path or
    falls to the `fallback`. `dispatch` is a `WebsocketRouter[T]` for
    `make_asgi_app(websocket=...)`. Routes are flat values (prefixes baked by
    `ws_mount`); only opaque `WebsocketDelegate`s stay as wrappers.
    """

    routes: tuple[WebsocketRoute[T] | WebsocketDelegate[T], ...]
    fallback: WebsocketEndpoint[T]
    middleware: WebsocketMiddleware[T] = _PASSTHROUGH_WEBSOCKET
    tree: Node[_WsLeaf[T]] = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "tree", build(_flatten_ws(self.routes)))

    def dispatch(self, state: T, scope: WebsocketScope) -> WebsocketHandler:
        return self.middleware(self._resolve(state, scope), state, scope)

    def _resolve(self, state: T, scope: WebsocketScope) -> WebsocketHandler:
        found = walk(self.tree, split_path(scope.path))
        if found is None:
            return self.fallback(state, Match(scope, {}))
        match found.leaf:
            case _Single(endpoint):
                return endpoint(state, Match(scope, found.params))
            case WebsocketDelegate(prefix, target):
                return target(state, _remount_ws(scope, prefix))
            case _ as unreachable:
                assert_never(unreachable)
