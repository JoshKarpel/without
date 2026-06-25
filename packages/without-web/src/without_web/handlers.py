from __future__ import annotations

from collections.abc import AsyncIterator
from collections.abc import Callable
from collections.abc import Mapping
from dataclasses import dataclass
from typing import overload

from without import Stream
from without_asgi import HttpHandler
from without_asgi import HttpScope
from without_asgi import Inbound
from without_asgi import Outbound
from without_asgi import Response
from without_asgi import WebsocketHandler
from without_asgi import WebsocketScope
from without_asgi import encode_response
from without_asgi import read_body

from without_web.extractors import Extractor
from without_web.extractors import Request
from without_web.extractors import single_body
from without_web.openapi import ResponseSpec
from without_web.openapi import RouteSpec
from without_web.openapi import describe
from without_web.router import HttpEndpoint
from without_web.router import Match
from without_web.router import Pattern
from without_web.router import Route
from without_web.router import WebsocketRoute

# What a handler returns: either a single `Response` (the common buffered-output
# case) or an already-streaming `Stream[Outbound]`. `handle` buffers the *input*
# so extractors can read the body, but it does not force the *output* to be
# buffered: a handler that wants to stream its response yields `Outbound` events.
type Reply = Response | Stream[Outbound]


@overload
def handle[T](
    *, fn: Callable[[T], Reply], summary: str = ..., responses: Mapping[int, ResponseSpec] | None = ...
) -> HttpEndpoint[T]: ...
@overload
def handle[T, A](
    a: Extractor[A],
    /,
    *,
    fn: Callable[[T, A], Reply],
    summary: str = ...,
    responses: Mapping[int, ResponseSpec] | None = ...,
) -> HttpEndpoint[T]: ...
@overload
def handle[T, A, B](
    a: Extractor[A],
    b: Extractor[B],
    /,
    *,
    fn: Callable[[T, A, B], Reply],
    summary: str = ...,
    responses: Mapping[int, ResponseSpec] | None = ...,
) -> HttpEndpoint[T]: ...
@overload
def handle[T, A, B, C](
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    /,
    *,
    fn: Callable[[T, A, B, C], Reply],
    summary: str = ...,
    responses: Mapping[int, ResponseSpec] | None = ...,
) -> HttpEndpoint[T]: ...
@overload
def handle[T, A, B, C, D](
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    d: Extractor[D],
    /,
    *,
    fn: Callable[[T, A, B, C, D], Reply],
    summary: str = ...,
    responses: Mapping[int, ResponseSpec] | None = ...,
) -> HttpEndpoint[T]: ...
@overload
def handle[T, A, B, C, D, E](
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    d: Extractor[D],
    e: Extractor[E],
    /,
    *,
    fn: Callable[[T, A, B, C, D, E], Reply],
    summary: str = ...,
    responses: Mapping[int, ResponseSpec] | None = ...,
) -> HttpEndpoint[T]: ...
@overload
def handle[T, A, B, C, D, E, F](
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    d: Extractor[D],
    e: Extractor[E],
    f: Extractor[F],
    /,
    *,
    fn: Callable[[T, A, B, C, D, E, F], Reply],
    summary: str = ...,
    responses: Mapping[int, ResponseSpec] | None = ...,
) -> HttpEndpoint[T]: ...
@overload
def handle[T, A, B, C, D, E, F, G](
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    d: Extractor[D],
    e: Extractor[E],
    f: Extractor[F],
    g: Extractor[G],
    /,
    *,
    fn: Callable[[T, A, B, C, D, E, F, G], Reply],
    summary: str = ...,
    responses: Mapping[int, ResponseSpec] | None = ...,
) -> HttpEndpoint[T]: ...
@overload
def handle[T, A, B, C, D, E, F, G, H](
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    d: Extractor[D],
    e: Extractor[E],
    f: Extractor[F],
    g: Extractor[G],
    h: Extractor[H],
    /,
    *,
    fn: Callable[[T, A, B, C, D, E, F, G, H], Reply],
    summary: str = ...,
    responses: Mapping[int, ResponseSpec] | None = ...,
) -> HttpEndpoint[T]: ...
@overload
def handle[T, A, B, C, D, E, F, G, H, J](
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    d: Extractor[D],
    e: Extractor[E],
    f: Extractor[F],
    g: Extractor[G],
    h: Extractor[H],
    j: Extractor[J],
    /,
    *,
    fn: Callable[[T, A, B, C, D, E, F, G, H, J], Reply],
    summary: str = ...,
    responses: Mapping[int, ResponseSpec] | None = ...,
) -> HttpEndpoint[T]: ...
@overload
def handle[T, A, B, C, D, E, F, G, H, J, K](
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    d: Extractor[D],
    e: Extractor[E],
    f: Extractor[F],
    g: Extractor[G],
    h: Extractor[H],
    j: Extractor[J],
    k: Extractor[K],
    /,
    *,
    fn: Callable[[T, A, B, C, D, E, F, G, H, J, K], Reply],
    summary: str = ...,
    responses: Mapping[int, ResponseSpec] | None = ...,
) -> HttpEndpoint[T]: ...


def handle[T](
    *extractors: Extractor[object],
    fn: Callable[..., Reply],
    summary: str = "",
    responses: Mapping[int, ResponseSpec] | None = None,
) -> HttpEndpoint[T]:
    """Build a self-describing endpoint from typed extractors and a handler.

    Each extractor is a typed piece of the request; the overloads tie the
    extractors' types to `fn`'s parameters, so a `path_param(..., INT)` paired
    with an `fn` that expects a `str` is a mypy error, not a runtime surprise.

    At dispatch the *input* body is buffered once, a `Request` is built, every
    extractor runs (raising to reject, mapped by the router's exception
    handlers), and `fn` is called with the typed values. The *output* is not
    forced to buffer: `fn` returns either a `Response` (encoded into the outbound
    stream) or a `Stream[Outbound]` that is relayed as it produces. The endpoint
    also answers `describe()`, recovering its query/header/body OpenAPI from the
    very same extractors.

    `handle` is the lower-level builder; reach for the `@get`/`@post`/... method
    decorators to co-locate the route with its handler.
    """
    return _build_endpoint(extractors, fn, summary, responses)


@dataclass(frozen=True, slots=True)
class _Method:
    """A method-bound route decorator.

    `@get(pattern, *extractors)` annotates a handler with its route and returns a
    single-method `Route`, tying each extractor's type to the handler's
    parameters exactly as `handle` does. It *registers nothing*: the returned
    `Route` is passed to a `Router` explicitly, so route assembly stays a
    declarative value, not an import-time side effect. The `Router` merges
    `Route`s that share a pattern, so `@get` and `@post` on one path combine.
    """

    method: str

    @overload
    def __call__[T](
        self, pattern: Pattern, /, *, summary: str = ..., responses: Mapping[int, ResponseSpec] | None = ...
    ) -> Callable[[Callable[[T], Reply]], Route[T]]: ...
    @overload
    def __call__[T, A](
        self,
        pattern: Pattern,
        a: Extractor[A],
        /,
        *,
        summary: str = ...,
        responses: Mapping[int, ResponseSpec] | None = ...,
    ) -> Callable[[Callable[[T, A], Reply]], Route[T]]: ...
    @overload
    def __call__[T, A, B](
        self,
        pattern: Pattern,
        a: Extractor[A],
        b: Extractor[B],
        /,
        *,
        summary: str = ...,
        responses: Mapping[int, ResponseSpec] | None = ...,
    ) -> Callable[[Callable[[T, A, B], Reply]], Route[T]]: ...
    @overload
    def __call__[T, A, B, C](
        self,
        pattern: Pattern,
        a: Extractor[A],
        b: Extractor[B],
        c: Extractor[C],
        /,
        *,
        summary: str = ...,
        responses: Mapping[int, ResponseSpec] | None = ...,
    ) -> Callable[[Callable[[T, A, B, C], Reply]], Route[T]]: ...
    @overload
    def __call__[T, A, B, C, D](
        self,
        pattern: Pattern,
        a: Extractor[A],
        b: Extractor[B],
        c: Extractor[C],
        d: Extractor[D],
        /,
        *,
        summary: str = ...,
        responses: Mapping[int, ResponseSpec] | None = ...,
    ) -> Callable[[Callable[[T, A, B, C, D], Reply]], Route[T]]: ...
    @overload
    def __call__[T, A, B, C, D, E](
        self,
        pattern: Pattern,
        a: Extractor[A],
        b: Extractor[B],
        c: Extractor[C],
        d: Extractor[D],
        e: Extractor[E],
        /,
        *,
        summary: str = ...,
        responses: Mapping[int, ResponseSpec] | None = ...,
    ) -> Callable[[Callable[[T, A, B, C, D, E], Reply]], Route[T]]: ...
    @overload
    def __call__[T, A, B, C, D, E, F](
        self,
        pattern: Pattern,
        a: Extractor[A],
        b: Extractor[B],
        c: Extractor[C],
        d: Extractor[D],
        e: Extractor[E],
        f: Extractor[F],
        /,
        *,
        summary: str = ...,
        responses: Mapping[int, ResponseSpec] | None = ...,
    ) -> Callable[[Callable[[T, A, B, C, D, E, F], Reply]], Route[T]]: ...
    @overload
    def __call__[T, A, B, C, D, E, F, G](
        self,
        pattern: Pattern,
        a: Extractor[A],
        b: Extractor[B],
        c: Extractor[C],
        d: Extractor[D],
        e: Extractor[E],
        f: Extractor[F],
        g: Extractor[G],
        /,
        *,
        summary: str = ...,
        responses: Mapping[int, ResponseSpec] | None = ...,
    ) -> Callable[[Callable[[T, A, B, C, D, E, F, G], Reply]], Route[T]]: ...
    @overload
    def __call__[T, A, B, C, D, E, F, G, H](
        self,
        pattern: Pattern,
        a: Extractor[A],
        b: Extractor[B],
        c: Extractor[C],
        d: Extractor[D],
        e: Extractor[E],
        f: Extractor[F],
        g: Extractor[G],
        h: Extractor[H],
        /,
        *,
        summary: str = ...,
        responses: Mapping[int, ResponseSpec] | None = ...,
    ) -> Callable[[Callable[[T, A, B, C, D, E, F, G, H], Reply]], Route[T]]: ...
    @overload
    def __call__[T, A, B, C, D, E, F, G, H, J](
        self,
        pattern: Pattern,
        a: Extractor[A],
        b: Extractor[B],
        c: Extractor[C],
        d: Extractor[D],
        e: Extractor[E],
        f: Extractor[F],
        g: Extractor[G],
        h: Extractor[H],
        j: Extractor[J],
        /,
        *,
        summary: str = ...,
        responses: Mapping[int, ResponseSpec] | None = ...,
    ) -> Callable[[Callable[[T, A, B, C, D, E, F, G, H, J], Reply]], Route[T]]: ...
    @overload
    def __call__[T, A, B, C, D, E, F, G, H, J, K](
        self,
        pattern: Pattern,
        a: Extractor[A],
        b: Extractor[B],
        c: Extractor[C],
        d: Extractor[D],
        e: Extractor[E],
        f: Extractor[F],
        g: Extractor[G],
        h: Extractor[H],
        j: Extractor[J],
        k: Extractor[K],
        /,
        *,
        summary: str = ...,
        responses: Mapping[int, ResponseSpec] | None = ...,
    ) -> Callable[[Callable[[T, A, B, C, D, E, F, G, H, J, K], Reply]], Route[T]]: ...

    def __call__(
        self,
        pattern: Pattern,
        *extractors: Extractor[object],
        summary: str = "",
        responses: Mapping[int, ResponseSpec] | None = None,
    ) -> Callable[[Callable[..., Reply]], Route[object]]:
        def decorate(fn: Callable[..., Reply]) -> Route[object]:
            endpoint = _build_endpoint(extractors, fn, summary, responses)
            return Route(pattern=pattern, methods={self.method: endpoint})

        return decorate


get = _Method("GET")
head = _Method("HEAD")
post = _Method("POST")
put = _Method("PUT")
patch = _Method("PATCH")
delete = _Method("DELETE")
options = _Method("OPTIONS")


@overload
def ws[T](pattern: Pattern, /) -> Callable[[Callable[[T], WebsocketHandler]], WebsocketRoute[T]]: ...
@overload
def ws[T, A](
    pattern: Pattern, a: Extractor[A], /
) -> Callable[[Callable[[T, A], WebsocketHandler]], WebsocketRoute[T]]: ...
@overload
def ws[T, A, B](
    pattern: Pattern, a: Extractor[A], b: Extractor[B], /
) -> Callable[[Callable[[T, A, B], WebsocketHandler]], WebsocketRoute[T]]: ...
@overload
def ws[T, A, B, C](
    pattern: Pattern, a: Extractor[A], b: Extractor[B], c: Extractor[C], /
) -> Callable[[Callable[[T, A, B, C], WebsocketHandler]], WebsocketRoute[T]]: ...
@overload
def ws[T, A, B, C, D](
    pattern: Pattern, a: Extractor[A], b: Extractor[B], c: Extractor[C], d: Extractor[D], /
) -> Callable[[Callable[[T, A, B, C, D], WebsocketHandler]], WebsocketRoute[T]]: ...
@overload
def ws[T, A, B, C, D, E](
    pattern: Pattern, a: Extractor[A], b: Extractor[B], c: Extractor[C], d: Extractor[D], e: Extractor[E], /
) -> Callable[[Callable[[T, A, B, C, D, E], WebsocketHandler]], WebsocketRoute[T]]: ...
@overload
def ws[T, A, B, C, D, E, F](
    pattern: Pattern,
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    d: Extractor[D],
    e: Extractor[E],
    f: Extractor[F],
    /,
) -> Callable[[Callable[[T, A, B, C, D, E, F], WebsocketHandler]], WebsocketRoute[T]]: ...
@overload
def ws[T, A, B, C, D, E, F, G](
    pattern: Pattern,
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    d: Extractor[D],
    e: Extractor[E],
    f: Extractor[F],
    g: Extractor[G],
    /,
) -> Callable[[Callable[[T, A, B, C, D, E, F, G], WebsocketHandler]], WebsocketRoute[T]]: ...
@overload
def ws[T, A, B, C, D, E, F, G, H](
    pattern: Pattern,
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    d: Extractor[D],
    e: Extractor[E],
    f: Extractor[F],
    g: Extractor[G],
    h: Extractor[H],
    /,
) -> Callable[[Callable[[T, A, B, C, D, E, F, G, H], WebsocketHandler]], WebsocketRoute[T]]: ...
@overload
def ws[T, A, B, C, D, E, F, G, H, J](
    pattern: Pattern,
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    d: Extractor[D],
    e: Extractor[E],
    f: Extractor[F],
    g: Extractor[G],
    h: Extractor[H],
    j: Extractor[J],
    /,
) -> Callable[[Callable[[T, A, B, C, D, E, F, G, H, J], WebsocketHandler]], WebsocketRoute[T]]: ...
@overload
def ws[T, A, B, C, D, E, F, G, H, J, K](
    pattern: Pattern,
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    d: Extractor[D],
    e: Extractor[E],
    f: Extractor[F],
    g: Extractor[G],
    h: Extractor[H],
    j: Extractor[J],
    k: Extractor[K],
    /,
) -> Callable[[Callable[[T, A, B, C, D, E, F, G, H, J, K], WebsocketHandler]], WebsocketRoute[T]]: ...


def ws[T](
    pattern: Pattern, *extractors: Extractor[object]
) -> Callable[[Callable[..., WebsocketHandler]], WebsocketRoute[T]]:
    """The websocket sibling of `@get`/`@post`, tying extractors to a handler.

    `@ws(t"/feed/{room}", room, since)` co-locates the route with the handler and
    ties each extractor's type to its parameters, just like `@get`, but the
    handler returns a `WebsocketHandler` (the frame processor) and there is no
    body to buffer: `path_param`, `query_param`, and `header_param` read the
    handshake (a `body` extractor is rejected, since a handshake carries none).
    Returns a `WebsocketRoute` to pass to a `WebsocketRouter`.
    """
    if any(extractor.request_body is not None for extractor in extractors):
        raise ValueError("a websocket route cannot take a body extractor; a handshake has no body")

    def decorate(fn: Callable[..., WebsocketHandler]) -> WebsocketRoute[T]:
        def endpoint(state: T, match: Match[WebsocketScope]) -> WebsocketHandler:
            request = Request(scope=match.scope, params=match.params, body=b"")
            return fn(state, *(extractor.extract(request) for extractor in extractors))

        return WebsocketRoute(pattern=pattern, endpoint=endpoint)

    return decorate


def _build_endpoint(
    extractors: tuple[Extractor[object], ...],
    fn: Callable[..., Reply],
    summary: str,
    responses: Mapping[int, ResponseSpec] | None,
) -> HttpEndpoint[object]:
    spec = RouteSpec(
        summary=summary,
        query=tuple(param for extractor in extractors for param in extractor.query),
        headers=tuple(param for extractor in extractors for param in extractor.headers),
        request_body=single_body(extractors),
        responses=dict(responses or {}),
    )

    def endpoint(state: object, match: Match[HttpScope]) -> HttpHandler:
        def processor(inputs: Stream[Inbound]) -> Stream[Outbound]:
            return _reply(inputs, state, match, extractors, fn)

        return processor

    return describe(spec)(endpoint)


async def _reply(
    inputs: Stream[Inbound],
    state: object,
    match: Match[HttpScope],
    extractors: tuple[Extractor[object], ...],
    fn: Callable[..., Reply],
) -> AsyncIterator[Outbound]:
    body = await read_body(inputs)
    request = Request(scope=match.scope, params=match.params, body=body)
    result = fn(state, *(extractor.extract(request) for extractor in extractors))
    if isinstance(result, Response):
        for event in encode_response(result):
            yield event
        return
    async for event in result:
        yield event
