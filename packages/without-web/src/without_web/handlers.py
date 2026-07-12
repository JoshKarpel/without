from __future__ import annotations

from collections.abc import AsyncIterator
from collections.abc import Awaitable
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
from without_asgi import WebsocketInbound
from without_asgi import WebsocketOutbound
from without_asgi import WebsocketScope
from without_asgi import encode_response
from without_asgi import read_body

from without_web.extractors import BufferedRequest
from without_web.extractors import ExtractionError
from without_web.extractors import Extractor
from without_web.extractors import HttpRequestHead
from without_web.extractors import WebsocketRequestHead
from without_web.extractors import single_body
from without_web.openapi import Body
from without_web.openapi import ResponseSpec
from without_web.openapi import RouteSpec
from without_web.openapi import describe
from without_web.router import HttpEndpoint
from without_web.router import Match
from without_web.router import Pattern
from without_web.router import Route
from without_web.router import WebsocketRoute
from without_web.router import _segments

# What a handler ultimately produces: a single `Response` (buffered output) or an
# already-streaming `Stream[Outbound]`. Input buffering is the one build-time axis
# (the `@post` vs `@post.stream` split); the output is always just what the
# handler hands back, relayed through `_emit`.
type Reply = Response | Stream[Outbound]

# What a handler may *return*. Handlers are always `async`, so the color is fixed:
# a buffered handler is an `async def` resolving to a `Reply` (an
# `Awaitable[Reply]`), and a streaming handler is an `async def ... yield`, already
# an `AsyncIterator[Outbound]` (a `Stream[Outbound]`). There is no synchronous arm,
# because a handler must be able to `await` I/O; a plain `def ... return Response`
# is a type error. `_emit` collapses both shapes into the outbound stream, keeping
# the output mode free of the input mode.
type Returned = Awaitable[Reply] | Stream[Outbound]

# What a websocket handler returns: a live stream of outbound frames. The
# websocket analog of `Returned`, but with no buffered arm (a connection is
# inherently a stream, never a single value), so it is simply
# `Stream[WebsocketOutbound]`. Naming it also keeps the generated `ws` ladder free
# of a triple close-bracket, which would collide with cog's end-of-generator
# marker, the same reason the HTTP ladders return the bare `Returned` alias rather
# than a raw stream type.
type WebsocketReturned = Stream[WebsocketOutbound]


# [[[cog import cog; from ladders import emit; cog.outl(emit("handle")) ]]]
@overload
def handle[T](
    *,
    fn: Callable[[T], Returned],
    summary: str = ...,
    responses: Mapping[int, ResponseSpec] | None = ...,
) -> HttpEndpoint[T]: ...


@overload
def handle[T, A](
    a: Extractor[BufferedRequest, A],
    /,
    *,
    fn: Callable[[T, A], Returned],
    summary: str = ...,
    responses: Mapping[int, ResponseSpec] | None = ...,
) -> HttpEndpoint[T]: ...


@overload
def handle[T, A, B](
    a: Extractor[BufferedRequest, A],
    b: Extractor[BufferedRequest, B],
    /,
    *,
    fn: Callable[[T, A, B], Returned],
    summary: str = ...,
    responses: Mapping[int, ResponseSpec] | None = ...,
) -> HttpEndpoint[T]: ...


@overload
def handle[T, A, B, C](
    a: Extractor[BufferedRequest, A],
    b: Extractor[BufferedRequest, B],
    c: Extractor[BufferedRequest, C],
    /,
    *,
    fn: Callable[[T, A, B, C], Returned],
    summary: str = ...,
    responses: Mapping[int, ResponseSpec] | None = ...,
) -> HttpEndpoint[T]: ...


@overload
def handle[T, A, B, C, D](
    a: Extractor[BufferedRequest, A],
    b: Extractor[BufferedRequest, B],
    c: Extractor[BufferedRequest, C],
    d: Extractor[BufferedRequest, D],
    /,
    *,
    fn: Callable[[T, A, B, C, D], Returned],
    summary: str = ...,
    responses: Mapping[int, ResponseSpec] | None = ...,
) -> HttpEndpoint[T]: ...


@overload
def handle[T, A, B, C, D, E](
    a: Extractor[BufferedRequest, A],
    b: Extractor[BufferedRequest, B],
    c: Extractor[BufferedRequest, C],
    d: Extractor[BufferedRequest, D],
    e: Extractor[BufferedRequest, E],
    /,
    *,
    fn: Callable[[T, A, B, C, D, E], Returned],
    summary: str = ...,
    responses: Mapping[int, ResponseSpec] | None = ...,
) -> HttpEndpoint[T]: ...


@overload
def handle[T, A, B, C, D, E, F](
    a: Extractor[BufferedRequest, A],
    b: Extractor[BufferedRequest, B],
    c: Extractor[BufferedRequest, C],
    d: Extractor[BufferedRequest, D],
    e: Extractor[BufferedRequest, E],
    f: Extractor[BufferedRequest, F],
    /,
    *,
    fn: Callable[[T, A, B, C, D, E, F], Returned],
    summary: str = ...,
    responses: Mapping[int, ResponseSpec] | None = ...,
) -> HttpEndpoint[T]: ...


@overload
def handle[T, A, B, C, D, E, F, G](
    a: Extractor[BufferedRequest, A],
    b: Extractor[BufferedRequest, B],
    c: Extractor[BufferedRequest, C],
    d: Extractor[BufferedRequest, D],
    e: Extractor[BufferedRequest, E],
    f: Extractor[BufferedRequest, F],
    g: Extractor[BufferedRequest, G],
    /,
    *,
    fn: Callable[[T, A, B, C, D, E, F, G], Returned],
    summary: str = ...,
    responses: Mapping[int, ResponseSpec] | None = ...,
) -> HttpEndpoint[T]: ...


@overload
def handle[T, A, B, C, D, E, F, G, H](
    a: Extractor[BufferedRequest, A],
    b: Extractor[BufferedRequest, B],
    c: Extractor[BufferedRequest, C],
    d: Extractor[BufferedRequest, D],
    e: Extractor[BufferedRequest, E],
    f: Extractor[BufferedRequest, F],
    g: Extractor[BufferedRequest, G],
    h: Extractor[BufferedRequest, H],
    /,
    *,
    fn: Callable[[T, A, B, C, D, E, F, G, H], Returned],
    summary: str = ...,
    responses: Mapping[int, ResponseSpec] | None = ...,
) -> HttpEndpoint[T]: ...


@overload
def handle[T, A, B, C, D, E, F, G, H, J](
    a: Extractor[BufferedRequest, A],
    b: Extractor[BufferedRequest, B],
    c: Extractor[BufferedRequest, C],
    d: Extractor[BufferedRequest, D],
    e: Extractor[BufferedRequest, E],
    f: Extractor[BufferedRequest, F],
    g: Extractor[BufferedRequest, G],
    h: Extractor[BufferedRequest, H],
    j: Extractor[BufferedRequest, J],
    /,
    *,
    fn: Callable[[T, A, B, C, D, E, F, G, H, J], Returned],
    summary: str = ...,
    responses: Mapping[int, ResponseSpec] | None = ...,
) -> HttpEndpoint[T]: ...


@overload
def handle[T, A, B, C, D, E, F, G, H, J, K](
    a: Extractor[BufferedRequest, A],
    b: Extractor[BufferedRequest, B],
    c: Extractor[BufferedRequest, C],
    d: Extractor[BufferedRequest, D],
    e: Extractor[BufferedRequest, E],
    f: Extractor[BufferedRequest, F],
    g: Extractor[BufferedRequest, G],
    h: Extractor[BufferedRequest, H],
    j: Extractor[BufferedRequest, J],
    k: Extractor[BufferedRequest, K],
    /,
    *,
    fn: Callable[[T, A, B, C, D, E, F, G, H, J, K], Returned],
    summary: str = ...,
    responses: Mapping[int, ResponseSpec] | None = ...,
) -> HttpEndpoint[T]: ...
# [[[end]]]
def handle[T](
    *extractors: Extractor[BufferedRequest, object],
    fn: Callable[..., Returned],
    summary: str = "",
    responses: Mapping[int, ResponseSpec] | None = None,
) -> HttpEndpoint[T]:
    """
    Build a self-describing endpoint from typed extractors and a handler.

    Each extractor is a typed piece of the request; the overloads tie the
    extractors' types to `fn`'s parameters, so a `path_param(..., INT)` paired
    with an `fn` that expects a `str` is a mypy error, not a runtime surprise.

    At dispatch the *input* body is buffered once, a `BufferedRequest` is built,
    every extractor runs (raising to reject, mapped by the router's exception
    handlers), and `fn` is called with the typed values. The handler is always
    `async`; the *output* is free: an `async def` that resolves to a `Response`,
    or an `async def ... yield` that streams `Outbound` events, and `_emit` relays
    whichever. The endpoint also answers `describe()`, recovering its
    query/header/body OpenAPI from the same extractors.

    `handle` is the lower-level builder; reach for the `@get`/`@post`/... method
    decorators to co-locate the route with its handler.
    """
    return _build_endpoint(extractors, fn, summary, responses)


# [[[cog import cog; from ladders import emit; cog.outl(emit("handle_stream")) ]]]
@overload
def handle_stream[T](
    *,
    fn: Callable[[T, Stream[Inbound]], Returned],
    summary: str = ...,
    responses: Mapping[int, ResponseSpec] | None = ...,
    request_body: Body | None = ...,
) -> HttpEndpoint[T]: ...


@overload
def handle_stream[T, A](
    a: Extractor[HttpRequestHead, A],
    /,
    *,
    fn: Callable[[T, A, Stream[Inbound]], Returned],
    summary: str = ...,
    responses: Mapping[int, ResponseSpec] | None = ...,
    request_body: Body | None = ...,
) -> HttpEndpoint[T]: ...


@overload
def handle_stream[T, A, B](
    a: Extractor[HttpRequestHead, A],
    b: Extractor[HttpRequestHead, B],
    /,
    *,
    fn: Callable[[T, A, B, Stream[Inbound]], Returned],
    summary: str = ...,
    responses: Mapping[int, ResponseSpec] | None = ...,
    request_body: Body | None = ...,
) -> HttpEndpoint[T]: ...


@overload
def handle_stream[T, A, B, C](
    a: Extractor[HttpRequestHead, A],
    b: Extractor[HttpRequestHead, B],
    c: Extractor[HttpRequestHead, C],
    /,
    *,
    fn: Callable[[T, A, B, C, Stream[Inbound]], Returned],
    summary: str = ...,
    responses: Mapping[int, ResponseSpec] | None = ...,
    request_body: Body | None = ...,
) -> HttpEndpoint[T]: ...


@overload
def handle_stream[T, A, B, C, D](
    a: Extractor[HttpRequestHead, A],
    b: Extractor[HttpRequestHead, B],
    c: Extractor[HttpRequestHead, C],
    d: Extractor[HttpRequestHead, D],
    /,
    *,
    fn: Callable[[T, A, B, C, D, Stream[Inbound]], Returned],
    summary: str = ...,
    responses: Mapping[int, ResponseSpec] | None = ...,
    request_body: Body | None = ...,
) -> HttpEndpoint[T]: ...


@overload
def handle_stream[T, A, B, C, D, E](
    a: Extractor[HttpRequestHead, A],
    b: Extractor[HttpRequestHead, B],
    c: Extractor[HttpRequestHead, C],
    d: Extractor[HttpRequestHead, D],
    e: Extractor[HttpRequestHead, E],
    /,
    *,
    fn: Callable[[T, A, B, C, D, E, Stream[Inbound]], Returned],
    summary: str = ...,
    responses: Mapping[int, ResponseSpec] | None = ...,
    request_body: Body | None = ...,
) -> HttpEndpoint[T]: ...


@overload
def handle_stream[T, A, B, C, D, E, F](
    a: Extractor[HttpRequestHead, A],
    b: Extractor[HttpRequestHead, B],
    c: Extractor[HttpRequestHead, C],
    d: Extractor[HttpRequestHead, D],
    e: Extractor[HttpRequestHead, E],
    f: Extractor[HttpRequestHead, F],
    /,
    *,
    fn: Callable[[T, A, B, C, D, E, F, Stream[Inbound]], Returned],
    summary: str = ...,
    responses: Mapping[int, ResponseSpec] | None = ...,
    request_body: Body | None = ...,
) -> HttpEndpoint[T]: ...


@overload
def handle_stream[T, A, B, C, D, E, F, G](
    a: Extractor[HttpRequestHead, A],
    b: Extractor[HttpRequestHead, B],
    c: Extractor[HttpRequestHead, C],
    d: Extractor[HttpRequestHead, D],
    e: Extractor[HttpRequestHead, E],
    f: Extractor[HttpRequestHead, F],
    g: Extractor[HttpRequestHead, G],
    /,
    *,
    fn: Callable[[T, A, B, C, D, E, F, G, Stream[Inbound]], Returned],
    summary: str = ...,
    responses: Mapping[int, ResponseSpec] | None = ...,
    request_body: Body | None = ...,
) -> HttpEndpoint[T]: ...


@overload
def handle_stream[T, A, B, C, D, E, F, G, H](
    a: Extractor[HttpRequestHead, A],
    b: Extractor[HttpRequestHead, B],
    c: Extractor[HttpRequestHead, C],
    d: Extractor[HttpRequestHead, D],
    e: Extractor[HttpRequestHead, E],
    f: Extractor[HttpRequestHead, F],
    g: Extractor[HttpRequestHead, G],
    h: Extractor[HttpRequestHead, H],
    /,
    *,
    fn: Callable[[T, A, B, C, D, E, F, G, H, Stream[Inbound]], Returned],
    summary: str = ...,
    responses: Mapping[int, ResponseSpec] | None = ...,
    request_body: Body | None = ...,
) -> HttpEndpoint[T]: ...


@overload
def handle_stream[T, A, B, C, D, E, F, G, H, J](
    a: Extractor[HttpRequestHead, A],
    b: Extractor[HttpRequestHead, B],
    c: Extractor[HttpRequestHead, C],
    d: Extractor[HttpRequestHead, D],
    e: Extractor[HttpRequestHead, E],
    f: Extractor[HttpRequestHead, F],
    g: Extractor[HttpRequestHead, G],
    h: Extractor[HttpRequestHead, H],
    j: Extractor[HttpRequestHead, J],
    /,
    *,
    fn: Callable[[T, A, B, C, D, E, F, G, H, J, Stream[Inbound]], Returned],
    summary: str = ...,
    responses: Mapping[int, ResponseSpec] | None = ...,
    request_body: Body | None = ...,
) -> HttpEndpoint[T]: ...


@overload
def handle_stream[T, A, B, C, D, E, F, G, H, J, K](
    a: Extractor[HttpRequestHead, A],
    b: Extractor[HttpRequestHead, B],
    c: Extractor[HttpRequestHead, C],
    d: Extractor[HttpRequestHead, D],
    e: Extractor[HttpRequestHead, E],
    f: Extractor[HttpRequestHead, F],
    g: Extractor[HttpRequestHead, G],
    h: Extractor[HttpRequestHead, H],
    j: Extractor[HttpRequestHead, J],
    k: Extractor[HttpRequestHead, K],
    /,
    *,
    fn: Callable[[T, A, B, C, D, E, F, G, H, J, K, Stream[Inbound]], Returned],
    summary: str = ...,
    responses: Mapping[int, ResponseSpec] | None = ...,
    request_body: Body | None = ...,
) -> HttpEndpoint[T]: ...
# [[[end]]]
def handle_stream[T](
    *extractors: Extractor[HttpRequestHead, object],
    fn: Callable[..., Returned],
    summary: str = "",
    responses: Mapping[int, ResponseSpec] | None = None,
    request_body: Body | None = None,
) -> HttpEndpoint[T]:
    """
    Build an endpoint whose handler reads the inbound stream live.

    The streaming-input sibling of `handle`. Where `handle` buffers the request
    body before the handler runs (so a `body` extractor can read it), this leaves
    the inbound stream untouched and hands it to the handler as a trailing
    `Stream[Inbound]` argument: the handler *is* the processor, taking the state,
    the typed extractor values, and the live stream, reading it as events arrive
    (a streaming upload, a long poll, a loop driven by request chunks). The
    extractors are scope-only (`path_param`/`query_param`/`header_param`/
    `http_scope`, whose context is the streaming route's `HttpRequestHead`); a `body`
    extractor is a static type error, since its `BufferedRequest` context is exactly
    the buffering a streaming route avoids. The *output* is free, exactly as in
    `handle`: yield `Outbound` to stream the response, or return (or await) a
    `Response` to buffer it.

    Reach for the `@get.stream`/`@post.stream`/... method decorators to co-locate
    the streaming route with its handler.
    """
    return _build_stream_endpoint(extractors, fn, summary, responses, request_body)


@dataclass(frozen=True, slots=True)
class _Method:
    """
    A method-bound route decorator.

    `@get(pattern, *extractors)` annotates a handler with its route and returns a
    single-method `Route`, tying each extractor's type to the handler's
    parameters exactly as `handle` does. It *registers nothing*: the returned
    `Route` is passed to a `Router` explicitly, so route assembly stays a
    declarative value, not an import-time side effect. The `Router` merges
    `Route`s that share a pattern, so `@get` and `@post` on one path combine.
    """

    method: str

    @property
    def stream(self) -> _StreamMethod:
        """The streaming-input form: `@post.stream(...)` reads the inbound stream live."""
        return _StreamMethod(self.method)

    # [[[cog import cog; from ladders import emit; cog.outl(emit("method")) ]]]
    @overload
    def __call__[T](
        self,
        pattern: Pattern,
        /,
        *,
        summary: str = ...,
        responses: Mapping[int, ResponseSpec] | None = ...,
    ) -> Callable[[Callable[[T], Returned]], Route[T]]: ...

    @overload
    def __call__[T, A](
        self,
        pattern: Pattern,
        a: Extractor[BufferedRequest, A],
        /,
        *,
        summary: str = ...,
        responses: Mapping[int, ResponseSpec] | None = ...,
    ) -> Callable[[Callable[[T, A], Returned]], Route[T]]: ...

    @overload
    def __call__[T, A, B](
        self,
        pattern: Pattern,
        a: Extractor[BufferedRequest, A],
        b: Extractor[BufferedRequest, B],
        /,
        *,
        summary: str = ...,
        responses: Mapping[int, ResponseSpec] | None = ...,
    ) -> Callable[[Callable[[T, A, B], Returned]], Route[T]]: ...

    @overload
    def __call__[T, A, B, C](
        self,
        pattern: Pattern,
        a: Extractor[BufferedRequest, A],
        b: Extractor[BufferedRequest, B],
        c: Extractor[BufferedRequest, C],
        /,
        *,
        summary: str = ...,
        responses: Mapping[int, ResponseSpec] | None = ...,
    ) -> Callable[[Callable[[T, A, B, C], Returned]], Route[T]]: ...

    @overload
    def __call__[T, A, B, C, D](
        self,
        pattern: Pattern,
        a: Extractor[BufferedRequest, A],
        b: Extractor[BufferedRequest, B],
        c: Extractor[BufferedRequest, C],
        d: Extractor[BufferedRequest, D],
        /,
        *,
        summary: str = ...,
        responses: Mapping[int, ResponseSpec] | None = ...,
    ) -> Callable[[Callable[[T, A, B, C, D], Returned]], Route[T]]: ...

    @overload
    def __call__[T, A, B, C, D, E](
        self,
        pattern: Pattern,
        a: Extractor[BufferedRequest, A],
        b: Extractor[BufferedRequest, B],
        c: Extractor[BufferedRequest, C],
        d: Extractor[BufferedRequest, D],
        e: Extractor[BufferedRequest, E],
        /,
        *,
        summary: str = ...,
        responses: Mapping[int, ResponseSpec] | None = ...,
    ) -> Callable[[Callable[[T, A, B, C, D, E], Returned]], Route[T]]: ...

    @overload
    def __call__[T, A, B, C, D, E, F](
        self,
        pattern: Pattern,
        a: Extractor[BufferedRequest, A],
        b: Extractor[BufferedRequest, B],
        c: Extractor[BufferedRequest, C],
        d: Extractor[BufferedRequest, D],
        e: Extractor[BufferedRequest, E],
        f: Extractor[BufferedRequest, F],
        /,
        *,
        summary: str = ...,
        responses: Mapping[int, ResponseSpec] | None = ...,
    ) -> Callable[[Callable[[T, A, B, C, D, E, F], Returned]], Route[T]]: ...

    @overload
    def __call__[T, A, B, C, D, E, F, G](
        self,
        pattern: Pattern,
        a: Extractor[BufferedRequest, A],
        b: Extractor[BufferedRequest, B],
        c: Extractor[BufferedRequest, C],
        d: Extractor[BufferedRequest, D],
        e: Extractor[BufferedRequest, E],
        f: Extractor[BufferedRequest, F],
        g: Extractor[BufferedRequest, G],
        /,
        *,
        summary: str = ...,
        responses: Mapping[int, ResponseSpec] | None = ...,
    ) -> Callable[[Callable[[T, A, B, C, D, E, F, G], Returned]], Route[T]]: ...

    @overload
    def __call__[T, A, B, C, D, E, F, G, H](
        self,
        pattern: Pattern,
        a: Extractor[BufferedRequest, A],
        b: Extractor[BufferedRequest, B],
        c: Extractor[BufferedRequest, C],
        d: Extractor[BufferedRequest, D],
        e: Extractor[BufferedRequest, E],
        f: Extractor[BufferedRequest, F],
        g: Extractor[BufferedRequest, G],
        h: Extractor[BufferedRequest, H],
        /,
        *,
        summary: str = ...,
        responses: Mapping[int, ResponseSpec] | None = ...,
    ) -> Callable[[Callable[[T, A, B, C, D, E, F, G, H], Returned]], Route[T]]: ...

    @overload
    def __call__[T, A, B, C, D, E, F, G, H, J](
        self,
        pattern: Pattern,
        a: Extractor[BufferedRequest, A],
        b: Extractor[BufferedRequest, B],
        c: Extractor[BufferedRequest, C],
        d: Extractor[BufferedRequest, D],
        e: Extractor[BufferedRequest, E],
        f: Extractor[BufferedRequest, F],
        g: Extractor[BufferedRequest, G],
        h: Extractor[BufferedRequest, H],
        j: Extractor[BufferedRequest, J],
        /,
        *,
        summary: str = ...,
        responses: Mapping[int, ResponseSpec] | None = ...,
    ) -> Callable[[Callable[[T, A, B, C, D, E, F, G, H, J], Returned]], Route[T]]: ...

    @overload
    def __call__[T, A, B, C, D, E, F, G, H, J, K](
        self,
        pattern: Pattern,
        a: Extractor[BufferedRequest, A],
        b: Extractor[BufferedRequest, B],
        c: Extractor[BufferedRequest, C],
        d: Extractor[BufferedRequest, D],
        e: Extractor[BufferedRequest, E],
        f: Extractor[BufferedRequest, F],
        g: Extractor[BufferedRequest, G],
        h: Extractor[BufferedRequest, H],
        j: Extractor[BufferedRequest, J],
        k: Extractor[BufferedRequest, K],
        /,
        *,
        summary: str = ...,
        responses: Mapping[int, ResponseSpec] | None = ...,
    ) -> Callable[[Callable[[T, A, B, C, D, E, F, G, H, J, K], Returned]], Route[T]]: ...
    # [[[end]]]
    def __call__(
        self,
        pattern: Pattern,
        *extractors: Extractor[BufferedRequest, object],
        summary: str = "",
        responses: Mapping[int, ResponseSpec] | None = None,
    ) -> Callable[[Callable[..., Returned]], Route[object]]:
        def decorate(fn: Callable[..., Returned]) -> Route[object]:
            endpoint = _build_endpoint(extractors, fn, summary, responses)
            return Route(_segments(pattern), {self.method: endpoint})

        return decorate


get = _Method("GET")
head = _Method("HEAD")
post = _Method("POST")
put = _Method("PUT")
patch = _Method("PATCH")
delete = _Method("DELETE")
options = _Method("OPTIONS")


@dataclass(frozen=True, slots=True)
class _StreamMethod:
    """
    The streaming-input form of a method decorator, reached as `post.stream`.

    `@post.stream(pattern, *extractors)` is to `handle_stream` what `@post` is to
    `handle`: it ties each extractor's type to the handler's parameters and returns
    a single-method `Route`, but the handler reads the inbound stream live, taking
    it as a trailing `Stream[Inbound]` argument after the typed values. A `body`
    extractor is rejected, since buffering is what a streaming route avoids. The
    output is free (yield to stream, return or await a `Response` to buffer). Like
    `@post`, it registers nothing; the `Router` merges it with other `Route`s on
    the same pattern.
    """

    method: str

    # [[[cog import cog; from ladders import emit; cog.outl(emit("stream_method")) ]]]
    @overload
    def __call__[T](
        self,
        pattern: Pattern,
        /,
        *,
        summary: str = ...,
        responses: Mapping[int, ResponseSpec] | None = ...,
        request_body: Body | None = ...,
    ) -> Callable[[Callable[[T, Stream[Inbound]], Returned]], Route[T]]: ...

    @overload
    def __call__[T, A](
        self,
        pattern: Pattern,
        a: Extractor[HttpRequestHead, A],
        /,
        *,
        summary: str = ...,
        responses: Mapping[int, ResponseSpec] | None = ...,
        request_body: Body | None = ...,
    ) -> Callable[[Callable[[T, A, Stream[Inbound]], Returned]], Route[T]]: ...

    @overload
    def __call__[T, A, B](
        self,
        pattern: Pattern,
        a: Extractor[HttpRequestHead, A],
        b: Extractor[HttpRequestHead, B],
        /,
        *,
        summary: str = ...,
        responses: Mapping[int, ResponseSpec] | None = ...,
        request_body: Body | None = ...,
    ) -> Callable[[Callable[[T, A, B, Stream[Inbound]], Returned]], Route[T]]: ...

    @overload
    def __call__[T, A, B, C](
        self,
        pattern: Pattern,
        a: Extractor[HttpRequestHead, A],
        b: Extractor[HttpRequestHead, B],
        c: Extractor[HttpRequestHead, C],
        /,
        *,
        summary: str = ...,
        responses: Mapping[int, ResponseSpec] | None = ...,
        request_body: Body | None = ...,
    ) -> Callable[[Callable[[T, A, B, C, Stream[Inbound]], Returned]], Route[T]]: ...

    @overload
    def __call__[T, A, B, C, D](
        self,
        pattern: Pattern,
        a: Extractor[HttpRequestHead, A],
        b: Extractor[HttpRequestHead, B],
        c: Extractor[HttpRequestHead, C],
        d: Extractor[HttpRequestHead, D],
        /,
        *,
        summary: str = ...,
        responses: Mapping[int, ResponseSpec] | None = ...,
        request_body: Body | None = ...,
    ) -> Callable[[Callable[[T, A, B, C, D, Stream[Inbound]], Returned]], Route[T]]: ...

    @overload
    def __call__[T, A, B, C, D, E](
        self,
        pattern: Pattern,
        a: Extractor[HttpRequestHead, A],
        b: Extractor[HttpRequestHead, B],
        c: Extractor[HttpRequestHead, C],
        d: Extractor[HttpRequestHead, D],
        e: Extractor[HttpRequestHead, E],
        /,
        *,
        summary: str = ...,
        responses: Mapping[int, ResponseSpec] | None = ...,
        request_body: Body | None = ...,
    ) -> Callable[[Callable[[T, A, B, C, D, E, Stream[Inbound]], Returned]], Route[T]]: ...

    @overload
    def __call__[T, A, B, C, D, E, F](
        self,
        pattern: Pattern,
        a: Extractor[HttpRequestHead, A],
        b: Extractor[HttpRequestHead, B],
        c: Extractor[HttpRequestHead, C],
        d: Extractor[HttpRequestHead, D],
        e: Extractor[HttpRequestHead, E],
        f: Extractor[HttpRequestHead, F],
        /,
        *,
        summary: str = ...,
        responses: Mapping[int, ResponseSpec] | None = ...,
        request_body: Body | None = ...,
    ) -> Callable[[Callable[[T, A, B, C, D, E, F, Stream[Inbound]], Returned]], Route[T]]: ...

    @overload
    def __call__[T, A, B, C, D, E, F, G](
        self,
        pattern: Pattern,
        a: Extractor[HttpRequestHead, A],
        b: Extractor[HttpRequestHead, B],
        c: Extractor[HttpRequestHead, C],
        d: Extractor[HttpRequestHead, D],
        e: Extractor[HttpRequestHead, E],
        f: Extractor[HttpRequestHead, F],
        g: Extractor[HttpRequestHead, G],
        /,
        *,
        summary: str = ...,
        responses: Mapping[int, ResponseSpec] | None = ...,
        request_body: Body | None = ...,
    ) -> Callable[[Callable[[T, A, B, C, D, E, F, G, Stream[Inbound]], Returned]], Route[T]]: ...

    @overload
    def __call__[T, A, B, C, D, E, F, G, H](
        self,
        pattern: Pattern,
        a: Extractor[HttpRequestHead, A],
        b: Extractor[HttpRequestHead, B],
        c: Extractor[HttpRequestHead, C],
        d: Extractor[HttpRequestHead, D],
        e: Extractor[HttpRequestHead, E],
        f: Extractor[HttpRequestHead, F],
        g: Extractor[HttpRequestHead, G],
        h: Extractor[HttpRequestHead, H],
        /,
        *,
        summary: str = ...,
        responses: Mapping[int, ResponseSpec] | None = ...,
        request_body: Body | None = ...,
    ) -> Callable[[Callable[[T, A, B, C, D, E, F, G, H, Stream[Inbound]], Returned]], Route[T]]: ...

    @overload
    def __call__[T, A, B, C, D, E, F, G, H, J](
        self,
        pattern: Pattern,
        a: Extractor[HttpRequestHead, A],
        b: Extractor[HttpRequestHead, B],
        c: Extractor[HttpRequestHead, C],
        d: Extractor[HttpRequestHead, D],
        e: Extractor[HttpRequestHead, E],
        f: Extractor[HttpRequestHead, F],
        g: Extractor[HttpRequestHead, G],
        h: Extractor[HttpRequestHead, H],
        j: Extractor[HttpRequestHead, J],
        /,
        *,
        summary: str = ...,
        responses: Mapping[int, ResponseSpec] | None = ...,
        request_body: Body | None = ...,
    ) -> Callable[[Callable[[T, A, B, C, D, E, F, G, H, J, Stream[Inbound]], Returned]], Route[T]]: ...

    @overload
    def __call__[T, A, B, C, D, E, F, G, H, J, K](
        self,
        pattern: Pattern,
        a: Extractor[HttpRequestHead, A],
        b: Extractor[HttpRequestHead, B],
        c: Extractor[HttpRequestHead, C],
        d: Extractor[HttpRequestHead, D],
        e: Extractor[HttpRequestHead, E],
        f: Extractor[HttpRequestHead, F],
        g: Extractor[HttpRequestHead, G],
        h: Extractor[HttpRequestHead, H],
        j: Extractor[HttpRequestHead, J],
        k: Extractor[HttpRequestHead, K],
        /,
        *,
        summary: str = ...,
        responses: Mapping[int, ResponseSpec] | None = ...,
        request_body: Body | None = ...,
    ) -> Callable[[Callable[[T, A, B, C, D, E, F, G, H, J, K, Stream[Inbound]], Returned]], Route[T]]: ...
    # [[[end]]]
    def __call__(
        self,
        pattern: Pattern,
        *extractors: Extractor[HttpRequestHead, object],
        summary: str = "",
        responses: Mapping[int, ResponseSpec] | None = None,
        request_body: Body | None = None,
    ) -> Callable[[Callable[..., Returned]], Route[object]]:
        def decorate(fn: Callable[..., Returned]) -> Route[object]:
            endpoint = _build_stream_endpoint(extractors, fn, summary, responses, request_body)
            return Route(_segments(pattern), {self.method: endpoint})

        return decorate


# [[[cog import cog; from ladders import emit; cog.outl(emit("ws")) ]]]
@overload
def ws[T](
    pattern: Pattern,
    /,
) -> Callable[[Callable[[T, Stream[WebsocketInbound]], WebsocketReturned]], WebsocketRoute[T]]: ...


@overload
def ws[T, A](
    pattern: Pattern,
    a: Extractor[WebsocketRequestHead, A],
    /,
) -> Callable[[Callable[[T, A, Stream[WebsocketInbound]], WebsocketReturned]], WebsocketRoute[T]]: ...


@overload
def ws[T, A, B](
    pattern: Pattern,
    a: Extractor[WebsocketRequestHead, A],
    b: Extractor[WebsocketRequestHead, B],
    /,
) -> Callable[[Callable[[T, A, B, Stream[WebsocketInbound]], WebsocketReturned]], WebsocketRoute[T]]: ...


@overload
def ws[T, A, B, C](
    pattern: Pattern,
    a: Extractor[WebsocketRequestHead, A],
    b: Extractor[WebsocketRequestHead, B],
    c: Extractor[WebsocketRequestHead, C],
    /,
) -> Callable[[Callable[[T, A, B, C, Stream[WebsocketInbound]], WebsocketReturned]], WebsocketRoute[T]]: ...


@overload
def ws[T, A, B, C, D](
    pattern: Pattern,
    a: Extractor[WebsocketRequestHead, A],
    b: Extractor[WebsocketRequestHead, B],
    c: Extractor[WebsocketRequestHead, C],
    d: Extractor[WebsocketRequestHead, D],
    /,
) -> Callable[[Callable[[T, A, B, C, D, Stream[WebsocketInbound]], WebsocketReturned]], WebsocketRoute[T]]: ...


@overload
def ws[T, A, B, C, D, E](
    pattern: Pattern,
    a: Extractor[WebsocketRequestHead, A],
    b: Extractor[WebsocketRequestHead, B],
    c: Extractor[WebsocketRequestHead, C],
    d: Extractor[WebsocketRequestHead, D],
    e: Extractor[WebsocketRequestHead, E],
    /,
) -> Callable[[Callable[[T, A, B, C, D, E, Stream[WebsocketInbound]], WebsocketReturned]], WebsocketRoute[T]]: ...


@overload
def ws[T, A, B, C, D, E, F](
    pattern: Pattern,
    a: Extractor[WebsocketRequestHead, A],
    b: Extractor[WebsocketRequestHead, B],
    c: Extractor[WebsocketRequestHead, C],
    d: Extractor[WebsocketRequestHead, D],
    e: Extractor[WebsocketRequestHead, E],
    f: Extractor[WebsocketRequestHead, F],
    /,
) -> Callable[[Callable[[T, A, B, C, D, E, F, Stream[WebsocketInbound]], WebsocketReturned]], WebsocketRoute[T]]: ...


@overload
def ws[T, A, B, C, D, E, F, G](
    pattern: Pattern,
    a: Extractor[WebsocketRequestHead, A],
    b: Extractor[WebsocketRequestHead, B],
    c: Extractor[WebsocketRequestHead, C],
    d: Extractor[WebsocketRequestHead, D],
    e: Extractor[WebsocketRequestHead, E],
    f: Extractor[WebsocketRequestHead, F],
    g: Extractor[WebsocketRequestHead, G],
    /,
) -> Callable[[Callable[[T, A, B, C, D, E, F, G, Stream[WebsocketInbound]], WebsocketReturned]], WebsocketRoute[T]]: ...


@overload
def ws[T, A, B, C, D, E, F, G, H](
    pattern: Pattern,
    a: Extractor[WebsocketRequestHead, A],
    b: Extractor[WebsocketRequestHead, B],
    c: Extractor[WebsocketRequestHead, C],
    d: Extractor[WebsocketRequestHead, D],
    e: Extractor[WebsocketRequestHead, E],
    f: Extractor[WebsocketRequestHead, F],
    g: Extractor[WebsocketRequestHead, G],
    h: Extractor[WebsocketRequestHead, H],
    /,
) -> Callable[
    [Callable[[T, A, B, C, D, E, F, G, H, Stream[WebsocketInbound]], WebsocketReturned]], WebsocketRoute[T]
]: ...


@overload
def ws[T, A, B, C, D, E, F, G, H, J](
    pattern: Pattern,
    a: Extractor[WebsocketRequestHead, A],
    b: Extractor[WebsocketRequestHead, B],
    c: Extractor[WebsocketRequestHead, C],
    d: Extractor[WebsocketRequestHead, D],
    e: Extractor[WebsocketRequestHead, E],
    f: Extractor[WebsocketRequestHead, F],
    g: Extractor[WebsocketRequestHead, G],
    h: Extractor[WebsocketRequestHead, H],
    j: Extractor[WebsocketRequestHead, J],
    /,
) -> Callable[
    [Callable[[T, A, B, C, D, E, F, G, H, J, Stream[WebsocketInbound]], WebsocketReturned]], WebsocketRoute[T]
]: ...


@overload
def ws[T, A, B, C, D, E, F, G, H, J, K](
    pattern: Pattern,
    a: Extractor[WebsocketRequestHead, A],
    b: Extractor[WebsocketRequestHead, B],
    c: Extractor[WebsocketRequestHead, C],
    d: Extractor[WebsocketRequestHead, D],
    e: Extractor[WebsocketRequestHead, E],
    f: Extractor[WebsocketRequestHead, F],
    g: Extractor[WebsocketRequestHead, G],
    h: Extractor[WebsocketRequestHead, H],
    j: Extractor[WebsocketRequestHead, J],
    k: Extractor[WebsocketRequestHead, K],
    /,
) -> Callable[
    [Callable[[T, A, B, C, D, E, F, G, H, J, K, Stream[WebsocketInbound]], WebsocketReturned]], WebsocketRoute[T]
]: ...
# [[[end]]]
def ws[T](
    pattern: Pattern, *extractors: Extractor[WebsocketRequestHead, object]
) -> Callable[[Callable[..., WebsocketReturned]], WebsocketRoute[T]]:
    """
    The websocket sibling of `@get`/`@post`, tying extractors to a handler.

    `@ws(t"/feed/{room}", room, since)` co-locates the route with the handler and
    ties each extractor's type to its parameters, just like `@get`. The handler
    *is* the frame processor (the same move as `@post.stream`): it takes the live
    inbound frames as a trailing `Stream[WebsocketInbound]` argument and yields
    `WebsocketOutbound`, rather than returning a processor. There is no body to
    buffer: `path_param`, `query_param`, and `header_param` read the handshake, and a
    `body` (or `http_scope`) token is a static type error, its context not the
    `WebsocketRequestHead` a websocket route provides. Returns a `WebsocketRoute` to
    pass to a `WebsocketRouter`.
    """

    def decorate(fn: Callable[..., WebsocketReturned]) -> WebsocketRoute[T]:
        def endpoint(state: T, match: Match[WebsocketScope]) -> WebsocketHandler:
            head = WebsocketRequestHead.parsed(scope=match.scope, path_params=match.params)
            values = _extract_all(extractors, head)

            def processor(inputs: Stream[WebsocketInbound]) -> Stream[WebsocketOutbound]:
                return fn(state, *values, inputs)

            return processor

        return WebsocketRoute(_segments(pattern), endpoint)

    return decorate


def _extract_all[C](extractors: tuple[Extractor[C, object], ...], head: C) -> tuple[object, ...]:
    """
    Run every extractor against the request head, guaranteeing a rejection crosses
    this boundary as an `ExtractionError`.

    The extractors already raise a rich, `field`-attributed `ExtractionError` (see
    `without_web.extractors`); this is the backstop for the rest: a stray `ValueError`
    from a custom extractor or an `into` factory is wrapped unattributed, and an
    `ExtractionError` passes through untouched. Any other exception is a bug and
    propagates, so a plain `ValueError` raised deeper in a handler still surfaces as a
    500 rather than a client 4xx.
    """
    values: list[object] = []
    for extractor in extractors:
        try:
            values.append(extractor.extract(head))
        except ExtractionError:
            raise
        except ValueError as exc:
            raise ExtractionError(str(exc), cause=exc) from exc
    return tuple(values)


async def _emit(result: Returned) -> AsyncIterator[Outbound]:
    """
    Relay a handler's result to the outbound stream, however it was produced.

    The single output path shared by buffered- and streamed-input endpoints, so
    the output mode is decided by what the handler returns, not by how its input
    was read. Handlers are always async, so there are two shapes: an
    `Awaitable[Reply]` (an `async def` resolving to a `Response` or a stream) is
    awaited, then its `Reply` is relayed; an `AsyncIterator[Outbound]` (an
    `async def ... yield` handler) is relayed event by event. A `Response` is
    encoded once (buffered output); a stream is forwarded as-is.
    """
    reply: Reply = await result if isinstance(result, Awaitable) else result
    if isinstance(reply, Response):
        for event in encode_response(reply):
            yield event
        return
    async for event in reply:
        yield event


def _build_stream_endpoint(
    extractors: tuple[Extractor[HttpRequestHead, object], ...],
    fn: Callable[..., Returned],
    summary: str,
    responses: Mapping[int, ResponseSpec] | None,
    request_body: Body | None,
) -> HttpEndpoint[object]:
    spec = RouteSpec(
        summary=summary,
        query=tuple(param for extractor in extractors for param in extractor.query),
        headers=tuple(param for extractor in extractors for param in extractor.headers),
        request_body=request_body,
        responses=dict(responses or {}),
    )

    def endpoint(state: object, match: Match[HttpScope]) -> HttpHandler:
        head = HttpRequestHead.parsed(scope=match.scope, path_params=match.params)
        values = _extract_all(extractors, head)

        def processor(inputs: Stream[Inbound]) -> Stream[Outbound]:
            return _emit(fn(state, *values, inputs))

        return processor

    return describe(spec)(endpoint)


def _build_endpoint(
    extractors: tuple[Extractor[BufferedRequest, object], ...],
    fn: Callable[..., Returned],
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
    extractors: tuple[Extractor[BufferedRequest, object], ...],
    fn: Callable[..., Returned],
) -> AsyncIterator[Outbound]:
    body = await read_body(inputs)
    head = BufferedRequest.buffered(scope=match.scope, path_params=match.params, body=body)
    async for event in _emit(fn(state, *_extract_all(extractors, head))):
        yield event
