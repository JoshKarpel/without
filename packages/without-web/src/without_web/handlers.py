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
from without_asgi import WebsocketScope
from without_asgi import encode_response
from without_asgi import read_body

from without_web.extractors import Extractor
from without_web.extractors import Request
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

# What a handler's *output* is, once produced: a single `Response` (buffered
# output) or an already-streaming `Stream[Outbound]`. Input buffering is the one
# build-time axis (the `@post` vs `@post.stream` split); the output is always just
# what the handler hands back, relayed through `_emit`.
type Reply = Response | Stream[Outbound]

# What a handler may *return*: a `Reply` directly (a sync handler), or an
# `Awaitable[Reply]` (an `async def` handler that does I/O, then returns one). An
# `async def ... yield` handler is already an `AsyncIterator[Outbound]`, i.e. a
# `Stream[Outbound]`, so it is a `Reply` too. `_emit` collapses all three shapes
# into the outbound stream, so the output mode is free of the input mode.
type Returned = Reply | Awaitable[Reply]


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
    a: Extractor[A],
    /,
    *,
    fn: Callable[[T, A], Returned],
    summary: str = ...,
    responses: Mapping[int, ResponseSpec] | None = ...,
) -> HttpEndpoint[T]: ...


@overload
def handle[T, A, B](
    a: Extractor[A],
    b: Extractor[B],
    /,
    *,
    fn: Callable[[T, A, B], Returned],
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
    fn: Callable[[T, A, B, C], Returned],
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
    fn: Callable[[T, A, B, C, D], Returned],
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
    fn: Callable[[T, A, B, C, D, E], Returned],
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
    fn: Callable[[T, A, B, C, D, E, F], Returned],
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
    fn: Callable[[T, A, B, C, D, E, F, G], Returned],
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
    fn: Callable[[T, A, B, C, D, E, F, G, H], Returned],
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
    fn: Callable[[T, A, B, C, D, E, F, G, H, J], Returned],
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
    fn: Callable[[T, A, B, C, D, E, F, G, H, J, K], Returned],
    summary: str = ...,
    responses: Mapping[int, ResponseSpec] | None = ...,
) -> HttpEndpoint[T]: ...
# [[[end]]]
def handle[T](
    *extractors: Extractor[object],
    fn: Callable[..., Returned],
    summary: str = "",
    responses: Mapping[int, ResponseSpec] | None = None,
) -> HttpEndpoint[T]:
    """Build a self-describing endpoint from typed extractors and a handler.

    Each extractor is a typed piece of the request; the overloads tie the
    extractors' types to `fn`'s parameters, so a `path_param(..., INT)` paired
    with an `fn` that expects a `str` is a mypy error, not a runtime surprise.

    At dispatch the *input* body is buffered once, a `Request` is built, every
    extractor runs (raising to reject, mapped by the router's exception
    handlers), and `fn` is called with the typed values. The *output* is free and
    may be produced synchronously or asynchronously: `fn` returns a `Response`, an
    `async def` that resolves to one, or an `async def ... yield` that streams
    `Outbound` events, and `_emit` relays whichever. The endpoint also answers
    `describe()`, recovering its query/header/body OpenAPI from the same extractors.

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
    a: Extractor[A],
    /,
    *,
    fn: Callable[[T, A, Stream[Inbound]], Returned],
    summary: str = ...,
    responses: Mapping[int, ResponseSpec] | None = ...,
    request_body: Body | None = ...,
) -> HttpEndpoint[T]: ...


@overload
def handle_stream[T, A, B](
    a: Extractor[A],
    b: Extractor[B],
    /,
    *,
    fn: Callable[[T, A, B, Stream[Inbound]], Returned],
    summary: str = ...,
    responses: Mapping[int, ResponseSpec] | None = ...,
    request_body: Body | None = ...,
) -> HttpEndpoint[T]: ...


@overload
def handle_stream[T, A, B, C](
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    /,
    *,
    fn: Callable[[T, A, B, C, Stream[Inbound]], Returned],
    summary: str = ...,
    responses: Mapping[int, ResponseSpec] | None = ...,
    request_body: Body | None = ...,
) -> HttpEndpoint[T]: ...


@overload
def handle_stream[T, A, B, C, D](
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    d: Extractor[D],
    /,
    *,
    fn: Callable[[T, A, B, C, D, Stream[Inbound]], Returned],
    summary: str = ...,
    responses: Mapping[int, ResponseSpec] | None = ...,
    request_body: Body | None = ...,
) -> HttpEndpoint[T]: ...


@overload
def handle_stream[T, A, B, C, D, E](
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    d: Extractor[D],
    e: Extractor[E],
    /,
    *,
    fn: Callable[[T, A, B, C, D, E, Stream[Inbound]], Returned],
    summary: str = ...,
    responses: Mapping[int, ResponseSpec] | None = ...,
    request_body: Body | None = ...,
) -> HttpEndpoint[T]: ...


@overload
def handle_stream[T, A, B, C, D, E, F](
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    d: Extractor[D],
    e: Extractor[E],
    f: Extractor[F],
    /,
    *,
    fn: Callable[[T, A, B, C, D, E, F, Stream[Inbound]], Returned],
    summary: str = ...,
    responses: Mapping[int, ResponseSpec] | None = ...,
    request_body: Body | None = ...,
) -> HttpEndpoint[T]: ...


@overload
def handle_stream[T, A, B, C, D, E, F, G](
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    d: Extractor[D],
    e: Extractor[E],
    f: Extractor[F],
    g: Extractor[G],
    /,
    *,
    fn: Callable[[T, A, B, C, D, E, F, G, Stream[Inbound]], Returned],
    summary: str = ...,
    responses: Mapping[int, ResponseSpec] | None = ...,
    request_body: Body | None = ...,
) -> HttpEndpoint[T]: ...


@overload
def handle_stream[T, A, B, C, D, E, F, G, H](
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
    fn: Callable[[T, A, B, C, D, E, F, G, H, Stream[Inbound]], Returned],
    summary: str = ...,
    responses: Mapping[int, ResponseSpec] | None = ...,
    request_body: Body | None = ...,
) -> HttpEndpoint[T]: ...


@overload
def handle_stream[T, A, B, C, D, E, F, G, H, J](
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
    fn: Callable[[T, A, B, C, D, E, F, G, H, J, Stream[Inbound]], Returned],
    summary: str = ...,
    responses: Mapping[int, ResponseSpec] | None = ...,
    request_body: Body | None = ...,
) -> HttpEndpoint[T]: ...


@overload
def handle_stream[T, A, B, C, D, E, F, G, H, J, K](
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
    fn: Callable[[T, A, B, C, D, E, F, G, H, J, K, Stream[Inbound]], Returned],
    summary: str = ...,
    responses: Mapping[int, ResponseSpec] | None = ...,
    request_body: Body | None = ...,
) -> HttpEndpoint[T]: ...
# [[[end]]]
def handle_stream[T](
    *extractors: Extractor[object],
    fn: Callable[..., Returned],
    summary: str = "",
    responses: Mapping[int, ResponseSpec] | None = None,
    request_body: Body | None = None,
) -> HttpEndpoint[T]:
    """Build an endpoint whose handler reads the inbound stream live.

    The streaming-input sibling of `handle`. Where `handle` buffers the request
    body before the handler runs (so a `body` extractor can read it), this leaves
    the inbound stream untouched and hands it to the handler as a trailing
    `Stream[Inbound]` argument: the handler *is* the processor, taking the state,
    the typed extractor values, and the live stream, reading it as events arrive
    (a streaming upload, a long poll, a loop driven by request chunks). The
    extractors are scope-only (`path_param`/`query_param`/`header_param`/
    `http_scope`); a `body` extractor is rejected, since buffering the body is
    exactly what a streaming route avoids. The *output* is free, exactly as in
    `handle`: yield `Outbound` to stream the response, or return (or await) a
    `Response` to buffer it.

    Reach for the `@get.stream`/`@post.stream`/... method decorators to co-locate
    the streaming route with its handler.
    """
    return _build_stream_endpoint(extractors, fn, summary, responses, request_body)


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
        a: Extractor[A],
        /,
        *,
        summary: str = ...,
        responses: Mapping[int, ResponseSpec] | None = ...,
    ) -> Callable[[Callable[[T, A], Returned]], Route[T]]: ...

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
    ) -> Callable[[Callable[[T, A, B], Returned]], Route[T]]: ...

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
    ) -> Callable[[Callable[[T, A, B, C], Returned]], Route[T]]: ...

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
    ) -> Callable[[Callable[[T, A, B, C, D], Returned]], Route[T]]: ...

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
    ) -> Callable[[Callable[[T, A, B, C, D, E], Returned]], Route[T]]: ...

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
    ) -> Callable[[Callable[[T, A, B, C, D, E, F], Returned]], Route[T]]: ...

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
    ) -> Callable[[Callable[[T, A, B, C, D, E, F, G], Returned]], Route[T]]: ...

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
    ) -> Callable[[Callable[[T, A, B, C, D, E, F, G, H], Returned]], Route[T]]: ...

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
    ) -> Callable[[Callable[[T, A, B, C, D, E, F, G, H, J], Returned]], Route[T]]: ...

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
    ) -> Callable[[Callable[[T, A, B, C, D, E, F, G, H, J, K], Returned]], Route[T]]: ...
    # [[[end]]]
    def __call__(
        self,
        pattern: Pattern,
        *extractors: Extractor[object],
        summary: str = "",
        responses: Mapping[int, ResponseSpec] | None = None,
    ) -> Callable[[Callable[..., Returned]], Route[object]]:
        def decorate(fn: Callable[..., Returned]) -> Route[object]:
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


@dataclass(frozen=True, slots=True)
class _StreamMethod:
    """The streaming-input form of a method decorator, reached as `post.stream`.

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
        a: Extractor[A],
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
        a: Extractor[A],
        b: Extractor[B],
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
        a: Extractor[A],
        b: Extractor[B],
        c: Extractor[C],
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
        a: Extractor[A],
        b: Extractor[B],
        c: Extractor[C],
        d: Extractor[D],
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
        a: Extractor[A],
        b: Extractor[B],
        c: Extractor[C],
        d: Extractor[D],
        e: Extractor[E],
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
        request_body: Body | None = ...,
    ) -> Callable[[Callable[[T, A, B, C, D, E, F, Stream[Inbound]], Returned]], Route[T]]: ...

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
        request_body: Body | None = ...,
    ) -> Callable[[Callable[[T, A, B, C, D, E, F, G, Stream[Inbound]], Returned]], Route[T]]: ...

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
        request_body: Body | None = ...,
    ) -> Callable[[Callable[[T, A, B, C, D, E, F, G, H, Stream[Inbound]], Returned]], Route[T]]: ...

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
        request_body: Body | None = ...,
    ) -> Callable[[Callable[[T, A, B, C, D, E, F, G, H, J, Stream[Inbound]], Returned]], Route[T]]: ...

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
        request_body: Body | None = ...,
    ) -> Callable[[Callable[[T, A, B, C, D, E, F, G, H, J, K, Stream[Inbound]], Returned]], Route[T]]: ...
    # [[[end]]]
    def __call__(
        self,
        pattern: Pattern,
        *extractors: Extractor[object],
        summary: str = "",
        responses: Mapping[int, ResponseSpec] | None = None,
        request_body: Body | None = None,
    ) -> Callable[[Callable[..., Returned]], Route[object]]:
        def decorate(fn: Callable[..., Returned]) -> Route[object]:
            endpoint = _build_stream_endpoint(extractors, fn, summary, responses, request_body)
            return Route(pattern=pattern, methods={self.method: endpoint})

        return decorate


# [[[cog import cog; from ladders import emit; cog.outl(emit("ws")) ]]]
@overload
def ws[T](
    pattern: Pattern,
    /,
) -> Callable[[Callable[[T], WebsocketHandler]], WebsocketRoute[T]]: ...


@overload
def ws[T, A](
    pattern: Pattern,
    a: Extractor[A],
    /,
) -> Callable[[Callable[[T, A], WebsocketHandler]], WebsocketRoute[T]]: ...


@overload
def ws[T, A, B](
    pattern: Pattern,
    a: Extractor[A],
    b: Extractor[B],
    /,
) -> Callable[[Callable[[T, A, B], WebsocketHandler]], WebsocketRoute[T]]: ...


@overload
def ws[T, A, B, C](
    pattern: Pattern,
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    /,
) -> Callable[[Callable[[T, A, B, C], WebsocketHandler]], WebsocketRoute[T]]: ...


@overload
def ws[T, A, B, C, D](
    pattern: Pattern,
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    d: Extractor[D],
    /,
) -> Callable[[Callable[[T, A, B, C, D], WebsocketHandler]], WebsocketRoute[T]]: ...


@overload
def ws[T, A, B, C, D, E](
    pattern: Pattern,
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    d: Extractor[D],
    e: Extractor[E],
    /,
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
# [[[end]]]
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


async def _emit(result: Returned) -> AsyncIterator[Outbound]:
    """Relay a handler's result to the outbound stream, however it was produced.

    The single output path shared by buffered- and streamed-input endpoints, so
    the output mode is decided by what the handler returns, not by how its input
    was read. Three shapes: a `Response` is encoded once (buffered output); an
    `Awaitable[Reply]` (an `async def` handler) is awaited, then its `Reply` is
    relayed the same way; an `AsyncIterator[Outbound]` (an `async def ... yield`
    handler) is relayed event by event. This is the runtime-value dispatch that
    keeps the four input/output buffering combinations all expressible.
    """
    if isinstance(result, Awaitable):
        result = await result
    if isinstance(result, Response):
        for event in encode_response(result):
            yield event
        return
    async for event in result:
        yield event


def _build_stream_endpoint(
    extractors: tuple[Extractor[object], ...],
    fn: Callable[..., Returned],
    summary: str,
    responses: Mapping[int, ResponseSpec] | None,
    request_body: Body | None,
) -> HttpEndpoint[object]:
    if any(extractor.request_body is not None for extractor in extractors):
        raise ValueError("a streaming route cannot take a body extractor; it would buffer the input it streams")
    spec = RouteSpec(
        summary=summary,
        query=tuple(param for extractor in extractors for param in extractor.query),
        headers=tuple(param for extractor in extractors for param in extractor.headers),
        request_body=request_body,
        responses=dict(responses or {}),
    )

    def endpoint(state: object, match: Match[HttpScope]) -> HttpHandler:
        request = Request(scope=match.scope, params=match.params, body=b"")
        values = tuple(extractor.extract(request) for extractor in extractors)

        def processor(inputs: Stream[Inbound]) -> Stream[Outbound]:
            return _emit(fn(state, *values, inputs))

        return processor

    return describe(spec)(endpoint)


def _build_endpoint(
    extractors: tuple[Extractor[object], ...],
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
    extractors: tuple[Extractor[object], ...],
    fn: Callable[..., Returned],
) -> AsyncIterator[Outbound]:
    body = await read_body(inputs)
    request = Request(scope=match.scope, params=match.params, body=body)
    async for event in _emit(fn(state, *(extractor.extract(request) for extractor in extractors))):
        yield event
