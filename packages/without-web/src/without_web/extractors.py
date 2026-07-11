from __future__ import annotations

from collections.abc import Callable
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Generic
from typing import TypeVar
from typing import cast
from typing import overload
from urllib.parse import parse_qs

from without_asgi import HttpScope
from without_asgi import WebsocketScope

from without_web.converters import PATH
from without_web.converters import Converter
from without_web.openapi import Body
from without_web.openapi import HeaderParam
from without_web.openapi import QueryParam
from without_web.openapi import SchemaRef
from without_web.openapi import Single
from without_web.patterns import PathSpec


@dataclass(frozen=True, slots=True)
class Request:
    """
    The parsed-once context an extractor reads from.

    `path_params` holds the path parameters the router already parsed during the
    trie walk (typed values, stored as `object`); `query_params` is the query
    string decoded and parsed *once* (via `parse_qs`), so a handler declaring N
    `query_param` tokens shares one parse rather than re-decoding the query string
    per token. Both are handed in already-parsed, in the same spirit: the shell
    parses at the boundary and this value just holds the result. `body` is the
    fully-buffered request body (empty for a websocket handshake, which has none).
    One value, built once per request, fed to every extractor a handler declares.

    `scope` is `HttpScope | WebsocketScope` so a `query_param`/`header_param`
    token reads either (both carry `query_string` and `headers`). The whole-scope
    read is split into the protocol-specific `http_scope()`/`websocket_scope()`,
    since only those know the concrete type.
    """

    scope: HttpScope | WebsocketScope
    path_params: Mapping[str, object]
    query_params: Mapping[str, list[str]]
    body: bytes

    @classmethod
    def parsed(cls, scope: HttpScope | WebsocketScope, path_params: Mapping[str, object], body: bytes) -> Request:
        """Assemble a `Request`, parsing the scope's query string once at the boundary."""
        return cls(
            scope=scope,
            path_params=path_params,
            query_params=parse_qs(scope.query_string.decode()),
            body=body,
        )


# Covariant: `V` appears only in `extract`'s return, so `Extractor[int]` is an
# `Extractor[object]`. This is what lets `handle` collect a heterogeneous mix of
# extractors as `*extractors: Extractor[object]`. The legacy `TypeVar` is needed
# because PEP 695's inferred variance treats a (frozen) dataclass field as
# invariant; the variance is sound here, so we state it explicitly.
_V_co = TypeVar("_V_co", covariant=True)


@dataclass(frozen=True, slots=True)
class Extractor(Generic[_V_co]):  # noqa: UP046 - PEP 695 infers a frozen dataclass field as invariant; the covariant TypeVar is deliberate (see above)
    """
    A typed piece of a request, paired with the OpenAPI it contributes.

    `extract` is a pure `Request -> V` that raises to *reject* a bad request
    (a `catching` middleware's `recover` maps the raised type to a 4xx); it never
    decides which handler runs. The same value carries its own OpenAPI fragment,
    so a handler's parameter list and request body are *recovered* from the
    extractors it declares, never restated: one declaration, two consumers
    (parse and describe).
    """

    extract: Callable[[Request], _V_co]
    query: tuple[QueryParam, ...] = ()
    headers: tuple[HeaderParam, ...] = ()
    request_body: Body | None = None
    path: PathSpec | None = None


def path_param[V](name: str, converter: Converter[V]) -> Extractor[V]:
    """
    A typed path parameter: one value that is both a pattern segment and a read.

    The same `converter` the router matches the segment with also fixes the type
    `V` the handler receives, so there is no second place to keep in sync: drop
    this extractor into the route pattern (`("todos", path_param("id", INT))`)
    *and* into the handler's argument list, and the name, converter, schema, and
    type are all declared exactly once. The read casts the value the router's
    walk already parsed with this very converter, so the cast is sound.
    """
    spec = PathSpec(name=name, converter=converter)

    def extract(request: Request) -> V:
        return cast(V, request.path_params[name])

    return Extractor(extract, path=spec)


def catch_all(name: str, converter: Converter[str] = PATH) -> Extractor[str]:
    """
    A typed catch-all path parameter: the `{name:path}` form as a token.

    Consumes the rest of the target into one segment (always the final one); the
    sibling of `path_param` for the rest-of-path case.
    """
    spec = PathSpec(name=name, converter=converter, catch_all=True)

    def extract(request: Request) -> str:
        return cast(str, request.path_params[name])

    return Extractor(extract, path=spec)


def query_param[V](
    name: str, parse: Callable[[list[str]], V], *, schema: SchemaRef, required: bool = False
) -> Extractor[V]:
    """
    Parse a query parameter into `V`, given all of its raw values.

    `parse` receives the (possibly empty, possibly repeated) values for `name`
    and decides what their absence and multiplicity mean, returning `V` or
    raising to reject. The `schema` is this value's OpenAPI contribution.
    """

    def extract(request: Request) -> V:
        values = request.query_params.get(name, [])
        return parse(values)

    return Extractor(extract, query=(QueryParam(name=name, schema=schema, required=required),))


def header_param[V](
    name: str, parse: Callable[[list[bytes]], V], *, schema: SchemaRef, required: bool = False
) -> Extractor[V]:
    """
    Parse a request header into `V`, given all of its raw values.

    Header names are matched case-insensitively (ASGI lower-cases them); `parse`
    receives every value sent under `name`, in order, and returns `V` or raises.
    """
    wanted = name.lower().encode()

    def extract(request: Request) -> V:
        values = [value for key, value in request.scope.headers if key == wanted]
        return parse(values)

    return Extractor(extract, headers=(HeaderParam(name=name, schema=schema, required=required),))


def body[V](parse: Callable[[bytes], V], *, schema: SchemaRef, media_type: str = "application/json") -> Extractor[V]:
    """
    Parse the buffered request body into `V`.

    `parse` is injected so `without-web` stays serialization-agnostic: an app
    passes a pydantic model's `model_validate_json`, a dataclass loader, or any
    `bytes -> V`, and the matching `schema` is this value's OpenAPI request body.
    """

    def extract(request: Request) -> V:
        return parse(request.body)

    return Extractor(extract, request_body=Body(media_type=media_type, shape=Single(schema)))


def http_scope() -> Extractor[HttpScope]:
    """
    Hand an HTTP handler the unparsed `HttpScope`.

    The escape hatch that keeps "pass the scope down" and "parse parts of it"
    from competing: a handler composes `http_scope()` alongside parsed extractors
    and gets the raw connection facts as just another typed argument. Using it off
    an HTTP route is the invariant; the assert turns misuse on a websocket route
    into a loud error rather than a confusing `AttributeError` downstream.
    """

    def extract(request: Request) -> HttpScope:
        scope = request.scope
        assert isinstance(scope, HttpScope)
        return scope

    return Extractor(extract)


def websocket_scope() -> Extractor[WebsocketScope]:
    """
    Hand a websocket handler the unparsed `WebsocketScope`.

    The websocket sibling of `http_scope()`; the assert guards against using it on
    an HTTP route.
    """

    def extract(request: Request) -> WebsocketScope:
        scope = request.scope
        assert isinstance(scope, WebsocketScope)
        return scope

    return Extractor(extract)


# [[[cog import cog; from ladders import emit; cog.outl(emit("into")) ]]]
@overload
def into[M, A](
    make: Callable[[A], M],
    a: Extractor[A],
    /,
) -> Extractor[M]: ...


@overload
def into[M, A, B](
    make: Callable[[A, B], M],
    a: Extractor[A],
    b: Extractor[B],
    /,
) -> Extractor[M]: ...


@overload
def into[M, A, B, C](
    make: Callable[[A, B, C], M],
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    /,
) -> Extractor[M]: ...


@overload
def into[M, A, B, C, D](
    make: Callable[[A, B, C, D], M],
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    d: Extractor[D],
    /,
) -> Extractor[M]: ...


@overload
def into[M, A, B, C, D, E](
    make: Callable[[A, B, C, D, E], M],
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    d: Extractor[D],
    e: Extractor[E],
    /,
) -> Extractor[M]: ...


@overload
def into[M, A, B, C, D, E, F](
    make: Callable[[A, B, C, D, E, F], M],
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    d: Extractor[D],
    e: Extractor[E],
    f: Extractor[F],
    /,
) -> Extractor[M]: ...


@overload
def into[M, A, B, C, D, E, F, G](
    make: Callable[[A, B, C, D, E, F, G], M],
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    d: Extractor[D],
    e: Extractor[E],
    f: Extractor[F],
    g: Extractor[G],
    /,
) -> Extractor[M]: ...


@overload
def into[M, A, B, C, D, E, F, G, H](
    make: Callable[[A, B, C, D, E, F, G, H], M],
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    d: Extractor[D],
    e: Extractor[E],
    f: Extractor[F],
    g: Extractor[G],
    h: Extractor[H],
    /,
) -> Extractor[M]: ...


@overload
def into[M, A, B, C, D, E, F, G, H, J](
    make: Callable[[A, B, C, D, E, F, G, H, J], M],
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
) -> Extractor[M]: ...


@overload
def into[M, A, B, C, D, E, F, G, H, J, K](
    make: Callable[[A, B, C, D, E, F, G, H, J, K], M],
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
) -> Extractor[M]: ...
# [[[end]]]
def into[M](make: Callable[..., M], *extractors: Extractor[object]) -> Extractor[M]:
    """
    Combine several extractors into one that builds a typed value.

    The escape hatch from a handler's extractor-arity ceiling, and the way to
    parse a group of inputs into one model: `make` is the model's constructor (or
    any factory) and each extractor supplies one positional argument to it, in
    order, with the types tied so a mismatch is a mypy error. The constituents'
    OpenAPI fragments (query/header/body) are carried through; path parameters
    still appear in the route *pattern*, so their schema comes from there.

    This reuses the existing tokens rather than re-reading the request: pass the
    same `path_param`/`query_param` values you would otherwise hand the handler,
    plus the type that assembles them.

    `make` is called positionally, which a frozen dataclass or `NamedTuple`
    constructor accepts directly. For a pydantic model (whose `__init__` is
    keyword-only, and whose validators you want to run), pass a small factory
    that constructs it by keyword: `into(lambda a, b: M(x=a, y=b), ea, eb)`. A
    validator that rejects raises `ValidationError`, which the router's exception
    handlers map like any other parse failure.
    """

    def extract(request: Request) -> M:
        return make(*(extractor.extract(request) for extractor in extractors))

    return Extractor(
        extract,
        query=tuple(param for extractor in extractors for param in extractor.query),
        headers=tuple(param for extractor in extractors for param in extractor.headers),
        request_body=single_body(extractors),
    )


def single_body(extractors: tuple[Extractor[object], ...]) -> Body | None:
    """The at-most-one request body among a group of extractors (a build fault if more)."""
    bodies = [extractor.request_body for extractor in extractors if extractor.request_body is not None]
    if len(bodies) > 1:
        raise ValueError("more than one body extractor was combined")
    return bodies[0] if bodies else None
