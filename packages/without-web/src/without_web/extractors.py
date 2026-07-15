from __future__ import annotations

from collections.abc import Callable
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Generic
from typing import Never
from typing import TypeVar
from typing import cast
from typing import overload
from urllib.parse import parse_qs

from without_asgi import HttpScope
from without_asgi import WebsocketScope
from without_asgi import headers

from without_web.converters import PATH
from without_web.converters import Converter
from without_web.openapi import Body
from without_web.openapi import HeaderParam
from without_web.openapi import QueryParam
from without_web.openapi import SchemaRef
from without_web.openapi import Single
from without_web.patterns import PathSpec


def _parse_query(scope: HttpScope | WebsocketScope) -> dict[str, tuple[str, ...]]:
    return {name: tuple(values) for name, values in parse_qs(scope.query_string.decode()).items()}


@dataclass(frozen=True, slots=True)
class RequestHead:
    """
    The parsed head of a request (or websocket handshake): everything an extractor
    reads *except* the body. The read-only, parsed-once context handed to each
    scope-derived extractor, and the *most general* context in the lattice below:
    `path_param`, `catch_all`, `query_param`, and `header_param` read only what is
    here, so they work on any route.

    `path_params` holds the path parameters the router already parsed during the
    trie walk (typed values, stored as `object`); `query_params` is the query
    string decoded and parsed *once* (via `parse_qs`), each name's values held as an
    immutable tuple, so a handler declaring N `query_param` tokens shares one parse
    rather than re-decoding the query string per token. `scope` is
    `HttpScope | WebsocketScope` so a `query_param`/`header_param` token reads either
    (both carry `query_string` and `headers`); the whole-scope read is split into the
    protocol-specific `http_scope()`/`websocket_scope()`, since only those know the
    concrete type. A `header_param` reads the scope's raw header pairs directly
    through the `without_asgi.headers` functions.

    The subtypes narrow the two facts a route fixes, so the wrong extractor on the
    wrong route is a *static* error rather than a runtime guard (see `Extractor`):

    - `HttpRequestHead` narrows `scope` to `HttpScope` (what `http_scope()` needs).
    - `WebsocketRequestHead` narrows `scope` to `WebsocketScope` (`websocket_scope()`).
    - `BufferedRequest` (an `HttpRequestHead`) adds the buffered `body` (`body()`).

    Each route builds exactly its concrete context: the buffered-HTTP path a
    `BufferedRequest`, the streaming-HTTP path an `HttpRequestHead`, a websocket a
    `WebsocketRequestHead`. This base is never built directly; it names the shared
    top so the permissive extractors can slot into all three.
    """

    scope: HttpScope | WebsocketScope
    path_params: Mapping[str, object]
    query_params: Mapping[str, tuple[str, ...]]


@dataclass(frozen=True, slots=True)
class HttpRequestHead(RequestHead):
    """
    A `RequestHead` whose `scope` is known to be an `HttpScope`: the context of any
    HTTP route (buffered or streaming). `http_scope()` reads its narrowed `scope`
    with no runtime check, and the streaming-HTTP path builds it directly.
    """

    scope: HttpScope

    @classmethod
    def parsed(cls, scope: HttpScope, path_params: Mapping[str, object]) -> HttpRequestHead:
        """Assemble an `HttpRequestHead`, parsing the scope's query string once at the boundary."""
        return HttpRequestHead(scope=scope, path_params=path_params, query_params=_parse_query(scope))


@dataclass(frozen=True, slots=True)
class WebsocketRequestHead(RequestHead):
    """
    A `RequestHead` whose `scope` is known to be a `WebsocketScope`: the context of a
    websocket route. `websocket_scope()` reads its narrowed `scope` with no runtime
    check, and the websocket path builds it directly.
    """

    scope: WebsocketScope

    @classmethod
    def parsed(cls, scope: WebsocketScope, path_params: Mapping[str, object]) -> WebsocketRequestHead:
        """Assemble a `WebsocketRequestHead`, parsing the scope's query string once at the boundary."""
        return WebsocketRequestHead(scope=scope, path_params=path_params, query_params=_parse_query(scope))


@dataclass(frozen=True, slots=True)
class BufferedRequest(HttpRequestHead):
    """
    An `HttpRequestHead` plus the fully-buffered request body, built only on the
    buffered-HTTP path. The `body` extractor's context is exactly this type, so a
    `body` token on a streaming or websocket route (which build a bodyless
    `HttpRequestHead`/`WebsocketRequestHead`) is a static type error, not a runtime
    guard.
    """

    body: bytes

    @classmethod
    def buffered(cls, scope: HttpScope, path_params: Mapping[str, object], body: bytes) -> BufferedRequest:
        """Assemble a `BufferedRequest`, parsing the query string and carrying the buffered body."""
        return cls(scope=scope, path_params=path_params, query_params=_parse_query(scope), body=body)


# `Extractor` is contravariant in its context `C` and covariant in its value `V`:
# both appear only through `extract: Callable[[C], V]` (C in an argument, V in the
# return). Contravariance in `C` is what makes the route-kind split work: a
# permissive `Extractor[RequestHead, V]` *is an* `Extractor[BufferedRequest, V]`
# (it needs less than the route provides), so it slots into a buffered handler,
# while a `body` token (`Extractor[BufferedRequest, V]`) is *not* an
# `Extractor[HttpRequestHead, V]`, so a streaming handler rejects it at type-check
# time. Covariance in `V` lets a handler collect a heterogeneous mix as
# `Extractor[Context, object]`. Legacy `TypeVar`s are needed because PEP 695's
# inferred variance treats a (frozen) dataclass field as invariant; both variances
# are sound here, so we state them explicitly.
_C_contra = TypeVar("_C_contra", contravariant=True)
_V_co = TypeVar("_V_co", covariant=True)

# An extractor of *any* context. `Never` is the bottom type, so by the
# contravariance above `Extractor[Never, V]` is a supertype of every
# `Extractor[C, V]`: the type of "an extractor, whatever request context it reads."
# The variadic implementations collect a heterogeneous mix this way; the `@overload`
# ladders restore the precise per-route context for callers.
type AnyExtractor = Extractor[Never, object]


@dataclass(frozen=True, slots=True)
class Extractor(Generic[_C_contra, _V_co]):  # noqa: UP046 - PEP 695 infers a frozen dataclass field as invariant; the explicit variances are deliberate (see above)
    """
    A typed piece of a request, paired with the OpenAPI it contributes.

    `extract` is a pure `C -> V` (where `C` is the request context it reads: the
    permissive `RequestHead`, or a narrower `HttpRequestHead`/`WebsocketRequestHead`/
    `BufferedRequest`) that raises to *reject* a bad request (a `catching`
    middleware's `recover` maps the raised type to a 4xx); it never decides which
    handler runs. The context parameter is what lets a handler refuse the wrong
    extractor for its route kind statically (a `body` on a streaming route, an
    `http_scope` on a websocket). The same value carries its own OpenAPI fragment,
    so a handler's parameter list and request body are *recovered* from the
    extractors it declares, never restated: one declaration, two consumers
    (parse and describe).
    """

    extract: Callable[[_C_contra], _V_co]
    query: tuple[QueryParam, ...] = ()
    headers: tuple[HeaderParam, ...] = ()
    request_body: Body | None = None
    path: PathSpec | None = None


class ExtractionError(ValueError):
    """
    A request rejected while one of its typed values was being extracted.

    The reject signal an extractor raises when a `parse` (a `once`/`optional`
    cardinality check, a converter, a pydantic model) refuses the input. It gathers
    at the raise site everything a `recover` policy needs to answer well: `field`
    names the request part that failed (a query/header parameter name, or `None` for
    the body), and `cause` carries the underlying error as a first-class value, so a
    policy matches `case ExtractionError(cause=ValidationError())` to answer a 422
    for an invalid body versus a 400 for a bad query/header value, without reaching
    into `__cause__`.

    A `ValueError` is the codebase's "reject" signal (the same one a converter
    raises to backtrack the trie walk), so the extractors turn one into a rich
    `ExtractionError`, and the router wraps any stray one left unattributed. Making
    the boundary a single matchable type is what lets a plain `ValueError` raised
    deeper in a handler still surface as a 500 rather than masquerading as a client
    error.
    """

    def __init__(self, message: str, *, field: str | None = None, cause: Exception | None = None) -> None:
        super().__init__(message)
        self.field = field
        self.cause = cause


def _parsed[X, V](field: str | None, parse: Callable[[X], V], value: X) -> V:
    """
    Apply `parse` to `value`, tagging a rejection with the `field` it came from.

    An extractor names the request part it reads, so it is the layer that can turn a
    bare `parse` `ValueError` into a `field`-attributed `ExtractionError` (carrying
    the original as `cause`). An `ExtractionError` a `parse` raises itself is already
    rich and passes through untouched.
    """
    try:
        return parse(value)
    except ExtractionError:
        raise
    except ValueError as exc:
        raise ExtractionError(str(exc), field=field, cause=exc) from exc


def path_param[V](name: str, converter: Converter[V]) -> Extractor[RequestHead, V]:
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

    def extract(head: RequestHead) -> V:
        return cast(V, head.path_params[name])

    return Extractor(extract, path=spec)


def catch_all(name: str, converter: Converter[str] = PATH) -> Extractor[RequestHead, str]:
    """
    A typed catch-all path parameter: the `{name:path}` form as a token.

    Consumes the rest of the target into one segment (always the final one); the
    sibling of `path_param` for the rest-of-path case.
    """
    spec = PathSpec(name=name, converter=converter, catch_all=True)

    def extract(head: RequestHead) -> str:
        return cast(str, head.path_params[name])

    return Extractor(extract, path=spec)


def query_param[V](
    name: str, parse: Callable[[tuple[str, ...]], V], *, schema: SchemaRef, required: bool = False
) -> Extractor[RequestHead, V]:
    """
    Parse a query parameter into `V`, given all of its raw values.

    `parse` receives the (possibly empty, possibly repeated) values for `name` as an
    immutable tuple and decides what their absence and multiplicity mean, returning
    `V` or raising a `ValueError` to reject, which becomes an `ExtractionError`
    naming this `name`. The `schema` is this value's OpenAPI contribution.
    """

    def extract(head: RequestHead) -> V:
        return _parsed(name, parse, head.query_params.get(name, ()))

    return Extractor(extract, query=(QueryParam(name=name, schema=schema, required=required),))


def header_param[V](
    name: str, parse: Callable[[tuple[bytes, ...]], V], *, schema: SchemaRef, required: bool = False
) -> Extractor[RequestHead, V]:
    """
    Parse a request header into `V`, given all of its raw values.

    Header names are matched case-insensitively; `parse` receives every value sent
    under `name` as an immutable tuple, in order, and returns `V` or raises a
    `ValueError` to reject, which becomes an `ExtractionError` naming this `name`.
    """
    wanted = name.encode()

    def extract(head: RequestHead) -> V:
        return _parsed(name, parse, headers.get_all(head.scope.headers, wanted))

    return Extractor(extract, headers=(HeaderParam(name=name, schema=schema, required=required),))


def once[E, V](parse: Callable[[E], V]) -> Callable[[tuple[E, ...]], V]:
    """
    Adapt a single-value `parse` into the tuple-taking form `query_param` and
    `header_param` expect, requiring the value to appear exactly once.

    Use it for a *singleton* field that must be present once: it raises `ValueError`
    when the value is absent or repeated (a duplicated singleton is a protocol
    violation, RFC 9110 §5.3) and otherwise applies `parse` to the sole value. For a
    genuinely list-valued field, skip this and let `parse` take every value.
    """

    def parse_once(values: tuple[E, ...]) -> V:
        match values:
            case (value,):
                return parse(value)
            case ():
                raise ValueError("expected exactly one value, got none")
            case _:
                raise ValueError(f"expected exactly one value, got {len(values)}")

    return parse_once


def optional[E, V](parse: Callable[[E], V]) -> Callable[[tuple[E, ...]], V | None]:
    """
    Like `once`, but for a field that may appear zero or one times.

    Returns `None` when the value is absent and `parse(value)` when it appears once;
    a repeated value still raises `ValueError` (a duplicated singleton is a protocol
    violation, RFC 9110 §5.3). Use it for an *optional* singleton field, and `once`
    when the field is required.
    """

    def parse_optional(values: tuple[E, ...]) -> V | None:
        match values:
            case ():
                return None
            case (value,):
                return parse(value)
            case _:
                raise ValueError(f"expected at most one value, got {len(values)}")

    return parse_optional


def body[V](
    parse: Callable[[bytes], V], *, schema: SchemaRef, media_type: str = "application/json"
) -> Extractor[BufferedRequest, V]:
    """
    Parse the buffered request body into `V`.

    `parse` is injected so `without-web` stays serialization-agnostic: an app
    passes a pydantic model's `model_validate_json`, a dataclass loader, or any
    `bytes -> V`, and the matching `schema` is this value's OpenAPI request body. A
    `parse` that raises to reject (a pydantic `ValidationError`, say) becomes an
    `ExtractionError` with no `field` (the body is unnamed), the original on `cause`.

    The buffered body lives on `BufferedRequest`, so that *is* this extractor's
    context: a `body` token on a bodyless streaming or websocket route is a static
    type error (its context is not the `HttpRequestHead`/`WebsocketRequestHead` those
    routes provide), not a runtime guard.
    """

    def extract(head: BufferedRequest) -> V:
        return _parsed(None, parse, head.body)

    return Extractor(extract, request_body=Body(media_type=media_type, shape=Single(schema)))


def http_scope() -> Extractor[HttpRequestHead, HttpScope]:
    """
    Hand an HTTP handler the unparsed `HttpScope`.

    The escape hatch that keeps "pass the scope down" and "parse parts of it"
    from competing: a handler composes `http_scope()` alongside parsed extractors
    and gets the raw connection facts as just another typed argument. Its context is
    `HttpRequestHead`, whose `scope` is already an `HttpScope`, so it reads it with
    no runtime check; using it on a websocket route (a `WebsocketRequestHead`) is a
    static type error.
    """

    def extract(head: HttpRequestHead) -> HttpScope:
        return head.scope

    return Extractor(extract)


def websocket_scope() -> Extractor[WebsocketRequestHead, WebsocketScope]:
    """
    Hand a websocket handler the unparsed `WebsocketScope`.

    The websocket sibling of `http_scope()`: its context is `WebsocketRequestHead`,
    so it reads the narrowed `scope` with no runtime check, and use on an HTTP route
    is a static type error.
    """

    def extract(head: WebsocketRequestHead) -> WebsocketScope:
        return head.scope

    return Extractor(extract)


# [[[cog import cog; from ladders import emit; cog.outl(emit("into")) ]]]
@overload
def into[R, M, A](
    make: Callable[[A], M],
    a: Extractor[R, A],
    /,
) -> Extractor[R, M]: ...


@overload
def into[R, M, A, B](
    make: Callable[[A, B], M],
    a: Extractor[R, A],
    b: Extractor[R, B],
    /,
) -> Extractor[R, M]: ...


@overload
def into[R, M, A, B, C](
    make: Callable[[A, B, C], M],
    a: Extractor[R, A],
    b: Extractor[R, B],
    c: Extractor[R, C],
    /,
) -> Extractor[R, M]: ...


@overload
def into[R, M, A, B, C, D](
    make: Callable[[A, B, C, D], M],
    a: Extractor[R, A],
    b: Extractor[R, B],
    c: Extractor[R, C],
    d: Extractor[R, D],
    /,
) -> Extractor[R, M]: ...


@overload
def into[R, M, A, B, C, D, E](
    make: Callable[[A, B, C, D, E], M],
    a: Extractor[R, A],
    b: Extractor[R, B],
    c: Extractor[R, C],
    d: Extractor[R, D],
    e: Extractor[R, E],
    /,
) -> Extractor[R, M]: ...


@overload
def into[R, M, A, B, C, D, E, F](
    make: Callable[[A, B, C, D, E, F], M],
    a: Extractor[R, A],
    b: Extractor[R, B],
    c: Extractor[R, C],
    d: Extractor[R, D],
    e: Extractor[R, E],
    f: Extractor[R, F],
    /,
) -> Extractor[R, M]: ...


@overload
def into[R, M, A, B, C, D, E, F, G](
    make: Callable[[A, B, C, D, E, F, G], M],
    a: Extractor[R, A],
    b: Extractor[R, B],
    c: Extractor[R, C],
    d: Extractor[R, D],
    e: Extractor[R, E],
    f: Extractor[R, F],
    g: Extractor[R, G],
    /,
) -> Extractor[R, M]: ...


@overload
def into[R, M, A, B, C, D, E, F, G, H](
    make: Callable[[A, B, C, D, E, F, G, H], M],
    a: Extractor[R, A],
    b: Extractor[R, B],
    c: Extractor[R, C],
    d: Extractor[R, D],
    e: Extractor[R, E],
    f: Extractor[R, F],
    g: Extractor[R, G],
    h: Extractor[R, H],
    /,
) -> Extractor[R, M]: ...


@overload
def into[R, M, A, B, C, D, E, F, G, H, J](
    make: Callable[[A, B, C, D, E, F, G, H, J], M],
    a: Extractor[R, A],
    b: Extractor[R, B],
    c: Extractor[R, C],
    d: Extractor[R, D],
    e: Extractor[R, E],
    f: Extractor[R, F],
    g: Extractor[R, G],
    h: Extractor[R, H],
    j: Extractor[R, J],
    /,
) -> Extractor[R, M]: ...


@overload
def into[R, M, A, B, C, D, E, F, G, H, J, K](
    make: Callable[[A, B, C, D, E, F, G, H, J, K], M],
    a: Extractor[R, A],
    b: Extractor[R, B],
    c: Extractor[R, C],
    d: Extractor[R, D],
    e: Extractor[R, E],
    f: Extractor[R, F],
    g: Extractor[R, G],
    h: Extractor[R, H],
    j: Extractor[R, J],
    k: Extractor[R, K],
    /,
) -> Extractor[R, M]: ...
# [[[end]]]
def into[M](make: Callable[..., M], *extractors: AnyExtractor) -> Extractor[Never, M]:
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

    # `head` is `Never` to match the `AnyExtractor` collection: the overloads pin
    # the real context (the meet of the constituents') for callers, and the route
    # hands this closure that concrete head at dispatch.
    def extract(head: Never) -> M:
        return make(*(extractor.extract(head) for extractor in extractors))

    return Extractor(
        extract,
        query=tuple(param for extractor in extractors for param in extractor.query),
        headers=tuple(param for extractor in extractors for param in extractor.headers),
        request_body=single_body(extractors),
    )


def single_body(extractors: tuple[AnyExtractor, ...]) -> Body | None:
    """The at-most-one request body among a group of extractors (a build fault if more)."""
    bodies = [extractor.request_body for extractor in extractors if extractor.request_body is not None]
    if len(bodies) > 1:
        raise ValueError("more than one body extractor was combined")
    return bodies[0] if bodies else None
