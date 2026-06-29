from __future__ import annotations

from collections.abc import Callable
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field
from typing import Protocol
from typing import assert_never
from typing import runtime_checkable

from without_asgi import HttpHandler
from without_asgi import HttpScope

from without_web.patterns import CatchAll
from without_web.patterns import Literal
from without_web.patterns import Param
from without_web.patterns import Segment
from without_web.router import Endpoint
from without_web.router import Match
from without_web.router import Router
from without_web.router import _flatten
from without_web.router import _Methods

# A schema reference is either a pre-built JSON Schema (a mapping) or a type the
# injected `schema_for` resolves into one. `without-web` stays schema-library
# agnostic: an app supplies `model_json_schema` (pydantic) or a dataclass walker.
type SchemaRef = Mapping[str, object] | type
type SchemaFor = Callable[[type], Mapping[str, object]]


@dataclass(frozen=True, slots=True)
class QueryParam:
    name: str
    schema: SchemaRef
    required: bool = False


@dataclass(frozen=True, slots=True)
class HeaderParam:
    name: str
    schema: SchemaRef
    required: bool = False


@dataclass(frozen=True, slots=True)
class Single:
    """
    Body content that is one complete document validating against `schema`.

    Renders as OpenAPI's `schema`: the whole request/response body is this value.
    """

    schema: SchemaRef


@dataclass(frozen=True, slots=True)
class Sequence:
    """
    Body content that is a sequence of items, each validating against `item_schema`.

    Renders as OpenAPI 3.2's `itemSchema`, the description of a sequential media
    type (NDJSON, JSON Lines, `application/json-seq`, SSE `text/event-stream`,
    `multipart/mixed`, ...). `without-web` is agnostic to the framing on the wire:
    the application names it via `Body.media_type` and emits the bytes itself.
    This is documentation only; nothing on the runtime path reads it.
    """

    item_schema: SchemaRef


type Shape = Single | Sequence


@dataclass(frozen=True, slots=True)
class Body:
    """
    One `content` entry: a media type paired with the shape of its payload.

    The same value describes a request or a response body. `Single` renders a
    `schema`; `Sequence` renders an `itemSchema`.
    """

    media_type: str
    shape: Shape


@dataclass(frozen=True, slots=True)
class ResponseSpec:
    description: str = ""
    body: Body | None = None


@dataclass(frozen=True, slots=True)
class RouteSpec:
    """
    The handler-owned half of an endpoint's OpenAPI description.

    The router never sees the body or interprets the query, so it cannot be the
    source of those schemas: an endpoint declares them here, in the one place
    they are also parsed. `openapi` merges this with the router's
    path/method/path-param half.
    """

    summary: str = ""
    query: tuple[QueryParam, ...] = ()
    headers: tuple[HeaderParam, ...] = ()
    request_body: Body | None = None
    responses: Mapping[int, ResponseSpec] = field(default_factory=dict)


@runtime_checkable
class Describable(Protocol):
    def describe(self) -> RouteSpec: ...


@dataclass(frozen=True, slots=True)
class _Described[T]:
    endpoint: Endpoint[T, HttpScope, HttpHandler]
    spec: RouteSpec

    def __call__(self, state: T, match: Match[HttpScope]) -> HttpHandler:
        return self.endpoint(state, match)

    def describe(self) -> RouteSpec:
        return self.spec


def describe[T](
    spec: RouteSpec,
) -> Callable[[Endpoint[T, HttpScope, HttpHandler]], Endpoint[T, HttpScope, HttpHandler]]:
    """
    Attach a `RouteSpec` to an endpoint, making it self-describing.

    The same value the handler is built around (its body/response types) becomes
    its OpenAPI contribution: one declaration, two consumers. Reads as a
    decorator above `buffered`, so the endpoint stays a plain callable that also
    answers `describe()`.
    """

    def attach(endpoint: Endpoint[T, HttpScope, HttpHandler]) -> Endpoint[T, HttpScope, HttpHandler]:
        return _Described(endpoint, spec)

    return attach


def _no_schema_for(annotation: type) -> Mapping[str, object]:
    raise TypeError(f"no schema_for provided to resolve {annotation!r}; pass schema_for=... to openapi()")


def _resolve(reference: SchemaRef, schema_for: SchemaFor) -> Mapping[str, object]:
    return schema_for(reference) if isinstance(reference, type) else reference


def openapi[T](
    router: Router[T],
    *,
    title: str = "without-web",
    version: str = "0.0.0",
    schema_for: SchemaFor = _no_schema_for,
) -> dict[str, object]:
    """
    Merge a router into an OpenAPI 3.2 document, a pure transform of the table.

    The router contributes the half it owns (path, methods, path-param schemas
    from its converters); each endpoint that answers `describe()` contributes
    the half it owns (request body, query params, responses). Opaque mounts are
    black boxes and contribute nothing. The router only ever *asks*.
    """
    paths: dict[str, dict[str, object]] = {}
    for segments, leaf in _flatten(router.routes):
        if not isinstance(leaf, _Methods):
            continue
        item = paths.setdefault(_template(segments), {})
        path_params = _path_parameters(segments, schema_for)
        for method, endpoint in leaf.methods.items():
            item[method.lower()] = _operation(endpoint, path_params, schema_for)
    return {"openapi": "3.2.0", "info": {"title": title, "version": version}, "paths": paths}


def _template(segments: tuple[Segment, ...]) -> str:
    return "/" + "/".join(_segment_template(segment) for segment in segments)


def _segment_template(segment: Segment) -> str:
    match segment:
        case Literal(text):
            return text
        case Param(name, _) | CatchAll(name, _):
            return "{" + name + "}"
        case _ as unreachable:
            assert_never(unreachable)


def _path_parameters(segments: tuple[Segment, ...], schema_for: SchemaFor) -> list[dict[str, object]]:
    parameters: list[dict[str, object]] = []
    for segment in segments:
        match segment:
            case Literal():
                continue
            case Param(name, converter) | CatchAll(name, converter):
                schema = dict(converter.schema)
        parameters.append({"name": name, "in": "path", "required": True, "schema": schema})
    return parameters


def _operation(endpoint: object, path_params: list[dict[str, object]], schema_for: SchemaFor) -> dict[str, object]:
    operation: dict[str, object] = {"responses": {}}
    parameters = list(path_params)
    if not isinstance(endpoint, Describable):
        if parameters:
            operation["parameters"] = parameters
        return operation
    spec = endpoint.describe()
    if spec.summary:
        operation["summary"] = spec.summary
    parameters.extend(_query_parameter(param, schema_for) for param in spec.query)
    parameters.extend(_header_parameter(param, schema_for) for param in spec.headers)
    if parameters:
        operation["parameters"] = parameters
    if spec.request_body is not None:
        operation["requestBody"] = {"content": _content(spec.request_body, schema_for)}
    operation["responses"] = {
        str(status): _response(response, schema_for) for status, response in spec.responses.items()
    }
    return operation


def _query_parameter(param: QueryParam, schema_for: SchemaFor) -> dict[str, object]:
    return {
        "name": param.name,
        "in": "query",
        "required": param.required,
        "schema": dict(_resolve(param.schema, schema_for)),
    }


def _header_parameter(param: HeaderParam, schema_for: SchemaFor) -> dict[str, object]:
    return {
        "name": param.name,
        "in": "header",
        "required": param.required,
        "schema": dict(_resolve(param.schema, schema_for)),
    }


def _content(body: Body, schema_for: SchemaFor) -> dict[str, object]:
    return {body.media_type: _media_type_object(body.shape, schema_for)}


def _media_type_object(shape: Shape, schema_for: SchemaFor) -> dict[str, object]:
    match shape:
        case Single(schema):
            return {"schema": _resolve(schema, schema_for)}
        case Sequence(item_schema):
            return {"itemSchema": _resolve(item_schema, schema_for)}
        case _ as unreachable:
            assert_never(unreachable)


def _response(response: ResponseSpec, schema_for: SchemaFor) -> dict[str, object]:
    rendered: dict[str, object] = {"description": response.description}
    if response.body is not None:
        rendered["content"] = _content(response.body, schema_for)
    return rendered
