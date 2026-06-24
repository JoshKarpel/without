from __future__ import annotations

from collections.abc import Callable
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from uuid import UUID


@dataclass(frozen=True, slots=True)
class Converter:
    """A path-segment parser paired with the JSON Schema it parses into.

    `parse` turns a single matched segment into a typed value, raising
    `ValueError` to *reject* a segment that does not fit (`{id:int}` against
    `"abc"`). Rejection is not a handler-side error: it makes that trie branch
    fail to match so the walk backtracks to a sibling, ultimately a 404 if
    nothing matches (parse, don't validate).

    `schema` is the half the router contributes to OpenAPI for a path parameter
    that uses this converter: the router owns the path-param schema because it
    owns the converter.

    Converters are values in a registry, injected into a router rather than
    hardcoded, so an app can add its own.
    """

    parse: Callable[[str], object]
    schema: Mapping[str, object]


def _to_str(segment: str) -> str:
    return segment


def _to_int(segment: str) -> int:
    return int(segment)


def _to_float(segment: str) -> float:
    return float(segment)


def _to_uuid(segment: str) -> UUID:
    return UUID(segment)


# `path` is the catch-all converter: the trie consumes the rest of the request
# target into one segment, so its `parse` is the identity on the joined string.
# An immutable mapping, so it is safe as a shared default on the router dataclasses.
DEFAULT_CONVERTERS: Mapping[str, Converter] = MappingProxyType(
    {
        "str": Converter(parse=_to_str, schema={"type": "string"}),
        "int": Converter(parse=_to_int, schema={"type": "integer"}),
        "float": Converter(parse=_to_float, schema={"type": "number"}),
        "uuid": Converter(parse=_to_uuid, schema={"type": "string", "format": "uuid"}),
        "path": Converter(parse=_to_str, schema={"type": "string"}),
    }
)
