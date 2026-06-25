from __future__ import annotations

import uuid
from collections.abc import Callable
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field
from typing import Generic
from typing import TypeVar

# Covariant: `V` appears only in `parse`'s return, so `Converter[int]` is a
# `Converter[object]`. That lets a path-param token store its converter as
# `Converter[object]` while `path_param` keeps the precise `V` for the handler.
# The legacy `TypeVar` is needed because PEP 695's inferred variance treats a
# (frozen) dataclass field as invariant; the variance is sound here.
_V_co = TypeVar("_V_co", covariant=True)


@dataclass(frozen=True, slots=True)
class Converter(Generic[_V_co]):
    """A path-segment parser paired with the JSON Schema it parses into.

    `parse` turns a single matched segment into a typed value, raising
    `ValueError` to *reject* a segment that does not fit (`int` against `"abc"`).
    Rejection is not a handler-side error: it makes that trie branch fail to
    match so the walk backtracks to a sibling, ultimately a 404 if nothing
    matches (parse, don't validate).

    `schema` is the half the router contributes to OpenAPI for a path parameter
    that uses this converter: the router owns the path-param schema because it
    owns the converter.

    `name` is the converter's identity (and its OpenAPI parameter style); a
    typed-token pattern reuses the converter *value* directly
    (`path_param("id", INT)`), so the name, parse, type, and schema are all
    declared in one place. Equality and hashing are by `name` alone (a converter
    is a trie key), so `parse` and `schema` are excluded from comparison.
    """

    name: str
    parse: Callable[[str], _V_co] = field(compare=False)
    schema: Mapping[str, object] = field(compare=False)


STR: Converter[str] = Converter(name="str", parse=str, schema={"type": "string"})
INT: Converter[int] = Converter(name="int", parse=int, schema={"type": "integer"})
FLOAT: Converter[float] = Converter(name="float", parse=float, schema={"type": "number"})
UUID: Converter[uuid.UUID] = Converter(name="uuid", parse=uuid.UUID, schema={"type": "string", "format": "uuid"})
# `path` is the catch-all converter: the trie consumes the rest of the request
# target into one segment, so its `parse` is the identity on the joined string.
PATH: Converter[str] = Converter(name="path", parse=str, schema={"type": "string"})
