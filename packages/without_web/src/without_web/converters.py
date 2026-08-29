from __future__ import annotations

import uuid
from collections.abc import Callable
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field
from enum import Enum
from functools import cache
from typing import Generic
from typing import TypeVar

# Covariant: `V` appears only in `parse`'s return, so `Converter[int]` is a
# `Converter[object]`. That lets a path-param token store its converter as
# `Converter[object]` while `path_param` keeps the precise `V` for the handler.
# The legacy `TypeVar` is needed because PEP 695's inferred variance treats a
# (frozen) dataclass field as invariant; the variance is sound here.
_V_co = TypeVar("_V_co", covariant=True)


@dataclass(frozen=True, slots=True)
class Converter(Generic[_V_co]):  # noqa: UP046 - PEP 695 infers a frozen dataclass field as invariant; the covariant TypeVar is deliberate (see above)
    """
    A path-segment parser paired with the JSON Schema it parses into.

    `parse` turns a single matched segment into a typed value, raising
    `ValueError` to *reject* a segment that does not fit (`int` against `"abc"`).
    Rejection is not a handler-side error: it makes that trie branch fail to
    match so the walk backtracks to a sibling, ultimately a 404 if nothing
    matches (parse, don't validate).

    `schema` is the half the router contributes to OpenAPI for a path parameter
    that uses this converter: the router owns the path-param schema because it
    owns the converter.

    `name` is the converter's label, in rejection messages and as its OpenAPI
    parameter style; a typed-token pattern reuses the converter *value* directly
    (`path_param("id", INT)`), so the name, parse, type, and schema are all
    declared in one place.

    **Equality is `name` and `parse` together, because a converter is a trie
    key.** Two path-param segments merge into one branch when their converters
    compare equal, and the merged branch keeps one of them, so converters that
    compare equal must *behave* identically or a route is handed a value its own
    converter never produced. Comparing `parse` is what makes that structural:
    the shipped converters are module-level singletons, so `INT` used by two
    routes is the same object and merges as it should, while two converters built
    separately with distinct `parse` functions stay distinct however they are
    named. A converter *factory* should be `@cache`d on its inputs (see `choice`)
    so that calling it twice for the same thing returns the same value and merges.

    `schema` is excluded, and not merely because a `Mapping` is unhashable and a
    trie key must not be: it takes no part in matching, and OpenAPI reads it off
    the route's own segments rather than through the trie, so two branches
    merging has never affected a rendered schema.
    """

    name: str
    parse: Callable[[str], _V_co]
    schema: Mapping[str, object] = field(compare=False)


@cache
def choice[E: Enum](enum: type[E]) -> Converter[E]:
    """
    A path segment that must be one of `enum`'s values, parsed to its member.

    Matching on the *value* rather than the member name is what lets the URL
    spelling and the Python identifier differ (`/logs/warning` for
    `Level.WARNING`). Values are compared as text, so a `StrEnum`, an `Enum` with
    string values, and an `IntEnum` all work, and a segment outside the set
    rejects, which makes that trie branch fail to match rather than 400 later.

    The `schema` is OpenAPI's own `enum`, so documenting the allowed values costs
    nothing beyond declaring them once on the enum.

    **`@cache` is load-bearing, not an optimization.** A converter is a trie key
    compared by `name` and `parse` (see `Converter`), and each call would
    otherwise build a fresh closure, so `path_param("p", choice(Profile))` written
    in two modules would produce two converters that do not compare equal, and
    therefore two sibling branches matching identical segments instead of one.
    Both routes would still resolve, by backtracking, but a genuine duplicate
    between them would stop being caught at build time. Caching on the enum makes
    repeated calls return the *same* converter, so they merge as they should.
    Any converter built by a function wants the same treatment.
    """
    by_value = {str(member.value): member for member in enum}
    if len(by_value) != len(tuple(enum)):
        raise ValueError(f"{enum.__name__} has two members whose values are spelled the same in a path")

    def parse(raw: str) -> E:
        try:
            return by_value[raw]
        except KeyError as exc:
            raise ValueError(f"{raw!r} is not one of {', '.join(by_value)}") from exc

    return Converter(name=enum.__name__, parse=parse, schema={"type": "string", "enum": list(by_value)})


STR: Converter[str] = Converter(name="str", parse=str, schema={"type": "string"})
INT: Converter[int] = Converter(name="int", parse=int, schema={"type": "integer"})
FLOAT: Converter[float] = Converter(name="float", parse=float, schema={"type": "number"})
UUID: Converter[uuid.UUID] = Converter(name="uuid", parse=uuid.UUID, schema={"type": "string", "format": "uuid"})
# `path` is the catch-all converter: the trie consumes the rest of the request
# target into one segment, so its `parse` is the identity on the joined string.
# Security note: the value is the raw remaining path, *not* normalized. It can
# contain `..` and encoded separators, so an app that joins it onto a filesystem
# path, a proxy target, or a redirect URL MUST normalize and confine it first
# (e.g. resolve and check it stays within a base directory); the router itself
# never touches the filesystem, so it cannot do this for you.
PATH: Converter[str] = Converter(name="path", parse=str, schema={"type": "string"})
