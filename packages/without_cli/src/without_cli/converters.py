from __future__ import annotations

import uuid
from collections.abc import Callable
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path
from typing import Generic
from typing import TypeVar

# Covariant: `V` appears only in `parse`'s return, so `Converter[int]` is a
# `Converter[object]`. The legacy `TypeVar` is needed because PEP 695's inferred
# variance treats a (frozen) dataclass field as invariant; the variance is sound here.
_V_co = TypeVar("_V_co", covariant=True)


@dataclass(frozen=True, slots=True)
class Converter(Generic[_V_co]):  # noqa: UP046 - PEP 695 infers a frozen dataclass field as invariant; the covariant TypeVar is deliberate (see above)
    """
    A single-string parser paired with the placeholder that names it in usage.

    `parse` turns one raw token into a typed value, raising `ValueError` to
    *reject* it. A rejection is an ordinary outcome: the extractor that applied
    it raises `ExtractionError`, which `parse_argv` turns into a `Rejected`
    naming the parameter, never a traceback.

    `metavar` is the converter's identity and the placeholder shown in usage
    (`--port PORT` uses the option's own name, `INT` names the type), so a token
    that reuses a converter value declares its name, parse, type, and usage
    placeholder exactly once. Equality is by `metavar` alone, so `parse` is
    excluded from comparison.
    """

    metavar: str
    parse: Callable[[str], _V_co] = field(compare=False)


def parse_boolean(text: str) -> bool:
    """
    Parse the spellings a shell or a mounted file is likely to hold.

    Deliberately narrow: anything outside these two sets is a rejection rather
    than a silent `False`, because a `MYAPP_DEBUG=maybe` that quietly reads as
    off is worse than one that says it is not a boolean.
    """
    lowered = text.strip().lower()
    if lowered in {"1", "true", "yes", "on"}:
        return True
    if lowered in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"expected a boolean, got {text!r}")


STR: Converter[str] = Converter(metavar="STR", parse=str)
INT: Converter[int] = Converter(metavar="INT", parse=int)
FLOAT: Converter[float] = Converter(metavar="FLOAT", parse=float)
UUID: Converter[uuid.UUID] = Converter(metavar="UUID", parse=uuid.UUID)
PATH: Converter[Path] = Converter(metavar="PATH", parse=Path)
BOOL: Converter[bool] = Converter(metavar="BOOL", parse=parse_boolean)
