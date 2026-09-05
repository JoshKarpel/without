from __future__ import annotations

import uuid
from collections.abc import Callable
from dataclasses import dataclass
from dataclasses import field
from enum import Enum
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
    excluded from comparison; nothing here keys a structure on a converter, so
    what equality means is a convenience rather than a correctness property.
    (`without-web`'s converter compares `parse` as well, because there a
    converter *is* the routing trie's branch key and two comparing equal merge.)
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


def choice[E: Enum](enum: type[E]) -> Converter[E]:
    """
    One member of `enum`, spelled on the command line as that member's value.

    Matching on the *value* rather than the member name is what lets the shell
    spelling and the Python identifier differ, which they usually want to
    (`--log-level warning` for `LogLevel.WARNING`). Values are compared as text,
    so a `StrEnum`, an `Enum` with string values, and an `IntEnum` all work.

    The placeholder is the alternation of the values (`[dev|prod]`), so the enum
    is the single place its members, their spellings, and the way `--help` and a
    rejection name them are declared.
    """
    by_value = {str(member.value): member for member in enum}
    if len(by_value) != len(tuple(enum)):
        raise ValueError(f"{enum.__name__} has two members whose values are spelled the same on a command line")

    def parse(raw: str) -> E:
        try:
            return by_value[raw]
        except KeyError as exc:
            raise ValueError(f"{raw!r} is not one of {', '.join(by_value)}") from exc

    return Converter(metavar=f"[{'|'.join(by_value)}]", parse=parse)


STR: Converter[str] = Converter(metavar="STR", parse=str)
INT: Converter[int] = Converter(metavar="INT", parse=int)
FLOAT: Converter[float] = Converter(metavar="FLOAT", parse=float)
UUID: Converter[uuid.UUID] = Converter(metavar="UUID", parse=uuid.UUID)
PATH: Converter[Path] = Converter(metavar="PATH", parse=Path)
BOOL: Converter[bool] = Converter(metavar="BOOL", parse=parse_boolean)
