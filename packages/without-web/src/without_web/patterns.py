from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Literal:
    """A segment matched verbatim."""

    text: str


@dataclass(frozen=True, slots=True)
class Param:
    """A single-segment typed parameter, `{name}` or `{name:converter}`."""

    name: str
    converter: str


@dataclass(frozen=True, slots=True)
class CatchAll:
    """A `{name:path}` parameter that consumes the rest of the target; always last."""

    name: str


type Segment = Literal | Param | CatchAll


def split_path(path: str) -> tuple[str, ...]:
    """Split a request target (or a pattern) into its segments.

    Leading and trailing slashes are stripped, so `/` is the empty tuple and a
    trailing slash never produces an empty segment: `/users` and `/users/` both
    split to `("users",)`. Matching is therefore trailing-slash insensitive,
    because patterns and targets are split by this same function.
    """
    trimmed = path.strip("/")
    return tuple(trimmed.split("/")) if trimmed else ()


def parse_pattern(pattern: str) -> tuple[Segment, ...]:
    """Parse a route pattern into typed segments.

    A whole segment wrapped in braces is a parameter: `{name}` defaults to the
    `str` converter, `{name:converter}` names one, and `{name:path}` is the
    catch-all, which MUST be the final segment. A brace anywhere else in a
    segment is a malformed pattern and raises, because partial-segment
    parameters are not supported.
    """
    segments = split_path(pattern)
    parsed = tuple(_parse_segment(piece) for piece in segments)
    for index, segment in enumerate(parsed):
        if isinstance(segment, CatchAll) and index != len(parsed) - 1:
            raise ValueError(f"a catch-all parameter must be the last segment in {pattern!r}")
    return parsed


def _parse_segment(piece: str) -> Segment:
    if not (piece.startswith("{") and piece.endswith("}")):
        if "{" in piece or "}" in piece:
            raise ValueError(f"a parameter must be a whole path segment, got {piece!r}")
        return Literal(piece)
    name, _, converter = piece[1:-1].partition(":")
    if not name.isidentifier():
        raise ValueError(f"invalid parameter name {name!r} in segment {piece!r}")
    if converter == "path":
        return CatchAll(name)
    return Param(name, converter or "str")
