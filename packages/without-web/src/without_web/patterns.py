from __future__ import annotations

from dataclasses import dataclass

from without_web.converters import Converter


@dataclass(frozen=True, slots=True)
class PathSpec:
    """How a path-param extractor appears as a route segment.

    The bridge that lets one `path_param(...)` value be both a pattern segment
    (the router matches and schemas it through `converter`) and a typed read in
    the handler. `name` binds the segment; `catch_all` marks the rest-consuming
    form.
    """

    name: str
    converter: Converter[object]
    catch_all: bool = False


@dataclass(frozen=True, slots=True)
class Literal:
    """A segment matched verbatim."""

    text: str


@dataclass(frozen=True, slots=True)
class Param:
    """A single-segment typed parameter, carrying the converter that parses it."""

    name: str
    converter: Converter[object]


@dataclass(frozen=True, slots=True)
class CatchAll:
    """A parameter that consumes the rest of the target; always the last segment."""

    name: str
    converter: Converter[object]


type Segment = Literal | Param | CatchAll


def split_path(path: str) -> tuple[str, ...]:
    """Split a request target into its segments.

    Leading and trailing slashes are stripped, so `/` is the empty tuple and a
    trailing slash never produces an empty segment: `/users` and `/users/` both
    split to `("users",)`. Matching is therefore trailing-slash insensitive,
    because targets and the literal parts of patterns are split by this same
    function.
    """
    trimmed = path.strip("/")
    return tuple(trimmed.split("/")) if trimmed else ()
