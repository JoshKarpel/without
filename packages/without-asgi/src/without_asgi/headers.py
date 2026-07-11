from __future__ import annotations

from collections.abc import Iterable

from without_asgi.types import RawHeaders

# Pure `RawHeaders -> RawHeaders` (and read) helpers. `RawHeaders` is already the one
# header representation the ASGI spec fixes on both edges (`scope["headers"]` inbound,
# `ResponseStart`/`ClientRequest` outbound), so these operate on that value directly
# rather than wrapping it: no type to construct at reads or unwrap at writes, and the
# hot path keeps handing the same immutable tuple through untouched. HTTP field names
# are case-insensitive (RFC 9110), so every lookup lower-cases both sides, and the
# functions that produce headers store names lower-cased (valid on the wire, required
# by HTTP/2). Duplicates are preserved: `get_all` returns every value under a name,
# the reason `Set-Cookie` can't collapse to one comma-joined value.

__all__ = [
    "add",
    "first",
    "get_all",
    "merge",
    "remove",
    "replace",
    "subset",
]


def get_all(headers: RawHeaders, name: bytes) -> tuple[bytes, ...]:
    """Every value under `name` (case-insensitive), in order; `()` if absent."""
    wanted = name.lower()
    return tuple(value for key, value in headers if key.lower() == wanted)


def first(headers: RawHeaders, name: bytes) -> bytes | None:
    """
    The first value under `name` (case-insensitive), or `None` if absent.

    Use this only for *singleton* fields (`content-type`, `authorization`, `host`),
    where a second occurrence is a protocol violation and the first is the only
    value. For a list-valued field (`accept-encoding`, `cache-control`, and other
    comma-separated `#rule` fields) the value is *all* occurrences joined by commas,
    in order (RFC 9110 §5.2), so `first` silently drops the rest: read those with
    `get_all` and join the values yourself.
    """
    wanted = name.lower()
    return next((value for key, value in headers if key.lower() == wanted), None)


def add(headers: RawHeaders, name: bytes, value: bytes) -> RawHeaders:
    """Append `value` under `name`, keeping any values already present."""
    return (*headers, (name.lower(), value))


def remove(headers: RawHeaders, name: bytes) -> RawHeaders:
    """Drop every value under `name` (case-insensitive); idempotent."""
    wanted = name.lower()
    return tuple((key, value) for key, value in headers if key.lower() != wanted)


def replace(headers: RawHeaders, name: bytes, value: bytes) -> RawHeaders:
    """Set `name` to exactly `value`, dropping any values it already had."""
    return (*remove(headers, name), (name.lower(), value))


def subset(headers: RawHeaders, names: Iterable[bytes]) -> RawHeaders:
    """Keep only the given names (case-insensitive), preserving order and duplicates."""
    wanted = frozenset(name.lower() for name in names)
    return tuple((key, value) for key, value in headers if key.lower() in wanted)


def merge(base: RawHeaders, over: RawHeaders) -> RawHeaders:
    """Combine `base` and `over`; names present in `over` replace those in `base`."""
    overridden = frozenset(key.lower() for key, _ in over)
    kept = tuple((key, value) for key, value in base if key.lower() not in overridden)
    return kept + over
