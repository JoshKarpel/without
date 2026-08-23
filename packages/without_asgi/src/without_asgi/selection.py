from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from email.utils import formatdate
from email.utils import parsedate_to_datetime

from without_asgi import headers
from without_asgi.types import RawHeaders

# The conditional-request (RFC 9110 §13) and range (§14) rules as one pure function of
# the representation's facts and the request's headers. Nothing here touches a
# filesystem, a socket, or a clock: `selection_for` takes a size and a pair of
# validators, so the same decision serves a file, an object store, or bytes held in
# memory, and the whole matrix tests as a table.

__all__ = [
    "NotModified",
    "Selection",
    "Span",
    "Unsatisfiable",
    "Whole",
    "http_date",
    "parse_http_date",
    "selection_for",
]

_READ_METHODS = frozenset({"GET", "HEAD"})
# 2**64 - 1 is 20 digits, so anything longer is not a range any client means. Bounding
# the length bounds the `int()` before it is attempted, rather than relying on CPython's
# own integer-parsing cap to notice.
_MAX_RANGE_DIGITS = 20
_ASCII = "ascii"
_LATIN1 = "latin-1"


@dataclass(frozen=True, slots=True)
class Whole:
    """The entire representation: a `200`."""


@dataclass(frozen=True, slots=True)
class Span:
    """
    One byte range, inclusive at both ends per RFC 9110 §14.1.2: a `206`.

    `last` is the index of the final byte, so a `Span(0, 0)` is one byte and
    `Content-Length` is `length`, not the size of the representation.
    """

    first: int
    last: int

    @property
    def length(self) -> int:
        return self.last - self.first + 1


@dataclass(frozen=True, slots=True)
class NotModified:
    """The client's cached copy is still current: a `304`, carrying no body."""


@dataclass(frozen=True, slots=True)
class Unsatisfiable:
    """The requested range lies outside the representation: a `416`."""


type Selection = Whole | Span | NotModified | Unsatisfiable

# The three field-less answers are values, not identities, so one instance of each is
# handed through rather than allocated per request.
_WHOLE = Whole()
_NOT_MODIFIED = NotModified()
_UNSATISFIABLE = Unsatisfiable()


def http_date(when: datetime) -> bytes:
    """Format `when` as an IMF-fixdate, the preferred form of RFC 9110 §5.6.7."""
    return formatdate(when.timestamp(), usegmt=True).encode(_ASCII)


def parse_http_date(raw: bytes) -> datetime | None:
    """
    Parse any of the three date forms RFC 9110 §5.6.7 requires a recipient to accept,
    returning `None` for a value that is not a date at all.

    The obsolete asctime form carries no zone, so `parsedate_to_datetime` hands back a
    naive value; HTTP dates are always UTC, so one is stamped as such rather than being
    left to mean whatever the server's local zone happens to be.
    """
    try:
        parsed = parsedate_to_datetime(raw.decode(_LATIN1))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def selection_for(
    *,
    size: int,
    method: str,
    request_headers: RawHeaders,
    etag: bytes | None,
    last_modified: datetime | None,
) -> Selection:
    """
    Decide what to send for a representation of `size` bytes carrying these validators.

    The precedence is RFC 9110 §13.2.2's, not one invented here: `If-None-Match` is
    evaluated before `If-Modified-Since` and *suppresses* it entirely when present, and
    `If-Range` gates whether a `Range` is honored at all. `etag` is the final header
    value, so a weak validator arrives `W/`-prefixed and the strong comparison
    `If-Range` requires (§13.1.5) fails on it by construction, which is what stops a
    client splicing a fresh range onto a stale prefix.

    Only **single** ranges are honored. A multi-range request needs a
    `multipart/byteranges` body, which is most of the implementation cost for a case
    almost nothing sends, and is the shape behind both
    [CVE-2011-3192](https://httpd.apache.org/security/CVE-2011-3192.txt) (one copy of
    the resource per range) and
    [CVE-2025-62727](https://github.com/Kludex/starlette/security/advisories/GHSA-7f5h-v6xp-fcq8)
    (quadratic range merging). §14 permits a server to ignore a `Range` it does not
    want to honor, so answering with the whole representation is conformant, and the
    check is a scan for a comma rather than a split, so a header naming a hundred
    thousand ranges costs one linear pass and allocates nothing.
    """
    if method not in _READ_METHODS:
        return _WHOLE
    if _is_current(request_headers, etag, last_modified):
        return _NOT_MODIFIED
    # §14.2: GET is the only method for which range handling is defined.
    if method != "GET":
        return _WHOLE
    spec = headers.first(request_headers, b"range")
    if spec is None or not _range_allowed(request_headers, etag, last_modified):
        return _WHOLE
    return _span_for(spec, size)


def _is_current(raw: RawHeaders, etag: bytes | None, last_modified: datetime | None) -> bool:
    candidates = _entity_tags(headers.get_all(raw, b"if-none-match"))
    if candidates:
        # §13.1.2: when `If-None-Match` is present, `If-Modified-Since` is not
        # evaluated at all, whether or not the tags matched.
        if b"*" in candidates:
            return True
        return etag is not None and any(_weakly_matches(candidate, etag) for candidate in candidates)
    since = headers.first(raw, b"if-modified-since")
    if since is None or last_modified is None:
        return False
    when = parse_http_date(since)
    # An HTTP date carries no sub-second part, so the comparison is made at the
    # resolution the validator was published with.
    return when is not None and int(last_modified.timestamp()) <= int(when.timestamp())


def _range_allowed(raw: RawHeaders, etag: bytes | None, last_modified: datetime | None) -> bool:
    condition = headers.first(raw, b"if-range")
    if condition is None:
        return True
    # §13.1.5: an entity-tag is told from a date by looking at the first two characters
    # for a DQUOTE.
    if condition.startswith((b'"', b"W/")):
        return etag is not None and _strongly_matches(condition, etag)
    when = parse_http_date(condition)
    return when is not None and last_modified is not None and int(last_modified.timestamp()) == int(when.timestamp())


def _span_for(spec: bytes, size: int) -> Selection:
    unit, separator, raw = spec.partition(b"=")
    if not separator or unit.strip().lower() != b"bytes":
        return _WHOLE  # §14.2: a range unit we do not understand is ignored.
    if b"," in raw:
        return _WHOLE  # See `selection_for`: single ranges only.
    first_raw, separator, last_raw = raw.strip().partition(b"-")
    if not separator:
        return _WHOLE
    if not first_raw:
        return _suffix_span(last_raw, size)
    first = _digits(first_raw)
    if first is None:
        return _WHOLE
    if first >= size:
        return _UNSATISFIABLE
    if not last_raw:
        return Span(first, size - 1)
    last = _digits(last_raw)
    if last is None:
        return _WHOLE
    if last < first:
        return _WHOLE  # §14.1.1: last-pos below first-pos is an invalid spec, so ignored.
    return Span(first, min(last, size - 1))


def _suffix_span(last_raw: bytes, size: int) -> Selection:
    suffix = _digits(last_raw)
    if suffix is None:
        return _WHOLE
    # A zero-length suffix names no bytes, and no range at all is satisfiable against an
    # empty representation.
    if suffix == 0 or size == 0:
        return _UNSATISFIABLE
    return Span(max(0, size - suffix), size - 1)


def _digits(raw: bytes) -> int | None:
    if not raw or len(raw) > _MAX_RANGE_DIGITS or not raw.isdigit():
        return None
    return int(raw)


def _entity_tags(values: tuple[bytes, ...]) -> tuple[bytes, ...]:
    """
    Split a list-valued conditional field into entity-tags, or `()` if it is malformed.

    Splitting on commas would be wrong: RFC 9110 §8.8.3 lets `etagc` hold a comma, so a
    tag can contain one and only the quoting says where a tag ends. A field that does
    not parse yields no tags, which reads as "no match" and serves the representation
    rather than failing the request.
    """
    joined = b",".join(values)
    tags: list[bytes] = []
    index = 0
    while index < len(joined):
        if joined[index : index + 1] in b" \t,":
            index += 1
            continue
        if joined[index : index + 1] == b"*":
            tags.append(b"*")
            index += 1
            continue
        start = index
        if joined.startswith(b"W/", index):
            index += 2
        if joined[index : index + 1] != b'"':
            return ()
        closing = joined.find(b'"', index + 1)
        if closing < 0:
            return ()
        index = closing + 1
        tags.append(joined[start:index])
    return tuple(tags)


def _opaque(tag: bytes) -> bytes:
    return tag[2:] if tag.startswith(b"W/") else tag


def _weakly_matches(candidate: bytes, current: bytes) -> bool:
    """§8.8.3.2 weak comparison: the opaque tags are equal, whatever their strength."""
    return _opaque(candidate) == _opaque(current)


def _strongly_matches(candidate: bytes, current: bytes) -> bool:
    """§8.8.3.2 strong comparison: equal tags, and neither of them weak."""
    return candidate == current and not candidate.startswith(b"W/")
