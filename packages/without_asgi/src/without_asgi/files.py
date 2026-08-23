from __future__ import annotations

import asyncio
import mimetypes
from collections.abc import AsyncIterator
from collections.abc import Mapping
from datetime import UTC
from datetime import datetime
from errno import EINVAL
from errno import EISDIR
from os import strerror
from pathlib import Path
from stat import S_ISDIR
from stat import S_ISREG
from types import MappingProxyType
from typing import assert_never

from without_asgi.outbound import Outbound
from without_asgi.outbound import ResponseBody
from without_asgi.outbound import ResponseStart
from without_asgi.scope import HttpScope
from without_asgi.selection import Head
from without_asgi.selection import NotModified
from without_asgi.selection import Selection
from without_asgi.selection import Span
from without_asgi.selection import Unsatisfiable
from without_asgi.selection import Whole
from without_asgi.selection import http_date
from without_asgi.selection import selection_for
from without_asgi.types import RawHeaders

# How many bytes each pool-thread read pulls off a file. Public because `serve_asset`
# and `without-web`'s `static_files` take the same argument, and one definition beats
# three copies of a literal drifting apart.
DEFAULT_CHUNK_SIZE = 65536

_OCTET_STREAM = "application/octet-stream"
# The media type each coding `mimetypes.encodings_map` can report is itself carried as,
# for a file handed over encoded (see `_handed_over`). Brotli is the one entry with no
# registered type of its own, so `.br` falls through to `application/octet-stream`.
_OPAQUE_TYPES: Mapping[bytes, str] = MappingProxyType(
    {
        b"gzip": "application/gzip",
        b"bzip2": "application/x-bzip2",
        b"xz": "application/x-xz",
        b"compress": "application/x-compress",
    }
)
_CONTENT_TYPE = b"content-type"
_CONTENT_ENCODING = b"content-encoding"
_CONTENT_LENGTH = b"content-length"
_CONTENT_RANGE = b"content-range"
_ACCEPT_RANGES = b"accept-ranges"
_ETAG = b"etag"
_LAST_MODIFIED = b"last-modified"
_BYTES = b"bytes"
# Header codecs, named once: the content type is latin-1 (byte-transparent), the length ASCII
# digits. Hoisting the codec names keeps mutmut's codec-name mutations on these lines rather
# than the call site (see docs/contributing/mutation-testing.md).
_LATIN1 = "latin-1"
_ASCII = "ascii"


async def file_response(
    path: Path,
    *,
    status: int = 200,
    content_type: str | None = None,
    charset: str | None = "utf-8",
    headers: RawHeaders = (),
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> AsyncIterator[Outbound]:
    """
    Stream a file as the `ResponseStart` + `ResponseBody` event stream a handler
    yields, with `Content-Type` and `Content-Length` filled in: guess the content
    type, compute the length, and chunk the bytes off the event loop into the
    `Outbound` events the framework already streams to `send`, so a large file is
    never slurped into one `bytes`.

    This is the helper for content with **no cacheable identity**: a report you just
    rendered, a temp file zipped for this one response. Its validator would change on
    every request, so conditional requests would buy nothing and advertising
    `Accept-Ranges` would invite a follow-up for a file that may already be gone. For a
    file that persists, reach for `serve_file`, which answers `Range` and conditional
    requests; for a tree of assets, build an `Inventory` and use `serve_asset`.

    `file_response` is a coroutine, not an async generator: awaiting it does the
    `stat` up front, so a missing file raises `FileNotFoundError` (or `stat`'s
    other `OSError`s) *before* any `ResponseStart` is emitted. Nothing has been
    committed to the wire yet, so a handler can still turn the miss into a clean
    `404` (the parse-don't-validate move). Hand the returned stream back from a
    handler:

    ```python
    async def download(state, match) -> Reply:
        try:
            return await file_response(Path("/srv/report.pdf"))
        except FileNotFoundError:
            return Response(status=404, ...)
    ```

    `Content-Type` is guessed from the file suffix with `mimetypes.guess_file_type`,
    falling back to `application/octet-stream`; pass `content_type` to override it, and
    `charset` to name the encoding appended to a textual type (`utf-8`, as in
    `inventory`; `None` states none).

    The bytes are **handed over whole**, so a file whose suffixes name a content coding
    is described by the coding's own media type (`report.tar.gz` is
    `application/gzip`) and never by `content-encoding`. Declaring the coding is right
    for a *resource* stored encoded, which is what `serve_file` and `inventory` do, and
    wrong for a download: a conformant client decodes `content-encoding` transparently,
    so the user asks for `report.tar.gz` and saves raw tar bytes under that name. This
    is the Apache `AddEncoding .gz` problem, and it is why this helper, whose whole job
    is handing a file over, does not do it.

    Any `headers` given are prepended, for things like `content-disposition`. The
    body is read in `chunk_size` pieces via `asyncio.to_thread`, so neither the open
    nor the reads block the event loop. The file is opened only once streaming begins
    and is closed when the stream is exhausted, errored, or closed early (`make_asgi_app`
    closes an abandoned outbound stream, e.g. on a client disconnect mid-download).
    """
    stat = await asyncio.to_thread(path.stat)
    start = ResponseStart(
        status=status,
        headers=(
            *headers,
            (_CONTENT_TYPE, _handed_over(path, content_type, charset)),
            (_CONTENT_LENGTH, str(stat.st_size).encode(_ASCII)),
        ),
    )
    return _stream_file(path, start, chunk_size)


async def serve_file(
    scope: HttpScope,
    path: Path,
    *,
    etag: bytes | None = None,
    content_type: str | None = None,
    charset: str | None = "utf-8",
    headers: RawHeaders = (),
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> AsyncIterator[Outbound]:
    """
    Serve one named file as a response to `scope`, answering `Range` and conditional
    requests: `200`, `206` with a `Content-Range`, `304`, or `416`.

    The path is named by the handler, never derived from the request, so nothing here
    confines a key to a directory; to serve a *tree*, build an `Inventory` and use
    `serve_asset`, which never derives a path from request input either.

    Like `file_response` this is a coroutine, and the ordering is the point: the `stat`
    runs on `await`, so a missing file raises `FileNotFoundError` and a directory
    raises `IsADirectoryError` while nothing is on the wire, and `304` and `416` are
    decided before a status is committed.

    The derived validator is **weak** (`W/"<size>-<mtime>"`), because a filesystem's
    timestamp granularity can be coarser than the interval between two writes, so two
    different bodies can share a size and an `st_mtime_ns`. A weak validator fails the
    strong comparison `If-Range` requires (RFC 9110 §13.1.5), so a resumed download
    correctly restarts rather than splicing bytes from two versions. Pass `etag` when
    you hold something better, such as a content hash you already store; it is emitted
    verbatim, so quote it yourself and mark it `W/` only if it is genuinely weak.

    The media type is guessed from the file's suffixes, with `charset` naming the
    encoding appended to a textual one (`utf-8`, as in `inventory`; `None` states none).
    A file whose suffixes name a content coding as well (`logo.svgz`) is served with
    that `content-encoding`, since this serves a *resource* the client is to decode;
    `file_response`, whose job is handing a file over, deliberately does not.

    `headers` are prepended to both what a `200` announces and what a `304` repeats, so
    a policy header (`cache-control`, `x-content-type-options`, a CORP or CSP value) is
    applied to a revalidated response as well, which is where a browser reading it from
    cache needs it. Compose them with the `headers` module's helpers.

    The `304` repeats the `content-type` and any `content-encoding` alongside the
    validators, which is what lets a `compress()` above this tell a representation it
    would never have encoded from one it would have. See `Representation.revalidation`
    for why that matters to the next `If-Range`.
    """
    stat = await asyncio.to_thread(path.stat)
    if S_ISDIR(stat.st_mode):
        raise IsADirectoryError(EISDIR, strerror(EISDIR), str(path))
    if not S_ISREG(stat.st_mode):
        # A fifo, socket, or device has no length to declare and no bytes to seek in.
        # Refusing here keeps the failure ahead of the `ResponseStart`, where the old
        # code would have committed a `200` and then raised mid-body.
        raise OSError(EINVAL, "not a regular file", str(path))
    guessed, encoding = _declared(path, content_type, charset)
    modified = datetime.fromtimestamp(stat.st_mtime, UTC)
    validator = etag if etag is not None else _derived_etag(stat.st_size, stat.st_mtime_ns)
    validators = ((_ETAG, validator), (_LAST_MODIFIED, http_date(modified)))
    described = (*headers, (_CONTENT_TYPE, guessed), *encoding, *validators, (_ACCEPT_RANGES, _BYTES))
    revalidation = (*headers, (_CONTENT_TYPE, guessed), *encoding, *validators)
    selection = selection_for(
        size=stat.st_size,
        method=scope.method,
        request_headers=scope.headers,
        etag=validator,
        last_modified=modified,
    )
    return stream_selection(path, selection, stat.st_size, described, revalidation, chunk_size)


def guessed_type(path: Path, charset: str | None) -> tuple[bytes, bytes | None]:
    """
    The media type the file's suffixes name, and the content coding its bytes are
    already stored in.

    `guess_file_type` reports both, and dropping the second is how `logo.svgz` goes out
    as `image/svg+xml` with no `content-encoding`, i.e. gzip bytes a browser renders as
    SVG, and `bundle.tar.gz` as a bare `application/x-tar`. The coding is only claimed
    alongside a media type: a bare `archive.gz` guesses `(None, "gzip")`, which names an
    opaque archive rather than a gzip-encoded something, and declaring a coding for it
    would have the client silently unwrap the file it asked to download.

    A `charset` is appended to a textual media type, because text with no charset leaves
    the encoding to the recipient's guess, which is how a UTF-8 stylesheet ends up
    rendering as mojibake.
    """
    guessed, coding = mimetypes.guess_file_type(path)
    if guessed is None:
        return _OCTET_STREAM.encode(_LATIN1), None
    if charset is not None and guessed.startswith("text/"):
        guessed = f"{guessed}; charset={charset}"
    return guessed.encode(_LATIN1), None if coding is None else coding.encode(_LATIN1)


def size_and_mtime_token(size: int, mtime_ns: int) -> bytes:
    """An entity-tag token from a `stat` alone: size and mtime, hex, joined by a dash."""
    return b"%x-%x" % (size, mtime_ns)


def _declared(path: Path, override: str | None, charset: str | None) -> tuple[bytes, RawHeaders]:
    """
    The media type and coding headers a named file is served *as a resource* under.

    An explicit `content_type` suppresses the coding entirely. A caller naming the type
    is describing the bytes as they are, so a `.gz` served as `application/gzip` is a
    body to hand over whole, not one to unwrap.
    """
    if override is not None:
        return override.encode(_LATIN1), ()
    guessed, coding = guessed_type(path, charset)
    return guessed, () if coding is None else ((_CONTENT_ENCODING, coding),)


def _handed_over(path: Path, override: str | None, charset: str | None) -> bytes:
    """
    The media type a file is handed over *whole* under, with no coding ever declared.

    A file whose suffixes name a content coding is described by that **coding's** own
    media type instead, so `report.tar.gz` is `application/gzip` rather than the
    `application/x-tar` its bytes decode to. Naming the decoded type while sending
    encoded bytes and no `content-encoding` would describe a tar and send gzip; naming
    the coding would have the client decode in transit, which is the failure this whole
    helper avoids. The coding's own type is the only description that is true of the
    bytes on the wire.

    Keyed on the coding rather than on the file's final suffix, because a suffix
    resolves through the *system* mime database, which knows `.svgz` as
    `image/svg+xml`: the decoded type again, by another route.
    """
    if override is not None:
        return override.encode(_LATIN1)
    guessed, coding = guessed_type(path, charset)
    if coding is None:
        return guessed
    return _OPAQUE_TYPES.get(coding, _OCTET_STREAM).encode(_LATIN1)


def _derived_etag(size: int, mtime_ns: int) -> bytes:
    return b'W/"%s"' % size_and_mtime_token(size, mtime_ns)


def start_for(
    selection: Selection,
    size: int,
    described: RawHeaders,
    revalidation: RawHeaders,
) -> ResponseStart:
    """
    Turn a `Selection` into the `ResponseStart` that announces it.

    `described` carries what a `200` says about the representation (content type,
    validators, `Accept-Ranges`); `revalidation` carries what its caller decided a `304`
    should repeat, which is RFC 9110 §15.4.5's required fields and whatever else
    identifies the stored variant, so a bodyless answer does not describe content it is
    not sending.

    `Head` announces exactly what `Whole` does, including the `content-length` of the
    body a `GET` would carry (§9.3.2), and differs only in that no body follows.
    """
    match selection:
        case Whole() | Head():
            return ResponseStart(status=200, headers=(*described, (_CONTENT_LENGTH, b"%d" % size)))
        case Span() as span:
            return ResponseStart(
                status=206,
                headers=(
                    *described,
                    (_CONTENT_LENGTH, b"%d" % span.length),
                    (_CONTENT_RANGE, b"bytes %d-%d/%d" % (span.first, span.last, size)),
                ),
            )
        case NotModified():
            # No `content-length`: a 304 carries no content, so declaring a length
            # would describe a body that is not coming.
            return ResponseStart(status=304, headers=revalidation)
        case Unsatisfiable():
            return ResponseStart(
                status=416,
                headers=(
                    *revalidation,
                    (_CONTENT_LENGTH, b"0"),
                    (_CONTENT_RANGE, b"bytes */%d" % size),
                ),
            )
        case _ as unreachable:
            assert_never(unreachable)


def stream_selection(
    path: Path,
    selection: Selection,
    size: int,
    described: RawHeaders,
    revalidation: RawHeaders,
    chunk_size: int,
) -> AsyncIterator[Outbound]:
    """Emit the answer for `selection`, opening `path` only when bytes are owed."""
    start = start_for(selection, size, described, revalidation)
    match selection:
        case Whole():
            return _stream_file(path, start, chunk_size)
        case Span() as span:
            return _stream_span(path, start, chunk_size, span.first, span.length)
        case Head() | NotModified() | Unsatisfiable():
            return no_body(start)
        case _ as unreachable:
            assert_never(unreachable)


async def no_body(start: ResponseStart) -> AsyncIterator[Outbound]:
    """The event stream for an answer that carries no content: a `HEAD`, a `304`, or a `416`."""
    yield start
    yield ResponseBody(body=b"", more_body=False)  # pragma: no mutate - values equal the field defaults


# A module-level generator, not a `yield` in `file_response` and not a nested closure.
# The `stat` in `file_response` must run when the coroutine is awaited (so a missing file
# fails before any event is emitted), which rules out making `file_response` itself an
# async generator; keeping the body generator at module scope means it is not rebuilt on
# every call, which matters on a hot file-serving path.
async def _stream_file(path: Path, start: ResponseStart, chunk_size: int) -> AsyncIterator[Outbound]:
    yield start
    # `open` resolves the path and hits the inode, which can block on a slow/networked
    # filesystem, so it goes to a pool thread like the reads rather than running on the loop.
    handle = await asyncio.to_thread(path.open, "rb")
    with handle:
        # Each read goes to a pool thread because a regular file cannot be polled by the
        # event loop. The point of doing it one chunk at a time is that a pool thread is
        # held only *briefly*, for the read itself, and released while the chunk is written
        # to the (possibly slow) socket. Running the whole read loop in a single thread that
        # feeds a queue would pay fewer thread hops but pin a thread for the entire transfer,
        # capping concurrent downloads at the thread-pool size, the wrong trade for a server
        # facing many slow clients.
        while chunk := await asyncio.to_thread(handle.read, chunk_size):
            yield ResponseBody(body=chunk, more_body=True)
    yield ResponseBody(body=b"", more_body=False)  # pragma: no mutate - values equal the field defaults


# The ranged sibling of `_stream_file`, kept separate rather than folded in behind an
# offset and a counter. A range is the rare case and the whole-file loop is the hot one,
# so the common path keeps its bare `while chunk :=` rather than paying a `min` and a
# decrement per chunk to support a branch it never takes.
async def _stream_span(
    path: Path, start: ResponseStart, chunk_size: int, offset: int, count: int
) -> AsyncIterator[Outbound]:
    yield start
    handle = await asyncio.to_thread(path.open, "rb")
    with handle:
        await asyncio.to_thread(handle.seek, offset)
        remaining = count
        while remaining > 0:
            chunk = await asyncio.to_thread(handle.read, min(chunk_size, remaining))
            if not chunk:
                # The file shrank under us mid-transfer. The declared `Content-Length`
                # is already on the wire, so stopping quietly would frame a short body;
                # raising aborts the response instead.
                raise OSError(EINVAL, "file shrank while a range was being served", str(path))
            remaining -= len(chunk)
            yield ResponseBody(body=chunk, more_body=True)
    yield ResponseBody(body=b"", more_body=False)  # pragma: no mutate - values equal the field defaults
