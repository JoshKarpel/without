from __future__ import annotations

import asyncio
import mimetypes
from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from errno import EINVAL
from errno import EISDIR
from os import strerror
from pathlib import Path
from stat import S_ISDIR
from stat import S_ISREG
from typing import assert_never

from without_asgi.outbound import Outbound
from without_asgi.outbound import ResponseBody
from without_asgi.outbound import ResponseStart
from without_asgi.scope import HttpScope
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
_CONTENT_TYPE = b"content-type"
_CONTENT_LENGTH = b"content-length"
_CONTENT_RANGE = b"content-range"
_ACCEPT_RANGES = b"accept-ranges"
_ETAG = b"etag"
_LAST_MODIFIED = b"last-modified"
_CACHE_CONTROL = b"cache-control"
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
    falling back to `application/octet-stream`; pass `content_type` to override it.
    Any `headers` given are prepended, for things like `content-disposition`. The
    body is read in `chunk_size` pieces via `asyncio.to_thread`, so neither the open
    nor the reads block the event loop. The file is opened only once streaming begins
    and is closed when the stream is exhausted, errored, or closed early (`make_asgi_app`
    closes an abandoned outbound stream, e.g. on a client disconnect mid-download).
    """
    stat = await asyncio.to_thread(path.stat)
    resolved = content_type or mimetypes.guess_file_type(path)[0] or _OCTET_STREAM
    start = ResponseStart(
        status=status,
        headers=(
            *headers,
            (_CONTENT_TYPE, resolved.encode(_LATIN1)),
            (_CONTENT_LENGTH, str(stat.st_size).encode(_ASCII)),
        ),
    )
    return _stream_file(path, start, chunk_size)


async def serve_file(
    scope: HttpScope,
    path: Path,
    *,
    etag: bytes | None = None,
    cache_control: bytes | None = None,
    content_type: str | None = None,
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
    """
    stat = await asyncio.to_thread(path.stat)
    if S_ISDIR(stat.st_mode):
        raise IsADirectoryError(EISDIR, strerror(EISDIR), str(path))
    if not S_ISREG(stat.st_mode):
        # A fifo, socket, or device has no length to declare and no bytes to seek in.
        # Refusing here keeps the failure ahead of the `ResponseStart`, where the old
        # code would have committed a `200` and then raised mid-body.
        raise OSError(EINVAL, "not a regular file", str(path))
    guessed = content_type or mimetypes.guess_file_type(path)[0] or _OCTET_STREAM
    modified = datetime.fromtimestamp(stat.st_mtime, UTC)
    validator = etag if etag is not None else _derived_etag(stat.st_size, stat.st_mtime_ns)
    described = (
        *headers,
        (_CONTENT_TYPE, guessed.encode(_LATIN1)),
        (_ETAG, validator),
        (_LAST_MODIFIED, http_date(modified)),
        (_ACCEPT_RANGES, _BYTES),
        *_cache_control(cache_control),
    )
    revalidation = (
        *headers,
        (_ETAG, validator),
        (_LAST_MODIFIED, http_date(modified)),
        *_cache_control(cache_control),
    )
    selection = selection_for(
        size=stat.st_size,
        method=scope.method,
        request_headers=scope.headers,
        etag=validator,
        last_modified=modified,
    )
    return stream_selection(path, selection, stat.st_size, described, revalidation, chunk_size)


def _derived_etag(size: int, mtime_ns: int) -> bytes:
    # Size and modification time only. An `st_ino` here would leak a filesystem
    # internal into every response, which is what Apache's FileETag default did
    # (CVE-2003-1418).
    return b'W/"%x-%x"' % (size, mtime_ns)


def _cache_control(value: bytes | None) -> RawHeaders:
    return () if value is None else ((_CACHE_CONTROL, value),)


def start_for(
    selection: Selection,
    size: int,
    described: RawHeaders,
    revalidation: RawHeaders,
) -> ResponseStart:
    """
    Turn a `Selection` into the `ResponseStart` that announces it.

    `described` carries what a `200` says about the representation (content type,
    validators, `Accept-Ranges`); `revalidation` carries only what RFC 9110 §15.4.5
    requires a `304` to repeat, so a bodyless answer does not describe content it is
    not sending.
    """
    match selection:
        case Whole():
            return ResponseStart(status=200, headers=(*described, (_CONTENT_LENGTH, b"%d" % size)))
        case Span(first, last):
            return ResponseStart(
                status=206,
                headers=(
                    *described,
                    (_CONTENT_LENGTH, b"%d" % (last - first + 1)),
                    (_CONTENT_RANGE, b"bytes %d-%d/%d" % (first, last, size)),
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
        case Span(first, last):
            return _stream_span(path, start, chunk_size, first, last - first + 1)
        case NotModified() | Unsatisfiable():
            return no_body(start)
        case _ as unreachable:
            assert_never(unreachable)


async def no_body(start: ResponseStart) -> AsyncIterator[Outbound]:
    """The event stream for an answer that carries no content: a `304` or a `416`."""
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
