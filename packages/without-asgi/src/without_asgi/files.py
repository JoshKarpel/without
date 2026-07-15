from __future__ import annotations

import asyncio
import mimetypes
from collections.abc import AsyncIterator
from pathlib import Path

from without_asgi.outbound import Outbound
from without_asgi.outbound import ResponseBody
from without_asgi.outbound import ResponseStart
from without_asgi.types import RawHeaders

_DEFAULT_CHUNK_SIZE = 65536
_OCTET_STREAM = "application/octet-stream"
_CONTENT_TYPE = b"content-type"
_CONTENT_LENGTH = b"content-length"


async def file_response(
    path: Path,
    *,
    status: int = 200,
    content_type: str | None = None,
    headers: RawHeaders = (),
    chunk_size: int = _DEFAULT_CHUNK_SIZE,
) -> AsyncIterator[Outbound]:
    """
    Stream a file as the `ResponseStart` + `ResponseBody` event stream a handler
    yields, with `Content-Type` and `Content-Length` filled in: guess the content
    type, compute the length, and chunk the bytes off the event loop into the
    `Outbound` events the framework already streams to `send`, so a large file is
    never slurped into one `bytes`.

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
            (_CONTENT_TYPE, resolved.encode("latin-1")),
            (_CONTENT_LENGTH, str(stat.st_size).encode("ascii")),
        ),
    )
    return _stream_file(path, start, chunk_size)


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
    yield ResponseBody(body=b"", more_body=False)
