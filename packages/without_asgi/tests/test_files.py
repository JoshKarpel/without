from __future__ import annotations

import asyncio
import os
from datetime import UTC
from datetime import datetime
from errno import EINVAL
from errno import EISDIR
from pathlib import Path

import pytest
from pytest_mock import MockerFixture
from without import collect
from without import spool
from without_asgi import DEFAULT_CHUNK_SIZE
from without_asgi import Outbound
from without_asgi import RawHeaders
from without_asgi import ResponseBody
from without_asgi import ResponseStart
from without_asgi import file_response
from without_asgi import serve_file
from without_asgi.selection import http_date

from .helpers import a_scope


def _start(events: list[Outbound]) -> ResponseStart:
    start = events[0]
    assert isinstance(start, ResponseStart)
    return start


def _headers(events: list[Outbound]) -> dict[bytes, bytes]:
    return dict(_start(events).headers)


async def test_guesses_content_type_from_suffix(tmp_path: Path) -> None:
    path = tmp_path / "report.pdf"
    path.write_bytes(b"%PDF-1.7 body")

    events = await collect(await file_response(path))

    assert _headers(events)[b"content-type"] == b"application/pdf"


async def test_content_type_override_wins_over_the_guess(tmp_path: Path) -> None:
    path = tmp_path / "report.pdf"
    path.write_bytes(b"not really a pdf")

    events = await collect(await file_response(path, content_type="text/csv"))

    assert _headers(events)[b"content-type"] == b"text/csv"


async def test_unknown_suffix_falls_back_to_octet_stream(tmp_path: Path) -> None:
    path = tmp_path / "mystery.unknownext"
    path.write_bytes(b"opaque bytes")

    events = await collect(await file_response(path))

    assert _headers(events)[b"content-type"] == b"application/octet-stream"


async def test_a_suffix_naming_a_coding_declares_it_alongside_the_type(tmp_path: Path) -> None:
    # Dropping the coding `guess_file_type` reports is how gzip bytes come to be
    # labelled image/svg+xml, which a browser renders rather than decompressing.
    path = tmp_path / "logo.svgz"
    path.write_bytes(b"pretend these are gzip bytes")

    headers = _headers(await collect(await file_response(path)))

    assert headers[b"content-type"] == b"image/svg+xml"
    assert headers[b"content-encoding"] == b"gzip"


async def test_a_suffix_naming_only_a_coding_is_an_opaque_archive(tmp_path: Path) -> None:
    path = tmp_path / "archive.gz"
    path.write_bytes(b"pretend these are gzip bytes")

    headers = _headers(await collect(await file_response(path)))

    assert headers[b"content-type"] == b"application/octet-stream"
    assert b"content-encoding" not in headers


async def test_a_content_type_override_suppresses_the_coding(tmp_path: Path) -> None:
    # Naming the type describes the bytes as they are: a body to hand over whole,
    # not one the client should silently unwrap.
    path = tmp_path / "logo.svgz"
    path.write_bytes(b"pretend these are gzip bytes")

    headers = _headers(await collect(await file_response(path, content_type="application/gzip")))

    assert headers[b"content-type"] == b"application/gzip"
    assert b"content-encoding" not in headers


async def test_content_length_is_the_file_size(tmp_path: Path) -> None:
    path = tmp_path / "note.txt"
    path.write_bytes(b"eleven byte")  # 11 bytes

    events = await collect(await file_response(path))

    assert _headers(events)[b"content-length"] == b"11"


async def test_default_status_is_200(tmp_path: Path) -> None:
    path = tmp_path / "note.txt"
    path.write_bytes(b"hi")

    events = await collect(await file_response(path))

    assert _start(events).status == 200


async def test_status_can_be_overridden(tmp_path: Path) -> None:
    path = tmp_path / "note.txt"
    path.write_bytes(b"hi")

    events = await collect(await file_response(path, status=206))

    assert _start(events).status == 206


async def test_extra_headers_are_prepended_before_the_computed_ones(tmp_path: Path) -> None:
    path = tmp_path / "report.pdf"
    path.write_bytes(b"body")

    events = await collect(
        await file_response(path, headers=((b"content-disposition", b"attachment; filename=report.pdf"),))
    )

    names = [name for name, _ in _start(events).headers]
    assert names == [b"content-disposition", b"content-type", b"content-length"]


async def test_streams_the_body_in_chunks_ending_in_an_empty_final_body(tmp_path: Path) -> None:
    path = tmp_path / "data.bin"
    path.write_bytes(b"abcdefghij")  # 10 bytes

    events = await collect(await file_response(path, chunk_size=4))

    assert events[1:] == [
        ResponseBody(body=b"abcd", more_body=True),
        ResponseBody(body=b"efgh", more_body=True),
        ResponseBody(body=b"ij", more_body=True),
        ResponseBody(body=b"", more_body=False),
    ]


async def test_body_bytes_round_trip_intact(tmp_path: Path) -> None:
    payload = bytes(range(256)) * 3  # 768 bytes, spans several small chunks
    path = tmp_path / "data.bin"
    path.write_bytes(payload)

    events = await collect(await file_response(path, chunk_size=100))

    body = b"".join(event.body for event in events if isinstance(event, ResponseBody))
    assert body == payload


async def test_empty_file_yields_start_then_only_the_terminating_body(tmp_path: Path) -> None:
    path = tmp_path / "empty.bin"
    path.write_bytes(b"")

    events = await collect(await file_response(path))

    assert events[1:] == [ResponseBody(body=b"", more_body=False)]
    assert _headers(events)[b"content-length"] == b"0"


async def test_missing_file_raises_before_any_response_start(tmp_path: Path) -> None:
    missing = tmp_path / "gone.pdf"

    with pytest.raises(FileNotFoundError):
        await file_response(missing)


async def test_read_ahead_with_spool_preserves_the_response(tmp_path: Path) -> None:
    payload = bytes(range(256)) * 3  # 768 bytes, several 100-byte chunks
    path = tmp_path / "data.bin"
    path.write_bytes(payload)

    events = await collect(spool(await file_response(path, chunk_size=100), ahead=2))

    assert isinstance(events[0], ResponseStart)
    assert events[-1] == ResponseBody(body=b"", more_body=False)
    body = b"".join(event.body for event in events if isinstance(event, ResponseBody))
    assert body == payload


_REPORT = b"%PDF-1.7\n" + bytes(range(256)) * 4  # 1033 bytes, several chunks at any size


def _body(events: list[Outbound]) -> bytes:
    return b"".join(event.body for event in events if isinstance(event, ResponseBody))


async def _serve(
    path: Path,
    *,
    headers: RawHeaders = (),
    method: str = "GET",
    etag: bytes | None = None,
    response_headers: RawHeaders = (),
    content_type: str | None = None,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> tuple[ResponseStart, bytes]:
    scope = a_scope(path="/report.pdf", headers=headers, method=method)
    events = await collect(
        await serve_file(
            scope,
            path,
            etag=etag,
            headers=response_headers,
            content_type=content_type,
            chunk_size=chunk_size,
        )
    )
    return _start(events), _body(events)


@pytest.fixture
def report(tmp_path: Path) -> Path:
    path = tmp_path / "report.pdf"
    path.write_bytes(_REPORT)
    return path


@pytest.fixture
def report_stat(report: Path) -> os.stat_result:
    """The report's `stat`, taken here so an async test body does no blocking file I/O."""
    return report.stat()


class TestServeFile:
    async def test_a_plain_request_is_a_200_advertising_ranges(self, report: Path) -> None:
        start, body = await _serve(report)

        assert start.status == 200
        assert body == _REPORT
        assert dict(start.headers)[b"accept-ranges"] == b"bytes"
        assert dict(start.headers)[b"content-length"] == b"%d" % len(_REPORT)

    async def test_the_content_type_is_guessed_from_the_suffix(self, report: Path) -> None:
        start, _body = await _serve(report)

        assert dict(start.headers)[b"content-type"] == b"application/pdf"

    async def test_an_unguessable_suffix_falls_back_to_octet_stream(self, tmp_path: Path) -> None:
        path = tmp_path / "data.unknownext"
        path.write_bytes(_REPORT)

        start, _body = await _serve(path)

        assert dict(start.headers)[b"content-type"] == b"application/octet-stream"

    async def test_the_derived_validator_is_lowercase_hex_of_size_and_mtime(
        self, report: Path, report_stat: os.stat_result
    ) -> None:
        start, _body = await _serve(report)

        assert dict(start.headers)[b"etag"] == b'W/"%x-%x"' % (report_stat.st_size, report_stat.st_mtime_ns)

    async def test_a_timestamp_condition_revalidates_too(self, report: Path, report_stat: os.stat_result) -> None:
        modified = datetime.fromtimestamp(report_stat.st_mtime, UTC)

        start, body = await _serve(report, headers=((b"if-modified-since", http_date(modified)),))

        assert (start.status, body) == (304, b"")

    async def test_a_head_is_described_like_the_get_but_sends_nothing(self, report: Path) -> None:
        start, body = await _serve(report, method="HEAD")

        assert (start.status, body) == (200, b"")
        # §9.3.2: the head describes the body a GET would carry.
        assert dict(start.headers)[b"content-length"] == b"%d" % len(_REPORT)

    async def test_a_head_never_opens_the_file(self, report: Path, mocker: MockerFixture) -> None:
        # Answered as `Whole`, a HEAD reads the whole file and streams it for the
        # transport to drop, so every `curl -I` costs a full read.
        opened = mocker.patch.object(Path, "open", autospec=True)

        await _serve(report, method="HEAD")

        opened.assert_not_called()

    async def test_a_head_still_revalidates(self, report: Path) -> None:
        etag = dict((await _serve(report))[0].headers)[b"etag"]

        start, _body = await _serve(report, method="HEAD", headers=((b"if-none-match", etag),))

        assert start.status == 304

    async def test_a_stored_coding_is_declared_and_repeated_on_the_304(self, tmp_path: Path) -> None:
        path = tmp_path / "logo.svgz"
        path.write_bytes(_REPORT)
        scope = a_scope(path="/logo.svgz")
        start = _start(await collect(await serve_file(scope, path)))
        etag = dict(start.headers)[b"etag"]

        revalidated = _start(
            await collect(await serve_file(a_scope(path="/logo.svgz", headers=((b"if-none-match", etag),)), path))
        )

        assert dict(start.headers)[b"content-encoding"] == b"gzip"
        assert (revalidated.status, dict(revalidated.headers)[b"content-encoding"]) == (304, b"gzip")

    async def test_a_304_repeats_the_caching_policy(self, report: Path) -> None:
        etag = dict((await _serve(report))[0].headers)[b"etag"]

        start, _body = await _serve(
            report,
            response_headers=((b"cache-control", b"no-cache"),),
            headers=((b"if-none-match", etag),),
        )

        assert dict(start.headers)[b"cache-control"] == b"no-cache"

    async def test_a_whole_file_is_chunked_at_the_requested_size(self, report: Path) -> None:
        scope = a_scope(path="/report.pdf")
        events = await collect(await serve_file(scope, report, chunk_size=64))
        bodies = [event for event in events if isinstance(event, ResponseBody)]

        assert len(bodies) == len(_REPORT) // 64 + 2  # the partial tail, then the terminator
        assert b"".join(event.body for event in bodies) == _REPORT

    async def test_the_derived_validator_is_weak(self, report: Path) -> None:
        # The filesystem's timestamp granularity can be coarser than the gap between two
        # writes, so a size-and-mtime tag cannot claim to be strong.
        start, _body = await _serve(report)

        assert dict(start.headers)[b"etag"].startswith(b'W/"')

    async def test_a_supplied_validator_is_emitted_verbatim(self, report: Path) -> None:
        start, _body = await _serve(report, etag=b'"from-a-content-hash"')

        assert dict(start.headers)[b"etag"] == b'"from-a-content-hash"'

    async def test_a_matching_validator_is_a_bodyless_304(self, report: Path) -> None:
        etag = dict((await _serve(report))[0].headers)[b"etag"]

        start, body = await _serve(report, headers=((b"if-none-match", etag),))

        assert start.status == 304
        assert body == b""
        assert b"content-length" not in dict(start.headers)

    async def test_a_range_is_a_206_framing_only_the_span(self, report: Path) -> None:
        start, body = await _serve(report, headers=((b"range", b"bytes=100-199"),))

        assert start.status == 206
        assert body == _REPORT[100:200]
        assert dict(start.headers)[b"content-range"] == b"bytes 100-199/%d" % len(_REPORT)
        assert dict(start.headers)[b"content-length"] == b"100"

    async def test_a_suffix_range_takes_the_tail(self, report: Path) -> None:
        start, body = await _serve(report, headers=((b"range", b"bytes=-16"),))

        assert start.status == 206
        assert body == _REPORT[-16:]

    async def test_a_span_crossing_several_chunks_is_reassembled(self, report: Path) -> None:
        start, body = await _serve(report, headers=((b"range", b"bytes=5-1000"),), chunk_size=64)

        assert start.status == 206
        assert body == _REPORT[5:1001]

    async def test_a_span_ending_one_byte_past_a_chunk_boundary_is_complete(self, report: Path) -> None:
        # 65 bytes at a chunk size of 64 leaves exactly one byte owed after the first
        # read, which is where an off-by-one in the loop bound drops the tail while the
        # declared Content-Length still promises it.
        start, body = await _serve(report, headers=((b"range", b"bytes=0-64"),), chunk_size=64)

        assert start.status == 206
        assert body == _REPORT[:65]
        assert dict(start.headers)[b"content-length"] == b"65"

    async def test_every_chunk_but_the_last_says_more_is_coming(self, report: Path) -> None:
        # Joining the bodies would pass even if the stream told the transport to stop
        # after the first chunk, so the framing flags are asserted rather than the bytes.
        scope = a_scope(path="/report.pdf", headers=((b"range", b"bytes=0-199"),))
        events = await collect(await serve_file(scope, report, chunk_size=64))
        bodies = [event for event in events if isinstance(event, ResponseBody)]

        assert [event.more_body for event in bodies] == [True, True, True, True, False]

    async def test_an_unsatisfiable_range_is_a_416_naming_the_size(self, report: Path) -> None:
        start, body = await _serve(report, headers=((b"range", b"bytes=99999-"),))

        assert start.status == 416
        assert body == b""
        assert dict(start.headers)[b"content-range"] == b"bytes */%d" % len(_REPORT)
        # A 416 carries no content, and says so with a length rather than leaving the
        # transport to pick a framing.
        assert dict(start.headers)[b"content-length"] == b"0"

    async def test_a_missing_file_raises_before_any_response_start(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError):
            await _serve(tmp_path / "gone.pdf")

    @pytest.mark.security("a directory is refused before a 200 is committed to the wire")
    async def test_a_directory_raises_rather_than_failing_mid_body(self, tmp_path: Path) -> None:
        # `file_response` has no such check, so a directory there stats fine, emits a
        # `200`, and only then raises out of the already-started body.
        with pytest.raises(IsADirectoryError) as raised:
            await _serve(tmp_path)

        # An error that does not name the file it is about sends the reader looking.
        assert raised.value.filename == str(tmp_path)
        assert raised.value.errno == EISDIR

    async def test_given_headers_are_emitted_on_the_response(self, report: Path) -> None:
        start, _body = await _serve(report, response_headers=((b"cache-control", b"no-cache"),))

        assert dict(start.headers)[b"cache-control"] == b"no-cache"

    async def test_a_content_type_override_is_honored(self, report: Path) -> None:
        start, _body = await _serve(report, content_type="application/x-custom")

        assert dict(start.headers)[b"content-type"] == b"application/x-custom"

    async def test_a_non_regular_file_is_refused_before_a_status_is_committed(
        self, report: Path, mocker: MockerFixture
    ) -> None:
        # Windows can make no fifo, socket, or device, and coverage runs on every
        # platform, so the mode is simulated here and the POSIX test below is the
        # control confirming a real fifo takes the same path.
        mocker.patch("without_asgi.files.S_ISREG", return_value=False)

        with pytest.raises(OSError, match="not a regular file") as raised:
            await _serve(report)

        assert raised.value.filename == str(report)
        assert raised.value.errno == EINVAL

    @pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="a fifo needs a POSIX filesystem")
    async def test_a_real_fifo_is_refused(self, tmp_path: Path) -> None:
        pipe = tmp_path / "pipe"
        os.mkfifo(pipe)

        with pytest.raises(OSError, match="not a regular file"):
            await _serve(pipe)

    @pytest.mark.security("a file truncated mid-transfer aborts rather than framing a short body")
    async def test_a_file_that_shrinks_mid_range_aborts_the_response(self, report: Path) -> None:
        # The stat, the selection, and the declared Content-Length all happen on the
        # await; the reads happen as the stream is drained. Truncating in between is
        # therefore deterministic, and is exactly the race the guard exists for.
        scope = a_scope(path="/report.pdf", headers=((b"range", b"bytes=0-1000"),))
        stream = await serve_file(scope, report, chunk_size=64)
        await asyncio.to_thread(report.write_bytes, b"much shorter")

        with pytest.raises(OSError, match="file shrank") as raised:
            await collect(stream)

        assert raised.value.filename == str(report)
        assert raised.value.errno == EINVAL
