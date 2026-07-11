from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path

import pytest
from without import spool
from without_asgi import Outbound
from without_asgi import ResponseBody
from without_asgi import ResponseStart
from without_asgi import file_response


async def _collect(stream: AsyncIterator[Outbound]) -> list[Outbound]:
    return [event async for event in stream]


def _start(events: list[Outbound]) -> ResponseStart:
    start = events[0]
    assert isinstance(start, ResponseStart)
    return start


def _headers(events: list[Outbound]) -> dict[bytes, bytes]:
    return dict(_start(events).headers)


async def test_guesses_content_type_from_suffix(tmp_path: Path) -> None:
    path = tmp_path / "report.pdf"
    path.write_bytes(b"%PDF-1.7 body")

    events = await _collect(await file_response(path))

    assert _headers(events)[b"content-type"] == b"application/pdf"


async def test_content_type_override_wins_over_the_guess(tmp_path: Path) -> None:
    path = tmp_path / "report.pdf"
    path.write_bytes(b"not really a pdf")

    events = await _collect(await file_response(path, content_type="text/csv"))

    assert _headers(events)[b"content-type"] == b"text/csv"


async def test_unknown_suffix_falls_back_to_octet_stream(tmp_path: Path) -> None:
    path = tmp_path / "mystery.unknownext"
    path.write_bytes(b"opaque bytes")

    events = await _collect(await file_response(path))

    assert _headers(events)[b"content-type"] == b"application/octet-stream"


async def test_content_length_is_the_file_size(tmp_path: Path) -> None:
    path = tmp_path / "note.txt"
    path.write_bytes(b"eleven byte")  # 11 bytes

    events = await _collect(await file_response(path))

    assert _headers(events)[b"content-length"] == b"11"


async def test_default_status_is_200(tmp_path: Path) -> None:
    path = tmp_path / "note.txt"
    path.write_bytes(b"hi")

    events = await _collect(await file_response(path))

    assert _start(events).status == 200


async def test_status_can_be_overridden(tmp_path: Path) -> None:
    path = tmp_path / "note.txt"
    path.write_bytes(b"hi")

    events = await _collect(await file_response(path, status=206))

    assert _start(events).status == 206


async def test_extra_headers_are_prepended_before_the_computed_ones(tmp_path: Path) -> None:
    path = tmp_path / "report.pdf"
    path.write_bytes(b"body")

    events = await _collect(
        await file_response(path, headers=((b"content-disposition", b"attachment; filename=report.pdf"),))
    )

    names = [name for name, _ in _start(events).headers]
    assert names == [b"content-disposition", b"content-type", b"content-length"]


async def test_streams_the_body_in_chunks_ending_in_an_empty_final_body(tmp_path: Path) -> None:
    path = tmp_path / "data.bin"
    path.write_bytes(b"abcdefghij")  # 10 bytes

    events = await _collect(await file_response(path, chunk_size=4))

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

    events = await _collect(await file_response(path, chunk_size=100))

    body = b"".join(event.body for event in events if isinstance(event, ResponseBody))
    assert body == payload


async def test_empty_file_yields_start_then_only_the_terminating_body(tmp_path: Path) -> None:
    path = tmp_path / "empty.bin"
    path.write_bytes(b"")

    events = await _collect(await file_response(path))

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

    events = await _collect(spool(await file_response(path, chunk_size=100), ahead=2))

    assert isinstance(events[0], ResponseStart)
    assert events[-1] == ResponseBody(body=b"", more_body=False)
    body = b"".join(event.body for event in events if isinstance(event, ResponseBody))
    assert body == payload
