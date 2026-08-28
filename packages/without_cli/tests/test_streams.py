from __future__ import annotations

import sys

import pytest
from without_cli import Streams
from without_cli import lines


class TestLines:
    @pytest.mark.parametrize(
        ("chunks", "expected"),
        [
            (["one\ntwo\n"], ["one\n", "two\n"]),
            (["on", "e\ntw", "o\n"], ["one\n", "two\n"]),
            (["one\ntwo"], ["one\n", "two"]),
            (["\n"], ["\n"]),
            ([], []),
            (["no newline at all"], ["no newline at all"]),
            (["a\r\nb\r\n"], ["a\r\n", "b\r\n"]),
        ],
    )
    def test_chunks_are_resplit_on_line_boundaries(self, chunks: list[str], expected: list[str]) -> None:
        assert list(lines(chunks)) == expected

    def test_a_line_split_across_many_chunks_is_rejoined(self) -> None:
        assert list(lines(["h", "e", "l", "l", "o", "\n"])) == ["hello\n"]


class TestCaptured:
    def test_a_bare_string_is_one_chunk(self) -> None:
        capture = Streams.captured("everything at once")
        assert list(capture.streams.stdin) == ["everything at once"]

    def test_an_iterable_controls_the_arrival_order(self) -> None:
        capture = Streams.captured(["first", "second"])
        assert list(capture.streams.stdin) == ["first", "second"]

    def test_the_buffers_read_back_what_was_written(self) -> None:
        capture = Streams.captured()
        capture.streams.stdout.write("out")
        capture.streams.stderr.write("err")
        assert (capture.stdout, capture.stderr) == ("out", "err")

    def test_standard_streams_are_the_real_ones(self) -> None:
        streams = Streams.standard()
        assert streams.stdout is sys.stdout
        assert streams.stderr is sys.stderr
        assert streams.stdin is sys.stdin
