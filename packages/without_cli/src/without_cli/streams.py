from __future__ import annotations

import io
import sys
from collections.abc import Iterable
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Protocol


class Writer(Protocol):
    """
    The part of a text output stream a command writes to.

    `flush` is here because a CLI's stdout is line-buffered on a terminal and
    *block*-buffered down a pipe, so a long-running command's progress would
    otherwise appear all at once when it exits. A command that writes
    incrementally has to say when its output should be visible, and only it
    knows.

    `sys.stdout` and `io.StringIO` both satisfy this as they are, so a test needs
    no wrapper; flushing a `StringIO` is a no-op that keeps its contents.
    """

    def write(self, text: str, /) -> int: ...

    def flush(self) -> None: ...


@dataclass(frozen=True, slots=True)
class Streams:
    """
    The three standard streams, handed to every command as an argument.

    Injecting them rather than letting a command reach for `sys.stdout` is what
    makes output testable without capturing a process: a test passes
    `Streams.captured()` and reads the buffers back, with no module global
    monkeypatched and no subprocess run. It is also what keeps this layer out of
    the encoding decision, since `without-cli` never writes anything a command
    did not write itself.

    `stdin` is an iterable of chunks rather than a string, because input arrives
    over time: a filter reading a pipe should see each line as it lands, not wait
    for the writer upstream to close. Iterating the real `sys.stdin` yields
    lines; a test supplies a list or a generator and controls the arrival order
    exactly. It is a consume-once *place* rather than a value (the same reason
    `without-web` passes an inbound stream as an argument instead of making it an
    extractor), so a command that iterates it twice sees nothing the second time.

    Iterating `sys.stdin` blocks, which for a CLI is usually right (there is
    nothing else to do) but stalls the event loop for a command that reads input
    *while* doing something else. That command wraps this in
    `without_streams.stream_from_blocking`, which runs the iteration on a worker
    thread. The plain iterable stays the type here so the common case pays
    nothing and `without-cli` needs no dependency for it.
    """

    stdin: Iterable[str]
    stdout: Writer
    stderr: Writer

    @classmethod
    def standard(cls) -> Streams:
        """The real process streams. The one place `sys` is touched."""
        return cls(stdin=sys.stdin, stdout=sys.stdout, stderr=sys.stderr)

    @classmethod
    def captured(cls, stdin: str | Iterable[str] = ()) -> Capture:
        """
        In-memory streams plus the buffers behind them, for tests.

        A bare string is one chunk, which is what a test asserting on whole input
        wants; pass a list or a generator to control how the input is split and
        when each piece arrives.
        """
        chunks: Iterable[str] = (stdin,) if isinstance(stdin, str) else stdin
        out = io.StringIO()
        err = io.StringIO()
        return Capture(streams=cls(stdin=chunks, stdout=out, stderr=err), out=out, err=err)


@dataclass(frozen=True, slots=True)
class Capture:
    """`Streams` writing into buffers a test can read back, from `Streams.captured`."""

    streams: Streams
    out: io.StringIO
    err: io.StringIO

    @property
    def stdout(self) -> str:
        return self.out.getvalue()

    @property
    def stderr(self) -> str:
        return self.err.getvalue()


def _terminated(line: str) -> bool:
    r"""
    Whether one element of a `keepends=True` split ends with a line terminator.

    Asked of `splitlines` rather than of `"\n"`, because it breaks on a dozen
    characters (`\v`, `\f`, `\x85`, the line and paragraph separators, the file
    and group separators), and a fragment ending in one of those is a line that
    *ended*, not one still arriving. Splitting it again is what tells the two
    apart: a terminator is the only thing `keepends=False` drops.
    """
    return line.splitlines()[0] != line


def lines(chunks: Iterable[str]) -> Iterator[str]:
    """
    Re-split a stream of arbitrary chunks into lines, keeping the terminators.

    Iterating `sys.stdin` already yields lines, but a chunk from a pipe, a socket,
    or a test can split anywhere, so a command that means "per line" says so with
    this rather than assuming its chunks arrived pre-split. A trailing fragment
    with no newline is yielded when the input ends.
    """
    held: list[str] = []
    for chunk in chunks:
        # An empty chunk (a zero-length read, or `Streams.captured("")`) carries no
        # boundary, and `splitlines` on an empty string yields nothing to unpack.
        if not chunk:
            continue
        # Each chunk is split on its own and only the *first* line it completes is
        # joined onto what was held, so a line spanning many chunks is copied and
        # scanned once at its boundary rather than once per chunk. The exception is
        # a held fragment ending in `\r`, which the next chunk may complete: those
        # two have to be split together for `\r\n` to read as one terminator.
        text = chunk
        if held and held[-1].endswith("\r"):
            held.append(chunk)
            text, held = "".join(held), []
        *complete, tail = text.splitlines(keepends=True)
        # `splitlines` cannot tell a final line that ended from one still arriving,
        # so a terminated tail is a complete line and only an unterminated one is
        # held. A tail ending in `\r` is held even so, because the next chunk may
        # open with the `\n` that completes a `\r\n` split across the boundary; a
        # lone `\r` terminator is then yielded when the input ends.
        if _terminated(tail) and not tail.endswith("\r"):
            complete.append(tail)
            tail = ""
        if complete:
            held.append(complete[0])
            yield "".join(held)
            yield from complete[1:]
            held = []
        if tail:
            held.append(tail)
    if held:
        yield "".join(held)
