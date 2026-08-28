from __future__ import annotations

import asyncio
import os
import sys
from collections.abc import Mapping
from collections.abc import Sequence
from pathlib import Path
from typing import assert_never

from without_cli.binding import Bound
from without_cli.binding import Helped
from without_cli.binding import Rejected
from without_cli.binding import parse_argv
from without_cli.binding import render_rejection
from without_cli.commands import Arm
from without_cli.commands import source_paths
from without_cli.sources import read_files
from without_cli.streams import Streams
from without_cli.usage import render

USAGE_EXIT = 2


def run(
    program: Arm[Streams],
    *,
    argv: Sequence[str] | None = None,
    env: Mapping[str, str] | None = None,
    streams: Streams | None = None,
    files: Mapping[Path, str] | None = None,
) -> int:
    """
    Parse a command line, run what it selected, and return the exit code.

    The imperative shell, and the only place this package reads `sys.argv`,
    reads the environment, touches the filesystem, or starts an event loop. Every
    one of those is an argument with a real default, so a test drives a whole
    program by passing values (`run(app, argv=[...], env={...}, streams=capture.streams)`)
    with nothing patched and no subprocess.

    It returns rather than exits, so the caller keeps the continuation:
    `raise SystemExit(run(app))` is the conventional entry point, and a program
    that wants to do something else after a command finishes simply does.

    The default policy is the obvious one: help to stdout with `0`, a bad command
    line to stderr with `2`, otherwise the command's own code. An application
    wanting different answers calls `parse_argv` and matches the outcome itself,
    which costs it this function and nothing else.
    """
    resolved = Streams.standard() if streams is None else streams
    arguments = sys.argv[1:] if argv is None else argv
    environment = os.environ if env is None else env
    contents = read_files(source_paths(program.node)) if files is None else files

    try:
        match parse_argv(program, argv=arguments, env=environment, files=contents):
            case Helped(described):
                resolved.stdout.write(render(described))
                return 0
            case Rejected() as rejected:
                resolved.stderr.write(render_rejection(rejected))
                return USAGE_EXIT
            case Bound(action):
                # The streams *are* the root state: the shell is the layer above
                # the top-level group, so it supplies what that group's children
                # derive from, exactly as a group supplies its own. This is the
                # only event loop this package starts, entered once a `Bound` has
                # proved there is something worth running.
                return asyncio.run(action(resolved))
            case _ as unreachable:
                assert_never(unreachable)
    finally:
        # The interpreter flushes the real streams on exit, but `run` returns
        # rather than exiting, so a caller that keeps working (or a test reading
        # the buffers) would otherwise see output that has not landed yet.
        resolved.stdout.flush()
        resolved.stderr.flush()
