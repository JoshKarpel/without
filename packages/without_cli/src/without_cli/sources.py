from __future__ import annotations

from collections.abc import Iterable
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import assert_never


@dataclass(frozen=True, slots=True)
class FromEnv:
    """An environment variable an option falls back to when the command line omits it."""

    name: str


@dataclass(frozen=True, slots=True)
class FromFile:
    """
    A file an option falls back to when the command line omits it.

    The shape a Docker or Kubernetes secret mount takes: the value is the file's
    whole contents. `strip` is on by default because those mounts almost always
    carry a trailing newline, and a token with one on the end fails
    authentication somewhere far away from the cause.

    A missing file is *absence*, not an error, so a mount that is not present
    reads as "not configured" and the option's own `parse` decides whether that
    is fatal (`once` rejects, `default` does not). A file that exists but cannot
    be read raises, because that is a broken deployment rather than an
    unconfigured one.
    """

    path: Path
    strip: bool = True


type Source = FromEnv | FromFile


def file_paths(sources: Iterable[Source]) -> tuple[Path, ...]:
    """Every path a group of sources names, for the shell to read before parsing."""
    return tuple(source.path for source in sources if isinstance(source, FromFile))


def read_files(paths: Iterable[Path]) -> Mapping[Path, str]:
    """
    Read the files a spec names, skipping the ones that are absent.

    The imperative half of source resolution, and the only I/O parsing needs:
    the shell reads once at the boundary and hands the result to `parse_argv` as
    a value, so parsing stays pure and a test supplies a mapping instead of a
    filesystem.
    """
    contents = {}
    for path in paths:
        try:
            contents[path] = path.read_text()
        except FileNotFoundError:
            continue
    return contents


def from_sources(
    sources: Iterable[Source],
    env: Mapping[str, str],
    files: Mapping[Path, str],
) -> tuple[str, ...]:
    """
    The raw values the first source that holds one supplies.

    Sources are tried in declaration order and the first hit wins outright; they
    do not accumulate, so the order in the tuple *is* the precedence. Each source
    yields at most one raw value, because splitting one string into several is a
    policy (a separator) that belongs to the option's own `parse`, not here.
    """
    for source in sources:
        match source:
            case FromEnv(name):
                if (value := env.get(name)) is not None:
                    return (value,)
            case FromFile(path, strip):
                if (text := files.get(path)) is not None:
                    return (text.strip() if strip else text,)
            case _ as unreachable:
                assert_never(unreachable)
    return ()
