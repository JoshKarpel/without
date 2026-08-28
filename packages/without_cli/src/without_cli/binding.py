from __future__ import annotations

from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path
from types import MappingProxyType
from typing import Generic
from typing import TypeVar

from without_cli.commands import Action
from without_cli.commands import Arm
from without_cli.commands import Level
from without_cli.commands import Node
from without_cli.sources import from_sources
from without_cli.tokens import Args
from without_cli.tokens import ExtractionError
from without_cli.tokens import Option
from without_cli.usage import Usage
from without_cli.usage import render
from without_cli.usage import usage

_HELP = ("-h", "--help")

# Shared empty defaults, so `parse_argv` can be called with only an argv while
# keeping its signature free of a mutable default.
_NO_ENV: Mapping[str, str] = MappingProxyType({})
_NO_FILES: Mapping[Path, str] = MappingProxyType({})

# Contravariant, matching `Action`: `Bound[Never]` is the type of "a bound
# invocation, whatever state it wants". The legacy `TypeVar` is needed because
# PEP 695's inferred variance treats a (frozen) dataclass field as invariant.
_T_contra = TypeVar("_T_contra", contravariant=True)


@dataclass(frozen=True, slots=True)
class Bound(Generic[_T_contra]):  # noqa: UP046 - PEP 695 infers a frozen dataclass field as invariant; the contravariant TypeVar is deliberate
    """
    A valid invocation: every value parsed, nothing run.

    Reaching this proves the command line was good, because all extraction has
    already happened. That is what lets a program open its resources only for
    invocations that were going to work, and it is why `run` never has to report
    a usage error out of the middle of a command.
    """

    action: Action[_T_contra] = field(compare=False)


@dataclass(frozen=True, slots=True)
class Helped:
    """The invocation asked for help. Not an error: `run` writes it to stdout and exits `0`."""

    usage: Usage


@dataclass(frozen=True, slots=True)
class Rejected:
    """
    The invocation was not valid, with the usage of the level that refused it.

    Carrying the `Usage` rather than a formatted string is what lets a program
    decide how much to show and where: `run`'s default is the synopsis plus a
    pointer to `--help`, and an application that wants the whole help text on a
    mistake renders `usage` in full instead.
    """

    message: str
    usage: Usage


type Outcome[T] = Bound[T] | Helped | Rejected


@dataclass(frozen=True, slots=True)
class _Scanned:
    options: dict[str, list[str]]
    positionals: list[str]
    tail: list[str]


@dataclass(frozen=True, slots=True)
class _Refused:
    message: str


def _scan(node: Node, argv: Sequence[str], *, stop_at_positional: bool) -> _Scanned | _Refused | None:
    """
    Split one level's argv into option occurrences and bare tokens.

    Returns `None` when help was asked for. A level with subcommands stops at the
    first bare token (that token is the subcommand's name and everything after it
    belongs to the child), which is what keeps `prog --verbose sub --flag`
    unambiguous without either level knowing about the other's options.
    """
    by_name = {name: option for option in node.options for name in option.names}
    options: dict[str, list[str]] = {}
    positionals: list[str] = []
    index = 0
    literal = False

    def record(option: Option, value: str) -> None:
        options.setdefault(option.canonical, []).append(value)

    while index < len(argv):
        token = argv[index]
        if literal or token == "-" or not token.startswith("-"):
            if stop_at_positional:
                return _Scanned(options, positionals, list(argv[index:]))
            positionals.append(token)
            index += 1
        elif token == "--":
            literal = True
            index += 1
        elif token in _HELP and token not in by_name:
            return None
        elif token.startswith("--"):
            name, separator, inline = token.partition("=")
            option = by_name.get(name)
            if option is None:
                return _Refused(f"unknown option {name}")
            if option.metavar is None:
                if separator:
                    return _Refused(f"option {name} takes no value")
                record(option, "1")
                index += 1
            elif separator:
                record(option, inline)
                index += 1
            elif index + 1 < len(argv):
                record(option, argv[index + 1])
                index += 2
            else:
                return _Refused(f"option {name} expects a value")
        else:
            # A short cluster: `-abc` is three flags, or one flag and a value, so
            # it is walked character by character rather than looked up whole.
            cluster = token[1:]
            position = 0
            while position < len(cluster):
                name = f"-{cluster[position]}"
                option = by_name.get(name)
                if option is None:
                    return _Refused(f"unknown option {name}")
                if option.metavar is None:
                    record(option, "1")
                    position += 1
                elif position + 1 < len(cluster):
                    record(option, cluster[position + 1 :])
                    position = len(cluster)
                elif index + 1 < len(argv):
                    record(option, argv[index + 1])
                    index += 1
                    position = len(cluster)
                else:
                    return _Refused(f"option {name} expects a value")
            index += 1
    return _Scanned(options, positionals, [])


def _merged(
    node: Node,
    scanned: Mapping[str, list[str]],
    env: Mapping[str, str],
    files: Mapping[Path, str],
) -> dict[str, tuple[str, ...]]:
    """
    Settle each option's raw values: the command line if it said anything, else the first source that did.

    Command line beats sources outright rather than merging with them, so a
    repeated option cannot be half-overridden by an environment variable that
    supplied a different count.
    """
    values: dict[str, tuple[str, ...]] = {}
    for option in node.options:
        occurrences = scanned.get(option.canonical)
        supplied = tuple(occurrences) if occurrences else from_sources(option.sources, env, files)
        if supplied:
            values[option.canonical] = supplied
    return values


def _assigned(node: Node, tokens: Sequence[str]) -> dict[str, tuple[str, ...]] | _Refused:
    """
    Hand the bare tokens to the level's positionals, in declaration order.

    A positional with nothing left for it is simply absent, so its own token
    decides whether that is a rejection; only a token with no positional left to
    take it is refused here.
    """
    assigned: dict[str, tuple[str, ...]] = {}
    index = 0
    for spec in node.positionals:
        if spec.variadic:
            assigned[spec.name] = tuple(tokens[index:])
            index = len(tokens)
        elif index < len(tokens):
            assigned[spec.name] = (tokens[index],)
            index += 1
    if index < len(tokens):
        return _Refused(f"unexpected argument {tokens[index]!r}")
    return assigned


def parse_argv[T](
    arm: Arm[T],
    *,
    argv: Sequence[str],
    env: Mapping[str, str] = _NO_ENV,
    files: Mapping[Path, str] = _NO_FILES,
) -> Outcome[T]:
    """
    Turn a command line into a valid invocation, a help request, or a rejection.

    A pure, total function of its four values, which is what makes the whole
    parser testable without a process: no `sys.argv`, no `os.environ`, no
    filesystem, no exit, no output. `env` and `files` are the already-read
    contents of an option's fallback sources (see `run`, which reads them).

    Every value is extracted here, so a `Bound` proves the invocation is good and
    nothing has been opened yet when a `Rejected` comes back.
    """
    node = arm.node
    path = [node]
    levels: list[Level] = []
    remaining = list(argv)

    while True:
        scanned = _scan(node, remaining, stop_at_positional=bool(node.children))
        if scanned is None:
            return Helped(usage(tuple(path)))
        if isinstance(scanned, _Refused):
            return Rejected(scanned.message, usage(tuple(path)))
        options = _merged(node, scanned.options, env, files)

        if not node.children:
            assigned = _assigned(node, scanned.positionals)
            if isinstance(assigned, _Refused):
                return Rejected(assigned.message, usage(tuple(path)))
            levels.append(Level(node.name, Args(options=options, arguments=assigned)))
            break

        if not scanned.tail:
            return Rejected("expected a command", usage(tuple(path)))
        child = node.child(scanned.tail[0])
        if child is None:
            known = ", ".join(sorted(entry.name for entry in node.children))
            return Rejected(f"unknown command {scanned.tail[0]!r} (expected one of: {known})", usage(tuple(path)))
        levels.append(Level(node.name, Args(options=options, arguments={})))
        node = child
        path.append(child)
        remaining = scanned.tail[1:]

    try:
        return Bound(arm.resolve(tuple(levels)))
    except ExtractionError as exc:
        return Rejected(f"{exc.parameter}: {exc}", usage(tuple(path)))


def render_rejection(rejected: Rejected) -> str:
    """
    The default rendering of a bad command line: what went wrong, then where to look.

    Deliberately not the whole help text. A rejection usually means one thing was
    wrong, and burying that line under fifty lines of options is how a CLI
    trains people to stop reading its errors.
    """
    program = " ".join(rejected.usage.path)
    return (
        "\n".join(
            [
                f"{program}: {rejected.message}",
                f"usage: {rejected.usage.invocation}",
                f"try '{program} --help' for more information",
            ]
        )
        + "\n"
    )


__all__ = ["Bound", "Helped", "Outcome", "Rejected", "parse_argv", "render", "render_rejection"]
