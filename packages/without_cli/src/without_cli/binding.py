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
from without_cli.tokens import PRESENT
from without_cli.tokens import Args
from without_cli.tokens import ExtractionError
from without_cli.tokens import Option
from without_cli.usage import Usage
from without_cli.usage import usage

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
class Answered:
    """
    The scan met one of the caller's `answered` spellings, so nothing was bound.

    This layer holds no opinion about what any spelling *means*: it reports which
    one it met and which level it was addressed to, and the shell decides whether
    `--help` prints usage, `--version` reads `node.version`, or `--license` prints
    something else entirely. That is why there is one outcome here rather than one
    per flag, and why adding a flag needs no change below `run`.

    Stopping has to happen in the scan even though deciding does not, because only
    the scan knows whether a token is a flag or the value of the option before it,
    and because a level's required options have not been checked yet: that is what
    lets `prog db migrate --help` answer instead of complaining about the `--dsn`
    you were asking how to spell.
    """

    spelling: str
    path: tuple[Node, ...]

    @property
    def node(self) -> Node:
        """The level the spelling was addressed to, whose `version` a shell may read."""
        return self.path[-1]

    @property
    def usage(self) -> Usage:
        """That level's usage, for a shell answering with help."""
        return usage(self.path)


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


type Outcome[T] = Bound[T] | Answered | Rejected


@dataclass(frozen=True, slots=True)
class _Scanned:
    """
    One level's argv, split into option occurrences and the bare tokens left over.

    What `bare` holds is the caller's `stop_at_positional`: a level with children
    stops at the first bare token, so `bare` is that token and everything after it
    (the subcommand and its own argv); a level without them consumes the whole
    line, so `bare` is its positionals.
    """

    options: dict[str, list[str]]
    bare: list[str]


@dataclass(frozen=True, slots=True)
class _Refused:
    message: str


@dataclass(frozen=True, slots=True)
class _Answered:
    """The scan met one of the caller's `answered` spellings."""

    spelling: str


type _Scan = _Scanned | _Refused | _Answered


def _scan(
    node: Node,
    argv: Sequence[str],
    *,
    stop_at_positional: bool,
    answered: Sequence[str],
) -> _Scan:
    """
    Split one level's argv into option occurrences and bare tokens.

    A spelling in `answered` short-circuits the scan, but only where this level
    has not declared an option by that name: shadowing is how a program that
    wants `--version` to mean something of its own takes it back. The scan runs
    left to right and stops at the first one it meets, so no spelling has a
    standing precedence over another.

    A level with subcommands stops at the first bare token (that token is the
    subcommand's name and everything after it belongs to the child), which is what
    keeps `prog --verbose sub --flag` unambiguous without either level knowing
    about the other's options.
    """
    by_name = {name: option for option in node.options for name in option.names}
    options: dict[str, list[str]] = {}
    bare: list[str] = []
    index = 0
    literal = False

    def record(option: Option, value: str) -> None:
        options.setdefault(option.canonical, []).append(value)

    while index < len(argv):
        token = argv[index]
        if literal or token == "-" or not token.startswith("-"):
            if stop_at_positional:
                return _Scanned(options, list(argv[index:]))
            bare.append(token)
            index += 1
        elif token == "--":
            literal = True
            index += 1
        elif token in answered and token not in by_name:
            return _Answered(token)
        elif token.startswith("--"):
            name, separator, inline = token.partition("=")
            option = by_name.get(name)
            if option is None:
                return _Refused(f"unknown option {name}")
            if option.metavar is None:
                if separator:
                    return _Refused(f"option {name} takes no value")
                record(option, PRESENT)
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
                    record(option, PRESENT)
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
    return _Scanned(options, bare)


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
    answered: Sequence[str] = (),
) -> Outcome[T]:
    """
    Turn a command line into a valid invocation, a rejection, or a stop the caller
    asked for.

    A pure, total function of its values, which is what makes the whole parser
    testable without a process: no `sys.argv`, no `os.environ`, no filesystem, no
    exit, no output. `env` and `files` are the already-read contents of an
    option's fallback sources (see `run`, which reads them).

    Nothing here is magic by default. `answered` is the caller's list of spellings
    that should stop the scan and come back as an `Answered` rather than being
    parsed, and it is empty unless asked for, so `--help` means nothing to this
    function on its own. `run` passes the conventional set and decides what each
    one does; a program wanting `-?`, a `help` subcommand, or nothing at all
    passes its own list and this function does not change.

    Every value is extracted here, so a `Bound` proves the invocation is good and
    nothing has been opened yet when a `Rejected` comes back.
    """
    node = arm.node
    path = [node]
    levels: list[Level] = []
    remaining = list(argv)

    while True:
        scanned = _scan(node, remaining, stop_at_positional=bool(node.children), answered=answered)
        if isinstance(scanned, _Answered):
            return Answered(scanned.spelling, tuple(path))
        if isinstance(scanned, _Refused):
            return Rejected(scanned.message, usage(tuple(path)))
        options = _merged(node, scanned.options, env, files)

        if not node.children:
            assigned = _assigned(node, scanned.bare)
            if isinstance(assigned, _Refused):
                return Rejected(assigned.message, usage(tuple(path)))
            levels.append(Level(node.name, Args(options=options, arguments=assigned)))
            break

        if not scanned.bare:
            return Rejected("expected a command", usage(tuple(path)))
        child = node.child(scanned.bare[0])
        if child is None:
            known = ", ".join(sorted(entry.name for entry in node.children))
            return Rejected(f"unknown command {scanned.bare[0]!r} (expected one of: {known})", usage(tuple(path)))
        levels.append(Level(node.name, Args(options=options, arguments={})))
        node = child
        path.append(child)
        remaining = scanned.bare[1:]

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


__all__ = ["Answered", "Bound", "Outcome", "Rejected", "parse_argv", "render_rejection"]
