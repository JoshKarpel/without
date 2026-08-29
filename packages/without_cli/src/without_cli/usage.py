from __future__ import annotations

from dataclasses import dataclass
from typing import assert_never

from without_cli.commands import Node
from without_cli.sources import FromEnv
from without_cli.sources import FromFile
from without_cli.tokens import Option
from without_cli.tokens import Positional


@dataclass(frozen=True, slots=True)
class Usage:
    """
    What one command path looks like, recovered from the tokens that parse it.

    A *value*, not a string, which is the whole point: the plain-text `render`
    below is one rendering of it, and a markdown page, a man page, a shell
    completion script, or a coloured terminal renderer are others, each chosen by
    whoever is doing the rendering. Nothing here knows about a styling library,
    so none is on the path every program crosses.

    `inherited` carries the options an ancestor declared, because
    `todos db migrate --help` should say that `--endpoint` exists even though it
    is spelled before `db`.
    """

    path: tuple[str, ...]
    summary: str
    positionals: tuple[Positional, ...]
    options: tuple[Option, ...]
    inherited: tuple[Option, ...]
    commands: tuple[tuple[str, str], ...]

    @property
    def invocation(self) -> str:
        """The one-line synopsis, e.g. `todos add [OPTIONS] TEXT [NOTE] [TAGS...]`."""
        parts = [" ".join(self.path)]
        if self.options or self.inherited:
            parts.append("[OPTIONS]")
        parts.extend(_positional_slot(p) for p in self.positionals)
        if self.commands:
            parts.append("COMMAND [ARGS]...")
        return " ".join(parts)


def usage(path: tuple[Node, ...]) -> Usage:
    """
    Describe the command path `path` (root first), merging what each level declares.

    A pure transform of the tree, so the description cannot drift from the parser:
    the same `Option` value the binder reads an arity from is the one rendered here.
    """
    *ancestors, node = path
    return Usage(
        path=tuple(level.name for level in path),
        summary=node.summary,
        positionals=node.positionals,
        options=node.options,
        inherited=tuple(option for level in ancestors for option in level.options),
        commands=tuple((child.name, child.summary) for child in node.children),
    )


def _positional_slot(positional: Positional) -> str:
    """
    One positional's place in the synopsis, bracketed when it may be omitted.

    `TEXT` must be given, `[NOTE]` need not be, and `[PATHS...]` takes whatever
    is left, which is the convention every parser in this space renders.
    """
    if positional.variadic:
        return f"[{positional.metavar}...]"
    return positional.metavar if positional.required else f"[{positional.metavar}]"


def _annotations(option: Option) -> str:
    """The trailing notes on an option's help line: where else it can come from, and whether it must."""
    notes = []
    for source in option.sources:
        match source:
            case FromEnv(name):
                notes.append(f"env: {name}")
            case FromFile(path, _):
                notes.append(f"file: {path}")
            case _ as unreachable:
                assert_never(unreachable)
    if option.required:
        notes.append("required")
    return f"  [{'; '.join(notes)}]" if notes else ""


def _option_line(option: Option) -> str:
    spelling = ", ".join(option.names)
    if option.metavar is not None:
        spelling = f"{spelling} {option.metavar}"
    if option.repeatable:
        spelling = f"{spelling} ..."
    return spelling


def _section(title: str, rows: list[tuple[str, str]]) -> list[str]:
    if not rows:
        return []
    width = max(len(left) for left, _ in rows)
    return ["", f"{title}:", *(f"  {left.ljust(width)}  {right}".rstrip() for left, right in rows)]


def render(described: Usage) -> str:
    """
    The plain-text rendering of a `Usage`, and deliberately the only one shipped.

    Everything here is `str`: no colour, no width detection, no styling
    dependency. A program that wants those renders the same `Usage` value
    differently, which is a choice it makes rather than one this layer makes for
    it.
    """
    lines = [f"usage: {described.invocation}"]
    if described.summary:
        lines.extend(["", described.summary])
    lines.extend(_section("Arguments", [(p.metavar, p.summary) for p in described.positionals]))
    lines.extend(_section("Commands", [(name, summary) for name, summary in described.commands]))
    lines.extend(
        _section("Options", [(_option_line(o), o.summary + _annotations(o)) for o in described.options]),
    )
    lines.extend(
        _section("Inherited options", [(_option_line(o), o.summary + _annotations(o)) for o in described.inherited]),
    )
    return "\n".join(lines) + "\n"
