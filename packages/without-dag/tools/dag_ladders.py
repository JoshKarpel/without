"""
Generate the typed `@overload` ladders for `without-dag`'s `Graph`.

Two ladders tie a graph's shape to types at every arity, so a mismatch is a mypy
error rather than a runtime surprise: `of` opens a graph over N entry types,
returning it (as `Graph[*Ins]`) alongside one `Handle` per type, and `node` ties
each dependency `Handle[X]` to the matching parameter of a node's function.
Because the graph carries its entry pack in its type, `build` recovers
`*Ins` for the resulting `CompiledGraph[*Ins, Out]` with no ladder of its own.
They are pure mechanical repetition over arity, so they are generated rather than
hand-maintained: `cog` invokes `emit(name)` in place (see the `# [[[cog ... ]]]`
blocks in `graph.py`), and a pre-commit hook keeps the checked-in output in sync.

This module is a build-time tool; it is deliberately outside the shipped
`without_dag` package and imported only via `cog -I tools`.
"""

from __future__ import annotations

from collections.abc import Callable

# Type-parameter letters for the per-slot types, skipping `I` (ambiguous with
# `1`/`l`). Arity N uses the first N of these; `T`/`Out` are added per ladder.
LETTERS = ("A", "B", "C", "D", "E", "F", "G", "H", "J", "K")


def _overload(name: str, typeparams: list[str], params: list[str], return_type: str, *, static: bool = False) -> str:
    """
    One `@overload` stub, fully expanded with a magic trailing comma.

    The trailing comma on the last parameter keeps `ruff format` from collapsing
    short signatures back onto one line, so this generated form is a fixed point
    of the formatter and `cog` stays idempotent.
    """
    decorators = "@overload\n@staticmethod\n" if static else "@overload\n"
    head = f"{decorators}def {name}[{', '.join(typeparams)}](" if typeparams else f"{decorators}def {name}("
    body = "\n".join(f"    {param}" for param in params)
    return f"{head}\n{body}\n) -> {return_type}: ..."


def _of_ladder() -> str:
    """`of`: open a graph over N entry types, returning it plus one `Handle` each (arity 1-10)."""
    blocks = []
    for arity in range(1, 11):
        letters = list(LETTERS[:arity])
        params = [*(f"{letter.lower()}: type[{letter}]," for letter in letters), "/,"]
        graph_type = f"Graph[{', '.join(letters)}]"
        handles = ", ".join(f"Handle[{letter}]" for letter in letters)
        return_type = f"tuple[{graph_type}, {handles}]"
        blocks.append(_overload("of", letters, params, return_type, static=True))
    return "\n\n".join(blocks)


def _node_ladder() -> str:
    """`node`: a step function plus one `Handle` per dependency (arity 0-10)."""
    blocks = []
    for arity in range(11):
        letters = list(LETTERS[:arity])
        params = ["self,", f"fn: Callable[[{', '.join(letters)}], Awaitable[T]],"]
        params.extend(f"{letter.lower()}: Handle[{letter}]," for letter in letters)
        params.append("/,")
        blocks.append(_overload("node", ["T", *letters], params, "Handle[T]"))
    return "\n\n".join(blocks)


def emit(name: str) -> str:
    """
    The generated ladder text for `name`, for a `cog.outl(emit("..."))` block.

    Blank-line spacing is left to `ruff format`, which runs immediately after
    `cog` in the same pre-commit hook (see `tools/regenerate.sh`).
    """
    ladders: dict[str, Callable[[], str]] = {
        "of": _of_ladder,
        "node": _node_ladder,
    }
    return ladders[name]()
