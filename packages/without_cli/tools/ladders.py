"""
Generate the typed `@overload` ladders for `without-cli`.

These ladders tie a variadic list of `Extractor` tokens to a handler's parameters
(or a state factory's) at each arity, so a `argument("id", INT)` paired with a
handler expecting a `str` is a mypy error rather than a runtime surprise. They
are pure mechanical repetition over arity, so they are generated rather than
hand-maintained: `cog` invokes `emit(name)` in place (see the `# [[[cog ... ]]]`
blocks in `commands.py` and `tokens.py`), and a pre-commit hook keeps the
checked-in output in sync.

This module is a build-time tool; it is deliberately outside the shipped
`without_cli` package and imported only via `cog -I tools`.
"""

from __future__ import annotations

from collections.abc import Callable

# Type-parameter letters for the token slots, skipping `I` (ambiguous with
# `1`/`l`). Arity N uses the first N of these; `T`/`U`/`M` are added per ladder.
LETTERS = ("A", "B", "C", "D", "E", "F", "G", "H", "J", "K")

MAX_ARITY = 10


def _overload(name: str, typeparams: list[str], params: list[str], return_type: str) -> str:
    """
    One `@overload` stub, fully expanded with a magic trailing comma.

    The trailing comma on the last parameter keeps `ruff format` from collapsing
    short signatures back onto one line, so this generated form is a fixed point
    of the formatter and `cog` stays idempotent.
    """
    head = f"@overload\ndef {name}[{', '.join(typeparams)}](" if typeparams else f"@overload\ndef {name}("
    body = "\n".join(f"    {param}" for param in params)
    return f"{head}\n{body}\n) -> {return_type}: ..."


def _slots(letters: list[str]) -> list[str]:
    return [f"{letter.lower()}: Extractor[{letter}]," for letter in letters]


def _command_ladder() -> str:
    """
    `command`: a name, then tokens, returning a decorator over the handler.

    The handler's one leading parameter is the state `T` its enclosing group
    built, so the ladder ties only the tail. At the root that state is the
    `Streams` the shell supplies, which is why a command needs no separate
    streams parameter.
    """
    blocks = []
    for arity in range(MAX_ARITY + 1):
        letters = list(LETTERS[:arity])
        params = ["name: str,", *_slots(letters), "/,", "*,", "summary: str = ...,"]
        # `Returned` is an alias for `Awaitable[int]` rather than the type spelled
        # out, because `Awaitable[int]]]` would close this very cog block.
        handler_params = ", ".join(["T", *letters])
        blocks.append(
            _overload("command", ["T", *letters], params, f"Callable[[Callable[[{handler_params}], Returned]], Arm[T]]")
        )
    return "\n\n".join(blocks)


def _group_ladder() -> str:
    """
    `group`: a name, tokens, a state factory, and the arms beneath it.

    `U` (what the factory builds) is solved from the factory's return *and* from
    the arms, so a command wanting a `Session` cannot be assembled under a group
    that builds something else, and two commands wanting different states cannot
    be siblings.

    There is no separate root form. A group derives its children's state from its
    parent's, and the root's parent is the shell, which supplies `Streams`, so
    the top of a tree is an ordinary `Arm[Streams]`.
    """
    blocks = []
    for arity in range(MAX_ARITY + 1):
        letters = list(LETTERS[:arity])
        factory_params = ["T", *letters]
        params = [
            "name: str,",
            *_slots(letters),
            "/,",
            "*,",
            f"state: Callable[[{', '.join(factory_params)}], AbstractAsyncContextManager[U]],",
            "commands: tuple[Arm[U], ...],",
            "summary: str = ...,",
        ]
        blocks.append(_overload("group", ["T", "U", *letters], params, "Arm[T]"))
    return "\n\n".join(blocks)


def _into_ladder() -> str:
    """`into`: a `make` constructor whose parameters are the tokens' values (arity 1-10)."""
    blocks = []
    for arity in range(1, MAX_ARITY + 1):
        letters = list(LETTERS[:arity])
        params = [f"make: Callable[[{', '.join(letters)}], M],", *_slots(letters), "/,"]
        blocks.append(_overload("into", ["M", *letters], params, "Extractor[M]"))
    return "\n\n".join(blocks)


_LADDERS: dict[str, Callable[[], str]] = {
    "command": _command_ladder,
    "group": _group_ladder,
    "into": _into_ladder,
}


def emit(name: str) -> str:
    """
    The generated ladder text for `name`, for a `cog.outl(emit("..."))` block.

    Blank-line spacing is left to `ruff format`, which runs immediately after
    `cog` in the same pre-commit hook (see `tools/regenerate.sh`): this emits the
    overloads separated by one blank line and lets the formatter normalize the
    rest, so the generator never has to mirror the formatter's rules.
    """
    return _LADDERS[name]()
