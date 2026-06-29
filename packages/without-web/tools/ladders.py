"""Generate the typed `@overload` ladders for `without-web`.

These ladders tie a variadic list of `Extractor` tokens to a handler's
parameters at each arity (0-10), so a mismatch is a mypy error rather than a
runtime surprise. They are pure mechanical repetition over arity, so they are
generated rather than hand-maintained: `cog` invokes `emit(name)` in place (see
the `# [[[cog ... ]]]` blocks in `handlers.py` and `extractors.py`), and a
pre-commit hook keeps the checked-in output in sync.

This module is a build-time tool; it is deliberately outside the shipped
`without_web` package and imported only via `cog -I tools`.
"""

from __future__ import annotations

from collections.abc import Callable

# Type-parameter letters for the extractor slots, skipping `I` (ambiguous with
# `1`/`l`). Arity N uses the first N of these; `T`/`M` are added per ladder.
LETTERS = ("A", "B", "C", "D", "E", "F", "G", "H", "J", "K")


def _keywords(stream: bool) -> list[str]:
    """The keyword-only tail for the handler ladders (the websocket ladder has none).

    A streaming route gets a `request_body` describing its inbound sequence: it
    has no `body` extractor (that would buffer the input it streams), so the
    description is passed here rather than recovered from one.
    """
    keywords = ["summary: str = ...,", "responses: Mapping[int, ResponseSpec] | None = ...,"]
    if stream:
        keywords.append("request_body: Body | None = ...,")
    return keywords


def _overload(name: str, typeparams: list[str], params: list[str], return_type: str) -> str:
    """One `@overload` stub, fully expanded with a magic trailing comma.

    The trailing comma on the last parameter keeps `ruff format` from collapsing
    short signatures back onto one line, so this generated form is a fixed point
    of the formatter and `cog` stays idempotent.
    """
    head = f"@overload\ndef {name}[{', '.join(typeparams)}](" if typeparams else f"@overload\ndef {name}("
    body = "\n".join(f"    {param}" for param in params)
    return f"{head}\n{body}\n) -> {return_type}: ..."


def _extractor_params(letters: list[str]) -> list[str]:
    return [f"{letter.lower()}: Extractor[{letter}]," for letter in letters]


def _handler_ladder(name: str, *, stream: bool) -> str:
    """`handle` / `handle_stream`: extractors, then a keyword-only `fn`.

    The streaming form appends a trailing `Stream[Inbound]` to `fn`'s parameters,
    which the handler reads live instead of a buffered body.
    """
    blocks = []
    for arity in range(11):
        letters = list(LETTERS[:arity])
        params = _extractor_params(letters)
        if letters:
            params.append("/,")
        params.append("*,")
        fn_params = ["T", *letters, *(["Stream[Inbound]"] if stream else [])]
        params.append(f"fn: Callable[[{', '.join(fn_params)}], Returned],")
        params.extend(_keywords(stream))
        blocks.append(_overload(name, ["T", *letters], params, "HttpEndpoint[T]"))
    return "\n\n".join(blocks)


def _method_ladder(*, stream: bool) -> str:
    """`_Method.__call__` / `_StreamMethod.__call__`: a leading `pattern`, then extractors.

    `fn` rides in the returned decorator's type, so the per-arity variation is in
    the return type rather than a parameter.
    """
    blocks = []
    for arity in range(11):
        letters = list(LETTERS[:arity])
        params = ["self,", "pattern: Pattern,", *_extractor_params(letters), "/,", "*,", *_keywords(stream)]
        fn_params = ["T", *letters, *(["Stream[Inbound]"] if stream else [])]
        return_type = f"Callable[[Callable[[{', '.join(fn_params)}], Returned]], Route[T]]"
        blocks.append(_overload("__call__", ["T", *letters], params, return_type))
    return "\n\n".join(blocks)


def _ws_ladder() -> str:
    """`ws`: a leading `pattern`, then extractors, no keyword tail (a handshake has no body).

    The handler *is* the frame processor (as in `@post.stream`): a trailing
    `Stream[WebsocketInbound]` carries the live inbound frames and the handler
    yields `Stream[WebsocketOutbound]`.
    """
    blocks = []
    for arity in range(11):
        letters = list(LETTERS[:arity])
        params = ["pattern: Pattern,", *_extractor_params(letters), "/,"]
        fn_params = ["T", *letters, "Stream[WebsocketInbound]"]
        return_type = f"Callable[[Callable[[{', '.join(fn_params)}], WebsocketReturned]], WebsocketRoute[T]]"
        blocks.append(_overload("ws", ["T", *letters], params, return_type))
    return "\n\n".join(blocks)


def _into_ladder() -> str:
    """`into`: a `make` constructor whose parameters are the extractors' values (arity 1-10)."""
    blocks = []
    for arity in range(1, 11):
        letters = list(LETTERS[:arity])
        params = [f"make: Callable[[{', '.join(letters)}], M],", *_extractor_params(letters), "/,"]
        blocks.append(_overload("into", ["M", *letters], params, "Extractor[M]"))
    return "\n\n".join(blocks)


_LADDERS: dict[str, Callable[[], str]] = {
    "handle": lambda: _handler_ladder("handle", stream=False),
    "handle_stream": lambda: _handler_ladder("handle_stream", stream=True),
    "method": lambda: _method_ladder(stream=False),
    "stream_method": lambda: _method_ladder(stream=True),
    "ws": _ws_ladder,
    "into": _into_ladder,
}


def emit(name: str) -> str:
    """The generated ladder text for `name`, for a `cog.outl(emit("..."))` block.

    Blank-line spacing is left to `ruff format`, which runs immediately after
    `cog` in the same pre-commit hook (see `tools/regenerate.sh`): this emits the
    overloads separated by one blank line and lets the formatter normalize the
    rest, so the generator never has to mirror the formatter's rules.
    """
    return _LADDERS[name]()
