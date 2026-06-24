from __future__ import annotations

from collections.abc import Iterable
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field

from without_web.converters import Converter
from without_web.patterns import CatchAll
from without_web.patterns import Literal
from without_web.patterns import Param
from without_web.patterns import Segment


@dataclass(frozen=True, slots=True)
class Found[L]:
    """A successful walk: the leaf reached and the parameters bound on the way."""

    leaf: L
    params: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class Node[L]:
    """One immutable trie node, generic over the leaf payload it terminates in.

    `params` is ordered so the walk tries more specific branches first: a typed
    converter (`int`, `uuid`) before the catch-most `str`. That recovers route
    precedence from the tree's structure rather than from registration order.
    """

    literals: Mapping[str, Node[L]]
    params: tuple[tuple[Param, Node[L]], ...]
    catchall: tuple[CatchAll, Node[L]] | None
    leaf: L | None


@dataclass(slots=True)
class _Builder[L]:
    literals: dict[str, _Builder[L]] = field(default_factory=dict)
    params: dict[tuple[str, str], _Builder[L]] = field(default_factory=dict)
    catchall: tuple[str, _Builder[L]] | None = None
    leaf: L | None = None


def build[L](routes: Iterable[tuple[tuple[Segment, ...], L]], converters: Mapping[str, Converter]) -> Node[L]:
    """Fold a flat route table into one immutable trie.

    A duplicate route (two leaves at the same path) and an unknown converter
    name are both build-time faults, so they raise here at construction rather
    than surfacing per request.
    """
    root: _Builder[L] = _Builder()
    for segments, leaf in routes:
        _insert(root, segments, leaf, converters)
    return _freeze(root)


def _insert[L](node: _Builder[L], segments: tuple[Segment, ...], leaf: L, converters: Mapping[str, Converter]) -> None:
    if not segments:
        if node.leaf is not None:
            raise ValueError("duplicate route: two endpoints resolve to the same path")
        node.leaf = leaf
        return
    head, *rest = segments
    match head:
        case Literal(text):
            child = node.literals.setdefault(text, _Builder())
        case Param(name, converter):
            if converter not in converters:
                raise ValueError(f"unknown converter {converter!r} for parameter {name!r}")
            child = node.params.setdefault((name, converter), _Builder())
        case CatchAll(name):
            if node.catchall is None:
                node.catchall = (name, _Builder())
            child = node.catchall[1]
    _insert(child, tuple(rest), leaf, converters)


def _freeze[L](builder: _Builder[L]) -> Node[L]:
    literals = {text: _freeze(child) for text, child in builder.literals.items()}
    params = tuple(
        (Param(name, converter), _freeze(child))
        for (name, converter), child in sorted(builder.params.items(), key=_param_precedence)
    )
    catchall = None if builder.catchall is None else (CatchAll(builder.catchall[0]), _freeze(builder.catchall[1]))
    return Node(literals=literals, params=params, catchall=catchall, leaf=builder.leaf)


def _param_precedence[L](item: tuple[tuple[str, str], _Builder[L]]) -> int:
    # A typed converter is more specific than the catch-most `str`, so it is
    # tried first; `sorted` is stable, so same-precedence params keep insertion
    # order.
    (_name, converter), _child = item
    return 1 if converter == "str" else 0


def walk[L](node: Node[L], segments: tuple[str, ...], converters: Mapping[str, Converter]) -> Found[L] | None:
    """Match a request target against the trie, backtracking on converter rejection.

    Because a typed converter can reject a segment, a literal-then-param descent
    is not a single forward walk: when a branch dead-ends downstream the walk
    falls back to the next sibling at this node. Resolution order is literal,
    then typed params, then catch-all.
    """
    if not segments:
        return None if node.leaf is None else Found(node.leaf, {})
    head, rest = segments[0], segments[1:]
    literal = node.literals.get(head)
    if literal is not None and (found := walk(literal, rest, converters)) is not None:
        return found
    for param, child in node.params:
        try:
            value = converters[param.converter].parse(head)
        except ValueError:
            continue
        if (found := walk(child, rest, converters)) is not None:
            return Found(found.leaf, {param.name: value, **found.params})
    if node.catchall is not None:
        catchall, child = node.catchall
        if child.leaf is not None:
            return Found(child.leaf, {catchall.name: "/".join(segments)})
    return None
