from __future__ import annotations

from collections.abc import Iterable
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field

from without_web.converters import STR
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
class _Bound[L]:
    """
    A leaf, plus the parameter names the path that reaches it declared, in order.

    Names live here rather than on the nodes because a node is shared: two routes
    whose segments convert identically travel the same branch even when they call
    the value different things, so the name is a property of the route, and only
    the leaf is per-route. The walk therefore collects values positionally and
    pairs them with these names once it arrives.
    """

    names: tuple[str, ...]
    leaf: L


@dataclass(frozen=True, slots=True)
class Node[L]:
    """
    One immutable trie node, generic over the leaf payload it terminates in.

    `params` and `catchall` are keyed by *converter alone*, so every route whose
    segment converts the same way shares one branch and each segment is converted
    at most once per branch however many routes pass through it.

    `params` is ordered so the walk tries more specific branches first: a typed
    converter (`int`, `uuid`) before the catch-most `str`. That recovers route
    precedence from the tree's structure rather than from registration order.

    `catchall` is a single slot rather than an ordered collection, because a
    catch-all consumes every remaining segment and so has no more specific
    sibling to be tried before it: whichever of two the walk reached first would
    answer for both. `build` refuses the second one rather than leaving it
    silently unreachable, and the type says so, so the walk has nothing to order
    and no sibling to fall back to.
    """

    literals: Mapping[str, Node[L]]
    params: tuple[tuple[Converter[object], Node[L]], ...]
    catchall: tuple[Converter[object], Node[L]] | None
    leaf: _Bound[L] | None


@dataclass(slots=True)
class _Builder[L]:
    literals: dict[str, _Builder[L]] = field(default_factory=dict)
    params: dict[Converter[object], _Builder[L]] = field(default_factory=dict)
    catchall: tuple[Converter[object], _Builder[L]] | None = None
    leaf: _Bound[L] | None = None


def build[L](routes: Iterable[tuple[tuple[Segment, ...], L]]) -> Node[L]:
    """
    Fold a flat route table into one immutable trie.

    A duplicate route (two leaves at the same path) is a build-time fault, so it
    raises here at construction rather than surfacing per request. Two routes that
    differ only in what they *call* a parameter are such a duplicate, because the
    request cannot tell them apart: `/u/{id:int}` and `/u/{other:int}` match
    exactly the same targets. Converters travel on the segments themselves, so
    there is no registry to resolve and no unknown-converter fault: a path-param
    token *is* its converter.

    Two catch-alls under one parent are the same fault wearing a different hat:
    each consumes every remaining segment, so the first the walk reaches answers
    for both however they convert, and the second is dead. That raises here too,
    rather than at the request that would have wanted it.
    """
    root: _Builder[L] = _Builder()
    for segments, leaf in routes:
        _insert(root, segments, leaf, ())
    return _freeze(root)


def _insert[L](node: _Builder[L], segments: tuple[Segment, ...], leaf: L, names: tuple[str, ...]) -> None:
    if not segments:
        if node.leaf is not None:
            raise ValueError("duplicate route: two endpoints resolve to the same path")
        node.leaf = _Bound(names, leaf)
        return
    head, *rest = segments
    match head:
        case Literal(text):
            child = node.literals.setdefault(text, _Builder())
        case Param(name, converter):
            child = node.params.setdefault(converter, _Builder())
            names = (*names, name)
        case CatchAll(name, converter):
            if rest:
                raise ValueError("invalid route: a catch-all must be the last segment")
            if node.catchall is None:
                node.catchall = (converter, _Builder())
            elif node.catchall[0] != converter:
                raise ValueError("ambiguous route: two catch-alls resolve at the same path")
            child = node.catchall[1]
            names = (*names, name)
    _insert(child, tuple(rest), leaf, names)


def _freeze[L](builder: _Builder[L]) -> Node[L]:
    literals = {text: _freeze(child) for text, child in builder.literals.items()}
    params = tuple((converter, _freeze(child)) for converter, child in sorted(builder.params.items(), key=_precedence))
    catchall = None if builder.catchall is None else (builder.catchall[0], _freeze(builder.catchall[1]))
    return Node(literals=literals, params=params, catchall=catchall, leaf=builder.leaf)


def _precedence[L](item: tuple[Converter[object], _Builder[L]]) -> int:
    # A typed converter is more specific than the catch-most `str`, so it is
    # tried first; `sorted` is stable, so same-precedence branches keep insertion
    # order. Compared against the `STR` *value*, not against the name "str", so a
    # converter that merely borrows the label does not inherit its precedence.
    converter, _child = item
    return 1 if converter == STR else 0


def walk[L](node: Node[L], segments: tuple[str, ...]) -> Found[L] | None:
    """
    Match a request target against the trie, backtracking on converter rejection.

    Because a typed converter can reject a segment, a literal-then-param descent
    is not a single forward walk: when a branch dead-ends downstream the walk
    falls back to the next sibling at this node. Resolution order is literal,
    then typed params, then catch-all.
    """
    return _walk(node, segments, ())


def _walk[L](node: Node[L], segments: tuple[str, ...], values: tuple[object, ...]) -> Found[L] | None:
    """
    The walk, carrying the values converted so far.

    Values travel *down* rather than being assembled on the way back up, because
    the names they belong to live on the leaf and so are not known until the walk
    arrives. Backtracking discards a branch's values for free: the tuple handed to
    a child is a value the parent frame still holds unchanged.
    """
    if not segments:
        return None if node.leaf is None else _found(node.leaf, values)
    head, rest = segments[0], segments[1:]
    literal = node.literals.get(head)
    if literal is not None and (found := _walk(literal, rest, values)) is not None:
        return found
    for converter, child in node.params:
        try:
            value = converter.parse(head)
        except ValueError:
            continue
        if (found := _walk(child, rest, (*values, value))) is not None:
            return found
    if node.catchall is None:
        return None
    converter, child = node.catchall
    if child.leaf is None:
        return None
    try:
        value = converter.parse("/".join(segments))
    except ValueError:
        return None
    return _found(child.leaf, (*values, value))


def _found[L](bound: _Bound[L], values: tuple[object, ...]) -> Found[L]:
    """Pair the values converted along the way with the names this leaf's own route gave them."""
    return Found(bound.leaf, dict(zip(bound.names, values, strict=True)))
