from __future__ import annotations

from collections.abc import Iterable

from markupsafe import Markup

from without_html.markup import SupportsHtml
from without_html.markup import escape_text
from without_html.nodes import Element
from without_html.nodes import Node
from without_html.nodes import VoidElement

# A tag's opening prefix (`<div`) and its closing tag (`</div>`).
type TagMarkup = tuple[str, Markup]

# Per-tag constant markup, memoized on first use. A tag's surrounding text never varies,
# so building it once per distinct tag turns two string formats per element into one
# dictionary lookup. The closing tag is held as a `Markup` so that pushing it onto the
# render stack allocates nothing, and it is built for void tags too, where it costs one
# unused string per process and saves every other element a second lookup.
TAG_MARKUP: dict[str, TagMarkup] = {}


def tag_markup(tag: str) -> TagMarkup:
    """The memoized opening and closing markup for `tag`."""
    markup = TAG_MARKUP.get(tag)
    if markup is None:
        markup = TAG_MARKUP[tag] = (f"<{tag}", Markup(f"</{tag}>"))
    return markup


def render(node: Node) -> str:
    """
    Render a node tree to markup.

    A pure function of the tree: no I/O, no ambient state, and no decision about how
    the result reaches a client. Pair it with `without_asgi.html_content` to answer a
    request, or write it to a file, or compare it in a test.

    The walk is iterative over an explicit stack rather than recursive, because a page
    is thousands of nodes in aggregate and a Python frame per node is the largest
    avoidable cost here. Chunks are collected and joined once, so each byte is copied
    a single time.
    """
    out: list[str] = []
    stack: list[Node] = [node]
    while stack:
        item = stack.pop()
        if isinstance(item, Element):
            opening, closing = tag_markup(item.tag)
            out.append(opening)
            for name, value in item.attributes:
                out.append(f" {name}" if value is None else f' {name}="{value}"')
            out.append(">")
            stack.append(closing)
            stack.extend(reversed(item.children))
        elif item.__class__ is Markup:
            # Every element pushes its closing tag back onto the stack as a `Markup`, so
            # this arm runs once per element and is worth reaching by pointer comparison
            # rather than by the `isinstance` pair it would otherwise take to tell an
            # already-safe string from one that still needs escaping.
            out.append(item)
        elif item.__class__ is str:
            out.append(escape_text(item))
        elif isinstance(item, str):
            out.append(item if isinstance(item, Markup) else escape_text(item))
        elif isinstance(item, VoidElement):
            out.append(tag_markup(item.tag)[0])
            for name, value in item.attributes:
                out.append(f" {name}" if value is None else f' {name}="{value}"')
            out.append(">")
        elif item is None:
            continue
        elif isinstance(item, SupportsHtml):
            out.append(item.__html__())
        elif isinstance(item, Iterable):
            stack.extend(reversed(list(item)))
        else:
            raise TypeError(f"not renderable: {item!r}")
    return "".join(out)
