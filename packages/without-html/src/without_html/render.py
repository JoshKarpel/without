from __future__ import annotations

from collections.abc import Iterable
from collections.abc import Iterator
from functools import cache
from itertools import islice

from markupsafe import Markup

from without_html.markup import SupportsHtml
from without_html.markup import escape_text
from without_html.nodes import Child
from without_html.nodes import Element
from without_html.nodes import Node
from without_html.nodes import VoidElement
from without_html.nodes import children_of

# A tag's opening prefix (`<div`) and its closing tag (`</div>`).
type TagMarkup = tuple[str, Markup]

# How many fragments `render_chunks` accumulates before emitting one chunk. Fine enough
# that a large page leaves in many chunks, coarse enough that the batching is a rounding
# error against the walk: measured on a 177 KB page, chunking costs about 8% over
# `fragments` alone, while checking a byte budget on every fragment costs over 30%.
FRAGMENTS_PER_CHUNK = 512


# Per-tag constant markup, memoized on first use. A tag's surrounding text never varies,
# so building it once per distinct tag turns two string formats per element into one cache
# hit. The closing tag is held as a `Markup` so that pushing it onto the render stack
# allocates nothing, and it is built for void tags too, where it costs one unused string
# per process and saves every other element a second lookup.
@cache
def tag_markup(tag: str) -> TagMarkup:
    """The memoized opening and closing markup for `tag`."""
    return (f"<{tag}", Markup(f"</{tag}>"))


def unrenderable(item: object) -> str:
    """Why `item` cannot be rendered, reached only once a walk has already failed."""
    if isinstance(item, Iterable):
        return f"an iterable in a child position is not flattened; unpack it with `*`: {item!r}"
    return f"not renderable: {item!r}"


# `render` and `fragments` are one walk, generated into two functions from
# `tools/walk.py` so that the escaping ladder has a single source and neither pays a
# function call or a generator resumption per fragment to share it.


# [[[cog import cog; from walk import emit; cog.outl(emit()) ]]]
def render(node: Node) -> str:
    """
    Render a node tree to markup.

    A pure function of the tree: no I/O, no ambient state, and no decision about how
    the result reaches a client. Pair it with `without_asgi.html_content` to answer a
    request, or write it to a file, or compare it in a test.

    Fragments are collected and joined once, so each byte is copied a single time. For
    a body that should leave the process as it is produced, `render_chunks` walks the
    same tree without holding the whole string.
    """
    out: list[str] = []
    stack: list[Child] = list(children_of(node))
    stack.reverse()
    while stack:
        item = stack.pop()
        if type(item) is Element:
            opening, closing = tag_markup(item.tag)
            out.append(opening)
            for name, value in item.attributes:
                out.append(f" {name}" if value is None else f' {name}="{value}"')
            out.append(">")
            children = item.children
            if not children:
                out.append(closing)
            elif len(children) == 1 and type(only := children[0]) is str:
                out.append(escape_text(only))
                out.append(closing)
            else:
                stack.append(closing)
                stack.extend(reversed(children))
        elif type(item) is Markup:
            # A closing tag pushed by the arm above, or markup the caller supplied.
            out.append(item)
        elif type(item) is str:
            out.append(escape_text(item))
        elif type(item) is VoidElement:
            out.append(tag_markup(item.tag)[0])
            for name, value in item.attributes:
                out.append(f" {name}" if value is None else f' {name}="{value}"')
            out.append(">")
        elif item is None:
            continue
        elif isinstance(item, Element):
            opening, closing = tag_markup(item.tag)
            out.append(opening)
            for name, value in item.attributes:
                out.append(f" {name}" if value is None else f' {name}="{value}"')
            out.append(">")
            children = item.children
            if not children:
                out.append(closing)
            elif len(children) == 1 and type(only := children[0]) is str:
                out.append(escape_text(only))
                out.append(closing)
            else:
                stack.append(closing)
                stack.extend(reversed(children))
        elif isinstance(item, str):
            out.append(item if isinstance(item, Markup) else escape_text(item))
        elif isinstance(item, VoidElement):
            out.append(tag_markup(item.tag)[0])
            for name, value in item.attributes:
                out.append(f" {name}" if value is None else f' {name}="{value}"')
            out.append(">")
        elif isinstance(item, SupportsHtml):
            out.append(item.__html__())
        else:
            raise TypeError(unrenderable(item))
    return "".join(out)


def fragments(node: Node) -> Iterator[str]:
    """
    Yield the tree's markup one fragment at a time: a tag, an attribute, a run of text.

    The granularity the walk itself produces, which is far finer than anything should
    be written or sent at: a page is tens of thousands of fragments. `render` joins them
    and `render_chunks` batches them; this is the shared walk both are built on.
    """
    stack: list[Child] = list(children_of(node))
    stack.reverse()
    while stack:
        item = stack.pop()
        if type(item) is Element:
            opening, closing = tag_markup(item.tag)
            yield opening
            for name, value in item.attributes:
                yield f" {name}" if value is None else f' {name}="{value}"'
            yield ">"
            children = item.children
            if not children:
                yield closing
            elif len(children) == 1 and type(only := children[0]) is str:
                yield escape_text(only)
                yield closing
            else:
                stack.append(closing)
                stack.extend(reversed(children))
        elif type(item) is Markup:
            # A closing tag pushed by the arm above, or markup the caller supplied.
            yield item
        elif type(item) is str:
            yield escape_text(item)
        elif type(item) is VoidElement:
            yield tag_markup(item.tag)[0]
            for name, value in item.attributes:
                yield f" {name}" if value is None else f' {name}="{value}"'
            yield ">"
        elif item is None:
            continue
        elif isinstance(item, Element):
            opening, closing = tag_markup(item.tag)
            yield opening
            for name, value in item.attributes:
                yield f" {name}" if value is None else f' {name}="{value}"'
            yield ">"
            children = item.children
            if not children:
                yield closing
            elif len(children) == 1 and type(only := children[0]) is str:
                yield escape_text(only)
                yield closing
            else:
                stack.append(closing)
                stack.extend(reversed(children))
        elif isinstance(item, str):
            yield item if isinstance(item, Markup) else escape_text(item)
        elif isinstance(item, VoidElement):
            yield tag_markup(item.tag)[0]
            for name, value in item.attributes:
                yield f" {name}" if value is None else f' {name}="{value}"'
            yield ">"
        elif isinstance(item, SupportsHtml):
            yield item.__html__()
        else:
            raise TypeError(unrenderable(item))


# [[[end]]]


def render_chunks(node: Node, *, fragments_per_chunk: int = FRAGMENTS_PER_CHUNK) -> Iterator[str]:
    """
    Render a node tree to markup a chunk at a time.

    The same walk `render` does and the same bytes in the same order, handed back as
    they are produced rather than held whole, so a large page starts reaching a client
    while the rest of it is still being built and the process never holds the finished
    string. `"".join(render_chunks(node))` is `render(node)`.

    A chunk is `fragments_per_chunk` fragments joined, not a byte budget: counting bytes
    on every fragment costs several times what batching them does, and the point of the
    knob is to bound how often a consumer is called, not to hand it uniform buffers.
    Chunks are therefore roughly even in size but not exactly, and a single large
    `Markup` child goes out whole in whatever chunk it lands in.

    What streaming costs is worth choosing deliberately rather than defaulting into: the
    total length is not known until the walk ends, and once the first chunk has been
    handed on there is no taking it back, so a failure partway through a tree can no
    longer be turned into something else.
    """
    remaining = fragments(node)
    while batch := list(islice(remaining, fragments_per_chunk)):
        yield "".join(batch)
