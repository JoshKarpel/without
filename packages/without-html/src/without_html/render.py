from __future__ import annotations

from collections.abc import Iterable
from collections.abc import Iterator
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

# Per-tag constant markup, memoized on first use. A tag's surrounding text never varies,
# so building it once per distinct tag turns two string formats per element into one
# dictionary lookup. The closing tag is held as a `Markup` so that pushing it onto the
# render stack allocates nothing, and it is built for void tags too, where it costs one
# unused string per process and saves every other element a second lookup.
TAG_MARKUP: dict[str, TagMarkup] = {}

# How many pieces `render_chunks` accumulates before emitting one. Fine enough that a
# large page leaves in many chunks, coarse enough that the batching is a rounding error
# against the walk: measured on a 177 KB page, chunking costs about 8% over `fragments`
# alone, while checking a byte budget on every piece costs over 30%.
CHUNK_PIECES = 512


def tag_markup(tag: str) -> TagMarkup:
    """The memoized opening and closing markup for `tag`."""
    markup = TAG_MARKUP.get(tag)
    if markup is None:
        markup = TAG_MARKUP[tag] = (f"<{tag}", Markup(f"</{tag}>"))
    return markup


def unrenderable(item: object) -> str:
    """Why `item` cannot be rendered, reached only once a walk has already failed."""
    if isinstance(item, Iterable):
        return f"an iterable in a child position is not flattened; unpack it with `*`: {item!r}"
    return f"not renderable: {item!r}"


# `render` and `fragments` are one walk, generated into two functions from
# `tools/walk.py` so that the escaping ladder has a single source and neither pays a
# function call or a generator resumption per piece to share it.


# [[[cog import cog; from walk import emit; cog.outl(emit()) ]]]
def render(node: Node) -> str:
    """
    Render a node tree to markup.

    A pure function of the tree: no I/O, no ambient state, and no decision about how
    the result reaches a client. Pair it with `without_asgi.html_content` to answer a
    request, or write it to a file, or compare it in a test.

    Pieces are collected and joined once, so each byte is copied a single time. For a
    body that should leave the process as it is produced, `render_chunks` walks the
    same tree without holding the whole string.
    """
    out: list[str] = []
    stack: list[Child] = list(children_of(node))
    stack.reverse()
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
        elif type(item) is Markup:
            # Every element pushes its closing tag back onto the stack as a `Markup`, so
            # this arm runs once per element and is worth reaching by pointer comparison
            # rather than by the `isinstance` pair it would otherwise take to tell an
            # already-safe string from one that still needs escaping.
            out.append(item)
        elif type(item) is str:
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
        else:
            raise TypeError(unrenderable(item))
    return "".join(out)


def fragments(node: Node) -> Iterator[str]:
    """
    Yield the tree's markup one piece at a time: a tag, an attribute, a run of text.

    The granularity the walk itself produces, which is far finer than anything should
    be written or sent at: a page is tens of thousands of pieces. `render` joins them
    and `render_chunks` batches them; this is the shared walk both are built on.
    """
    stack: list[Child] = list(children_of(node))
    stack.reverse()
    while stack:
        item = stack.pop()
        if isinstance(item, Element):
            opening, closing = tag_markup(item.tag)
            yield opening
            for name, value in item.attributes:
                yield f" {name}" if value is None else f' {name}="{value}"'
            yield ">"
            stack.append(closing)
            stack.extend(reversed(item.children))
        elif type(item) is Markup:
            # Every element pushes its closing tag back onto the stack as a `Markup`, so
            # this arm runs once per element and is worth reaching by pointer comparison
            # rather than by the `isinstance` pair it would otherwise take to tell an
            # already-safe string from one that still needs escaping.
            yield item
        elif type(item) is str:
            yield escape_text(item)
        elif isinstance(item, str):
            yield item if isinstance(item, Markup) else escape_text(item)
        elif isinstance(item, VoidElement):
            yield tag_markup(item.tag)[0]
            for name, value in item.attributes:
                yield f" {name}" if value is None else f' {name}="{value}"'
            yield ">"
        elif item is None:
            continue
        elif isinstance(item, SupportsHtml):
            yield item.__html__()
        else:
            raise TypeError(unrenderable(item))


# [[[end]]]


def render_chunks(node: Node, *, pieces: int = CHUNK_PIECES) -> Iterator[str]:
    """
    Render a node tree to markup a chunk at a time.

    The same walk `render` does and the same bytes in the same order, handed back as
    they are produced rather than held whole, so a large page starts reaching a client
    while the rest of it is still being built and the process never holds the finished
    string. `"".join(render_chunks(node))` is `render(node)`.

    A chunk is `pieces` fragments joined, not a byte budget: counting bytes on every
    fragment costs several times what batching them does, and the point of the knob is
    to bound how often a consumer is called, not to hand it uniform buffers. Chunks are
    therefore roughly even in size but not exactly, and a single large `Markup` child
    goes out whole in whatever chunk it lands in.

    What streaming costs is worth choosing deliberately rather than defaulting into: the
    length is not known in advance, so a response is framed as `transfer-encoding:
    chunked`, and once the first chunk is gone the status line is gone with it, so a
    failure partway through a tree can no longer become a 500.
    """
    remaining = fragments(node)
    while batch := list(islice(remaining, pieces)):
        chunk = "".join(batch)
        if chunk:
            yield chunk
