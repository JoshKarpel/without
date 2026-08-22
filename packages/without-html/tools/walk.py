"""
Generate the two renderers in `without_html.render` from one walk.

`render` and `fragments` differ in exactly one thing: what they do with each piece of
markup as it is produced. Written by hand they would be thirty near-identical lines
apiece, and the lines that would drift are the escaping ladder, where a divergence is
a security bug rather than a cosmetic one.

Sharing them at *runtime* was measured instead of assumed, and costs more than it
saves: defining `render` as `"".join(fragments(node))` is one walk, but generator
resumption puts it about 8% behind the loop that appends to a list, on a function
every response crosses. So the walk is shared at the source and inlined into both,
which is what `cog` is for here, the same trade `tools/tags.py` makes.

This module is a build-time tool; it is deliberately outside the shipped
`without_html` package and imported only via `cog -I tools`.
"""

from __future__ import annotations

import re

# The walk itself, once. `EMIT(x)` marks a piece of markup leaving the renderer and is
# rewritten per target; everything else is copied verbatim into both functions.
#
# Iterative over an explicit stack rather than recursive, because a page is thousands of
# nodes in aggregate and a Python frame per node is the largest avoidable cost here.
#
# The ladder is ordered by how often each shape appears, and reaches the common arms by
# pointer comparison: `type()` rather than `isinstance` where an exact match is what is
# meant, and rather than `__class__` because an object can set that to whatever it likes
# and here it decides whether text is escaped.
WALK = """\
    stack: list[Child] = list(children_of(node))
    stack.reverse()
    while stack:
        item = stack.pop()
        if isinstance(item, Element):
            opening, closing = tag_markup(item.tag)
            EMIT(opening)
            for name, value in item.attributes:
                EMIT(f" {name}" if value is None else f' {name}="{value}"')
            EMIT(">")
            stack.append(closing)
            stack.extend(reversed(item.children))
        elif type(item) is Markup:
            # Every element pushes its closing tag back onto the stack as a `Markup`, so
            # this arm runs once per element and is worth reaching by pointer comparison
            # rather than by the `isinstance` pair it would otherwise take to tell an
            # already-safe string from one that still needs escaping.
            EMIT(item)
        elif type(item) is str:
            EMIT(escape_text(item))
        elif isinstance(item, str):
            EMIT(item if isinstance(item, Markup) else escape_text(item))
        elif isinstance(item, VoidElement):
            EMIT(tag_markup(item.tag)[0])
            for name, value in item.attributes:
                EMIT(f" {name}" if value is None else f' {name}="{value}"')
            EMIT(">")
        elif item is None:
            continue
        elif isinstance(item, SupportsHtml):
            EMIT(item.__html__())
        else:
            raise TypeError(unrenderable(item))
"""

EMITTED = re.compile(r"^(?P<indent> *)EMIT\((?P<piece>.*)\)$", re.MULTILINE)

RENDER_DOC = '''    """
    Render a node tree to markup.

    A pure function of the tree: no I/O, no ambient state, and no decision about how
    the result reaches a client. Pair it with `without_asgi.html_content` to answer a
    request, or write it to a file, or compare it in a test.

    Pieces are collected and joined once, so each byte is copied a single time. For a
    body that should leave the process as it is produced, `render_chunks` walks the
    same tree without holding the whole string.
    """
'''

FRAGMENTS_DOC = '''    """
    Yield the tree's markup one piece at a time: a tag, an attribute, a run of text.

    The granularity the walk itself produces, which is far finer than anything should
    be written or sent at: a page is tens of thousands of pieces. `render` joins them
    and `render_chunks` batches them; this is the shared walk both are built on.
    """
'''


def emitted(target: str) -> str:
    """The walk, with each `EMIT(x)` rewritten to `target`'s way of emitting `x`."""
    replacement = r"\g<indent>yield \g<piece>" if target == "yield" else rf"\g<indent>{target}(\g<piece>)"
    return EMITTED.sub(replacement, WALK)


def emit() -> str:
    """Both renderers, for a `cog.outl(emit())` block."""
    render = f'def render(node: Node) -> str:\n{RENDER_DOC}    out: list[str] = []\n{emitted("out.append")}    return "".join(out)'
    fragments = f"def fragments(node: Node) -> Iterator[str]:\n{FRAGMENTS_DOC}{emitted('yield')}"
    return f"{render}\n\n\n{fragments}"
