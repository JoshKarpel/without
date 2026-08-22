"""
Generate the two renderers in `without_html.render` from one walk.

`render` and `fragments` differ in exactly one thing: what they do with each piece of
markup as it is produced. Written by hand they would be two long near-identical
functions, and the lines that would drift are the escaping ladder, where a divergence
is a security bug rather than a cosmetic one.

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

# The two bodies that appear in both an exact-type arm and its subclass arm, written once
# here and substituted into both.
#
# The closing tag is pushed back onto the stack because it has to follow the children, so
# the push can be skipped exactly when the children are already known. Two shapes qualify,
# and between them they cover most of a page: an element with nothing inside, and one
# wrapping a single run of text (`<td>7</td>`, `<h1>Runs</h1>`, `<li>tea</li>`). Both close
# in place, which drops a push, a pop, and a trip round the ladder each, and the text case
# drops a second set for the child. Worth 20-35% on ordinary pages, and nothing on a tree
# with no text leaves, where the two tests fail cheaply.
#
# `type(only) is str` is exact, so a `Markup` child does not take this path and still
# reaches the arm that emits it verbatim.
ELEMENT_BODY = """\
            opening, closing = tag_markup(item.tag)
            EMIT(opening)
            for name, value in item.attributes:
                EMIT(f" {name}" if value is None else f' {name}="{value}"')
            EMIT(">")
            children = item.children
            if not children:
                EMIT(closing)
            elif len(children) == 1 and type(only := children[0]) is str:
                EMIT(escape_text(only))
                EMIT(closing)
            else:
                stack.append(closing)
                stack.extend(reversed(children))
"""

VOID_BODY = """\
            EMIT(tag_markup(item.tag)[0])
            for name, value in item.attributes:
                EMIT(f" {name}" if value is None else f' {name}="{value}"')
            EMIT(">")
"""

# The walk itself, once. `EMIT(x)` marks a fragment of markup leaving the renderer and is
# rewritten per target; everything else is copied verbatim into both functions.
#
# Iterative over an explicit stack rather than recursive, because a page is thousands of
# nodes in aggregate and a Python frame per node is the largest avoidable cost here.
#
# Every shape that can appear in a well-typed tree is reached by pointer comparison, with
# `isinstance` demoted to the arms below `None`, which only a subclass reaches. That split
# is worth 7-9% on ordinary pages and a third on a tree of pre-rendered `Markup`, because
# an exact-type test is a pointer comparison where `isinstance` is a call. `type()` rather
# than `__class__` because an object can set the latter to whatever it likes, and here it
# decides whether text is escaped.
#
# `type()` is repeated per arm rather than hoisted into a local, which is surprising enough
# to be worth stating outright: hoisting it looks like it should turn up to four calls into
# one, and it does not measure that way. Across four workloads the two are within a couple
# of percent of each other in *both* directions, so whatever a repeated `type()` costs is
# already below what the ladder around it costs. Since mypy narrows `type(item) is Element`
# but not a variable holding the result, hoisting would trade the type checker's guarantee
# for nothing, in the one function where taking the wrong arm means emitting unescaped text.
# Re-measure before assuming the obvious version is faster.
#
# `Element` is tested before `Markup` because closing in place left `Markup` the rarer of
# the two: only an element that could not close in place pushes a closing tag, so on an
# ordinary page most of what reaches this arm is author-supplied. That ordering is worth
# 2-5%, and costs about a quarter on a tree that is mostly pre-rendered `Markup`, which is
# what caching whole subtrees produces. It stays this way round because the saving lands on
# the larger number: the ordinary page is where the milliseconds are, and a tree of cached
# `Markup` is already an order of magnitude cheaper to render whichever arm comes first.
WALK = (
    """\
    stack: list[Child] = list(children_of(node))
    stack.reverse()
    while stack:
        item = stack.pop()
        if type(item) is Element:
ELEMENT_BODY
        elif type(item) is Markup:
            # A closing tag pushed by the arm above, or markup the caller supplied.
            EMIT(item)
        elif type(item) is str:
            EMIT(escape_text(item))
        elif type(item) is VoidElement:
VOID_BODY
        elif item is None:
            continue
        elif isinstance(item, Element):
ELEMENT_BODY
        elif isinstance(item, str):
            EMIT(item if isinstance(item, Markup) else escape_text(item))
        elif isinstance(item, VoidElement):
VOID_BODY
        elif isinstance(item, SupportsHtml):
            EMIT(item.__html__())
        else:
            raise TypeError(unrenderable(item))
"""
    # Not an f-string: the walk contains `{name}` and `{value}` that must survive verbatim.
    .replace("ELEMENT_BODY\n", ELEMENT_BODY).replace("VOID_BODY\n", VOID_BODY)
)

EMITTED = re.compile(r"^(?P<indent> *)EMIT\((?P<piece>.*)\)$", re.MULTILINE)

RENDER_DOC = '''    """
    Render a node tree to markup.

    A pure function of the tree: no I/O, no ambient state, and no decision about how
    the result reaches a client. Pair it with `without_asgi.html_content` to answer a
    request, or write it to a file, or compare it in a test.

    Fragments are collected and joined once, so each byte is copied a single time. For
    a body that should leave the process as it is produced, `render_chunks` walks the
    same tree without holding the whole string.
    """
'''

FRAGMENTS_DOC = '''    """
    Yield the tree's markup one fragment at a time: a tag, an attribute, a run of text.

    The granularity the walk itself produces, which is far finer than anything should
    be written or sent at: a page is tens of thousands of fragments. `render` joins them
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
