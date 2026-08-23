"""
Generate the named element constructors for `without-html`.

One `def` per HTML element, differing only in a tag string and in which arguments
the element can take, so they are generated rather than hand-maintained: `cog`
invokes `emit()` in place (see the `# [[[cog ... ]]]` block in `elements.py`), and
a pre-commit hook keeps the checked-in output in sync.

Generating them rather than building the constructors dynamically is what keeps
each one's signature real: a void element has no `children` parameter and a
raw-text element's is `Markup | None`, so misuse is a type error at the call site
rather than a `ValueError` at runtime.

This module is a build-time tool; it is deliberately outside the shipped
`without_html` package and imported only via `cog -I tools`.
"""

from __future__ import annotations

# Which tags are void and which are raw-text is a property of HTML that the renderer
# also needs at runtime, so it is declared once in the package and read here rather
# than restated.
from without_html.nodes import RAW_TEXT_TAGS
from without_html.nodes import VOID_TAGS

# Every element this package names, in the order the reference page should read:
# document structure outward to leaves, then tables, then forms.
#
# `xmp`, `noembed`, and `noframes` are obsolete and nothing should reach for them; they are
# named anyway because they are raw-text elements, and `RAW_TEXT_TAGS` refuses a raw-text
# tag to the generic factory by telling the caller to use its constructor. Leaving them out
# would make that message name something that does not exist.
TAGS = (
    "html",
    "head",
    "base",
    "link",
    "meta",
    "style",
    "title",
    "body",
    "address",
    "article",
    "aside",
    "footer",
    "header",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "hgroup",
    "main",
    "nav",
    "search",
    "section",
    "blockquote",
    "dd",
    "div",
    "dl",
    "dt",
    "figcaption",
    "figure",
    "hr",
    "li",
    "menu",
    "ol",
    "p",
    "pre",
    "xmp",
    "ul",
    "a",
    "abbr",
    "b",
    "bdi",
    "bdo",
    "br",
    "cite",
    "code",
    "data",
    "dfn",
    "em",
    "i",
    "kbd",
    "mark",
    "q",
    "rp",
    "rt",
    "ruby",
    "s",
    "samp",
    "small",
    "span",
    "strong",
    "sub",
    "sup",
    "time",
    "u",
    "var",
    "wbr",
    "area",
    "audio",
    "img",
    "map",
    "track",
    "video",
    "embed",
    "noembed",
    "iframe",
    "noframes",
    "object",
    "picture",
    "source",
    "svg",
    "canvas",
    "noscript",
    "script",
    "del",
    "ins",
    "caption",
    "col",
    "colgroup",
    "table",
    "tbody",
    "td",
    "tfoot",
    "th",
    "thead",
    "tr",
    "button",
    "datalist",
    "fieldset",
    "form",
    "input",
    "label",
    "legend",
    "meter",
    "optgroup",
    "option",
    "output",
    "progress",
    "select",
    "textarea",
    "details",
    "dialog",
    "summary",
    "slot",
    "template",
)

# Tags whose name is a Python keyword or shadows a builtin (which ruff's
# flake8-builtins rules reject), suffixed rather than renamed so the tag stays
# recognizable.
RESERVED = frozenset({"del", "input", "map", "object"})


def constructor_name(tag: str) -> str:
    """The Python name for `tag`, suffixed where the tag itself cannot be one."""
    return f"{tag}_" if tag in RESERVED else tag


def definition(tag: str) -> str:
    """
    One element constructor, with the parameters that element can actually take.

    The body builds its node directly rather than going through the generic `element`
    factory: which constraints apply is decided here, at generation time, so repeating
    the tag lookups on every call would be paying at runtime for an answer the
    signature already encodes.
    """
    name = constructor_name(tag)
    common = "*, cls: ClassNames = None, attrs: Attributes | None = None"
    if tag in VOID_TAGS:
        signature = f"def {name}({common}) -> VoidElement:"
        body = f'return VoidElement("{tag}", attributes_of(cls, attrs))'
    elif tag in RAW_TEXT_TAGS:
        signature = f"def {name}({common}, children: Markup | None = None) -> Element:"
        body = f'return Element("{tag}", attributes_of(cls, attrs), raw_text_of("{tag}", children))'
    else:
        signature = f"def {name}({common}, children: Node = None) -> Element:"
        body = f'return Element("{tag}", attributes_of(cls, attrs), children_of(children))'
    return f'{signature}\n    """The `<{tag}>` element."""\n    {body}'


def emit() -> str:
    """The whole generated body, for a `cog.outl(emit())` block."""
    return "\n\n\n".join(definition(tag) for tag in TAGS)


def constructor_names() -> list[str]:
    """Every generated constructor name, sorted."""
    return sorted(constructor_name(tag) for tag in TAGS)


def emit_imports() -> str:
    """The element constructor imports, for a `cog.outl(emit_imports())` block in `__init__.py`."""
    return "\n".join(f"from without_html.elements import {name}" for name in constructor_names())


def emit_names(extra: list[str]) -> str:
    """
    The public names as `__all__` entries: the generated constructors merged with `extra`.

    Sorted the way ruff's `RUF022` wants an `__all__` sorted (constants first, then
    the rest lexicographically) so that the linter leaves the generated block alone
    and running `cog` then `ruff` is a fixed point.
    """
    names = sorted(constructor_names() + extra, key=lambda name: (not name.isupper(), name))
    return "\n".join(f'    "{name}",' for name in names)
