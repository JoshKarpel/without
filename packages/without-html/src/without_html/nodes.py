from __future__ import annotations

from collections.abc import Iterable
from collections.abc import Mapping
from dataclasses import dataclass
from types import GeneratorType
from typing import Protocol

from markupsafe import Markup

from without_html.markup import SupportsHtml
from without_html.markup import escape_attribute

# What an attribute may be set to before rendering. `True` renders the attribute bare
# (`disabled`), `False` and `None` drop it entirely, and `int` is accepted because the
# attributes that take numbers (`colspan`, `tabindex`, `aria-level`) are machine-read and
# have one obvious spelling. Text in a *child* position is deliberately not: how a number
# reads to a person is a formatting decision this layer should not make.
type AttributeValue = str | int | bool | None

# Attribute names as written, mapped to their values. Names pass through verbatim.
type Attributes = Mapping[str, AttributeValue]

# One class string, or several to be joined with spaces.
type ClassNames = str | Iterable[str] | None

# One rendered attribute: an escaped value, or `None` for a bare attribute.
type Attribute = tuple[str, str | None]

# Either kind of element, for code that walks a tree without caring which.
type AnyElement = Element | VoidElement

# Anything that can appear in a child position. A `str` renders escaped; a `Markup` (or
# any `SupportsHtml`) renders verbatim; `None` renders nothing, so
# `paragraph if visible else None` needs no branch around it; and any other iterable
# flattens, so a generator expression over rows is a child like any other.
type Node = Element | VoidElement | str | SupportsHtml | Iterable[Node] | None

# Elements with no children and no closing tag. Read at build time to decide which
# constructors produce a `VoidElement`, and to reject the generic `element` factory for a
# tag that has a named constructor already. HTML's void set is closed and custom elements
# may not join it, so nothing needs to declare a *new* void tag.
VOID_TAGS = frozenset(
    {"area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "source", "track", "wbr"}
)

# Elements whose content is *not* parsed as markup, so it must not be escaped. Escaping
# here would corrupt the script or stylesheet (`a && b` becoming `a &amp;&amp; b`), while
# not escaping arbitrary text would be an injection hole. The way out is to accept neither
# decision on the caller's behalf: content for these elements must arrive already
# `Markup`, so the author states that the text is theirs rather than a visitor's.
RAW_TEXT_TAGS = frozenset({"script", "style"})

# Attribute names already checked, so each distinct name is validated once per process.
# Names are overwhelmingly literals repeated across every row of a table, so caching turns
# a per-attribute check into a per-vocabulary one.
CHECKED_ATTRIBUTE_NAMES: set[str] = set()

FORBIDDEN_IN_NAME = frozenset(" \t\n\r\f\v\"'>/=")


def checked_name(name: str) -> str:
    """
    The attribute name, having proven it cannot break out of the attribute list.

    A name is normally a literal the application author wrote, so a bad one is a bug
    in code they own and fails loudly here. It is checked at all because a name
    assembled from outside input would otherwise be an injection point that no amount
    of value escaping closes.

    `class` is rejected outright rather than merged with `cls`, so classes have one
    source and not two kept in sync. Costing nothing extra falls out of the cache: it
    is never admitted to it, so every occurrence takes the slow path and raises, while
    every other name is a single set lookup.
    """
    if name in CHECKED_ATTRIBUTE_NAMES:
        return name
    if name == "class":
        raise ValueError("set classes with `cls`, not as an attribute")
    if not name or not FORBIDDEN_IN_NAME.isdisjoint(name):
        raise ValueError(f"invalid attribute name: {name!r}")
    CHECKED_ATTRIBUTE_NAMES.add(name)
    return name


def attributes_of(cls: ClassNames, attrs: Attributes | None) -> tuple[Attribute, ...]:
    """
    Normalize `cls` and `attrs` into rendered attributes, escaped and in order.

    Escaping happens here rather than at render so that an element is a value that has
    already been proven safe to emit, and so one built once and rendered many times
    pays for it once.
    """
    if cls is None and not attrs:
        return ()
    rendered: list[Attribute] = []
    if cls is not None:
        names = cls if isinstance(cls, str) else " ".join(cls)
        if names:
            rendered.append(("class", escape_attribute(names)))
    if attrs:
        for name, value in attrs.items():
            if value is None or value is False:
                continue
            if value is True:
                rendered.append((checked_name(name), None))
            else:
                rendered.append((checked_name(name), escape_attribute(value if type(value) is str else str(value))))
    return tuple(rendered)


def children_of(children: Node) -> tuple[Node, ...]:
    """
    Normalize a child argument into a tuple of nodes.

    A caller's list is copied rather than held, so an element cannot change after it is
    built: what arrives as a place becomes a value at the edge. A generator is consumed
    here for the same reason, which also means an element can be rendered more than once.

    Dispatch is a ladder of exact-type identity checks before any `isinstance`, ordered by
    how often each shape actually appears. The `isinstance` checks behind them still decide
    every case the ladder does not, subclasses included; they are just no longer on the
    common path, where the `Iterable` ABC check in particular costs several times a pointer
    comparison. Their type tuples are bound once rather than rebuilt on every call.
    """
    kind = children.__class__
    if kind is str or kind is Markup:
        return (children,)
    if kind is Element or kind is VoidElement:
        return (children,)
    if children is None:
        return ()
    if isinstance(children, FLATTENED_TYPES):
        return tuple(children)
    if isinstance(children, SINGLE_NODE_TYPES):
        return (children,)
    if isinstance(children, Iterable):
        return tuple(children)
    return (children,)


def raw_text_of(tag: str, children: Node) -> tuple[Node, ...]:
    """
    Normalize the content of a raw-text element, which must already be `Markup`.

    The type of every raw-text constructor's `children` says the same thing, so a
    caller under a type checker never reaches this. It is still checked because the
    alternative failure is silent: a plain string would be escaped, and a stylesheet
    or script that arrives entity-encoded is broken in a way that points nowhere near
    the code that caused it.
    """
    if children is None:
        return ()
    if not isinstance(children, Markup):
        raise ValueError(f"<{tag}> content is not parsed as markup, so it must be `Markup`")
    return (children,)


@dataclass(frozen=True, slots=True)
class VoidElement:
    """
    An element with no content and no closing tag: `<br>`, `<img>`, `<input>`.

    A separate type rather than a flag, so that giving one children is not a mistake
    to be caught but a thing that cannot be written: there is no field to put them in
    and no parameter on the constructors that build these.
    """

    tag: str
    attributes: tuple[Attribute, ...] = ()


@dataclass(frozen=True, slots=True)
class Element:
    """
    An element with content: a tag, its rendered attributes, and its children.

    Build these through a named constructor in `without_html.elements`, or `element`
    for a tag HTML does not define. The fields here are the already-parsed form, with
    attributes escaped and children flattened to a tuple.
    """

    tag: str
    attributes: tuple[Attribute, ...] = ()
    children: tuple[Node, ...] = ()


# The `isinstance` arguments `children_of` uses, bound once instead of built into a fresh
# tuple on every call. They live here because they name the element classes above.
SINGLE_NODE_TYPES = (str, Element, VoidElement)
FLATTENED_TYPES = (list, tuple, GeneratorType)


class ElementConstructor(Protocol):
    """The call signature every named element constructor shares, and its tag identity."""

    __name__: str

    def __call__(self, *, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
        """Build one element of this kind."""
        ...


class VoidElementConstructor(Protocol):
    """The call signature every named void element constructor shares, and its tag identity."""

    __name__: str

    def __call__(self, *, cls: ClassNames = None, attrs: Attributes | None = None) -> VoidElement:
        """Build one void element of this kind."""
        ...


def named(construct: object, tag: str) -> None:
    """Give a generated constructor the tag's identity, so it reprs and documents as itself."""
    construct.__name__ = tag  # type: ignore[attr-defined]
    construct.__qualname__ = tag  # type: ignore[attr-defined]
    construct.__doc__ = f"The `<{tag}>` element."


def refuse_named_tag(tag: str) -> None:
    """Reject a tag whose element carries a constraint the generic form cannot express."""
    if tag in VOID_TAGS:
        raise ValueError(f"<{tag}> is a void element; use the `{tag}` constructor or `void_element_type`")
    if tag in RAW_TEXT_TAGS:
        raise ValueError(f"<{tag}> content is not parsed as markup; use the `{tag}` constructor")


def element_type(tag: str) -> ElementConstructor:
    """
    Define a constructor for `tag`, equal in standing to the named ones.

    How a custom element joins the vocabulary: bind it once at module scope and use it
    exactly like `div`, rather than repeating a tag string at every call site.

    ```python
    chart = element_type("x-chart")
    chart(attrs={"data-series": series}, children=caption)
    ```

    Whether the tag is one this package handles specially is settled here, when the
    constructor is defined, so calling it does no checking at all. That is the same
    trade the generated constructors make, available to a tag that was not known when
    they were generated.
    """
    refuse_named_tag(tag)

    def construct(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
        return Element(tag, attributes_of(cls, attrs), children_of(children))

    named(construct, tag)
    return construct


def void_element_type(tag: str) -> VoidElementConstructor:
    """
    Define a constructor for `tag` as an element with no content and no closing tag.

    The other arm of `element_type`, for a tag whose content model is empty. HTML's own
    void elements are named already and a custom element may not be void, so this is for
    markup that is not quite HTML: an XML-ish document, or a foreign vocabulary rendered
    through the same tree.
    """

    def construct(*, cls: ClassNames = None, attrs: Attributes | None = None) -> VoidElement:
        return VoidElement(tag, attributes_of(cls, attrs))

    named(construct, tag)
    return construct


def element(tag: str, *, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """
    Build one element with any tag, without naming a constructor first.

    The one-shot form of `element_type`, for a tag used once (an SVG child, a custom
    element that appears in a single component). Where the tag appears more than once,
    `element_type` reads better and moves the tag check out of the call.
    """
    refuse_named_tag(tag)
    return Element(tag, attributes_of(cls, attrs), children_of(children))
