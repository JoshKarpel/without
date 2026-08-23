from __future__ import annotations

from collections.abc import Iterable
from collections.abc import Iterator
from collections.abc import Mapping
from collections.abc import Sequence
from collections.abc import Set as AbstractSet
from dataclasses import dataclass
from string import ascii_letters
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

# One class string, or several to be joined with spaces. An entry may itself hold several
# names, so `("card", size_classes)` needs no thought about which form a part is in, and an
# entry may be `None` or empty, so `("card", "card-active" if active else None)` needs no
# filtering around it.
#
# Looser than `Node` about *lifetime*, and for a reason that does not extend past it: what
# makes an iterable in a child position a problem is that the element goes on holding it,
# and this one never survives the call. It is joined into a string before the element
# exists, so a caller's list cannot be mutated afterwards and a generator cannot be left
# half-consumed.
#
# `Sequence | Iterator` all the same, because order and meaning are not lifetime. A `set`
# joins in an order that varies between processes, so the same tree renders differently
# each run; a `Mapping` joins its keys, so `{"card": True, "active": False}`, the shape
# `classnames` and `clsx` made the idiom for this job in JavaScript, renders *both* names.
# The `filter` below drops falsy class names, not falsy values, and it cannot do otherwise:
# the conditional spelling this type is built around is
# `("card", "card-active" if active else None)`, and a second one meaning the same thing
# would be two channels into one attribute, which is what `attrs` rejects `class` for.
type ClassNames = str | Sequence[str | None] | Iterator[str | None] | None

# One rendered attribute: an escaped value, or `None` for a bare attribute.
type Attribute = tuple[str, str | None]

# Either kind of element, for code that walks a tree without caring which.
type AnyElement = Element | VoidElement

# One thing in a child position. A `str` renders escaped; a `Markup` (or any
# `SupportsHtml`) renders verbatim; and `None` renders nothing, so
# `paragraph if visible else None` needs no branch around it.
type Child = Element | VoidElement | str | SupportsHtml | None

# What a child slot accepts: one child, or a sequence or iterator of them, so a generator
# expression over rows is a child like any other.
#
# Deliberately one level and not recursive. A recursive type would buy only the ability to
# nest iterables without unpacking them, and would charge for it three times over: an
# element holding a list is not hashable, so nothing above can key a cache on a subtree; a
# nested generator survives construction and is consumed by the *renderer*, so the element
# renders once and then renders empty; and an element holding a caller's list is not a
# value, since the caller can still mutate it. Flattening a level here is exactly the `*`
# the caller can write, so it is written where it is visible: `children=[header, *rows]`.
#
# `Sequence | Iterator` rather than `Iterable`, which would say the same thing about what
# is flattened while also admitting the two shapes `REFUSED_ITERABLES` exists to turn away:
# a `Mapping` and a `set` are both `Iterable[Child]` structurally, so under `Iterable` the
# checker passes `children={"label": value}` and the refusal can only arrive at runtime.
# Neither is a `Sequence` or an `Iterator`, so naming those makes the shape unwritable
# instead. What it costs is an iterable that is neither, such as `rows.values()` or a class
# defining only `__iter__`; those unpack with the same `*` this type already asks for.
type Node = Child | Sequence[Child] | Iterator[Child]

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
#
# The set is HTML's, not a shortlist of the ones that matter: every tag the tokenizer puts
# into RAWTEXT is here, because the property is the parser's and a tag left out is one
# whose entities render literally. Every one of them has a constructor, so the refusal
# `refuse_invalid_tag` raises can name the thing to use instead.
#
# `noscript` is not one, which is worth stating so it does not get added. It is RAWTEXT
# only where scripting is *enabled*, and that is exactly where its content is never
# displayed; where the content is shown, the parser reads it as markup and escaping it is
# correct. `textarea` and `title` are RCDATA rather than RAWTEXT: tags do not open inside
# them but entities are still decoded, so escaping is correct there too.
RAW_TEXT_TAGS = frozenset({"script", "style", "iframe", "noembed", "noframes", "xmp"})

# Attribute names already checked, so each distinct name is validated once per process.
# Names are overwhelmingly literals repeated across every row of a table, so caching turns
# a per-attribute check into a per-vocabulary one.
CHECKED_ATTRIBUTE_NAMES: set[str] = set()

# How many proven names the memo above holds. A vocabulary an application author writes is
# orders of magnitude smaller than this, so the cap is not a limit anyone reaches by
# writing names; it is there for the case the vocabulary is not closed after all, where a
# name built from outside input would otherwise grow the memo for the life of the process.
# Past the cap the memo stops admitting rather than growing, so a further name is checked
# on every use instead of once, which is slower but bounded.
CHECKED_NAME_CAPACITY = 4096

# Characters that must not appear in a tag or attribute name, because each of them ends
# the name where it stands and starts something else: whitespace and `/` begin the next
# attribute, `=` and the quotes begin a value, and `<` and `>` open or close a tag.
FORBIDDEN_IN_NAME = frozenset(" \t\n\r\f\v\"'<>/=")

# What a tag name may begin with, which is what HTML's own tag-name grammar allows there.
# The check is separate from `FORBIDDEN_IN_NAME` because the characters it excludes do not
# end the name where they stand; they change what the `<` before them opened. `<!` opens a
# comment, which runs until `-->` rather than until the `>` that follows, so a tag
# beginning `!--` swallows the rest of the document and resumes markup wherever the content
# happens to hold `-->`. `<?` opens a bogus comment, which drops the element in silence.
TAG_NAME_START = frozenset(ascii_letters)


def admit_name(name: str) -> None:
    """
    Prove `name` cannot break out of the attribute list, and admit it to the cache.

    The slow path, reached once per distinct name per process: the caller holds the
    membership test, so a name already proven never gets here at all.

    A name is normally a literal the application author wrote, so a bad one is a bug
    in code they own and fails loudly here. It is checked at all because a name
    assembled from outside input would otherwise be an injection point that no amount
    of value escaping closes.

    `class` is rejected outright rather than merged with `cls`, so classes have one
    source and not two kept in sync. Costing nothing extra falls out of the cache: it
    is never admitted, so every occurrence takes this path and raises, while every
    other name is a single set lookup and never returns here.

    The `class` test lowercases first, because attribute names are case-insensitive to
    a parser: an exact-match test would let `Class` through, and the browser would then
    see two `class` attributes on one element and silently drop the second. That is the
    same two-sources-in-sync problem the rejection exists to prevent, arriving in the
    one form where it fails quietly instead of loudly.
    """
    if name.lower() == "class":
        raise ValueError("set classes with `cls`, not as an attribute")
    if not name or not FORBIDDEN_IN_NAME.isdisjoint(name):
        raise ValueError(f"invalid attribute name: {name!r}")
    if len(CHECKED_ATTRIBUTE_NAMES) < CHECKED_NAME_CAPACITY:
        CHECKED_ATTRIBUTE_NAMES.add(name)


def attributes_of(cls: ClassNames, attrs: Attributes | None) -> tuple[Attribute, ...]:
    """
    Normalize `cls` and `attrs` into rendered attributes, escaped and in order.

    Escaping happens here rather than at render so that an element is a value that has
    already been proven safe to emit, and so one built once and rendered many times
    pays for it once. That is also why there is no way to hand this function attributes
    that are already rendered: the conversion it does is the escaping, so requiring the
    parsed form from a caller would be requiring the caller to escape.

    Which is also why an element is never built from an attribute tuple written by
    hand. `Element.with_attributes` is the way to change one, and it comes back through
    here; assembling `(("nonce", nonce),)` at the call site type-checks and renders, and
    puts an unescaped value straight into the attribute list, where a quote in it opens
    an attribute of the supplier's choosing.

    Empty and `None` class entries are dropped rather than joined, which is what lets a
    conditional class be written inline. `filter` does it in one C-level pass, where a
    comprehension would be a Python-level loop on the hottest path in the package.

    The name check is the cache lookup, with `admit_name` reached only by a name not yet
    proven, so a repeated name costs one set membership rather than that plus a call.
    """
    if cls is None and not attrs:
        return ()
    rendered: list[Attribute] = []
    if cls is not None:
        names = cls if isinstance(cls, str) else " ".join(filter(None, cls))
        if names:
            rendered.append(("class", escape_attribute(names)))
    if attrs:
        for name, value in attrs.items():
            if value is None or value is False:
                continue
            if name not in CHECKED_ATTRIBUTE_NAMES:
                admit_name(name)
            if value is True:
                rendered.append((name, None))
            else:
                rendered.append((name, escape_attribute(value if type(value) is str else str(value))))
    return tuple(rendered)


def merged_attributes(
    existing: tuple[Attribute, ...], cls: ClassNames, attrs: Attributes | None
) -> tuple[Attribute, ...]:
    """
    Lay `cls` and `attrs` over `existing`, replacing what they name and keeping the rest.

    A name already on the element is replaced *where it is* rather than appended, because
    HTML's own rule for a duplicate attribute is that the first occurrence wins and the
    rest are dropped. Appending would therefore be a no-op on exactly the elements a
    transform is trying to change, and a silent one: the markup carries both spellings
    and the browser reads the old value.

    Names are matched case-insensitively, for the same reason `admit_name` rejects
    `Class`: a parser sees `data-id` and `Data-Id` as one attribute, so treating them as
    two here would produce the duplicate this exists to avoid. A replacement keeps the
    old attribute's *position* and takes the new one's *spelling*, so the markup says
    what the caller asked for and the browser reads it where it always was.

    An `attrs` entry set to `None` or `False` removes the attribute, which falls out of
    `attributes_of` dropping those values: the name was mentioned and nothing was
    rendered for it. `cls` replaces the element's classes when given, and leaves them
    alone at its `None` default, so there is no spelling of `with_attributes` that
    changes classes by accident.
    """
    pending = {name.lower(): (name, value) for name, value in attributes_of(cls, attrs)}
    mentioned = set(pending)
    if attrs:
        mentioned.update(name.lower() for name in attrs)
    if cls is not None:
        mentioned.add("class")
    merged: list[Attribute] = []
    for name, value in existing:
        key = name.lower()
        if key not in mentioned:
            merged.append((name, value))
        elif key in pending:
            merged.append(pending.pop(key))
    merged.extend(pending.values())
    return tuple(merged)


def children_of(children: Node) -> tuple[Child, ...]:
    """
    Normalize a child argument into a tuple of children.

    A caller's list is copied rather than held, so an element cannot change after it is
    built: what arrives as a place becomes a value at the edge. A generator is consumed
    here for the same reason, which is what makes an element renderable more than once.

    One level, matching `Node`. What that buys is an element holding no iterables, so it
    is a hashable value a cache can be keyed on and the renderer never has to walk one.

    It is not *proven* here, and deliberately: what this refuses is the shape that would
    otherwise render something plausible and wrong, not every shape that is not a child.
    A mapping renders its keys and a set renders in an order that varies between runs, so
    both have to be caught where they were written or they are never caught at all. An
    unflattened `[rows]` is the other kind: it has no rendering, so it raises out of the
    walk, naming the mistake and the `*` that fixes it. Proving the rest here as well
    would put a second ladder over child kinds beside the renderer's, which is the one
    place that has to look at every child anyway, and it is the drift between two such
    ladders that would be the security bug. So there is one, and this is not it.

    Dispatch is a ladder of exact-type identity checks before any `isinstance`, ordered by
    how often each shape actually appears. `type()` rather than `__class__` because it is
    both faster (CPython specializes the one-argument call) and honest: an object can set
    `__class__` to whatever it likes, and here that would decide whether text is escaped.
    The `isinstance` checks behind the ladder still decide every case it does not,
    subclasses included; they are just no longer on the common path, where the `Iterable`
    ABC check in particular costs several times a pointer comparison. Their type tuples are
    bound once rather than rebuilt on every call.
    """
    kind = type(children)
    if kind is str or kind is Markup:
        return (children,)  # type: ignore[return-value]
    if kind is Element or kind is VoidElement:
        return (children,)  # type: ignore[return-value]
    if children is None:
        return ()
    if isinstance(children, FLATTENED_TYPES):
        return tuple(children)
    if isinstance(children, SINGLE_CHILD_TYPES):
        return (children,)
    if isinstance(children, REFUSED_ITERABLES):
        raise TypeError(refused_iterable(children))
    if isinstance(children, Iterable):
        return tuple(children)
    return (children,)


def refused_iterable(children: Mapping[object, object] | AbstractSet[object]) -> str:
    """Why `children` is not a child, reached only once a child position has already failed."""
    if isinstance(children, Mapping):
        return f"a mapping in a child position renders only its keys; pass what you meant: {children!r}"
    return f"a set in a child position renders in an order that varies between runs; pass a list: {children!r}"


def raw_text_of(tag: str, children: Node) -> tuple[Child, ...]:
    """
    Normalize the content of a raw-text element, which must already be `Markup`.

    Every raw-text constructor's `children` is typed `Markup | None`, so a caller under
    a type checker reaches the refusal only through `Element.with_children`, where the
    element's tag is not known statically and the wider type is the honest one. It is
    checked because the alternative failure is silent: a plain string would be escaped,
    and a stylesheet or script that arrives entity-encoded is broken in a way that points
    nowhere near the code that caused it.

    The wider shapes are accepted and not only refused, because a walk hands every element
    its children back the way it found them, as a sequence. Requiring a bare `Markup` here
    would make a raw-text element the one node a generic transform could not rebuild. The
    exact-type test comes first so the constructors, which do pass a bare `Markup`, still
    reach the answer in one pointer comparison.
    """
    if children is None:
        return ()
    if type(children) is Markup:
        return (children,)
    content = children_of(children)
    if not all(isinstance(child, Markup) for child in content):
        raise ValueError(f"<{tag}> content is not parsed as markup, so it must be `Markup`")
    return content


# `frozen=True, slots=True` on both element types, which is the expensive combination and
# is kept deliberately. Construction is where a tree renderer spends about half its time,
# and `frozen` roughly triples it (79 ns unfrozen against 245 frozen for three fields),
# because the generated `__init__` routes every field through `object.__setattr__` rather
# than a plain store. The workarounds were measured and none of them help here:
#
# - Caching `object.__setattr__` in a default argument, a closure, or a module global, the
#   way attrs does for slotted classes, is *slower* than the stdlib's generated `__init__`
#   (291-301 ns), because the extra parameter costs more at the call than the saved lookup
#   returns. attrs' own documentation says the same of its version.
# - attrs' genuinely fast path, writing straight into `self.__dict__`, needs a dict class
#   and so gives up `slots`. It does win on construction (168 ns), but an element then
#   costs 247 bytes instead of 142, and since collection is what bends this package's
#   scaling curve, the trade inverts exactly where it matters: measured per element on a
#   table build, 508 ns against 579 at 1,000 rows, but 1,225 against 721 at 30,000.
#
# So the choice is binary: pay for immutability at runtime, or drop `frozen` and rely on
# the type checker. It stays on, because these are values.
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

    def with_attributes(self, *, cls: ClassNames = None, attrs: Attributes | None = None) -> VoidElement:
        """
        A new element with `cls` and `attrs` laid over this one's attributes.

        `Element.with_attributes`, for an element with no content; the semantics are
        described there.
        """
        return VoidElement(self.tag, merged_attributes(self.attributes, cls, attrs))


@dataclass(frozen=True, slots=True)
class Element:
    """
    An element with content: a tag, its rendered attributes, and its children.

    Build these through a named constructor in `without_html.elements`, or `element`
    for a tag HTML does not define, and change one with `with_attributes` and
    `with_children`. The fields here are the already-parsed form, with attributes
    escaped and children flattened to a tuple, so writing them directly is writing the
    output of a parse that never ran: an attribute value assembled at the call site
    reaches the markup unescaped.
    """

    tag: str
    attributes: tuple[Attribute, ...] = ()
    children: tuple[Child, ...] = ()

    def with_attributes(self, *, cls: ClassNames = None, attrs: Attributes | None = None) -> Element:
        """
        A new element with `cls` and `attrs` laid over this one's attributes.

        The transform half of the constructors, taking its arguments in the same shape
        they do, so that changing an element during a walk is spelled the way building
        one is and goes through the same escaping:

        ```python
        el.with_attributes(attrs={"nonce": nonce}) if el.tag == "script" else el
        ```

        A name already on the element is replaced where it stands, an `attrs` entry set
        to `None` removes it, and `cls` replaces the classes when given. See
        `merged_attributes` for why replacing in place is the only correct arm of that.
        """
        return Element(self.tag, merged_attributes(self.attributes, cls, attrs), self.children)

    def with_children(self, children: Node) -> Element:
        """
        A new element with `children` in place of this one's.

        Wholesale rather than an insert or an append, because the children are already a
        value: `el.with_children([*el.children, footer])` adds one and reads as what it
        does, and nothing here has to grow a second way to say it.

        A raw-text element (`<script>`, `<style>`) keeps its own rule about content, which
        is why this exists rather than `dataclasses.replace`: replace would put an escaped
        string inside a `<script>`, where nothing escapes and the entities are the
        program.
        """
        if self.tag in RAW_TEXT_TAGS:
            return Element(self.tag, self.attributes, raw_text_of(self.tag, children))
        return Element(self.tag, self.attributes, children_of(children))


# The `isinstance` arguments `children_of` uses, bound once instead of built into a fresh
# tuple on every call. They live here because they name the element classes above.
SINGLE_CHILD_TYPES = (str, Element, VoidElement)
FLATTENED_TYPES = (list, tuple, GeneratorType)

# Iterables that mean something other than what a child position means. A `Mapping`
# iterates its keys, so `children={"label": value}` renders `label` and silently drops the
# rest; a `set` iterates in an order that varies between processes, so the same tree
# renders differently each run. Neither is a tree anyone meant to write.
#
# `Node` names `Sequence` and `Iterator` so that a caller under a type checker cannot write
# either one, which leaves this for the caller who is not: nothing narrower than `object`
# can be assumed about what actually arrives at runtime.
REFUSED_ITERABLES = (Mapping, set, frozenset)


class ElementConstructor(Protocol):
    """The call signature every named element constructor shares, and its tag identity."""

    __name__: str

    def __call__(self, *, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
        """Build one element of this kind."""
        ...


class RawTextElementConstructor(Protocol):
    """The call signature every raw-text element constructor shares, and its tag identity."""

    __name__: str

    def __call__(
        self, *, cls: ClassNames = None, attrs: Attributes | None = None, children: Markup | None = None
    ) -> Element:
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


def refuse_invalid_tag(tag: str) -> None:
    """
    Reject a tag that could break out of the markup it names.

    The tag half of `admit_name`, and it exists for the same reason: a tag is written into
    `<...>` verbatim, so a space or a `>` in one assembled from outside input opens markup
    of the supplier's choosing, and no amount of escaping in the *values* closes that. What
    a tag may *begin* with is checked as well as what it may contain, because a leading `!`
    or `?` turns the whole element into a comment rather than ending the name early.

    Raw-text tags are refused here rather than beside the void check, so that a tag
    declared void reaches it too: a `<script>` with no closing tag leaves the rest of the
    document in script context.

    Unlike `admit_name` there is no cache behind this, because there is nothing a cache
    would save. `element_type` and `void_element_type` settle a tag once when the
    constructor is defined, and `element` is the one-shot form, which already pays a
    frozenset lookup per call and is the form to move off when a tag repeats.
    """
    if not tag or tag[0] not in TAG_NAME_START or not FORBIDDEN_IN_NAME.isdisjoint(tag):
        raise ValueError(f"invalid tag name: {tag!r}")
    if tag in RAW_TEXT_TAGS:
        raise ValueError(f"<{tag}> content is not parsed as markup; use the `{tag}` constructor")


def refuse_named_tag(tag: str) -> None:
    """Reject a tag whose element carries a constraint the generic form cannot express."""
    if tag in VOID_TAGS:
        raise ValueError(f"<{tag}> is a void element; use the `{tag}` constructor or `void_element_type`")
    refuse_invalid_tag(tag)


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
    refuse_invalid_tag(tag)

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
