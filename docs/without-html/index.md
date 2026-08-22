# without-html

HTML as immutable Python values: build a node tree with plain function calls, render
it to a string with a pure function. See the
[`without_html` API reference](../without-html/reference.md) for the full surface.

This package depends on nothing else in the workspace. It is a value library, so it
composes with [`without-web`](../without-web/index.md) the way any value does, and
works just as well under a different framework or in a script that writes files.

```python
from without_html import div, h1, li, render, ul


def menu(items):  # a component is a function that returns a node
    return div(cls="menu", children=[h1(children="Menu"), ul(children=(li(children=i) for i in items))])


render(menu(["tea", "toast"]))
# '<div class="menu"><h1>Menu</h1><ul><li>tea</li><li>toast</li></ul></div>'
```

Markup here is built rather than written, which is the axis this sits on against a template
engine. A component is an ordinary Python function of its arguments returning a value, and
pages compose by calling functions; a template is text in a file of its own, and composition
there needs machinery of its own (`{% extends %}`, `{% block %}`, `{% include %}`,
`{% macro %}`) to do what `def` and a call already do.

The resemblance to React is the shape of a component and stops there: a pure function from
data to a tree. There is no state, no hooks, no lifecycle, and no reconciliation. What the
tree does share with a virtual DOM is being the kind of thing those could be built on, which
is the reason it is the interface.

## The tree is the interface, not the string

A constructor returns a value, and `render` is a separate pure function over it. That
split is the whole design: a component can be rendered whole for a page or on its own
for a fragment, two components compose without either knowing it, and there is
somewhere to stand for anything later that wants structure rather than text.

A component is a plain function. There is no decorator, no base class, and no registry
to register into, so a component travels the way a `Route` does: you hold the function,
and moving it between applications is moving the function.

## Classes, attributes, children

Every constructor takes the same three arguments, all keyword-only.

```python
p(cls=["lead", "muted"], attrs={"id": "intro", "hx-get": "/more"}, children=["text ", em(children="here")])
```

- **`cls`** is one class string or several to be joined. An entry may itself hold several
  names and an entry may be `None` or empty, both dropped rather than joined, so
  `cls=("card", "card-active" if active else None)` needs no filtering around it and a
  caller never has to know which form a part arrived in. It is separate from `attrs`
  because it is the one attribute routinely built up from parts, and it is the *only*
  way to set classes: `attrs={"class": ...}` raises. Two channels into one attribute
  would be two sources to keep in sync, and forbidding the second one means the mistake
  fails the first time it is written rather than later, when someone adds `cls` beside
  it. A component that forwards arbitrary attributes should take its own `cls`
  parameter and pass it along, the same shape the constructors have.
- **`attrs`** is a mapping, so names pass through exactly as written. `hx-get`,
  `data-run-id`, `aria-label`, and SVG's `viewBox` need no escape hatch and no
  underscore-to-hyphen convention to remember. A value of `True` renders the attribute
  bare (`disabled`), `False` and `None` drop it entirely, and an `int` renders as
  digits, since the attributes that take numbers are machine-read.
- **`children`** is any `Node`: one `Child` (another element, a string, `None`) or a
  sequence or iterator of them. `None` rendering as nothing is what lets
  `card if visible else None` sit inline with no branch around it, and flattening is what
  lets a generator expression over rows be a child like any other.

A number in a *child* position is deliberately not accepted. How a number reads to a
person is a formatting decision, and this layer picking one (`1000`? `1,000`? `1_000`?)
would be deciding for the layer above.

Children are copied when the element is built, so a generator is consumed once and the
element stays a value that renders the same every time.

## One level of flattening

An iterable in a child position is flattened once, so unpacking goes at the call site,
where it is visible:

```python
div(children=[header, *rows])
```

An element's children are therefore always flat, which is what makes every element a
hashable value: the hash of a subtree is the hash of its strings and its children's
hashes, so a tree is content-addressed and anything above can key on one. It is also why
rendering never mutates anything, since a generator is consumed when the element is built
rather than while it is being rendered. A nested iterable raises, and says what to write
instead.

What a child slot takes is a `Sequence` or an `Iterator`, not any `Iterable`. A `Mapping`
and a `set` are both `Iterable[Child]` structurally, so naming `Iterable` would type-check
`children={"label": value}`, which renders the keys and drops the values, and
`children={"a", "b"}`, which renders in an order that varies between processes. Both raise
either way, and naming the two narrower types is what moves the refusal from runtime to
the type checker. The cost is an iterable that is neither, such as `rows.values()`, which
unpacks with the same `*` as anything else: `children=[*rows.values()]`.

`cls` is looser about *lifetime*, since it is joined into a string before the element
exists, so nothing survives the call for anyone to mutate or exhaust. It names the same two
types all the same, because order and meaning are not lifetime. A `set` of class names
joins in an order that varies between processes. A `Mapping` joins its *keys*, so
`cls={"card": True, "active": False}` would render both names: the shape
[classnames](https://github.com/JedWatson/classnames) and
[clsx](https://github.com/lukeed/clsx) made the idiom for this job in JavaScript is the one
spelling that means the opposite of what it says here. The spelling that works is the one
`cls` is built around, where `None` and empty entries drop out:

```python
div(cls=("card", "card-active" if active else None))
```

## Escaping is a type

Text in a child position and every attribute value are escaped. What renders verbatim is
[`Markup`](https://markupsafe.palletsprojects.com/en/stable/escaping/), MarkupSafe's
type, used under its own name rather than wrapped in a local one.

```python
p(children="<script>alert(1)</script>")  # renders escaped
p(children=Markup("<em>trusted</em>"))  # renders verbatim
```

Keeping the name is what makes the interoperation real rather than nominal: a fragment
rendered by Jinja, Flask, or tdom already *is* one of these, with no adapter, and
anything carrying `__html__` renders verbatim in a child position whether or not it is a
`Markup` (Django's `SafeString` is a `str` carrying `__html__` and no relation to
`Markup`, and renders as its author declared it). `Markup` also carries safety through
string operations, so `Markup("<b>") + untrusted` escapes the right-hand side and stays
safe, which is the part a local `str` subclass would get quietly wrong.

Escaping happens when the element is built rather than when it is rendered, so an
element is a value that has already been proven safe to emit, and one built once and
rendered many times pays for it once.

*Names* are checked rather than escaped: attribute names once per distinct name per
process, and tag names wherever a tag enters, so `element`, `element_type`, and
`void_element_type` all reject one carrying whitespace, quotes, `/`, `=`, `<`, or `>`. A
tag must also begin with an ASCII letter, which is all HTML's own tag-name grammar allows
there: a leading `!` or `?` does not end the name early but changes what the `<` in front
of it opened, and `<!--` runs to the next `-->` rather than to the `>` that follows, so
everything after such a tag would be swallowed as comment content. A name is normally a
literal, so a bad one is a bug in code you own and fails loudly. It is
checked at all because a name goes into the markup verbatim, which is exactly what
escaping the values around it cannot reach: a name assembled from outside input would be
an injection point no amount of value escaping closes.

## HTML's own constraints live in the signatures

Three kinds of element behave differently, and the difference is in the types rather
than in a runtime check you can forget.

**Void elements** (`br`, `img`, `input_`) return a `VoidElement`, which has no children
field at all. Giving one children is not a mistake to be caught but a thing that cannot
be written.

**Raw-text elements** (`script`, `style`) take `Markup | None`. Their content is not
parsed as markup, so escaping it would corrupt the script (`a && b` becoming
`a &amp;&amp; b`) while not escaping it would be an injection hole. Requiring `Markup`
takes neither decision on your behalf.

**Everything else** takes any `Node`.

Because which constraint applies is decided when the constructors are generated, each
one builds its node directly and does no checking at call time.

Four tag names collide with Python: `del_`, `input_`, `map_`, and `object_`.

## Custom elements are first class

A custom element gets a constructor of its own, equal in standing to the built-in ones,
and the tag check happens once when you define it rather than on every call.

```python
chart = element_type("x-chart")
spacer = void_element_type("x-spacer")

chart(attrs={"data-series": series}, children=caption(children="Runs per day"))
```

Bind it at module scope and use it like `div`. `element(tag, ...)` is the one-shot form
for a tag that appears once, and `void_element_type` is its void counterpart, for markup
that is not quite HTML (HTML's own void set is closed, and a custom element is never
void). It refuses `script` and `style`, since a raw-text element with no closing tag
leaves everything after it in script or stylesheet context.

This is the seam for anything the browser has to do itself. A charting library, an
editor, or a drag-and-drop surface lives behind a custom element: the server owns the
element's attributes, the library owns everything inside it, and neither has to know
about the other.

## Answering a request

`render` returns a string and stops there. Pairing it with a content type is
[`without-asgi`](../without-asgi/index.md)'s `html_content`, alongside `json_content`
and `form_content`:

```python
Response.from_content(200, html_content(render(page)))
```

The split is deliberate in both directions. `without-html` never learns about HTTP, and
`without-asgi` never learns about node trees, so either can be replaced without the
other noticing.

For a whole document, `DOCTYPE` is a `Markup` constant to put in front of the root:

```python
render([DOCTYPE, html(children=[head(children=title(children="runs")), body(children=page)])])
```

## Streaming is the same walk

`render_chunks` walks the tree `render` walks and produces the same bytes in the same
order, handing them back as they are made rather than holding the finished string.

```python
for chunk in render_chunks(page):
    ...  # `"".join(render_chunks(page))` is `render(page)`
```

A chunk is a fixed number of fragments joined rather than a byte budget, so chunks come
out roughly even in size but not exactly, and a single large `Markup` child goes out whole
in whatever chunk it lands in. Streaming costs about 20% over `render` for the same tree,
which buys a first chunk before the tree is finished and a process that never holds the
whole page.

What it gives up is worth choosing rather than defaulting into: the total length is not
known until the walk ends, and once the first chunk has been handed on there is no taking
it back, so a failure partway through a tree can no longer be turned into something else.
`render` stays the default for that reason.

## Caching is yours, not this layer's

Every element is a hashable value, and the hash of a subtree is the hash of its strings
and its children's hashes, so a tree is content-addressed for free. On CPython 3.14 a
tuple caches its own hash, so hashing a tree costs one traversal and every hash after that
is a pointer chase.

That makes a render cache easy to reach for, and mostly the wrong tool. Building a tree
costs more than rendering it, so memoizing the render half is memoizing the smaller half,
and a dict hit on a rebuilt-but-equal subtree still pays a full recursive `==` to confirm
the key. Cache at the component boundary instead, where the key is the arguments: a small
tuple that hashes in nanoseconds rather than a tree that hashes in hundreds of microseconds.

```python
@cache
def sidebar(tier: str) -> Element:
    return nav(cls="sidebar", children=[...])
```

What the component hands back is then a second choice, and a real trade:

- **Return the `Element`** and a hit skips construction, the larger half, while the result
  stays a value anything can still walk, hash, transform, or render more than once. This is
  the option that keeps what the tree is for.
- **Return `Markup`** (`Markup(render(...))`) and a hit skips rendering too, so a cached
  subtree costs a few hundred nanoseconds against the hundreds of microseconds to rebuild
  and re-render it. The price is that the result is opaque: it drops into a tree as an
  ordinary child, and nothing can look inside it again.

Take the second only where nothing downstream needs the structure. Either way the cached
value is safe to share across requests and render repeatedly, because an element is
immutable and a `Markup` is a string; there is no defensive copy to make and no way for one
caller's page to disturb another's.

Use an unbounded `@cache` only where the key space is bounded. Where it is not, reach for
a bounded cache, and note that eviction bookkeeping is several times a plain dict lookup,
so keep it at component boundaries rather than at every node.

Nothing about this is limited to static markup. A row rendered from the database, identical
for every viewer over a window shorter than the data changes in, is the same shape with a
time bound on it:

```python
@dataclass(frozen=True)
class Run:
    id: str
    status: str


@ttl_cache(maxsize=1_000, ttl=30)
def run_row(run: Run) -> Element:
    return tr(attrs={"id": run.id}, children=[td(children=run.id), td(children=run.status)])
```

`Run` is frozen, so it hashes, so it *is* the key: two requests that fetched the same row
share the subtree built from it, and a row whose data changed misses on its own.

The axis that decides where a cache goes is not static against dynamic. It is whether the
key can be named *before* the tree is built. A component is a function of its arguments, so
the answer is almost always yes, and then caching on those arguments dominates caching on
the tree: the key is small, and a hit skips the construction a tree-keyed cache would have
to do first in order to have a key at all.

Caching on the arguments also leaves the granularity with you. When a row is mostly stable
around one volatile field, decompose it and cache the stable part; a cache keyed on nodes
would take the whole row as it found it.

## What it costs

Measured by `just bench-render`, which times every renderer over the same workloads and
fails unless they all produce byte-identical output. Numbers are from one machine and
will not be yours; the ratios travel better than the absolutes.

| | 1,000-row table | 200-card page | ns per element |
|---|---|---|---|
| hand-written f-strings | 0.91 ms | 0.15 ms | ~185 |
| Jinja2 | 1.73 ms | 0.27 ms | ~340 |
| `without-html` | 3.91 ms | 0.85 ms | ~720-1,050 |
| htpy | 18.6 ms | 5.49 ms | ~3,600-6,800 |

So a node tree costs about four times what writing the string by hand does on table-shaped
markup and five and a half on an attribute-heavy page, and two to three times a compiled
template. For a console page rendered in single-digit milliseconds against a database query
and a network round trip, that disappears. But it is worth being exact about where the cost
goes and what it is for, because the obvious answer is wrong.

### The cost is the tree, not the walk

Split the work and the comparison inverts: rendering an already-built tree is *faster* than
a compiled Jinja template rendering the same table. All of the difference, and more, is in
building the tree.

| | 1,000-row table |
|---|---|
| `without-html`, build the tree | 2.58 ms |
| `without-html`, render the built tree | 1.48 ms |
| Jinja2, render a compiled template | 1.72 ms |

From `just bench-render --phases`, which times the halves separately, so they do not sum to
the table above.

Jinja is not faster because compiled code beats a tree walk. It is faster because its
compiler already did the structural work: a template collapses to constants like
`yield '</td><td>'`, where a closing tag, an opening tag and both tag names have been
folded into one string at compile time. That happens once per process. We rediscover the
same boundary on every request, by allocating an element and walking it.

Which means the lever is building less often rather than walking faster. Hoisting a static
subtree to module scope, or memoizing a component to `Markup`, is the same constant-folding
Jinja's compiler does, by hand and under your control. It is why the caching section above
matters more than any change to the renderer.

### What the cost buys

Not speed. And not, by itself, the split at the top of this page: rendering a component
whole or on its own as a fragment, and two components composing without either knowing it,
follow from a component being a function that returns a value. A function returning `Markup`
is also a function returning a value, and would buy all of that with no tree and no build
cost, settling escaping at construction just as well.

What the tree buys is everything that needs the markup to still be *structured* after the
component has returned: holding a cached subtree that can still be composed into a larger
page rather than only pasted into one, walking a document to add a nonce to every
`<script>`, asserting on shape rather than on text in a test, or rendering the same tree to
something that is not HTML. It is the difference between a layer that hands you an answer
and one that hands you something you can still ask questions of.

That is optionality, and it is worth being plain that this layer stops at handing it over:
the diffing, transforming, and asserting are things you do with a tree, above, not things
done for you here. The build cost is the premium on the option. If you will never open the
box, a function returning `Markup` is the cheaper design and nothing here will beat it; the
bet is that having somewhere to stand is worth more, over the life of an application, than
the milliseconds it costs to stand there.

### Properties worth knowing when the page gets large

- **Cost is linear in elements, and the collector is what bends it.** Across the sweep
  `just bench-render --scaling` runs, per-element cost climbs about 30% from 100 to 10,000
  rows, and about half that with `--gc-off`, because a growing live tree is a growing thing
  to scan. A long-running server is the workload the default thresholds suit least, so we
  recommend tuning the collector at startup with
  [`gc.set_threshold`](https://docs.python.org/3/library/gc.html#gc.set_threshold).
- **Attributes are the expensive part.** An element with two attributes costs roughly
  twice one with none, because each attribute is a name check and an escape scan. Both
  are already about as cheap as Python allows: the name check is one set membership for
  a name seen before, and the escape is a guarded containment scan, which beats both
  `str.translate` and a regex pre-guard by several times on real values.

## What this deliberately does not do

- **No template language.** Markup is Python, so there is no loader, no search path, no
  autoescape setting, and no second syntax to learn. What it costs is real: a designer
  cannot edit a template file, and complex bespoke markup reads worse in Python than in
  HTML.
- **No components with lifecycle, state, or hooks.** A component is a function of its
  arguments. Anything stateful belongs above this layer.
- **No render cache.** Elements are hashable values, so one could key a cache on any
  subtree, and this layer does not: the cheaper form of the same idea belongs in the
  application, at the component boundary, where a hit skips building the tree as well as
  rendering it.
- **No diffing, patching, or client runtime.** The tree is what they would be built on,
  and that is a layer above this one.
- **No CSS, no widgets, no layout.** The browser has those. `cls` takes complete class
  names, which is also what a scanner like Tailwind's needs in order to see them: build
  class names as whole strings rather than assembling them from fragments, since
  `f"text-{colour}-500"` is invisible to it.
