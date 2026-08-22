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
- **`children`** is any `Node`: one `Child` (another element, a string, `None`) or an
  iterable of them. `None` rendering as nothing is what lets `card if visible else None`
  sit inline with no branch around it, and an iterable flattening is what lets a
  generator expression over rows be a child like any other.

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

`cls` is the exception, taking any iterable, because it is joined into a string before the
element exists: nothing survives the call for anyone to mutate or exhaust.

## Escaping is a type

Text in a child position and every attribute value are escaped. The only thing that
renders verbatim is
[`Markup`](https://markupsafe.palletsprojects.com/en/stable/escaping/), MarkupSafe's
type, used under its own name rather than wrapped in a local one.

```python
p(children="<script>alert(1)</script>")  # renders escaped
p(children=Markup("<em>trusted</em>"))  # renders verbatim
```

Keeping the name is what makes the interoperation real rather than nominal: a fragment
rendered by Jinja, Flask, or tdom already *is* one of these, with no adapter, and
anything carrying `__html__` can sit in a child position. It also carries safety through
string operations, so `Markup("<b>") + untrusted` escapes the right-hand side and stays
safe, which is the part a local `str` subclass would get quietly wrong.

Escaping happens when the element is built rather than when it is rendered, so an
element is a value that has already been proven safe to emit, and one built once and
rendered many times pays for it once.

Attribute *names* are checked too, once per distinct name per process. A name is
normally a literal, so a bad one is a bug in code you own and fails loudly; it is
checked at all because a name assembled from outside input would be an injection point
that no amount of value escaping closes.

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
void).

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
costs slightly more than rendering it, so memoizing the render half is the smaller half,
and a dict hit on a rebuilt-but-equal subtree still pays a full recursive `==` to confirm
the key. Cache the thing that skips both instead:

```python
@cache
def sidebar(tier: str) -> Markup:
    return Markup(render(nav(cls="sidebar", children=[...])))
```

A `Markup` renders verbatim, so the result drops into any tree as an ordinary child. The
key is a small argument tuple that hashes in nanoseconds rather than a tree that hashes in
hundreds of microseconds, and a hit skips construction as well as rendering: measured on a
177 KB page, re-rendering a cached subtree is a few hundred nanoseconds against the
hundreds of microseconds to rebuild and re-render it.

Use an unbounded `@cache` only where the key space is bounded. Where it is not, reach for
a bounded cache, and note that eviction bookkeeping is several times a plain dict lookup,
so keep it at component boundaries rather than at every node.

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
template. What that buys is the tree: escaping that cannot be
forgotten, components that compose, and a value a fragment can be rendered from. Whether
that is a good trade depends on the page, and for a console page rendered in single-digit
milliseconds against a database query and a network round trip, it disappears.

Two properties worth knowing when the page gets large:

- **Building the tree costs more than rendering it**, about a 63/37 split. An element that
  is built once and rendered many times pays the larger half once, which is what makes
  caching at a component boundary worth more than caching a rendered subtree.
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
- **No diffing, patching, or client runtime.** The tree would support all of it, which
  is why the interface is the tree; none of it exists.
- **No CSS, no widgets, no layout.** The browser has those. `cls` takes complete class
  names, which is also what a scanner like Tailwind's needs in order to see them: build
  class names as whole strings rather than assembling them from fragments, since
  `f"text-{colour}-500"` is invisible to it.
