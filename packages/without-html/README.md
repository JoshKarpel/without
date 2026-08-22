# without-html

HTML as immutable Python values. Build a node tree with plain function calls,
render it to a string with a pure function. No template language, no loader
search path, no ambient application object, and no dependency on anything else in
the workspace: this is a value library that happens to be useful next to
[`without-web`](../without-web).

```python
from without_asgi import Response, html_content
from without_html import render, table, tbody, td, th, thead, tr


def runs_table(runs):  # a component is a function that returns a node
    return table(
        cls=["table", "table-striped"],
        children=[
            thead(children=tr(children=[th(children="run"), th(children="status")])),
            tbody(children=(run_row(run) for run in runs)),
        ],
    )


def run_row(run):
    return tr(attrs={"id": f"run-{run.id}"}, children=[td(children=run.id), td(children=run.status)])


Response.from_content(200, html_content(render(runs_table(runs))))
```

Escaping is a type rather than a setting: text is escaped on the way in, and
`Markup` (MarkupSafe's own, kept under its own name, so anything Jinja or tdom
produced is already one) is the only thing that renders verbatim. The element
constructors carry HTML's own constraints in their signatures, so a void element
with children and an unescaped `<script>` body are type errors rather than runtime
surprises.

`render` returns the whole string; `render_chunks` walks the same tree and yields
the same bytes a chunk at a time, so a large page starts reaching a client while
the rest of it is still being built.

The tree is the interface, not the string: a component can be rendered whole for a
page or on its own for a fragment, and syntax sugar can be layered on later without
changing anything written against it.

See the
[`without-html` guide](https://without.help/without-html/)
(with the [API reference](https://without.help/without-html/reference/))
for nodes and escaping, the attribute rules, custom elements, and what this
deliberately does not do.
