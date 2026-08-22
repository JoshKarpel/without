from __future__ import annotations

import html
from collections.abc import Callable
from collections.abc import Sequence
from dataclasses import dataclass

import htpy
from jinja2 import Environment
from without_html import DOCTYPE
from without_html import Element
from without_html import a
from without_html import article
from without_html import body
from without_html import div
from without_html import h2
from without_html import head
from without_html import html as html_tag
from without_html import link
from without_html import main
from without_html import meta
from without_html import nav
from without_html import p
from without_html import render
from without_html import span
from without_html import table
from without_html import tbody
from without_html import td
from without_html import th
from without_html import thead
from without_html import title
from without_html import tr

# Four renderers over one set of workloads, each of which must produce byte-identical
# output from the same data. That constraint is what makes the numbers a comparison
# rather than four unrelated measurements, and `benchmarks.render.bench` asserts it
# before timing anything.
#
# The contenders span the design space rather than sampling it randomly:
# `without-html` and `htpy` are the same idea (an immutable node tree built by Python
# calls, rendered by a walk), Jinja is the incumbent template engine and compiles to
# Python bytecode, and the f-string renderer is the floor. The floor matters most: it
# has no tree, no dispatch, and no policy, so it is the cost of the string work alone
# and every other number is that plus what the abstraction charges.
#
# The data deliberately avoids quote characters in *text* positions. `without-html`
# escapes three characters there (`&`, `<`, `>`) where MarkupSafe escapes five, so a
# stray apostrophe would make two correct renderers disagree and fail the equality
# check for a reason that has nothing to do with speed. Quotes in *attribute* values
# are fine, since every contender escapes those the same way.

CARD_CLASSES = ("card", "card-bordered", "card-compact")
HEADERS = ("run", "loop", "status", "started")


@dataclass(frozen=True, slots=True)
class Run:
    """One row of the runs table: the shape escapement's console actually renders."""

    id: str
    loop: str
    status: str
    started: str


def runs(count: int) -> tuple[Run, ...]:
    """
    Rows with an escapable character every few entries, so escaping is on the measured
    path at a realistic density rather than never or always.
    """
    return tuple(
        Run(
            id=f"run-{index}",
            loop=f"loop & {index % 7}" if index % 5 == 0 else f"loop-{index % 7}",
            status="succeeded" if index % 3 else "failed",
            started=f"2026-08-{20 + index % 10}",
        )
        for index in range(count)
    )


def table_tree_without(rows: Sequence[Run]) -> Element:
    """The table as a node tree, stopping short of rendering it, for the phase split."""
    return table(
        cls="runs",
        children=[
            thead(children=tr(children=[th(children=header) for header in HEADERS])),
            tbody(
                children=[
                    tr(
                        attrs={"id": row.id},
                        children=[
                            td(children=row.id),
                            td(children=row.loop),
                            td(children=row.status),
                            td(children=row.started),
                        ],
                    )
                    for row in rows
                ]
            ),
        ],
    )


def table_without(rows: Sequence[Run]) -> str:
    return render(table_tree_without(rows))


def table_tree_htpy(rows: Sequence[Run]) -> htpy.Element:
    return htpy.table(".runs")[
        htpy.thead[htpy.tr[[htpy.th[header] for header in HEADERS]]],
        htpy.tbody[
            [
                htpy.tr(id=row.id)[
                    htpy.td[row.id],
                    htpy.td[row.loop],
                    htpy.td[row.status],
                    htpy.td[row.started],
                ]
                for row in rows
            ]
        ],
    ]


def table_htpy(rows: Sequence[Run]) -> str:
    return str(table_tree_htpy(rows))


TABLE_TEMPLATE = (
    '<table class="runs"><thead><tr>'
    "{% for header in headers %}<th>{{ header }}</th>{% endfor %}"
    "</tr></thead><tbody>"
    '{% for row in rows %}<tr id="{{ row.id }}">'
    "<td>{{ row.id }}</td><td>{{ row.loop }}</td><td>{{ row.status }}</td><td>{{ row.started }}</td>"
    "</tr>{% endfor %}</tbody></table>"
)


def table_fstring(rows: Sequence[Run]) -> str:
    escape = html.escape
    headers = "".join(f"<th>{header}</th>" for header in HEADERS)
    body_rows = "".join(
        f'<tr id="{escape(row.id)}"><td>{escape(row.id)}</td><td>{escape(row.loop)}</td>'
        f"<td>{escape(row.status)}</td><td>{escape(row.started)}</td></tr>"
        for row in rows
    )
    return f'<table class="runs"><thead><tr>{headers}</tr></thead><tbody>{body_rows}</tbody></table>'


def page_without(rows: Sequence[Run]) -> str:
    return render([DOCTYPE, page_tree_without(rows)])


def page_tree_without(rows: Sequence[Run]) -> Element:
    return html_tag(
        attrs={"lang": "en"},
        children=[
            head(
                children=[
                    meta(attrs={"charset": "utf-8"}),
                    title(children="runs"),
                    link(attrs={"rel": "stylesheet", "href": "/static/app.css"}),
                ]
            ),
            body(
                children=[
                    nav(
                        cls="nav",
                        children=[
                            a(attrs={"href": f"/{name}"}, children=name) for name in ("runs", "loops", "targets")
                        ],
                    ),
                    main(
                        cls="grid",
                        children=[
                            article(
                                cls=CARD_CLASSES,
                                attrs={"id": row.id},
                                children=[
                                    h2(cls="card-title", children=row.loop),
                                    p(cls="card-body", children=row.started),
                                    span(cls="badge", children=row.status),
                                ],
                            )
                            for row in rows
                        ],
                    ),
                ]
            ),
        ],
    )


def page_htpy(rows: Sequence[Run]) -> str:
    return str(
        htpy.html(lang="en")[
            htpy.head[
                htpy.meta(charset="utf-8"),
                htpy.title["runs"],
                htpy.link(rel="stylesheet", href="/static/app.css"),
            ],
            htpy.body[
                htpy.nav(".nav")[[htpy.a(href=f"/{name}")[name] for name in ("runs", "loops", "targets")]],
                htpy.main(".grid")[
                    [
                        htpy.article(".card.card-bordered.card-compact", id=row.id)[
                            htpy.h2(".card-title")[row.loop],
                            htpy.p(".card-body")[row.started],
                            htpy.span(".badge")[row.status],
                        ]
                        for row in rows
                    ]
                ],
            ],
        ]
    )


PAGE_TEMPLATE = (
    '<!doctype html><html lang="en"><head><meta charset="utf-8"><title>runs</title>'
    '<link rel="stylesheet" href="/static/app.css"></head><body><nav class="nav">'
    '{% for name in names %}<a href="/{{ name }}">{{ name }}</a>{% endfor %}'
    '</nav><main class="grid">'
    '{% for row in rows %}<article class="card card-bordered card-compact" id="{{ row.id }}">'
    '<h2 class="card-title">{{ row.loop }}</h2><p class="card-body">{{ row.started }}</p>'
    '<span class="badge">{{ row.status }}</span></article>{% endfor %}</main></body></html>'
)


def page_fstring(rows: Sequence[Run]) -> str:
    escape = html.escape
    links = "".join(f'<a href="/{name}">{name}</a>' for name in ("runs", "loops", "targets"))
    cards = "".join(
        f'<article class="card card-bordered card-compact" id="{escape(row.id)}">'
        f'<h2 class="card-title">{escape(row.loop)}</h2>'
        f'<p class="card-body">{escape(row.started)}</p>'
        f'<span class="badge">{escape(row.status)}</span></article>'
        for row in rows
    )
    return (
        '<!doctype html><html lang="en"><head><meta charset="utf-8"><title>runs</title>'
        '<link rel="stylesheet" href="/static/app.css"></head><body>'
        f'<nav class="nav">{links}</nav><main class="grid">{cards}</main></body></html>'
    )


def deep_without(depth: int) -> str:
    node = div(cls="leaf", children="bottom")
    for _ in range(depth):
        node = div(cls="wrap", children=node)
    return render(node)


def deep_htpy(depth: int) -> str:
    node = htpy.div(".leaf")["bottom"]
    for _ in range(depth):
        node = htpy.div(".wrap")[node]
    return str(node)


def deep_fstring(depth: int) -> str:
    # Written as a per-level loop rather than `'<div>' * depth`, which would measure
    # string multiplication instead of rendering and make the floor meaningless. Even so,
    # this shape carries no data, so the floor here is unusually cheap: read the two tree
    # renderers against each other rather than against it.
    parts = ['<div class="wrap">' for _ in range(depth)]
    parts.append('<div class="leaf">bottom</div>')
    parts.extend("</div>" for _ in range(depth))
    return "".join(parts)


@dataclass(frozen=True, slots=True)
class Workload:
    """
    One measurable shape, with a renderer per contender.

    `elements` is what the shape costs in nodes, so a per-element figure can be
    compared across workloads of different sizes. A contender that cannot express a
    shape is absent from `renderers` rather than faked.
    """

    name: str
    description: str
    elements: int
    renderers: dict[str, Callable[[], str]]


def jinja_renderer(environment: Environment, source: str, **context: object) -> Callable[[], str]:
    """
    A Jinja renderer with the template compiled once, as a served app would have it.

    Compiling per call would measure the compiler, which no deployment pays per request.
    """
    template = environment.from_string(source)
    return lambda: template.render(**context)


def workloads(*, rows: int, cards: int, fragment_rows: int, depth: int) -> tuple[Workload, ...]:
    """Every workload, sized by the caller so the sweep can vary one dimension at a time."""
    environment = Environment(autoescape=True)
    table_rows = runs(rows)
    card_rows = runs(cards)
    fragment = runs(fragment_rows)
    return (
        Workload(
            name="table",
            description=f"a {rows}-row runs table, the shape a console page is mostly made of",
            elements=2 + len(HEADERS) + rows * 5,
            renderers={
                "without-html": lambda: table_without(table_rows),
                "htpy": lambda: table_htpy(table_rows),
                "jinja2": jinja_renderer(environment, TABLE_TEMPLATE, headers=HEADERS, rows=table_rows),
                "f-string": lambda: table_fstring(table_rows),
            },
        ),
        Workload(
            name="fragment",
            description=f"a {fragment_rows}-row table, the size an htmx swap returns",
            elements=2 + len(HEADERS) + fragment_rows * 5,
            renderers={
                "without-html": lambda: table_without(fragment),
                "htpy": lambda: table_htpy(fragment),
                "jinja2": jinja_renderer(environment, TABLE_TEMPLATE, headers=HEADERS, rows=fragment),
                "f-string": lambda: table_fstring(fragment),
            },
        ),
        Workload(
            name="page",
            description=f"a whole document: head, nav, and {cards} cards, attribute- and class-heavy",
            elements=11 + cards * 4,
            renderers={
                "without-html": lambda: page_without(card_rows),
                "htpy": lambda: page_htpy(card_rows),
                "jinja2": jinja_renderer(
                    environment, PAGE_TEMPLATE, names=("runs", "loops", "targets"), rows=card_rows
                ),
                "f-string": lambda: page_fstring(card_rows),
            },
        ),
        Workload(
            name="deep",
            description=f"{depth} nested elements, where a recursive renderer would pay a frame each",
            elements=depth + 1,
            # Jinja has no idiomatic way to nest to an arbitrary depth, so it sits this
            # one out rather than being given a contrived recursive macro.
            renderers={
                "without-html": lambda: deep_without(depth),
                "htpy": lambda: deep_htpy(depth),
                "f-string": lambda: deep_fstring(depth),
            },
        ),
    )
