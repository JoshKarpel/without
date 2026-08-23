from __future__ import annotations

from markupsafe import Markup

from without_html.nodes import Attributes
from without_html.nodes import ClassNames
from without_html.nodes import Element
from without_html.nodes import Node
from without_html.nodes import VoidElement
from without_html.nodes import attributes_of
from without_html.nodes import children_of
from without_html.nodes import raw_text_of

# One constructor per HTML element, generated from the tag list in `tools/tags.py`
# so that each carries the parameters its element can actually take: a void element
# has none for children and returns a `VoidElement`, and a raw-text element's must be
# `Markup`. Each builds its node directly, since which constraints apply was settled
# when the constructor was generated. Use `element` for a tag HTML does not define.


# [[[cog import cog; from tags import emit; cog.outl(emit()) ]]]
def html(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<html>` element."""
    return Element("html", attributes_of(cls, attrs), children_of(children))


def head(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<head>` element."""
    return Element("head", attributes_of(cls, attrs), children_of(children))


def base(*, cls: ClassNames = None, attrs: Attributes | None = None) -> VoidElement:
    """The `<base>` element."""
    return VoidElement("base", attributes_of(cls, attrs))


def link(*, cls: ClassNames = None, attrs: Attributes | None = None) -> VoidElement:
    """The `<link>` element."""
    return VoidElement("link", attributes_of(cls, attrs))


def meta(*, cls: ClassNames = None, attrs: Attributes | None = None) -> VoidElement:
    """The `<meta>` element."""
    return VoidElement("meta", attributes_of(cls, attrs))


def style(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Markup | None = None) -> Element:
    """The `<style>` element."""
    return Element("style", attributes_of(cls, attrs), raw_text_of("style", children))


def title(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<title>` element."""
    return Element("title", attributes_of(cls, attrs), children_of(children))


def body(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<body>` element."""
    return Element("body", attributes_of(cls, attrs), children_of(children))


def address(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<address>` element."""
    return Element("address", attributes_of(cls, attrs), children_of(children))


def article(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<article>` element."""
    return Element("article", attributes_of(cls, attrs), children_of(children))


def aside(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<aside>` element."""
    return Element("aside", attributes_of(cls, attrs), children_of(children))


def footer(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<footer>` element."""
    return Element("footer", attributes_of(cls, attrs), children_of(children))


def header(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<header>` element."""
    return Element("header", attributes_of(cls, attrs), children_of(children))


def h1(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<h1>` element."""
    return Element("h1", attributes_of(cls, attrs), children_of(children))


def h2(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<h2>` element."""
    return Element("h2", attributes_of(cls, attrs), children_of(children))


def h3(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<h3>` element."""
    return Element("h3", attributes_of(cls, attrs), children_of(children))


def h4(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<h4>` element."""
    return Element("h4", attributes_of(cls, attrs), children_of(children))


def h5(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<h5>` element."""
    return Element("h5", attributes_of(cls, attrs), children_of(children))


def h6(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<h6>` element."""
    return Element("h6", attributes_of(cls, attrs), children_of(children))


def hgroup(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<hgroup>` element."""
    return Element("hgroup", attributes_of(cls, attrs), children_of(children))


def main(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<main>` element."""
    return Element("main", attributes_of(cls, attrs), children_of(children))


def nav(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<nav>` element."""
    return Element("nav", attributes_of(cls, attrs), children_of(children))


def search(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<search>` element."""
    return Element("search", attributes_of(cls, attrs), children_of(children))


def section(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<section>` element."""
    return Element("section", attributes_of(cls, attrs), children_of(children))


def blockquote(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<blockquote>` element."""
    return Element("blockquote", attributes_of(cls, attrs), children_of(children))


def dd(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<dd>` element."""
    return Element("dd", attributes_of(cls, attrs), children_of(children))


def div(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<div>` element."""
    return Element("div", attributes_of(cls, attrs), children_of(children))


def dl(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<dl>` element."""
    return Element("dl", attributes_of(cls, attrs), children_of(children))


def dt(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<dt>` element."""
    return Element("dt", attributes_of(cls, attrs), children_of(children))


def figcaption(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<figcaption>` element."""
    return Element("figcaption", attributes_of(cls, attrs), children_of(children))


def figure(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<figure>` element."""
    return Element("figure", attributes_of(cls, attrs), children_of(children))


def hr(*, cls: ClassNames = None, attrs: Attributes | None = None) -> VoidElement:
    """The `<hr>` element."""
    return VoidElement("hr", attributes_of(cls, attrs))


def li(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<li>` element."""
    return Element("li", attributes_of(cls, attrs), children_of(children))


def menu(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<menu>` element."""
    return Element("menu", attributes_of(cls, attrs), children_of(children))


def ol(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<ol>` element."""
    return Element("ol", attributes_of(cls, attrs), children_of(children))


def p(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<p>` element."""
    return Element("p", attributes_of(cls, attrs), children_of(children))


def pre(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<pre>` element."""
    return Element("pre", attributes_of(cls, attrs), children_of(children))


def xmp(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Markup | None = None) -> Element:
    """The `<xmp>` element."""
    return Element("xmp", attributes_of(cls, attrs), raw_text_of("xmp", children))


def ul(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<ul>` element."""
    return Element("ul", attributes_of(cls, attrs), children_of(children))


def a(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<a>` element."""
    return Element("a", attributes_of(cls, attrs), children_of(children))


def abbr(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<abbr>` element."""
    return Element("abbr", attributes_of(cls, attrs), children_of(children))


def b(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<b>` element."""
    return Element("b", attributes_of(cls, attrs), children_of(children))


def bdi(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<bdi>` element."""
    return Element("bdi", attributes_of(cls, attrs), children_of(children))


def bdo(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<bdo>` element."""
    return Element("bdo", attributes_of(cls, attrs), children_of(children))


def br(*, cls: ClassNames = None, attrs: Attributes | None = None) -> VoidElement:
    """The `<br>` element."""
    return VoidElement("br", attributes_of(cls, attrs))


def cite(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<cite>` element."""
    return Element("cite", attributes_of(cls, attrs), children_of(children))


def code(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<code>` element."""
    return Element("code", attributes_of(cls, attrs), children_of(children))


def data(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<data>` element."""
    return Element("data", attributes_of(cls, attrs), children_of(children))


def dfn(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<dfn>` element."""
    return Element("dfn", attributes_of(cls, attrs), children_of(children))


def em(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<em>` element."""
    return Element("em", attributes_of(cls, attrs), children_of(children))


def i(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<i>` element."""
    return Element("i", attributes_of(cls, attrs), children_of(children))


def kbd(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<kbd>` element."""
    return Element("kbd", attributes_of(cls, attrs), children_of(children))


def mark(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<mark>` element."""
    return Element("mark", attributes_of(cls, attrs), children_of(children))


def q(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<q>` element."""
    return Element("q", attributes_of(cls, attrs), children_of(children))


def rp(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<rp>` element."""
    return Element("rp", attributes_of(cls, attrs), children_of(children))


def rt(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<rt>` element."""
    return Element("rt", attributes_of(cls, attrs), children_of(children))


def ruby(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<ruby>` element."""
    return Element("ruby", attributes_of(cls, attrs), children_of(children))


def s(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<s>` element."""
    return Element("s", attributes_of(cls, attrs), children_of(children))


def samp(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<samp>` element."""
    return Element("samp", attributes_of(cls, attrs), children_of(children))


def small(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<small>` element."""
    return Element("small", attributes_of(cls, attrs), children_of(children))


def span(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<span>` element."""
    return Element("span", attributes_of(cls, attrs), children_of(children))


def strong(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<strong>` element."""
    return Element("strong", attributes_of(cls, attrs), children_of(children))


def sub(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<sub>` element."""
    return Element("sub", attributes_of(cls, attrs), children_of(children))


def sup(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<sup>` element."""
    return Element("sup", attributes_of(cls, attrs), children_of(children))


def time(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<time>` element."""
    return Element("time", attributes_of(cls, attrs), children_of(children))


def u(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<u>` element."""
    return Element("u", attributes_of(cls, attrs), children_of(children))


def var(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<var>` element."""
    return Element("var", attributes_of(cls, attrs), children_of(children))


def wbr(*, cls: ClassNames = None, attrs: Attributes | None = None) -> VoidElement:
    """The `<wbr>` element."""
    return VoidElement("wbr", attributes_of(cls, attrs))


def area(*, cls: ClassNames = None, attrs: Attributes | None = None) -> VoidElement:
    """The `<area>` element."""
    return VoidElement("area", attributes_of(cls, attrs))


def audio(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<audio>` element."""
    return Element("audio", attributes_of(cls, attrs), children_of(children))


def img(*, cls: ClassNames = None, attrs: Attributes | None = None) -> VoidElement:
    """The `<img>` element."""
    return VoidElement("img", attributes_of(cls, attrs))


def map_(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<map>` element."""
    return Element("map", attributes_of(cls, attrs), children_of(children))


def track(*, cls: ClassNames = None, attrs: Attributes | None = None) -> VoidElement:
    """The `<track>` element."""
    return VoidElement("track", attributes_of(cls, attrs))


def video(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<video>` element."""
    return Element("video", attributes_of(cls, attrs), children_of(children))


def embed(*, cls: ClassNames = None, attrs: Attributes | None = None) -> VoidElement:
    """The `<embed>` element."""
    return VoidElement("embed", attributes_of(cls, attrs))


def noembed(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Markup | None = None) -> Element:
    """The `<noembed>` element."""
    return Element("noembed", attributes_of(cls, attrs), raw_text_of("noembed", children))


def iframe(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Markup | None = None) -> Element:
    """The `<iframe>` element."""
    return Element("iframe", attributes_of(cls, attrs), raw_text_of("iframe", children))


def noframes(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Markup | None = None) -> Element:
    """The `<noframes>` element."""
    return Element("noframes", attributes_of(cls, attrs), raw_text_of("noframes", children))


def object_(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<object>` element."""
    return Element("object", attributes_of(cls, attrs), children_of(children))


def picture(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<picture>` element."""
    return Element("picture", attributes_of(cls, attrs), children_of(children))


def source(*, cls: ClassNames = None, attrs: Attributes | None = None) -> VoidElement:
    """The `<source>` element."""
    return VoidElement("source", attributes_of(cls, attrs))


def svg(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<svg>` element."""
    return Element("svg", attributes_of(cls, attrs), children_of(children))


def canvas(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<canvas>` element."""
    return Element("canvas", attributes_of(cls, attrs), children_of(children))


def noscript(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<noscript>` element."""
    return Element("noscript", attributes_of(cls, attrs), children_of(children))


def script(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Markup | None = None) -> Element:
    """The `<script>` element."""
    return Element("script", attributes_of(cls, attrs), raw_text_of("script", children))


def del_(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<del>` element."""
    return Element("del", attributes_of(cls, attrs), children_of(children))


def ins(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<ins>` element."""
    return Element("ins", attributes_of(cls, attrs), children_of(children))


def caption(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<caption>` element."""
    return Element("caption", attributes_of(cls, attrs), children_of(children))


def col(*, cls: ClassNames = None, attrs: Attributes | None = None) -> VoidElement:
    """The `<col>` element."""
    return VoidElement("col", attributes_of(cls, attrs))


def colgroup(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<colgroup>` element."""
    return Element("colgroup", attributes_of(cls, attrs), children_of(children))


def table(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<table>` element."""
    return Element("table", attributes_of(cls, attrs), children_of(children))


def tbody(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<tbody>` element."""
    return Element("tbody", attributes_of(cls, attrs), children_of(children))


def td(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<td>` element."""
    return Element("td", attributes_of(cls, attrs), children_of(children))


def tfoot(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<tfoot>` element."""
    return Element("tfoot", attributes_of(cls, attrs), children_of(children))


def th(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<th>` element."""
    return Element("th", attributes_of(cls, attrs), children_of(children))


def thead(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<thead>` element."""
    return Element("thead", attributes_of(cls, attrs), children_of(children))


def tr(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<tr>` element."""
    return Element("tr", attributes_of(cls, attrs), children_of(children))


def button(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<button>` element."""
    return Element("button", attributes_of(cls, attrs), children_of(children))


def datalist(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<datalist>` element."""
    return Element("datalist", attributes_of(cls, attrs), children_of(children))


def fieldset(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<fieldset>` element."""
    return Element("fieldset", attributes_of(cls, attrs), children_of(children))


def form(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<form>` element."""
    return Element("form", attributes_of(cls, attrs), children_of(children))


def input_(*, cls: ClassNames = None, attrs: Attributes | None = None) -> VoidElement:
    """The `<input>` element."""
    return VoidElement("input", attributes_of(cls, attrs))


def label(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<label>` element."""
    return Element("label", attributes_of(cls, attrs), children_of(children))


def legend(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<legend>` element."""
    return Element("legend", attributes_of(cls, attrs), children_of(children))


def meter(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<meter>` element."""
    return Element("meter", attributes_of(cls, attrs), children_of(children))


def optgroup(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<optgroup>` element."""
    return Element("optgroup", attributes_of(cls, attrs), children_of(children))


def option(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<option>` element."""
    return Element("option", attributes_of(cls, attrs), children_of(children))


def output(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<output>` element."""
    return Element("output", attributes_of(cls, attrs), children_of(children))


def progress(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<progress>` element."""
    return Element("progress", attributes_of(cls, attrs), children_of(children))


def select(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<select>` element."""
    return Element("select", attributes_of(cls, attrs), children_of(children))


def textarea(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<textarea>` element."""
    return Element("textarea", attributes_of(cls, attrs), children_of(children))


def details(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<details>` element."""
    return Element("details", attributes_of(cls, attrs), children_of(children))


def dialog(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<dialog>` element."""
    return Element("dialog", attributes_of(cls, attrs), children_of(children))


def summary(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<summary>` element."""
    return Element("summary", attributes_of(cls, attrs), children_of(children))


def slot(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<slot>` element."""
    return Element("slot", attributes_of(cls, attrs), children_of(children))


def template(*, cls: ClassNames = None, attrs: Attributes | None = None, children: Node = None) -> Element:
    """The `<template>` element."""
    return Element("template", attributes_of(cls, attrs), children_of(children))


# [[[end]]]
