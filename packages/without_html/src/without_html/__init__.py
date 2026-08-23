from markupsafe import Markup
from markupsafe import escape

from without_html.markup import DOCTYPE
from without_html.markup import SupportsHtml
from without_html.nodes import AnyElement
from without_html.nodes import Attributes
from without_html.nodes import AttributeValue
from without_html.nodes import Child
from without_html.nodes import ClassNames
from without_html.nodes import Element
from without_html.nodes import ElementConstructor
from without_html.nodes import Node
from without_html.nodes import RawTextElementConstructor
from without_html.nodes import VoidElement
from without_html.nodes import VoidElementConstructor
from without_html.nodes import element
from without_html.nodes import element_type
from without_html.nodes import void_element_type
from without_html.render import render
from without_html.render import render_chunks

# The element constructors are re-exported here so that `from without_html import div`
# works; the block is generated, and isort is held off it so that the generator and the
# formatter do not disagree about where it starts.
# isort: off
# [[[cog import cog; from tags import emit_imports; cog.outl(emit_imports()) ]]]
from without_html.elements import a
from without_html.elements import abbr
from without_html.elements import address
from without_html.elements import area
from without_html.elements import article
from without_html.elements import aside
from without_html.elements import audio
from without_html.elements import b
from without_html.elements import base
from without_html.elements import bdi
from without_html.elements import bdo
from without_html.elements import blockquote
from without_html.elements import body
from without_html.elements import br
from without_html.elements import button
from without_html.elements import canvas
from without_html.elements import caption
from without_html.elements import cite
from without_html.elements import code
from without_html.elements import col
from without_html.elements import colgroup
from without_html.elements import data
from without_html.elements import datalist
from without_html.elements import dd
from without_html.elements import del_
from without_html.elements import details
from without_html.elements import dfn
from without_html.elements import dialog
from without_html.elements import div
from without_html.elements import dl
from without_html.elements import dt
from without_html.elements import em
from without_html.elements import embed
from without_html.elements import fieldset
from without_html.elements import figcaption
from without_html.elements import figure
from without_html.elements import footer
from without_html.elements import form
from without_html.elements import h1
from without_html.elements import h2
from without_html.elements import h3
from without_html.elements import h4
from without_html.elements import h5
from without_html.elements import h6
from without_html.elements import head
from without_html.elements import header
from without_html.elements import hgroup
from without_html.elements import hr
from without_html.elements import html
from without_html.elements import i
from without_html.elements import iframe
from without_html.elements import img
from without_html.elements import input_
from without_html.elements import ins
from without_html.elements import kbd
from without_html.elements import label
from without_html.elements import legend
from without_html.elements import li
from without_html.elements import link
from without_html.elements import main
from without_html.elements import map_
from without_html.elements import mark
from without_html.elements import menu
from without_html.elements import meta
from without_html.elements import meter
from without_html.elements import nav
from without_html.elements import noembed
from without_html.elements import noframes
from without_html.elements import noscript
from without_html.elements import object_
from without_html.elements import ol
from without_html.elements import optgroup
from without_html.elements import option
from without_html.elements import output
from without_html.elements import p
from without_html.elements import picture
from without_html.elements import pre
from without_html.elements import progress
from without_html.elements import q
from without_html.elements import rp
from without_html.elements import rt
from without_html.elements import ruby
from without_html.elements import s
from without_html.elements import samp
from without_html.elements import script
from without_html.elements import search
from without_html.elements import section
from without_html.elements import select
from without_html.elements import slot
from without_html.elements import small
from without_html.elements import source
from without_html.elements import span
from without_html.elements import strong
from without_html.elements import style
from without_html.elements import sub
from without_html.elements import summary
from without_html.elements import sup
from without_html.elements import svg
from without_html.elements import table
from without_html.elements import tbody
from without_html.elements import td
from without_html.elements import template
from without_html.elements import textarea
from without_html.elements import tfoot
from without_html.elements import th
from without_html.elements import thead
from without_html.elements import time
from without_html.elements import title
from without_html.elements import tr
from without_html.elements import track
from without_html.elements import u
from without_html.elements import ul
from without_html.elements import var
from without_html.elements import video
from without_html.elements import wbr
from without_html.elements import xmp
# [[[end]]]
# isort: on

__all__ = [
    # [[[cog
    # import cog
    # from tags import emit_names
    # cog.outl(emit_names([
    #     "DOCTYPE", "AttributeValue", "Attributes", "Child", "ClassNames", "Element",
    #     "Node", "Markup", "SupportsHtml", "AnyElement", "VoidElement",
    #     "ElementConstructor", "RawTextElementConstructor", "VoidElementConstructor",
    #     "element", "element_type", "void_element_type",
    #     "escape", "render", "render_chunks",
    # ]))
    # ]]]
    "DOCTYPE",
    "AnyElement",
    "AttributeValue",
    "Attributes",
    "Child",
    "ClassNames",
    "Element",
    "ElementConstructor",
    "Markup",
    "Node",
    "RawTextElementConstructor",
    "SupportsHtml",
    "VoidElement",
    "VoidElementConstructor",
    "a",
    "abbr",
    "address",
    "area",
    "article",
    "aside",
    "audio",
    "b",
    "base",
    "bdi",
    "bdo",
    "blockquote",
    "body",
    "br",
    "button",
    "canvas",
    "caption",
    "cite",
    "code",
    "col",
    "colgroup",
    "data",
    "datalist",
    "dd",
    "del_",
    "details",
    "dfn",
    "dialog",
    "div",
    "dl",
    "dt",
    "element",
    "element_type",
    "em",
    "embed",
    "escape",
    "fieldset",
    "figcaption",
    "figure",
    "footer",
    "form",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "head",
    "header",
    "hgroup",
    "hr",
    "html",
    "i",
    "iframe",
    "img",
    "input_",
    "ins",
    "kbd",
    "label",
    "legend",
    "li",
    "link",
    "main",
    "map_",
    "mark",
    "menu",
    "meta",
    "meter",
    "nav",
    "noembed",
    "noframes",
    "noscript",
    "object_",
    "ol",
    "optgroup",
    "option",
    "output",
    "p",
    "picture",
    "pre",
    "progress",
    "q",
    "render",
    "render_chunks",
    "rp",
    "rt",
    "ruby",
    "s",
    "samp",
    "script",
    "search",
    "section",
    "select",
    "slot",
    "small",
    "source",
    "span",
    "strong",
    "style",
    "sub",
    "summary",
    "sup",
    "svg",
    "table",
    "tbody",
    "td",
    "template",
    "textarea",
    "tfoot",
    "th",
    "thead",
    "time",
    "title",
    "tr",
    "track",
    "u",
    "ul",
    "var",
    "video",
    "void_element_type",
    "wbr",
    "xmp",
    # [[[end]]]
]
