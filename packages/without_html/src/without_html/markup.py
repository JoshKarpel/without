from __future__ import annotations

from typing import Protocol
from typing import runtime_checkable

from markupsafe import Markup

# `Markup` is markup that is already safe to emit, and the one thing `render` passes
# through untouched: everything else in a text position is escaped, so producing
# unescaped output is a deliberate act with a name rather than a flag someone forgets.
#
# It is MarkupSafe's own type, kept under its own name, rather than a local one wrapping
# it. That is what makes a fragment rendered by Jinja, Flask, or tdom already one of
# these here, with no adapter; and it carries safety through string operations
# (`Markup("<b>") + untrusted` escapes the right-hand side and stays safe, and so do
# `%`, `format`, and `join`), which is the part a naive `str` subclass gets quietly
# wrong. Renaming it would also mean every traceback and `repr` naming a type this
# package's own documentation never mentions.


@runtime_checkable
class SupportsHtml(Protocol):
    """
    An object that can represent itself as markup, via the `__html__` convention.

    The protocol MarkupSafe established and the wider templating ecosystem honours.
    Accepting it is what lets a value that knows its own markup (a domain type, a
    template rendered elsewhere) sit in a child position without this package
    knowing anything about it.
    """

    def __html__(self) -> str: ...


# The HTML5 doctype, to prepend to a whole document: `render([DOCTYPE, html(...)])`.
DOCTYPE = Markup("<!doctype html>")


# Two escapes, because the two positions genuinely differ: a text position needs only
# `&`, `<`, and `>`, while a double-quoted attribute value needs both quotes as well.
# Neither is MarkupSafe's `escape`, for a reason that is measured rather than assumed.
# MarkupSafe's C `_escape_inner` is the fastest scan available, but `escape` wraps it in a
# Python call and a `Markup` construction, and those dominate: 245 ns against roughly 106
# for the guarded form below on a short clean string. Nothing here needs the `Markup` back,
# since escaped text goes straight into the output list and escaped attribute values are
# stored as plain strings on an already-parsed element.
#
# Guarded rather than a single pass, which is also measured. `str.translate` and a regex
# `sub` each look like the better shape (one scan instead of five) and each lose badly,
# because their per-character work happens in Python (a dict lookup, a callback) where
# `str.replace` stays in C: on a dirty 195-character string, 2.4 us and 2.6 us against
# 0.44 us here. The guard earns its place separately, since the common case is text with
# nothing to escape, where a containment scan beats a `replace` that must build a result
# either way.


def escape_text(text: str) -> str:
    """
    Escape `text` for a text position, where only `&`, `<`, and `>` are special.

    Leaving quotes alone is not only cheaper: it keeps prose full of apostrophes from
    rendering larger than it has to, which MarkupSafe's five-character escape does not.
    """
    if "&" in text:
        text = text.replace("&", "&amp;")
    if "<" in text:
        text = text.replace("<", "&lt;")
    if ">" in text:
        text = text.replace(">", "&gt;")
    return text


def escape_attribute(text: str) -> str:
    """
    Escape `text` for a double-quoted attribute value: the three text characters plus both
    quotes.

    Character for character what MarkupSafe's `escape` produces, which the tests pin with a
    property rather than leave as a claim.
    """
    if "&" in text:
        text = text.replace("&", "&amp;")
    if "<" in text:
        text = text.replace("<", "&lt;")
    if ">" in text:
        text = text.replace(">", "&gt;")
    if '"' in text:
        text = text.replace('"', "&#34;")
    if "'" in text:
        text = text.replace("'", "&#39;")
    return text
