from __future__ import annotations

from without_html import Element
from without_html import VoidElement


class Widget(Element):
    """An `Element` subclass, which a walk reaches by its `isinstance` arms."""

    __slots__ = ()


class Spacer(VoidElement):
    """A `VoidElement` subclass, likewise."""

    __slots__ = ()
