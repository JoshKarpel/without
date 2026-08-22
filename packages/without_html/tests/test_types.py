from __future__ import annotations

from typing import TYPE_CHECKING
from typing import assert_type

from markupsafe import Markup
from without_html import Element
from without_html import ElementConstructor
from without_html import RawTextElementConstructor
from without_html import VoidElement
from without_html import VoidElementConstructor
from without_html import br
from without_html import div
from without_html import element
from without_html import element_type
from without_html import script
from without_html import void_element_type

# The constraints this package puts in its types have no runtime failure to catch,
# because the whole point was to turn them into static errors. mypy checks this block
# and the runtime never runs it, so each pinned error code is the regression guard:
# with `warn_unused_ignores`, an ignore that stops being needed fails the build.

if TYPE_CHECKING:
    assert_type(div(), Element)
    assert_type(br(), VoidElement)
    assert_type(element("x-chart"), Element)
    assert_type(element_type("x-chart"), ElementConstructor)
    assert_type(void_element_type("x-spacer"), VoidElementConstructor)
    assert_type(script(children=Markup("f()")), Element)

    # A raw-text constructor takes a narrower `children` than `ElementConstructor`
    # promises, so it does not satisfy that protocol, which is why there is a second one
    # naming `Markup | None` itself. The relationship only runs one way: an ordinary
    # constructor accepts everything a raw-text one must, so `div` satisfies both.
    ordinary: ElementConstructor = div
    raw_text: RawTextElementConstructor = script
    ordinary = script  # type: ignore[assignment]

    br(children="text")  # type: ignore[call-arg]
    script(children="alert(1)")  # type: ignore[arg-type]
    div(children=object())  # type: ignore[arg-type]
    div(cls=3)  # type: ignore[arg-type]
    div(attrs={"colspan": 1.5})  # type: ignore[dict-item]

    # `ClassNames` and `Node` name `Sequence` and `Iterator` rather than `Iterable`, so the
    # two iterables that mean something other than what these positions mean cannot be
    # written. A `set` renders in an order that varies between processes. A `Mapping`
    # renders its keys, which for `cls` means `{"card": True, "active": False}`, the shape
    # `classnames` and `clsx` made the idiom in JavaScript, renders *both* names; the
    # spelling here is `("card", "card-active" if active else None)`.
    div(cls={"card", "active"})  # type: ignore[arg-type]
    div(cls={"card": True, "active": False})  # type: ignore[arg-type]

    # What naming those two costs: an iterable that is neither, which unpacks at the call
    # site like anything else this one-level flattening asks to be unpacked.
    classes: dict[str, str] = {}
    div(cls=classes.values())  # type: ignore[arg-type]
    div(cls=[*classes.values()])
