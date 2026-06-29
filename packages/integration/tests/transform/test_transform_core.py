from __future__ import annotations

import pytest
from integration.transform.core import Mode
from integration.transform.core import TransformConfig
from integration.transform.core import UnknownMode
from integration.transform.core import apply_mode
from integration.transform.core import resolve_mode
from integration.transform.core import transform


@pytest.mark.parametrize(
    ("mode", "expected"),
    [
        (Mode.UPPER, "HELLO WORLD"),
        (Mode.LOWER, "hello world"),
        (Mode.TITLE, "Hello World"),
    ],
)
def test_apply_mode_transforms_text(mode: Mode, expected: str) -> None:
    assert apply_mode(mode, "heLLo wORLd") == expected


def test_resolve_mode_falls_back_to_the_configured_default() -> None:
    assert resolve_mode(TransformConfig(default_mode=Mode.TITLE), None) is Mode.TITLE


def test_resolve_mode_rejects_an_unknown_mode() -> None:
    with pytest.raises(UnknownMode) as caught:
        resolve_mode(TransformConfig(default_mode=Mode.UPPER), "shout")

    assert caught.value.requested == "shout"
    assert str(caught.value) == "unknown mode: shout"


def test_transform_uses_the_config_default_when_no_mode_is_requested() -> None:
    assert transform(TransformConfig(default_mode=Mode.LOWER), None, "LOUD") == "loud"


def test_transform_query_mode_overrides_the_config_default() -> None:
    assert transform(TransformConfig(default_mode=Mode.LOWER), "title", "the quiet part") == "The Quiet Part"
