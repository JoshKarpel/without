from __future__ import annotations

import pytest
from integration.transform.core import Mode
from integration.transform.core import Settings
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
    assert resolve_mode(Settings(default_mode=Mode.TITLE, max_bytes=64), None) is Mode.TITLE


def test_resolve_mode_rejects_an_unknown_mode() -> None:
    with pytest.raises(UnknownMode) as caught:
        resolve_mode(Settings(default_mode=Mode.UPPER, max_bytes=64), "shout")

    assert caught.value.requested == "shout"
    assert str(caught.value) == "unknown mode: shout"


def test_transform_uses_the_config_default_when_no_mode_is_requested() -> None:
    assert transform(Settings(default_mode=Mode.LOWER, max_bytes=64), None, "LOUD") == "loud"


def test_transform_query_mode_overrides_the_config_default() -> None:
    assert transform(Settings(default_mode=Mode.LOWER, max_bytes=64), "title", "the quiet part") == "The Quiet Part"
