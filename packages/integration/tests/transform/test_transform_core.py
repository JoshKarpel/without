from __future__ import annotations

import json

import pytest
from integration.transform.core import Mode
from integration.transform.core import Settings
from integration.transform.core import apply_mode
from integration.transform.core import mode_param
from integration.transform.core import render_modes
from integration.transform.core import route_not_found
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


def test_transform_uses_the_config_default_when_no_mode_is_requested() -> None:
    response = transform(Settings(default_mode=Mode.LOWER, max_bytes=64), None, b"LOUD")

    assert response.status == 200
    assert response.body == b"loud"


def test_transform_query_mode_overrides_the_config_default() -> None:
    response = transform(Settings(default_mode=Mode.LOWER, max_bytes=64), "title", b"the quiet part")

    assert response.status == 200
    assert response.body == b"The Quiet Part"


def test_transform_rejects_an_unknown_mode() -> None:
    response = transform(Settings(default_mode=Mode.UPPER, max_bytes=64), "shout", b"hi")

    assert response.status == 400
    assert json.loads(response.body) == {"error": "unknown mode: shout"}


def test_transform_rejects_a_body_over_the_configured_limit() -> None:
    response = transform(Settings(default_mode=Mode.UPPER, max_bytes=4), None, b"toolong")

    assert response.status == 413
    assert json.loads(response.body) == {"error": "body exceeds 4 bytes"}


def test_transform_rejects_a_non_utf8_body() -> None:
    response = transform(Settings(default_mode=Mode.UPPER, max_bytes=64), None, b"\xff\xfe")

    assert response.status == 400
    assert json.loads(response.body) == {"error": "body is not valid UTF-8"}


def test_render_modes_lists_modes_and_the_current_default() -> None:
    response = render_modes(Settings(default_mode=Mode.TITLE, max_bytes=64))

    assert response.status == 200
    assert json.loads(response.body) == {"modes": ["upper", "lower", "title"], "default": "title"}


def test_route_not_found_names_the_route() -> None:
    response = route_not_found("POST", "/nope")

    assert response.status == 404
    assert json.loads(response.body) == {"error": "no route for POST /nope"}


@pytest.mark.parametrize(
    ("query_string", "expected"),
    [
        (b"mode=title", "title"),
        (b"mode=title&other=1", "title"),
        (b"other=1", None),
        (b"", None),
    ],
)
def test_mode_param_reads_the_mode_parameter(query_string: bytes, expected: str | None) -> None:
    assert mode_param(query_string) == expected
