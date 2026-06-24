from __future__ import annotations

import json

import pytest
from without_integration.flags.core import Flags
from without_integration.flags.core import bad_request
from without_integration.flags.core import flag_name
from without_integration.flags.core import render_all
from without_integration.flags.core import render_one
from without_integration.flags.core import route_not_found


def test_render_all_lists_every_flag() -> None:
    response = render_all(Flags(flags={"dark_mode": True, "beta": False}))

    assert response.status == 200
    assert json.loads(response.body) == {"flags": {"dark_mode": True, "beta": False}}


def test_render_one_returns_a_known_flag() -> None:
    response = render_one(Flags(flags={"dark_mode": True}), "dark_mode")

    assert response.status == 200
    assert json.loads(response.body) == {"name": "dark_mode", "enabled": True}


def test_render_one_is_404_for_an_unknown_flag() -> None:
    response = render_one(Flags(flags={"dark_mode": True}), "missing")

    assert response.status == 404
    assert json.loads(response.body) == {"error": "unknown flag: missing"}


def test_bad_request_carries_the_message() -> None:
    response = bad_request("missing 'name' query parameter")

    assert response.status == 400
    assert json.loads(response.body) == {"error": "missing 'name' query parameter"}


def test_route_not_found_names_the_route() -> None:
    response = route_not_found("GET", "/nope")

    assert response.status == 404
    assert json.loads(response.body) == {"error": "no route for GET /nope"}


@pytest.mark.parametrize(
    ("query_string", "expected"),
    [
        (b"name=dark_mode", "dark_mode"),
        (b"name=dark_mode&other=1", "dark_mode"),
        (b"other=1", None),
        (b"", None),
    ],
)
def test_flag_name_reads_the_name_parameter(query_string: bytes, expected: str | None) -> None:
    assert flag_name(query_string) == expected
