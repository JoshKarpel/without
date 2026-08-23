from __future__ import annotations

import pytest
from without_asgi.narrow import narrow


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("POST", str),
        (b"name=dark_mode", bytes),
        (54321, int),
    ],
)
def test_narrow_returns_the_value_when_the_type_matches(value: object, expected: type) -> None:
    assert narrow(value, expected) is value


@pytest.mark.parametrize(
    ("value", "expected", "message"),
    [
        (b"POST", str, "expected str, got bytes"),
        (42, bytes, "expected bytes, got int"),
        ("54321", int, "expected int, got str"),
    ],
)
def test_narrow_rejects_a_mismatched_type(value: object, expected: type, message: str) -> None:
    with pytest.raises(TypeError, match=message):
        narrow(value, expected)
