from __future__ import annotations

import pytest
from without_asgi.narrow import narrow
from without_asgi.narrow import narrow_to_bytes
from without_asgi.narrow import narrow_to_int
from without_asgi.narrow import narrow_to_str


def test_narrow_returns_the_value_when_the_type_matches() -> None:
    assert narrow(b"payload", bytes) == b"payload"


def test_narrow_rejects_a_mismatched_type() -> None:
    with pytest.raises(TypeError, match="expected bytes, got str"):
        narrow("payload", bytes)


def test_narrow_to_str_returns_a_str() -> None:
    assert narrow_to_str("POST") == "POST"


def test_narrow_to_str_rejects_non_str() -> None:
    with pytest.raises(TypeError, match="expected str, got bytes"):
        narrow_to_str(b"POST")


def test_narrow_to_bytes_returns_bytes() -> None:
    assert narrow_to_bytes(b"name=dark_mode") == b"name=dark_mode"


def test_narrow_to_bytes_rejects_non_bytes() -> None:
    with pytest.raises(TypeError, match="expected bytes, got int"):
        narrow_to_bytes(42)


def test_narrow_to_int_returns_an_int() -> None:
    assert narrow_to_int(54321) == 54321


def test_narrow_to_int_rejects_non_int() -> None:
    with pytest.raises(TypeError, match="expected int, got str"):
        narrow_to_int("54321")
