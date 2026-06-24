from __future__ import annotations

from uuid import UUID

import pytest
from without_web import DEFAULT_CONVERTERS


@pytest.mark.parametrize(
    ("name", "segment", "expected"),
    [
        ("str", "anything", "anything"),
        ("int", "42", 42),
        ("float", "3.5", 3.5),
        ("uuid", "12345678-1234-5678-1234-567812345678", UUID("12345678-1234-5678-1234-567812345678")),
        ("path", "a/b/c", "a/b/c"),
    ],
)
def test_converter_parses_a_valid_segment(name: str, segment: str, expected: object) -> None:
    assert DEFAULT_CONVERTERS[name].parse(segment) == expected


@pytest.mark.parametrize(
    ("name", "segment"),
    [("int", "abc"), ("int", "3.5"), ("float", "words"), ("uuid", "not-a-uuid")],
)
def test_converter_rejects_an_unfit_segment_with_value_error(name: str, segment: str) -> None:
    with pytest.raises(ValueError):
        DEFAULT_CONVERTERS[name].parse(segment)


@pytest.mark.parametrize(
    ("name", "schema"),
    [
        ("str", {"type": "string"}),
        ("int", {"type": "integer"}),
        ("float", {"type": "number"}),
        ("uuid", {"type": "string", "format": "uuid"}),
    ],
)
def test_converter_carries_its_path_param_schema(name: str, schema: dict[str, object]) -> None:
    assert DEFAULT_CONVERTERS[name].schema == schema
