from __future__ import annotations

from uuid import UUID as Uuid

import pytest
from without_web import FLOAT
from without_web import INT
from without_web import PATH
from without_web import STR
from without_web import UUID
from without_web import Converter


@pytest.mark.parametrize(
    ("converter", "segment", "expected"),
    [
        (STR, "anything", "anything"),
        (INT, "42", 42),
        (FLOAT, "3.5", 3.5),
        (UUID, "12345678-1234-5678-1234-567812345678", Uuid("12345678-1234-5678-1234-567812345678")),
        (PATH, "a/b/c", "a/b/c"),
    ],
)
def test_converter_parses_a_valid_segment(converter: Converter[object], segment: str, expected: object) -> None:
    assert converter.parse(segment) == expected


@pytest.mark.parametrize(
    ("converter", "segment"),
    [(INT, "abc"), (INT, "3.5"), (FLOAT, "words"), (UUID, "not-a-uuid")],
)
def test_converter_rejects_an_unfit_segment_with_value_error(converter: Converter[object], segment: str) -> None:
    with pytest.raises(ValueError):
        converter.parse(segment)


@pytest.mark.parametrize(
    ("converter", "schema"),
    [
        (STR, {"type": "string"}),
        (INT, {"type": "integer"}),
        (FLOAT, {"type": "number"}),
        (UUID, {"type": "string", "format": "uuid"}),
    ],
)
def test_converter_carries_its_path_param_schema(converter: Converter[object], schema: dict[str, object]) -> None:
    assert converter.schema == schema
