from __future__ import annotations

from enum import Enum
from enum import IntEnum
from enum import StrEnum
from uuid import UUID as Uuid  # noqa: N811 - aliased to avoid clashing with without_web's UUID converter

import pytest
from without_web import FLOAT
from without_web import INT
from without_web import PATH
from without_web import STR
from without_web import UUID
from without_web import Converter
from without_web import choice


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
    with pytest.raises(ValueError):  # noqa: PT011 - parametrized over converters whose stdlib parsers raise differing messages; only the rejection type is asserted
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


class Profile(StrEnum):
    DEV = "dev"
    PROD = "prod"


class Stage(StrEnum):
    DEV = "dev"
    PROD = "prod"


class Level(IntEnum):
    LOW = 1
    HIGH = 9


class TestEquality:
    def test_a_converter_is_its_name_and_its_parse(self) -> None:
        # Both halves matter because a converter is a trie key: branches merge
        # when converters compare equal, and a merged branch keeps one of them.
        assert Converter(name="int", parse=int, schema={}) == Converter(name="int", parse=int, schema={})
        assert Converter(name="int", parse=int, schema={}) != Converter(name="num", parse=int, schema={})
        assert Converter(name="int", parse=int, schema={}) != Converter(name="int", parse=float, schema={})

    def test_a_shared_parse_is_what_lets_two_declarations_merge(self) -> None:
        # The shipped converters are module-level singletons, so two routes
        # writing `path_param("id", INT)` hold the same object; this is the same
        # property spelled out for a converter built twice from shared parts.
        assert Converter(name="slug", parse=str, schema={"type": "string"}) == Converter(
            name="slug", parse=str, schema={"a": "different schema"}
        )

    def test_the_schema_is_not_part_of_the_comparison(self) -> None:
        # It takes no part in matching, and OpenAPI reads it off the route's own
        # segments rather than through the trie, so merging cannot affect it. It
        # also could not be compared: a `Mapping` is unhashable and a trie key
        # must not be.
        assert hash(Converter(name="int", parse=int, schema={"type": "integer"})) == hash(
            Converter(name="int", parse=int, schema={})
        )

    def test_two_closures_over_the_same_logic_are_still_distinct(self) -> None:
        # The conservative answer, and the reason a converter *factory* must
        # cache: nothing can prove two closures behave alike, so an uncached
        # factory yields converters that will not merge.
        def build() -> Converter[str]:
            return Converter(name="slug", parse=lambda raw: raw.lower(), schema={"type": "string"})

        assert build() != build()


class TestChoice:
    @pytest.mark.parametrize(
        ("enum", "segment", "expected"),
        [(Profile, "prod", Profile.PROD), (Level, "9", Level.HIGH)],
    )
    def test_choice_parses_a_member_by_its_value(self, enum: type[Enum], segment: str, expected: Enum) -> None:
        # Values, not member names, so the URL spelling and the Python identifier
        # are free to differ.
        assert choice(enum).parse(segment) == expected

    def test_choice_rejects_a_segment_outside_the_enum(self) -> None:
        # A `ValueError` makes the trie branch fail to match and the walk
        # backtrack, rather than raising out of the request.
        with pytest.raises(ValueError, match="'staging' is not one of dev, prod"):
            choice(Profile).parse("staging")

    def test_choice_documents_its_values_as_an_openapi_enum(self) -> None:
        assert choice(Profile).schema == {"type": "string", "enum": ["dev", "prod"]}
        assert choice(Profile).name == "Profile"

    def test_choice_returns_the_same_converter_for_the_same_enum(self) -> None:
        # Load-bearing, not an optimization: an uncached factory would build a
        # fresh closure per call, so the same enum used in two modules would give
        # two converters that do not compare equal and therefore do not merge.
        assert choice(Profile) is choice(Profile)

    def test_two_enums_that_look_alike_do_not_collide(self) -> None:
        # `Profile` and `Stage` have identical values, so nothing about the
        # segments they match tells them apart; only `parse` does.
        assert choice(Profile) != choice(Stage)
        assert choice(Profile).parse("dev") is Profile.DEV
        assert choice(Stage).parse("dev") is Stage.DEV

    def test_choice_refuses_an_enum_whose_members_share_a_spelling(self) -> None:
        # `Ambiguous.TEXT` would be unreachable from a URL, because the segment
        # "1" that would select it already selects `Ambiguous.NUMBER`.
        class Ambiguous(Enum):
            NUMBER = 1
            TEXT = "1"

        with pytest.raises(ValueError, match="two members whose values are spelled the same"):
            choice(Ambiguous)
