from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from enum import IntEnum
from enum import StrEnum

import pytest
from without_cli import BOOL
from without_cli import FLOAT
from without_cli import INT
from without_cli import PATH
from without_cli import STR
from without_cli import UUID
from without_cli import Bound
from without_cli import Converter
from without_cli import ExtractionError
from without_cli import FromEnv
from without_cli import Positional
from without_cli import Rejected
from without_cli import Streams
from without_cli import argument
from without_cli import choice
from without_cli import command
from without_cli import flag
from without_cli import group
from without_cli import into
from without_cli import once
from without_cli import option
from without_cli import parse_argv

from .helpers import unreached


class Profile(StrEnum):
    DEV = "dev"
    PROD = "prod"


class Colour(Enum):
    RED = "red"
    BLUE = "blue"


class Level(IntEnum):
    LOW = 1
    HIGH = 9


class TestConverters:
    @pytest.mark.parametrize(
        ("converter", "raw", "expected"),
        [
            (STR, "text", "text"),
            (INT, "-17", -17),
            (FLOAT, "2.5", 2.5),
            (BOOL, "yes", True),
            (BOOL, "OFF", False),
        ],
    )
    def test_a_converter_parses_its_shape(self, converter: Converter[object], raw: str, expected: object) -> None:
        assert converter.parse(raw) == expected

    def test_uuid_and_path_parse_to_their_types(self) -> None:
        assert str(UUID.parse("6ba7b810-9dad-11d1-80b4-00c04fd430c8")).startswith("6ba7b810")
        assert PATH.parse("/a/b").name == "b"

    @pytest.mark.parametrize("raw", ["maybe", "", "2", "yesplease"])
    def test_a_boolean_outside_the_known_spellings_is_refused(self, raw: str) -> None:
        # A `DEBUG=maybe` that quietly reads as off is worse than one that says
        # it is not a boolean.
        with pytest.raises(ValueError, match="expected a boolean"):
            BOOL.parse(raw)

    @pytest.mark.parametrize(
        ("enum", "raw", "expected"),
        [
            (Profile, "prod", Profile.PROD),
            (Colour, "blue", Colour.BLUE),
            (Level, "9", Level.HIGH),
        ],
    )
    def test_choice_parses_a_member_by_its_value(self, enum: type[Enum], raw: str, expected: Enum) -> None:
        # Values, not member names, so the shell spelling and the Python
        # identifier are free to differ.
        assert choice(enum).parse(raw) == expected

    def test_choice_names_the_alternation_it_accepts(self) -> None:
        assert choice(Profile).metavar == "[dev|prod]"
        assert choice(Level).metavar == "[1|9]"

    def test_choice_refuses_a_value_outside_the_enum(self) -> None:
        with pytest.raises(ValueError, match="'staging' is not one of dev, prod"):
            choice(Profile).parse("staging")

    def test_choice_refuses_an_enum_whose_members_share_a_spelling(self) -> None:
        # `Ambiguous.TEXT` is unreachable from a command line, because the raw
        # `"1"` that would select it already selects `Ambiguous.NUMBER`.
        class Ambiguous(Enum):
            NUMBER = 1
            TEXT = "1"

        with pytest.raises(ValueError, match="two members whose values are spelled the same"):
            choice(Ambiguous)

    def test_converters_compare_by_their_placeholder(self) -> None:
        # Equality ignores `parse`, which a CLI can afford because nothing here
        # keys a structure on a converter. `without-web` compares `parse` too,
        # because there a converter *is* the routing trie's branch key and two
        # that compare equal are merged.
        assert Converter(metavar="INT", parse=int) == Converter(metavar="INT", parse=float)
        assert Converter(metavar="INT", parse=int) != Converter(metavar="NUM", parse=int)


class TestRejectionAttribution:
    def test_a_bad_flag_source_names_the_option(self) -> None:
        loud = flag("--loud", sources=(FromEnv("LOUD"),))
        app = group("app", commands=(command("show", loud)(unreached),))
        outcome = parse_argv(app, argv=["show"], env={"LOUD": "perhaps"})
        assert isinstance(outcome, Rejected)
        assert outcome.message == "--loud: expected a boolean, got 'perhaps'"

    def test_a_rich_rejection_from_a_converter_passes_through_untouched(self) -> None:
        # A converter that already knows which parameter it belongs to (a shared
        # one that validates against a registry, say) keeps its own message and
        # attribution rather than having them overwritten.
        def refuse(raw: str) -> str:
            raise ExtractionError("no such profile", parameter="profile")

        profile = Converter(metavar="PROFILE", parse=refuse)
        app = group("app", commands=(command("show", argument("name", once(profile)))(unreached),))
        outcome = parse_argv(app, argv=["show", "anything"])
        assert isinstance(outcome, Rejected)
        assert outcome.message == "profile: no such profile"

    def test_a_non_value_error_from_a_command_is_not_a_usage_error(self) -> None:
        # The boundary is one matchable type, so a bug inside a command surfaces
        # as itself rather than masquerading as a bad command line.
        def explode(raw: str) -> str:
            raise TypeError("a bug, not a bad argument")

        broken = Converter(metavar="BROKEN", parse=explode)
        app = group("app", commands=(command("show", argument("name", once(broken)))(unreached),))
        with pytest.raises(TypeError, match="a bug"):
            parse_argv(app, argv=["show", "anything"])


@dataclass(frozen=True, slots=True)
class Filter:
    name: str
    limit: int
    loud: bool


class TestInto:
    async def test_tokens_combine_into_one_typed_value(self) -> None:
        combined = into(Filter, argument("name", once(STR)), option("--limit", once(INT)), flag("--loud"))

        @command("show", combined)
        async def show(state: Streams, value: Filter) -> int:
            state.stdout.write(repr(value))
            return 0

        app = group("app", commands=(show,))
        capture = Streams.captured()
        outcome = parse_argv(app, argv=["show", "widgets", "--limit", "5", "--loud"])
        assert isinstance(outcome, Bound)
        await outcome.action(capture.streams)
        assert capture.stdout == "Filter(name='widgets', limit=5, loud=True)"

    def test_a_combined_token_still_describes_every_part(self) -> None:
        combined = into(Filter, argument("name", once(STR)), option("--limit", once(INT)), flag("--loud"))
        # The constituents' usage carries through, so combining changes nothing
        # about how the command is documented.
        named = [p.name if isinstance(p, Positional) else p.canonical for p in combined.parameters]
        assert named == ["name", "--limit", "--loud"]
