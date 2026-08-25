from __future__ import annotations

from collections.abc import Callable
from datetime import timedelta

import pytest
from without_streams import Milliseconds
from without_streams import Seconds


class TestCounts:
    @pytest.mark.parametrize(
        ("counted", "expected"),
        [
            (Seconds(45), timedelta(seconds=45)),
            (Seconds(0), timedelta()),
            (Seconds(-7), timedelta(seconds=-7)),
            (Milliseconds(1500), timedelta(seconds=1.5)),
            (Milliseconds(1), timedelta(microseconds=1000)),
        ],
    )
    def test_a_count_hands_back_the_duration_it_names(
        self, counted: Seconds | Milliseconds, expected: timedelta
    ) -> None:
        assert counted.duration == expected

    def test_the_two_units_are_separate_types_because_the_boundaries_are(self) -> None:
        # The same number means a different duration at each, which is the whole reason
        # the unit rides on the type rather than on a variable name.
        assert Seconds(5).duration != Milliseconds(5).duration


class TestParsingADuration:
    @pytest.mark.parametrize(
        ("parse", "duration", "expected"),
        [
            (Seconds.of, timedelta(minutes=2), 120),
            (Seconds.of, timedelta(), 0),
            (Seconds.of, timedelta(seconds=-7), -7),
            (Milliseconds.of, timedelta(seconds=1.5), 1500),
            (Milliseconds.of, timedelta(minutes=1), 60_000),
        ],
    )
    def test_a_duration_that_divides_evenly_becomes_its_count(
        self, parse: Callable[[timedelta], Seconds | Milliseconds], duration: timedelta, expected: int
    ) -> None:
        assert parse(duration).count == expected

    @pytest.mark.parametrize(
        ("parse", "fractional", "units"),
        [
            (Seconds.of, timedelta(milliseconds=1500), "seconds"),
            (Seconds.of, timedelta(microseconds=1), "seconds"),
            (Milliseconds.of, timedelta(microseconds=500), "milliseconds"),
            (Milliseconds.of, timedelta(seconds=1.0005), "milliseconds"),
        ],
    )
    def test_a_finer_duration_is_refused_rather_than_truncated(
        self, parse: Callable[[timedelta], Seconds | Milliseconds], fractional: timedelta, units: str
    ) -> None:
        # The case these exist for: truncating half a millisecond does not produce a short
        # wait, it produces none at all.
        with pytest.raises(ValueError, match=f"a whole number of {units} cannot express"):
            parse(fractional)

    def test_a_negative_duration_is_measured_by_the_same_rule(self) -> None:
        # `timedelta` normalizes a negative value onto a negative day and a positive
        # remainder, which a check written against the components alone gets wrong.
        with pytest.raises(ValueError, match="a whole number of seconds cannot express"):
            Seconds.of(timedelta(microseconds=-500))

    def test_the_message_names_the_value_that_would_not_fit(self) -> None:
        with pytest.raises(ValueError, match=r"^a whole number of seconds cannot express 0:00:07\.500000$"):
            Seconds.of(timedelta(seconds=7.5))

    @pytest.mark.parametrize(
        ("parse", "duration"),
        [(Seconds.of, timedelta(minutes=2)), (Milliseconds.of, timedelta(seconds=1.5))],
    )
    def test_parsing_and_handing_back_is_the_duration_unchanged(
        self, parse: Callable[[timedelta], Seconds | Milliseconds], duration: timedelta
    ) -> None:
        assert parse(duration).duration == duration
