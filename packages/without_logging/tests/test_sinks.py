import io
import logging
from collections.abc import Iterator
from datetime import UTC
from datetime import datetime
from datetime import time
from datetime import timedelta
from datetime import timezone
from pathlib import Path

import pytest
from hypothesis import given
from hypothesis import strategies as st
from without_logging import Level
from without_logging import Record
from without_logging import at_least
from without_logging import at_times
from without_logging import capture
from without_logging import to_rotating_file
from without_logging import to_stream
from without_logging.sinks import now_utc
from without_streams import compose
from without_streams import from_map
from without_streams import from_selector
from without_streams import offload

EPOCH = datetime(2026, 1, 1, tzinfo=UTC)


async def render(record: Record) -> str:
    return record.message


def test_to_stream_writes_each_line_to_the_stream_and_leaves_it_open() -> None:
    buffer = io.StringIO()

    to_stream(buffer)(iter([["first", "second"], ["third"]]))

    assert buffer.getvalue() == "first\nsecond\nthird\n"
    assert not buffer.closed  # the caller owns the stream; the worker does not close it


def test_to_rotating_file_passes_the_index_and_open_time_to_name(tmp_path: Path) -> None:
    calls: list[tuple[int, datetime]] = []

    def name(index: int, when: datetime) -> Path:
        calls.append((index, when))
        return tmp_path / f"app.{index}.log"

    to_rotating_file(name, max_bytes=1_000_000, now=lambda: EPOCH)(iter([["first", "second"]]))

    assert calls == [(0, EPOCH)]
    assert (tmp_path / "app.0.log").read_text(encoding="utf-8") == "first\nsecond\n"


def test_to_rotating_file_rotates_to_the_next_file_when_the_size_limit_would_be_exceeded(tmp_path: Path) -> None:
    def name(index: int, when: datetime) -> Path:
        return tmp_path / f"app.{index}.log"

    # Each line is 6 bytes ("aaaaa\n"); with a 12-byte cap two fit per file, the third rotates.
    to_rotating_file(name, max_bytes=12)(iter([["aaaaa", "bbbbb", "ccccc"]]))

    assert (tmp_path / "app.0.log").read_text(encoding="utf-8") == "aaaaa\nbbbbb\n"
    assert (tmp_path / "app.1.log").read_text(encoding="utf-8") == "ccccc\n"


def test_to_rotating_file_rotates_when_the_current_file_is_older_than_the_age_limit(tmp_path: Path) -> None:
    def name(index: int, when: datetime) -> Path:
        return tmp_path / f"app.{index}.log"

    clock = EPOCH

    def now() -> datetime:
        return clock

    def batches() -> Iterator[list[str]]:
        nonlocal clock
        yield ["fresh"]
        clock = EPOCH + timedelta(seconds=11)  # older than the 10s limit before the next burst
        yield ["stale"]

    to_rotating_file(name, max_age=timedelta(seconds=10), now=now)(batches())

    assert (tmp_path / "app.0.log").read_text(encoding="utf-8") == "fresh\n"
    assert (tmp_path / "app.1.log").read_text(encoding="utf-8") == "stale\n"


def test_to_rotating_file_rotates_when_a_scheduled_boundary_is_crossed(tmp_path: Path) -> None:
    def name(index: int, when: datetime) -> Path:
        return tmp_path / f"app.{index}.log"

    clock = EPOCH

    def now() -> datetime:
        return clock

    def schedule(after: datetime) -> datetime:
        return after + timedelta(seconds=10)  # next boundary is 10s after each file opens

    def batches() -> Iterator[list[str]]:
        nonlocal clock
        yield ["a", "b"]  # both before the boundary -> same file
        clock = EPOCH + timedelta(seconds=15)  # past the boundary before the next burst
        yield ["c"]

    to_rotating_file(name, schedule=schedule, now=now)(batches())

    assert (tmp_path / "app.0.log").read_text(encoding="utf-8") == "a\nb\n"
    assert (tmp_path / "app.1.log").read_text(encoding="utf-8") == "c\n"


def test_to_rotating_file_requires_at_least_one_rotation_policy() -> None:
    # `name` is never called: the guard rejects the all-None policy before opening any file.
    with pytest.raises(ValueError, match=r"^to_rotating_file needs at least one rotation policy"):
        to_rotating_file(lambda index, when: Path(f"app.{index}.log"))


def test_at_times_returns_the_next_time_later_today() -> None:
    schedule = at_times(time(0, 0), time(12, 0))

    assert schedule(datetime(2026, 1, 1, 9, 0, tzinfo=UTC)) == datetime(2026, 1, 1, 12, 0, tzinfo=UTC)


def test_at_times_wraps_to_tomorrow_once_all_of_todays_times_have_passed() -> None:
    schedule = at_times(time(0, 0), time(12, 0))

    assert schedule(datetime(2026, 1, 1, 18, 0, tzinfo=UTC)) == datetime(2026, 1, 2, 0, 0, tzinfo=UTC)


def test_at_times_is_strictly_after_so_a_boundary_just_hit_advances() -> None:
    schedule = at_times(time(12, 0))

    assert schedule(datetime(2026, 1, 1, 12, 0, tzinfo=UTC)) == datetime(2026, 1, 2, 12, 0, tzinfo=UTC)


def test_at_times_requires_at_least_one_time() -> None:
    with pytest.raises(ValueError, match=r"^at_times requires at least one time-of-day$"):
        at_times()


# Fixed offsets, not zoneinfo: they exercise the `tz` parameter and the `astimezone` conversion
# across the full offset range without DST's nonexistent/ambiguous wall times (a separate concern
# for a rotation schedule). The moment stays in UTC so the conversion into `tz` actually does work.
fixed_offset_timezones = st.integers(min_value=-14 * 60, max_value=14 * 60).map(
    lambda minutes: timezone(timedelta(minutes=minutes))
)


@given(
    times=st.lists(st.times(), min_size=1, max_size=4, unique=True),
    tz=fixed_offset_timezones,
    # hypothesis requires naive min/max bounds when a `timezones` strategy is given; bounded to a
    # realistic range so `+ timedelta(days=1)` cannot overflow near `datetime.max`.
    moment=st.datetimes(
        min_value=datetime(2000, 1, 1),  # noqa: DTZ001
        max_value=datetime(2100, 1, 1),  # noqa: DTZ001
        timezones=st.just(UTC),
    ),
)
def test_at_times_yields_the_soonest_scheduled_boundary_strictly_after(
    times: list[time],
    tz: timezone,
    moment: datetime,
) -> None:
    boundary = at_times(*times, tz=tz)(moment)
    local = moment.astimezone(tz)

    # Strictly after, and within a day, since the times recur daily.
    assert moment < boundary <= moment + timedelta(days=1)
    # The boundary lands on one of the scheduled wall-clock times, read in `tz`.
    assert boundary.astimezone(tz).time() in set(times)
    # Nothing scheduled is skipped: no boundary falls strictly between `moment` and the result.
    for scheduled in times:
        for day in (local.date(), local.date() + timedelta(days=1)):
            candidate = datetime.combine(day, scheduled, tzinfo=tz)
            assert not (moment < candidate < boundary)


def test_now_utc_returns_a_utc_aware_datetime() -> None:
    assert now_utc().tzinfo == UTC


def test_to_rotating_file_keeps_an_oversized_first_line_in_the_initial_empty_file(tmp_path: Path) -> None:
    def name(index: int, when: datetime) -> Path:
        return tmp_path / f"app.{index}.log"

    # "aaaaa\n" is 6 bytes, over the 3-byte cap, but the initial file is empty: rotating would only
    # produce an empty file and still overshoot, so the first line stays put.
    to_rotating_file(name, max_bytes=3)(iter([["aaaaa"]]))

    assert (tmp_path / "app.0.log").read_text(encoding="utf-8") == "aaaaa\n"
    assert not (tmp_path / "app.1.log").exists()


def test_to_rotating_file_counts_bytes_already_present_in_the_initial_file(tmp_path: Path) -> None:
    def name(index: int, when: datetime) -> Path:
        return tmp_path / f"app.{index}.log"

    (tmp_path / "app.0.log").write_bytes(b"x")  # one pre-existing byte the size check must include
    # "yy\n" is 3 bytes; with the pre-existing byte that is 4, over the 3-byte cap, so it rotates.
    to_rotating_file(name, max_bytes=3)(iter([["yy"]]))

    assert (tmp_path / "app.0.log").read_bytes() == b"x"
    assert (tmp_path / "app.1.log").read_text(encoding="utf-8") == "yy\n"


def test_to_rotating_file_counts_each_rotated_file_from_zero_across_several_size_rotations(tmp_path: Path) -> None:
    def name(index: int, when: datetime) -> Path:
        return tmp_path / f"app.{index}.log"

    # Each line is 6 bytes; with a 12-byte cap exactly two land per file, so six lines fill three files.
    lines = ["aaaaa", "bbbbb", "ccccc", "ddddd", "eeeee", "fffff"]
    to_rotating_file(name, max_bytes=12)(iter([lines]))

    assert (tmp_path / "app.0.log").read_text(encoding="utf-8") == "aaaaa\nbbbbb\n"
    assert (tmp_path / "app.1.log").read_text(encoding="utf-8") == "ccccc\nddddd\n"
    assert (tmp_path / "app.2.log").read_text(encoding="utf-8") == "eeeee\nfffff\n"


def test_to_rotating_file_counts_bytes_already_present_in_a_rotated_into_file(tmp_path: Path) -> None:
    def name(index: int, when: datetime) -> Path:
        return tmp_path / f"app.{index}.log"

    (tmp_path / "app.1.log").write_bytes(b"AAAAAAAAAA")  # ten bytes already in the rotation target
    # First rotation lands on app.1; its ten pre-existing bytes plus "ccccc\n" push past the 12-byte cap,
    # so the next line rotates again to app.2 rather than being appended to app.1.
    to_rotating_file(name, max_bytes=12)(iter([["aaaaa", "bbbbb", "ccccc", "ddddd"]]))

    assert (tmp_path / "app.2.log").read_text(encoding="utf-8") == "ddddd\n"


def test_to_rotating_file_rotates_when_the_age_exactly_reaches_the_limit(tmp_path: Path) -> None:
    def name(index: int, when: datetime) -> Path:
        return tmp_path / f"app.{index}.log"

    clock = EPOCH

    def now() -> datetime:
        return clock

    def batches() -> Iterator[list[str]]:
        nonlocal clock
        yield ["fresh"]
        clock = EPOCH + timedelta(seconds=10)  # exactly the age limit, not past it
        yield ["aged"]

    to_rotating_file(name, max_age=timedelta(seconds=10), now=now)(batches())

    assert (tmp_path / "app.0.log").read_text(encoding="utf-8") == "fresh\n"
    assert (tmp_path / "app.1.log").read_text(encoding="utf-8") == "aged\n"


def test_to_rotating_file_rotates_at_each_scheduled_boundary_passing_the_open_time_to_name(tmp_path: Path) -> None:
    calls: list[tuple[int, datetime]] = []

    def name(index: int, when: datetime) -> Path:
        calls.append((index, when))
        return tmp_path / f"app.{index}.log"

    clock = EPOCH

    def now() -> datetime:
        return clock

    def schedule(after: datetime) -> datetime:
        return after + timedelta(seconds=10)  # a fresh boundary ten seconds after each file opens

    def batches() -> Iterator[list[str]]:
        nonlocal clock
        yield ["a"]
        clock = EPOCH + timedelta(seconds=10)  # exactly the first boundary
        yield ["b"]
        clock = EPOCH + timedelta(seconds=20)  # exactly the second boundary, recomputed after the first
        yield ["c"]

    to_rotating_file(name, schedule=schedule, now=now)(batches())

    assert (tmp_path / "app.0.log").read_text(encoding="utf-8") == "a\n"
    assert (tmp_path / "app.1.log").read_text(encoding="utf-8") == "b\n"
    assert (tmp_path / "app.2.log").read_text(encoding="utf-8") == "c\n"
    assert calls == [
        (0, EPOCH),
        (1, EPOCH + timedelta(seconds=10)),
        (2, EPOCH + timedelta(seconds=20)),
    ]


async def test_capture_renders_and_writes_records_to_a_file_off_thread(tmp_path: Path) -> None:
    logger = logging.getLogger("test.sinks.file")

    def name(index: int, when: datetime) -> Path:
        return tmp_path / f"app.{index}.log"

    writer = to_rotating_file(name, max_bytes=1_000_000)
    async with (
        offload(writer) as sink,
        capture(compose(from_selector(at_least(Level.WARNING)), compose(from_map(render), sink)), logger=logger),
    ):
        logger.info("informational, dropped")
        logger.warning("first warning")
        logger.error("an error")

    assert (tmp_path / "app.0.log").read_text(encoding="utf-8") == "first warning\nan error\n"
