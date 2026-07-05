import logging
from datetime import UTC
from datetime import datetime

from without_logging import Level
from without_logging import Record
from without_logging import parse_record


def make_log_record(**overrides: object) -> logging.LogRecord:
    record = logging.LogRecord(
        name="svc.auth",
        level=logging.WARNING,
        pathname="/app/auth.py",
        lineno=42,
        msg="login failed for %s",
        args=("alice",),
        exc_info=None,
        func="authenticate",
    )
    record.created = 1_700_000_000.0
    for key, value in overrides.items():
        setattr(record, key, value)
    return record


def test_parse_renders_the_message_with_its_arguments() -> None:
    assert parse_record(make_log_record()).message == "login failed for alice"


def test_parse_carries_the_numeric_level_and_logger_name() -> None:
    record = parse_record(make_log_record())

    assert record.level == logging.WARNING
    assert record.logger == "svc.auth"


def test_parse_produces_a_utc_timestamp_from_the_record_creation_time() -> None:
    record = parse_record(make_log_record())

    assert record.timestamp == datetime.fromtimestamp(1_700_000_000.0, tz=UTC)
    assert record.timestamp.tzinfo == UTC


def test_parse_lifts_extra_attributes_into_structured_fields() -> None:
    record = parse_record(make_log_record(request_id="r-123", attempt=3))

    assert record.fields == {"request_id": "r-123", "attempt": 3}


def test_parse_excludes_the_standard_envelope_from_fields() -> None:
    record = parse_record(make_log_record(request_id="r-123"))

    assert set(record.fields) == {"request_id"}


def test_parse_carries_a_non_standard_numeric_level_through_unchanged() -> None:
    record = parse_record(make_log_record(levelno=25))

    assert record.level == 25
    assert record.level_name == "Level 25"


def test_level_name_reads_the_standard_name_for_a_standard_level() -> None:
    assert parse_record(make_log_record()).level_name == "WARNING"


def test_with_fields_returns_a_new_record_and_leaves_the_original_untouched() -> None:
    original = parse_record(make_log_record(request_id="r-123"))

    enriched = original.with_fields(region="eu-west", attempt=2)

    assert enriched.fields == {"request_id": "r-123", "region": "eu-west", "attempt": 2}
    assert original.fields == {"request_id": "r-123"}


def test_level_members_equal_the_stdlib_numeric_levels() -> None:
    assert Level.WARNING == logging.WARNING
    assert Level.DEBUG < Level.ERROR


def test_a_record_at_a_level_compares_against_the_level_enum() -> None:
    record = Record(
        timestamp=datetime.fromtimestamp(1_700_000_000.0, tz=UTC),
        level=logging.ERROR,
        logger="svc.billing",
        message="charge declined",
        fields={},
    )

    assert record.level >= Level.WARNING
    assert not record.level >= Level.CRITICAL
