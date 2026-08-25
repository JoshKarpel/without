import logging
from datetime import UTC
from datetime import datetime

from without_logging import Level
from without_logging import Record
from without_logging import add_fields
from without_logging import at_least
from without_streams import collect
from without_streams import from_selector
from without_streams import stream_from_iterable


def record(level: int, message: str, **fields: object) -> Record:
    return Record(
        timestamp=datetime.fromtimestamp(1_700_000_000.0, tz=UTC),
        level=level,
        logger="svc.orders",
        message=message,
        exception=None,
        fields=fields,
    )


async def test_at_least_accepts_a_record_at_the_threshold() -> None:
    assert await at_least(Level.WARNING)(record(logging.WARNING, "at the line"))


async def test_at_least_rejects_a_record_below_the_threshold() -> None:
    assert not await at_least(Level.WARNING)(record(logging.INFO, "below the line"))


async def test_at_least_selects_records_at_or_above_the_threshold() -> None:
    inputs = stream_from_iterable(
        [
            record(logging.DEBUG, "chatter"),
            record(logging.ERROR, "boom"),
            record(logging.INFO, "fyi"),
            record(logging.CRITICAL, "meltdown"),
        ]
    )

    kept = await collect(from_selector(at_least(Level.WARNING))(inputs))

    assert [r.message for r in kept] == ["boom", "meltdown"]


async def test_add_fields_merges_static_fields_onto_every_record() -> None:
    inputs = stream_from_iterable([record(logging.INFO, "started", run="a1")])

    enriched = await collect(add_fields(service="orders", region="eu-west")(inputs))

    assert enriched[0].fields == {"run": "a1", "service": "orders", "region": "eu-west"}
