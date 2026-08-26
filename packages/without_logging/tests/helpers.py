from __future__ import annotations

import logging
from datetime import UTC
from datetime import datetime
from traceback import TracebackException

from without_logging import Record
from without_streams import Sink
from without_streams import from_sink


def sink_into(collected: list[Record]) -> Sink[Record]:
    """A `Sink` that appends every record it is handed to `collected`."""

    async def append(record: Record) -> None:
        collected.append(record)

    return from_sink(append)


def a_record(
    *,
    message: str = "charge accepted",
    level: int = logging.INFO,
    logger: str = "svc.billing",
    exception: TracebackException | None = None,
    **fields: object,
) -> Record:
    """
    One already-parsed `Record`, with everything a caller does not name fixed.

    The timestamp is a constant rather than the current time so a renderer's output is
    the same string on every run.
    """
    return Record(
        timestamp=datetime(2026, 7, 5, 14, 56, 9, tzinfo=UTC),
        level=level,
        logger=logger,
        message=message,
        exception=exception,
        fields=fields,
    )
