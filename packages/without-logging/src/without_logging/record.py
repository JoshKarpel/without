from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import replace
from datetime import UTC
from datetime import datetime
from enum import IntEnum
from types import MappingProxyType


class Level(IntEnum):
    """
    The five standard severities as an ordered value, for writing filters.

    Members are the stdlib numeric levels, so a `Record.level` (a plain `int`)
    can be compared straight against them: `record.level >= Level.WARNING`.
    Keeping `Record.level` an `int` rather than this enum is deliberate: a
    third-party library that logs at a non-standard numeric level is carried
    through as that number, not rejected, so the enum is a vocabulary for
    predicates rather than a closed set the record must belong to.
    """

    DEBUG = logging.DEBUG
    INFO = logging.INFO
    WARNING = logging.WARNING
    ERROR = logging.ERROR
    CRITICAL = logging.CRITICAL


# The attributes stdlib sets on every LogRecord. Anything on a record outside
# this set arrived via `extra=` (or was set by a filter) and is structured data,
# so it becomes a Record field; everything here is the standard envelope we lift
# into typed fields or drop.
RESERVED: frozenset[str] = frozenset(
    {
        "name",
        "msg",
        "args",
        "levelname",
        "levelno",
        "pathname",
        "filename",
        "module",
        "exc_info",
        "exc_text",
        "stack_info",
        "lineno",
        "funcName",
        "created",
        "msecs",
        "relativeCreated",
        "thread",
        "threadName",
        "processName",
        "process",
        "taskName",
        "message",
        "asctime",
    }
)


@dataclass(frozen=True, slots=True)
class Record:
    """
    One log event as an immutable value, parsed from a stdlib `LogRecord`.

    Where a `LogRecord` is a mutable bag that formatters and filters rewrite in
    place, a `Record` is a value: it always means the same thing, so it can be
    filtered, enriched, fanned out, and sunk without any stage disturbing
    another's copy. Enrichment returns a *new* record (`with_fields`) rather than
    mutating this one. `fields` holds the structured data logged via `extra=`,
    exposed read-only.
    """

    timestamp: datetime
    level: int
    logger: str
    message: str
    fields: Mapping[str, object]

    @property
    def level_name(self) -> str:
        return logging.getLevelName(self.level)

    def with_fields(self, **fields: object) -> Record:
        """Return a copy of this record with `fields` merged onto its own."""
        return replace(self, fields=MappingProxyType({**self.fields, **fields}))


def parse_record(log_record: logging.LogRecord) -> Record:
    """
    Parse a mutable stdlib `LogRecord` into an immutable `Record` value.

    The ingestion boundary: this is where a pushed `LogRecord`, from the app's
    own calls or any third-party library's, becomes the typed value the rest of
    the pipeline works with. It is a pure function (`LogRecord -> Record`), so the
    whole translation is testable without touching the logging machinery. The
    structured `fields` are every attribute on the record outside the standard
    envelope (`RESERVED`), which is exactly what `logging`'s `extra=` merges in.
    """
    fields = {key: value for key, value in log_record.__dict__.items() if key not in RESERVED}
    return Record(
        timestamp=datetime.fromtimestamp(log_record.created, tz=UTC),
        level=log_record.levelno,
        logger=log_record.name,
        message=log_record.getMessage(),
        fields=MappingProxyType(fields),
    )
