from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import replace
from datetime import UTC
from datetime import datetime
from enum import IntEnum
from traceback import TracebackException
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


# The attributes stdlib's LogRecord factory sets. Anything on a record outside
# this set arrived via `extra=` (or was set by a filter) and is structured data,
# so it becomes a Record field. The names here split two ways: the caller-supplied
# content of the log call (`msg`/`args`, `levelno`, `name`, `exc_info`, `created`)
# is lifted into typed fields, and the rest is factory-synthesized ambient metadata
# (source location `pathname`/`lineno`/`funcName`; `thread`/`process`/`taskName`)
# that is deliberately dropped: it is gated by module flags (`logThreads`,
# `logProcesses`, `logAsyncioTasks`) and a swappable factory, so it may be `None` or
# absent, and it exists for stdlib's formatter `%(...)s` placeholders rather than as
# a stable per-event contract. A caller who wants some of it lifts it into `fields`
# with a custom `parse=` on `capture`.
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
    mutating this one. `fields` is the record's structured data, exposed read-only:
    *seeded* from the log call's `extra=` at the parse edge, then *grown* by
    enrichment (`add_fields`, a `bind` merged by `merge_context`, any `with_fields`).
    It is named for what it holds, not for `extra=` alone, because those later
    sources write here too. `exception` is a `TracebackException` captured at the
    ingestion edge (`None` when the event carried none): a *structured* value,
    not a rendered string, so how the traceback is formatted stays a downstream
    boundary the app owns (`"".join(exc.format())` for the stdlib text, or walk
    `exc.stack` for JSON frames), the same way rendering the record itself is.
    Extracting it drops all references to the live frames, so the record stays a
    self-contained value with no traceback pinned to it.
    """

    timestamp: datetime
    level: int
    logger: str
    message: str
    exception: TracebackException | None
    fields: Mapping[str, object]

    @property
    def level_name(self) -> str:
        return logging.getLevelName(self.level)

    def with_fields(self, **fields: object) -> Record:
        """Return a copy of this record with `fields` merged onto its own."""
        return replace(self, fields=MappingProxyType({**self.fields, **fields}))


def extract_exception(log_record: logging.LogRecord) -> TracebackException | None:
    """
    Capture a `LogRecord`'s exception as a structured value, or `None` when it carries none.

    A live traceback is a *place*, not a value: it pins its frames and their
    locals in memory, is mutable, and cannot ride the queue to another thread
    safely (stdlib's own `QueueHandler` flattens it to a string for exactly this
    reason). So the exception is turned into a value at the ingestion edge, while
    it is still live, the same way `getMessage()` resolves the message. Unlike the
    message, which has one sensible rendering, a traceback has many (locals,
    chaining, style), so this keeps the *structure* rather than pre-rendering:
    `TracebackException.from_exception` extracts frame summaries and drops all
    references to the live frames, leaving a value the app can format however it
    likes downstream (`format` for stdlib text, `stack` for JSON frames).
    """
    if log_record.exc_info:
        _, exc_value, _ = log_record.exc_info
        if exc_value is not None:
            return TracebackException.from_exception(exc_value)
    return None


def parse_record(log_record: logging.LogRecord) -> Record:
    """
    Parse a mutable stdlib `LogRecord` into an immutable `Record` value.

    The ingestion boundary: this is where a pushed `LogRecord`, from the app's
    own calls or any third-party library's, becomes the typed value the rest of
    the pipeline works with. It is a pure function (`LogRecord -> Record`), so the
    whole translation is testable without touching the logging machinery. The
    structured `fields` are every attribute on the record outside the standard
    envelope (`RESERVED`), which is exactly what `logging`'s `extra=` merges in;
    the message is rendered and any exception is captured as a structured value
    here (see `extract_exception`) so nothing live is carried past this point.
    """
    fields = {key: value for key, value in log_record.__dict__.items() if key not in RESERVED}
    return Record(
        timestamp=datetime.fromtimestamp(log_record.created, tz=UTC),
        level=log_record.levelno,
        logger=log_record.name,
        message=log_record.getMessage(),
        exception=extract_exception(log_record),
        fields=MappingProxyType(fields),
    )
