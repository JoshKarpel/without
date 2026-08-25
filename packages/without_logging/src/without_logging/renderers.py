from __future__ import annotations

import json
from collections.abc import Callable
from datetime import datetime
from traceback import TracebackException

from without_streams.interfaces import Processor
from without_streams.interfaces import from_map

from without_logging.record import Record


def exception_to_dict(exception: TracebackException) -> dict[str, object]:
    """
    Convert a captured `TracebackException` into a JSON-serializable dict: structured traceback.

    Each frame becomes `{file, line, function, code}` (from the extracted
    `FrameSummary` values, no live frames), and a chained exception is nested under
    `cause`, following Python's own rule: an explicit `raise ... from` (`__cause__`)
    takes precedence, else the implicit `__context__` unless it was suppressed. This
    is the read half `render_json` uses, exposed so a custom JSON renderer can reuse
    it.
    """
    cause: dict[str, object] | None = None
    if exception.__cause__ is not None:
        cause = exception_to_dict(exception.__cause__)
    elif exception.__context__ is not None and not exception.__suppress_context__:
        cause = exception_to_dict(exception.__context__)
    result: dict[str, object] = {
        "type": exception.exc_type_str,
        "message": "".join(exception.format_exception_only()).strip(),
        "frames": [
            {"file": frame.filename, "line": frame.lineno, "function": frame.name, "code": frame.line}
            for frame in exception.stack
        ],
    }
    if cause is not None:
        result["cause"] = cause
    return result


def exception_to_text(exception: TracebackException) -> str:
    """
    Render a captured `TracebackException` to its full traceback text.

    stdlib's own multi-line rendering (`format`), chained cause/context and all,
    as a single string. The flat-string alternative to the structured
    `exception_to_dict` when encoding a record's exception, and what
    `render_console` uses.
    """
    return "".join(exception.format()).rstrip("\n")


def iso_timestamp(when: datetime) -> str:
    """The default timestamp format for the renderers: ISO 8601 (`datetime.isoformat`)."""
    return when.isoformat()


def render_json(
    *,
    timestamp: Callable[[datetime], object] = iso_timestamp,
    exception: Callable[[TracebackException], object] = exception_to_dict,
) -> Processor[Record, str]:
    """
    A `Record -> str` renderer emitting one JSON object per record: structured logging.

    The envelope (`timestamp` ISO-8601, `level`, `logger`, `message`) and every
    field flat at the top level, so a log aggregator can index them directly; the
    envelope wins a name clash, so a field cannot shadow `level` or `message`.

    `timestamp` chooses how the time is encoded (default `iso_timestamp`; pass, say,
    a `lambda when: when.timestamp()` for an epoch number). `exception` chooses how a
    traceback is encoded: `exception_to_dict` (default, structured frames, stays
    queryable) or `exception_to_text` (the flat traceback string), or any
    `TracebackException -> <json value>` of your own. A
    non-serializable *field* value is coerced with `str` (`default=str`): the log
    JSON is machine-consumed, so a clean indexable value beats a Python `repr`, and
    `str` still falls back to the `repr` form for an object without its own
    `__str__` (`render_console` uses `repr`, matching its human-scanning medium). A
    stray value therefore never tears down the log pipeline.

    Optional and opt-in: the core ships no mandatory formatter (encoding is the app's
    boundary), so compose this before a string sink yourself, e.g.
    `compose(render_json(), offload(to_stream(sys.stdout)))`.
    """

    async def render(record: Record) -> str:
        payload: dict[str, object] = {
            "timestamp": timestamp(record.timestamp),
            "level": record.level_name,
            "logger": record.logger,
            "message": record.message,
        }
        for key, value in record.fields.items():
            payload.setdefault(key, value)
        if record.exception is not None:
            payload["exception"] = exception(record.exception)
        return json.dumps(payload, default=str)

    return from_map(render)


def render_console(
    *,
    timestamp: Callable[[datetime], str] = iso_timestamp,
) -> Processor[Record, str]:
    """
    A `Record -> str` renderer emitting one human-readable line per record.

    `TIMESTAMP LEVEL logger "message" {key=value, ...}`: the message is quoted and
    the fields grouped in braces (each value `repr`'d), so the free-text message and
    the structured fields never blur into each other or the leading metadata. The
    message is quoted with `json.dumps` and each field value is `repr`'d, so a control
    character in either (a newline from a decoded request path, say) is escaped rather
    than forging a new log line. The braces are omitted when there are no
    fields. An exception is appended as its full traceback text (`exception_to_text`,
    cause chain and all), indented, on following lines. `timestamp` chooses the time
    format (default `iso_timestamp`). No coloring: wrap the line, or write your own
    `from_map(Record -> str)`, if you want ANSI.

    Optional and opt-in, like `render_json`: `compose(render_console(),
    offload(to_stream(sys.stderr)))`.
    """

    async def render(record: Record) -> str:
        quoted = json.dumps(record.message, ensure_ascii=False)  # pragma: no mutate - None acts as False here
        parts = [
            timestamp(record.timestamp),
            record.level_name,
            record.logger,
            quoted,
        ]
        if record.fields:
            fields = ", ".join(f"{key}={value!r}" for key, value in record.fields.items())
            parts.append("{" + fields + "}")
        summary = " ".join(parts)
        if record.exception is None:
            return summary
        body = "\n".join(f"  {line}" if line else "" for line in exception_to_text(record.exception).splitlines())
        return f"{summary}\n{body}"

    return from_map(render)
