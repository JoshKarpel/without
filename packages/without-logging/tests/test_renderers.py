import json
import logging
from datetime import UTC
from datetime import datetime
from decimal import Decimal
from traceback import TracebackException

import pytest
from without import Processor
from without import collect
from without import stream_from_iterable
from without_logging import Record
from without_logging import exception_to_text
from without_logging import render_console
from without_logging import render_json


def a_record(
    *,
    message: str = "charge accepted",
    level: int = logging.INFO,
    logger: str = "svc.billing",
    exception: TracebackException | None = None,
    **fields: object,
) -> Record:
    return Record(
        timestamp=datetime(2026, 7, 5, 14, 56, 9, tzinfo=UTC),
        level=level,
        logger=logger,
        message=message,
        exception=exception,
        fields=fields,
    )


def a_chained_traceback() -> TracebackException:
    try:
        try:
            raise KeyError("region")
        except KeyError as missing:
            raise ValueError("token expired") from missing
    except ValueError as exc:
        return TracebackException.from_exception(exc)


def a_context_chained_traceback() -> TracebackException:
    try:
        try:
            raise KeyError("region")
        except KeyError:
            raise ValueError("token expired")  # noqa: B904 - no `from`: the implicit __context__ is the point
    except ValueError as exc:
        return TracebackException.from_exception(exc)


async def rendered(renderer: Processor[Record, str], record: Record) -> str:
    (line,) = await collect(renderer(stream_from_iterable([record])))
    return line


async def test_render_json_carries_the_envelope_and_message() -> None:
    payload = json.loads(await rendered(render_json(), a_record(level=logging.WARNING)))

    assert payload["timestamp"] == "2026-07-05T14:56:09+00:00"
    assert payload["level"] == "WARNING"
    assert payload["logger"] == "svc.billing"
    assert payload["message"] == "charge accepted"


async def test_render_json_puts_extra_fields_flat_at_the_top_level() -> None:
    payload = json.loads(await rendered(render_json(), a_record(order_id="ord-77", amount=4200)))

    assert payload["order_id"] == "ord-77"
    assert payload["amount"] == 4200


async def test_render_json_lets_the_envelope_win_a_name_clash_with_a_field() -> None:
    record = Record(
        timestamp=datetime(2026, 7, 5, 14, 56, 9, tzinfo=UTC),
        level=logging.ERROR,
        logger="svc.billing",
        message="charge accepted",
        exception=None,
        fields={"level": "sneaky"},
    )

    payload = json.loads(await rendered(render_json(), record))

    assert payload["level"] == "ERROR"


async def test_render_json_encodes_the_exception_as_structured_frames() -> None:
    payload = json.loads(await rendered(render_json(), a_record(exception=a_chained_traceback())))

    exception = payload["exception"]
    assert exception["type"] == "ValueError"
    assert exception["message"] == "ValueError: token expired"
    assert exception["frames"][0]["function"] == "a_chained_traceback"


async def test_render_json_nests_the_chained_cause() -> None:
    payload = json.loads(await rendered(render_json(), a_record(exception=a_chained_traceback())))

    assert payload["exception"]["cause"]["type"] == "KeyError"


async def test_render_json_nests_an_implicitly_chained_context() -> None:
    payload = json.loads(await rendered(render_json(), a_record(exception=a_context_chained_traceback())))

    assert payload["exception"]["cause"]["type"] == "KeyError"


async def test_render_json_stringifies_a_non_serializable_field_value() -> None:
    payload = json.loads(await rendered(render_json(), a_record(rate=Decimal("1.5"))))

    assert payload["rate"] == "1.5"


async def test_render_json_can_emit_the_exception_as_a_single_text_string() -> None:
    renderer = render_json(exception=exception_to_text)

    payload = json.loads(await rendered(renderer, a_record(exception=a_chained_traceback())))

    assert isinstance(payload["exception"], str)
    assert payload["exception"].startswith("Traceback (most recent call last):")
    assert "ValueError: token expired" in payload["exception"]


async def test_render_json_uses_an_injected_timestamp_format() -> None:
    renderer = render_json(timestamp=lambda when: when.timestamp())

    payload = json.loads(await rendered(renderer, a_record()))

    assert payload["timestamp"] == datetime(2026, 7, 5, 14, 56, 9, tzinfo=UTC).timestamp()


async def test_render_console_line_quotes_the_message_after_the_metadata() -> None:
    line = await rendered(render_console(), a_record(level=logging.WARNING))

    assert line == '2026-07-05T14:56:09+00:00 WARNING svc.billing "charge accepted"'


@pytest.mark.security("a newline in a log message is escaped, preventing log-line forging")
async def test_render_console_escapes_a_newline_in_the_message() -> None:
    line = await rendered(render_console(), a_record(message="ok\nWARNING forged line"))

    assert "\n" not in line
    assert r"ok\nWARNING forged line" in line


async def test_render_console_groups_extra_fields_in_braces() -> None:
    line = await rendered(render_console(), a_record(order_id="ord-77", attempt=3))

    assert line.endswith("\"charge accepted\" {order_id='ord-77', attempt=3}")


async def test_render_console_omits_the_braces_when_there_are_no_fields() -> None:
    line = await rendered(render_console(), a_record())

    assert "{" not in line


async def test_render_console_appends_the_indented_traceback_when_present() -> None:
    line = await rendered(render_console(), a_record(exception=a_chained_traceback()))

    assert "\n  Traceback (most recent call last):" in line
    assert "ValueError: token expired" in line


async def test_render_console_uses_an_injected_timestamp_format() -> None:
    line = await rendered(render_console(timestamp=lambda when: when.strftime("%H:%M:%S")), a_record())

    assert line.startswith('14:56:09 INFO svc.billing "charge accepted"')
