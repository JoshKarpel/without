import asyncio
import logging

from without_logging import Record
from without_logging import bind
from without_logging import capture
from without_logging import merge_context
from without_logging import parse_record

from .helpers import sink_into


def make_log_record(**overrides: object) -> logging.LogRecord:
    record = logging.LogRecord(
        name="svc.web",
        level=logging.INFO,
        pathname="/app/web.py",
        lineno=17,
        msg="handling %s",
        args=("GET /orders",),
        exc_info=None,
        func="handle",
    )
    for key, value in overrides.items():
        setattr(record, key, value)
    return record


def test_merge_context_stamps_the_bound_fields_onto_the_record() -> None:
    with bind(request_id="req-42"):
        record = merge_context(parse_record(make_log_record()))

    assert record.fields == {"request_id": "req-42"}


def test_merge_context_leaves_fields_empty_outside_any_bind() -> None:
    record = merge_context(parse_record(make_log_record()))

    assert record.fields == {}


def test_a_field_the_call_set_explicitly_wins_over_a_bound_field() -> None:
    with bind(request_id="bound"):
        record = merge_context(parse_record(make_log_record(request_id="per-call")))

    assert record.fields == {"request_id": "per-call"}


def test_nested_binds_accumulate_and_the_inner_scope_unwinds_on_exit() -> None:
    with bind(request_id="req-9"):
        outer = merge_context(parse_record(make_log_record()))
        with bind(user="alice"):
            inner = merge_context(parse_record(make_log_record()))
        after_inner = merge_context(parse_record(make_log_record()))

    assert outer.fields == {"request_id": "req-9"}
    assert inner.fields == {"request_id": "req-9", "user": "alice"}
    assert after_inner.fields == {"request_id": "req-9"}


def test_merge_context_composes_on_top_of_a_custom_base_parser() -> None:
    def parse_with_source(log_record: logging.LogRecord) -> Record:
        return merge_context(parse_record(log_record).with_fields(line=log_record.lineno))

    with bind(request_id="req-3"):
        record = parse_with_source(make_log_record())

    assert record.fields == {"line": 17, "request_id": "req-3"}


async def test_binds_on_concurrent_tasks_do_not_leak_across_records() -> None:
    logger = logging.getLogger("test.context.isolation")
    collected: list[Record] = []

    async def one_request(request_id: str) -> None:
        with bind(request_id=request_id):
            await asyncio.sleep(0)  # yield so the tasks interleave between bind and log
            logger.info(request_id)

    async with capture(sink_into(collected), logger=logger, level=logging.INFO):  # default parse
        await asyncio.gather(*(one_request(f"req-{n}") for n in range(20)))

    assert len(collected) == 20
    assert all(record.fields["request_id"] == record.message for record in collected)
