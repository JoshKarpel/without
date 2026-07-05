import asyncio
import logging
from datetime import UTC
from datetime import datetime

from without import Sink
from without import compose
from without import from_selector
from without import from_sink
from without import tee
from without_logging import Level
from without_logging import Record
from without_logging import add_fields
from without_logging import at_least
from without_logging import capture
from without_logging import parse_record
from without_logging.capture import CaptureHandler


def sink_into(collected: list[Record]) -> Sink[Record]:
    async def append(record: Record) -> None:
        collected.append(record)

    return from_sink(append)


def a_record(message: str) -> Record:
    return Record(
        timestamp=datetime.fromtimestamp(1_700_000_000.0, tz=UTC),
        level=logging.INFO,
        logger="svc.test",
        message=message,
        fields={},
    )


async def test_capture_delivers_logged_records_to_the_sink() -> None:
    logger = logging.getLogger("test.capture.delivery")
    collected: list[Record] = []

    async with capture(sink_into(collected), logger=logger, level=logging.INFO):
        logger.info("hello %s", "world")
        logger.warning("careful", extra={"code": 7})

    assert [r.message for r in collected] == ["hello world", "careful"]
    assert collected[1].fields == {"code": 7}


async def test_capture_fans_out_to_several_sinks_sharing_a_prefix() -> None:
    logger = logging.getLogger("test.capture.tee")
    everything: list[Record] = []
    warnings: list[Record] = []

    pipeline = compose(
        add_fields(service="api"),
        tee(
            sink_into(everything),
            compose(from_selector(at_least(Level.WARNING)), sink_into(warnings)),
        ),
    )

    async with capture(pipeline, logger=logger, level=logging.DEBUG):
        logger.debug("starting")
        logger.info("handled")
        logger.warning("slow")
        logger.error("boom")

    assert [r.message for r in everything] == ["starting", "handled", "slow", "boom"]
    assert [r.message for r in warnings] == ["slow", "boom"]
    # the shared prefix ran once, so both branches see the enriched records
    assert all(r.fields["service"] == "api" for r in everything)
    assert all(r.fields["service"] == "api" for r in warnings)


async def test_capture_restores_the_logger_level_and_detaches_on_exit() -> None:
    logger = logging.getLogger("test.capture.restore")
    logger.setLevel(logging.ERROR)
    collected: list[Record] = []

    async with capture(sink_into(collected), logger=logger, level=logging.DEBUG) as handler:
        assert logger.level == logging.DEBUG
        assert handler in logger.handlers

    assert logger.level == logging.ERROR
    assert handler not in logger.handlers


async def test_capture_defaults_to_the_root_logger() -> None:
    collected: list[Record] = []

    async with capture(sink_into(collected), level=logging.CRITICAL):
        logging.getLogger("some.deep.child").critical("boom-unique-42")

    assert any(r.message == "boom-unique-42" for r in collected)


async def test_capture_accepts_an_unbounded_queue_and_never_drops() -> None:
    logger = logging.getLogger("test.capture.unbounded")
    collected: list[Record] = []

    async with capture(sink_into(collected), logger=logger, capacity=None) as handler:
        logger.info("no bound here")

    assert [r.message for r in collected] == ["no bound here"]
    assert handler.dropped == 0


async def test_offer_counts_a_record_dropped_when_the_queue_is_full() -> None:
    loop = asyncio.get_running_loop()
    queue: asyncio.Queue[Record] = asyncio.Queue(1)
    handler = CaptureHandler(loop, queue, parse_record)
    retained = a_record("retained")
    queue.put_nowait(retained)

    handler._offer(a_record("overflow"))

    assert handler.dropped == 1
    assert queue.get_nowait() is retained


async def test_offer_counts_a_record_dropped_after_the_queue_is_shut_down() -> None:
    loop = asyncio.get_running_loop()
    queue: asyncio.Queue[Record] = asyncio.Queue(4)
    handler = CaptureHandler(loop, queue, parse_record)
    queue.shutdown()

    handler._offer(a_record("too late"))

    assert handler.dropped == 1
