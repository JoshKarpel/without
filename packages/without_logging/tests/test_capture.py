import asyncio
import logging
from datetime import UTC
from datetime import datetime

from pytest_mock import MockerFixture
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
        exception=None,
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


async def test_capture_carries_the_traceback_from_logger_exception() -> None:
    logger = logging.getLogger("test.capture.exception")
    collected: list[Record] = []

    async with capture(sink_into(collected), logger=logger, level=logging.INFO):
        try:
            raise RuntimeError("downstream unavailable")
        except RuntimeError:
            logger.exception("request failed")

    (record,) = collected
    assert record.message == "request failed"
    assert record.exception is not None
    assert "RuntimeError: downstream unavailable" in "".join(record.exception.format())


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


async def test_emit_from_another_thread_reaches_the_queue() -> None:
    loop = asyncio.get_running_loop()
    queue: asyncio.Queue[Record] = asyncio.Queue(4)
    handler = CaptureHandler(loop, queue, parse_record)
    log_record = logging.LogRecord(
        name="svc.test", level=logging.WARNING, pathname=__file__, lineno=7, msg="off thread", args=(), exc_info=None
    )

    await asyncio.to_thread(handler.emit, log_record)

    offered = await queue.get()
    assert offered.message == "off thread"
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


async def test_offer_counts_each_dropped_record_separately() -> None:
    loop = asyncio.get_running_loop()
    queue: asyncio.Queue[Record] = asyncio.Queue(1)
    handler = CaptureHandler(loop, queue, parse_record)
    queue.put_nowait(a_record("kept"))

    handler._offer(a_record("first overflow"))
    handler._offer(a_record("second overflow"))

    assert handler.dropped == 2


async def test_emit_on_the_loop_thread_takes_the_cheap_call_soon(mocker: MockerFixture) -> None:
    loop = asyncio.get_running_loop()
    queue: asyncio.Queue[Record] = asyncio.Queue(4)
    handler = CaptureHandler(loop, queue, parse_record)
    cheap = mocker.spy(loop, "call_soon")
    threadsafe = mocker.spy(loop, "call_soon_threadsafe")
    log_record = logging.LogRecord(
        name="svc.test", level=logging.WARNING, pathname=__file__, lineno=7, msg="on loop", args=(), exc_info=None
    )

    handler.emit(log_record)  # emit runs on the loop thread, so it must skip the cross-thread wakeup

    cheap.assert_called_once()
    threadsafe.assert_not_called()
