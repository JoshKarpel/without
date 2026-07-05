import logging
from collections.abc import Iterator
from pathlib import Path

import pytest
from without import compose
from without import from_map
from without import from_selector
from without import stream_from_iterable
from without_logging import Level
from without_logging import Record
from without_logging import at_least
from without_logging import capture
from without_logging import offload
from without_logging import write_lines


async def render(record: Record) -> str:
    return record.message


async def test_offload_delivers_every_item_to_the_worker_in_order() -> None:
    received: list[int] = []

    def work(items: Iterator[int]) -> None:
        received.extend(items)

    async with offload(work) as sink:
        await sink(stream_from_iterable([7, 3, 9, 5]))

    assert received == [7, 3, 9, 5]


async def test_offload_surfaces_a_worker_failure_when_the_block_exits() -> None:
    def work(items: Iterator[int]) -> None:
        raise RuntimeError("worker could not open its resource")

    with pytest.raises(RuntimeError, match="worker could not open its resource"):
        async with offload(work) as sink:
            await sink(stream_from_iterable([1]))


async def test_write_lines_appends_each_string_as_a_newline_delimited_line(tmp_path: Path) -> None:
    path = tmp_path / "app.log"

    async with offload(write_lines(path)) as sink:
        await sink(stream_from_iterable(["first line", "second line"]))

    assert path.read_text(encoding="utf-8") == "first line\nsecond line\n"


async def test_capture_renders_and_writes_filtered_records_to_a_file_off_thread(tmp_path: Path) -> None:
    path = tmp_path / "warnings.log"
    logger = logging.getLogger("test.sinks.file")

    async with offload(write_lines(path)) as writer:
        lines = compose(from_map(render), writer)  # Record -> str -> file
        pipeline = compose(from_selector(at_least(Level.WARNING)), lines)
        async with capture(pipeline, logger=logger):
            logger.info("informational, dropped")
            logger.warning("first warning")
            logger.error("an error")

    assert path.read_text(encoding="utf-8") == "first warning\nan error\n"
