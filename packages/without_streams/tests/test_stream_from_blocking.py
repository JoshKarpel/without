from __future__ import annotations

import asyncio
import threading
from collections.abc import Iterator

import pytest
from without_streams import close_stream
from without_streams import collect
from without_streams import stream_from_blocking


class TestValues:
    async def test_every_value_arrives_in_order(self) -> None:
        assert await collect(stream_from_blocking(["a", "b", "c"])) == ["a", "b", "c"]

    async def test_an_empty_source_ends_immediately(self) -> None:
        assert await collect(stream_from_blocking([])) == []

    async def test_a_generator_is_driven_to_exhaustion(self) -> None:
        pulled: list[int] = []

        def source() -> Iterator[int]:
            for index in range(3):
                pulled.append(index)
                yield index

        assert await collect(stream_from_blocking(source())) == [0, 1, 2]
        assert pulled == [0, 1, 2]

    @pytest.mark.parametrize("ahead", [1, 2, 10])
    async def test_the_read_ahead_bound_does_not_change_the_values(self, ahead: int) -> None:
        assert await collect(stream_from_blocking(range(20), ahead=ahead)) == list(range(20))

    async def test_a_read_ahead_below_one_is_refused(self) -> None:
        # The bound *is* the backpressure, so an unbounded stream is a
        # `ValueError` rather than a silent default, exactly as `spool` treats it.
        with pytest.raises(ValueError, match="ahead must be at least 1"):
            await collect(stream_from_blocking(["a"], ahead=0))


class TestConcurrency:
    async def test_the_loop_keeps_running_while_the_source_blocks(self) -> None:
        release = threading.Event()
        ticks = 0

        def blocking() -> Iterator[str]:
            release.wait(timeout=5)
            yield "arrived"

        async def tick() -> None:
            nonlocal ticks
            for _ in range(3):
                await asyncio.sleep(0)
                ticks += 1
            release.set()

        # The whole point: other tasks advance while the sync iterator is parked,
        # which a plain `for` over the same source would prevent.
        values, _ = await asyncio.gather(collect(stream_from_blocking(blocking())), tick())
        assert values == ["arrived"]
        assert ticks == 3

    async def test_the_producer_runs_ahead_of_a_slow_consumer(self) -> None:
        produced: list[int] = []
        filled = threading.Event()

        def source() -> Iterator[int]:
            for index in range(5):
                produced.append(index)
                if index == 3:
                    filled.set()
                yield index

        values = stream_from_blocking(source(), ahead=3)
        try:
            assert await anext(values) == 0
            assert await asyncio.to_thread(filled.wait, 5)
            # One value delivered plus a full queue behind it: the producer kept
            # working instead of waiting to be pulled, which is the pipelining a
            # per-item handoff cannot give you.
            assert produced == [0, 1, 2, 3]
            assert [0, *[value async for value in values]] == [0, 1, 2, 3, 4]
        finally:
            await close_stream(values)

    async def test_backpressure_holds_the_producer_at_the_bound(self) -> None:
        produced: list[int] = []
        filled = threading.Event()

        def source() -> Iterator[int]:
            # Unbounded on purpose: if backpressure were broken this would run
            # away, and the assertion below is that it did not.
            index = 0
            while True:
                produced.append(index)
                if index == 2:
                    filled.set()
                yield index
                index += 1

        values = stream_from_blocking(source(), ahead=2)
        try:
            assert await anext(values) == 0
            assert await asyncio.to_thread(filled.wait, 5)
            # Exactly one delivered plus `ahead` buffered, then the worker parks.
            # Unbounded, this source would have produced all thousand by now.
            assert produced == [0, 1, 2]
        finally:
            await close_stream(values)


class TestFailure:
    async def test_an_exception_from_the_source_reaches_the_consumer(self) -> None:
        def source() -> Iterator[int]:
            yield 1
            raise RuntimeError("source broke")

        stream = stream_from_blocking(source())
        assert await anext(stream) == 1
        with pytest.raises(RuntimeError, match="source broke"):
            await anext(stream)

    async def test_values_before_a_failure_are_still_delivered(self) -> None:
        def source() -> Iterator[int]:
            yield 1
            yield 2
            raise RuntimeError("late")

        seen: list[int] = []

        async def drain() -> None:
            # The loop always ends by raising, never by running out, so it is
            # written as a `while` over `anext` rather than an `async for` whose
            # normal-exit arc could not be taken.
            values = stream_from_blocking(source(), ahead=5)
            while True:
                seen.append(await anext(values))

        with pytest.raises(RuntimeError, match="late"):
            await drain()
        assert seen == [1, 2]


class TestAbandonment:
    async def test_closing_early_releases_a_producer_parked_on_backpressure(self) -> None:
        produced: list[int] = []
        filled = threading.Event()
        stopped = threading.Event()

        def source() -> Iterator[int]:
            index = 0
            try:
                # Unbounded on purpose: only the consumer abandoning it ends this.
                while True:
                    produced.append(index)
                    if index == 1:
                        filled.set()
                    yield index
                    index += 1
            finally:
                stopped.set()

        values = stream_from_blocking(source(), ahead=1)
        try:
            assert await anext(values) == 0
            # Synchronize on the producer actually reaching its bound rather
            # than racing it: past here the worker holds no capacity and nothing
            # is left to release it, which is the state teardown has to break.
            assert await asyncio.to_thread(filled.wait, 5)
        finally:
            await close_stream(values)

        # Abandoning hands the worker the capacity it is parked on, so it wakes,
        # sees it has been abandoned, and closes its source. Without that it
        # would wait on a semaphore nobody will ever post to.
        assert await asyncio.to_thread(stopped.wait, 5)
        assert len(produced) < 10
