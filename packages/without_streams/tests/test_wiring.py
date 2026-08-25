import asyncio
from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from time import monotonic

import pytest
from without_streams import Transition
from without_streams import close_stream
from without_streams import collect
from without_streams import compose
from without_streams import from_scan
from without_streams import from_sink
from without_streams import sample
from without_streams import spool
from without_streams import stream_from_iterable
from without_streams import stream_from_queue
from without_streams import tee
from without_streams import ticks
from without_streams.testing import yield_once


async def test_compose_runs_first_then_second() -> None:
    async def double(event: int, _: None) -> Transition[None, int]:
        return Transition(state=None, output=event * 2)

    async def increment(event: int, _: None) -> Transition[None, int]:
        return Transition(state=None, output=event + 1)

    chained = compose(from_scan(None, double), from_scan(None, increment))

    assert await collect(chained(stream_from_iterable([6, 7, 8]))) == [13, 15, 17]


async def test_compose_adapts_the_join_type() -> None:
    async def measure(event: str, _: None) -> Transition[None, int]:
        return Transition(state=None, output=len(event))

    async def label(event: int, _: None) -> Transition[None, str]:
        return Transition(state=None, output=f"len={event}")

    chained = compose(from_scan(None, measure), from_scan(None, label))

    assert await collect(chained(stream_from_iterable(["ab", "cdef"]))) == ["len=2", "len=4"]


async def test_compose_prefixes_a_processor_onto_a_sink() -> None:
    async def measure(event: str, _: None) -> Transition[None, int]:
        return Transition(state=None, output=len(event))

    drained: list[int] = []

    async def collect_length(length: int) -> None:
        drained.append(length)

    lengths_into = compose(from_scan(None, measure), from_sink(collect_length))

    await lengths_into(stream_from_iterable(["ab", "cdef", "x"]))

    assert drained == [2, 4, 1]


async def test_tee_delivers_every_value_to_every_sink_in_order() -> None:
    left: list[int] = []
    right: list[int] = []

    async def push_left(value: int) -> None:
        left.append(value)

    async def push_right(value: int) -> None:
        right.append(value)

    await tee(from_sink(push_left), from_sink(push_right))(stream_from_iterable([5, 6, 7]))

    assert left == [5, 6, 7]
    assert right == [5, 6, 7]


async def test_tee_delivers_every_value_to_a_slower_branch() -> None:
    fast: list[int] = []
    slow: list[int] = []

    async def push_fast(value: int) -> None:
        fast.append(value)

    async def push_slow(value: int) -> None:
        await yield_once()  # let the fast branch pull ahead before this one records
        slow.append(value)

    await tee(from_sink(push_fast), from_sink(push_slow), buffer=4)(stream_from_iterable([8, 9, 10]))

    assert fast == [8, 9, 10]
    assert slow == [8, 9, 10]


async def test_tee_requires_at_least_one_sink() -> None:
    with pytest.raises(ValueError, match=r"^tee requires at least one sink$"):
        tee()


async def test_tee_rejects_a_nonpositive_buffer() -> None:
    async def discard(value: int) -> None: ...

    with pytest.raises(ValueError, match="at least 1"):
        tee(from_sink(discard), buffer=0)


async def test_tee_surfaces_a_sink_failure() -> None:
    class SinkFailed(Exception):
        pass

    seen: list[int] = []

    async def record(value: int) -> None:
        seen.append(value)

    async def fail(value: int) -> None:
        raise SinkFailed("branch broke")

    with pytest.raises(ExceptionGroup) as excinfo:
        await tee(from_sink(record), from_sink(fail))(stream_from_iterable([1, 2, 3]))

    assert any(isinstance(error, SinkFailed) for error in excinfo.value.exceptions)


async def test_close_stream_runs_an_abandoned_generators_cleanup() -> None:
    released = asyncio.Event()

    async def holding() -> AsyncIterator[int]:
        try:
            while True:
                yield 1
        finally:
            released.set()

    source = holding()
    assert await anext(source) == 1

    await close_stream(source)

    assert released.is_set()


async def test_close_stream_leaves_a_source_that_is_not_a_generator_alone() -> None:
    # `Stream` is `__aiter__`-only, so a source may be an object with nothing to release.
    class Counting:
        def __aiter__(self) -> Counting:
            return self

        async def __anext__(self) -> int:
            return 9

    source = Counting()

    await close_stream(source)

    assert await anext(aiter(source)) == 9


async def test_stream_from_queue_yields_pushed_values_in_order() -> None:
    queue: asyncio.Queue[int] = asyncio.Queue()
    for value in (5, 6, 7):
        queue.put_nowait(value)

    pushed = stream_from_queue(queue)

    received = [await anext(pushed) for _ in range(3)]

    assert received == [5, 6, 7]


async def test_spool_yields_every_item_from_the_source() -> None:
    assert await collect(spool(stream_from_iterable([1, 2, 3]), ahead=2)) == [1, 2, 3]


async def test_spool_accepts_an_ahead_of_one() -> None:
    assert await collect(spool(stream_from_iterable([1, 2, 3]), ahead=1)) == [1, 2, 3]


async def test_spool_rejects_a_nonpositive_ahead() -> None:
    with pytest.raises(ValueError, match="at least 1"):
        await anext(spool(stream_from_iterable(["x"]), ahead=0))


async def test_spool_lets_the_producer_run_ahead_of_the_consumer() -> None:
    all_produced = asyncio.Event()

    async def source() -> AsyncIterator[str]:
        for value in ("a", "b", "c"):
            yield value
        all_produced.set()

    spooled = spool(source(), ahead=5)

    assert await anext(spooled) == "a"
    await all_produced.wait()

    assert [item async for item in spooled] == ["b", "c"]


async def test_spool_surfaces_a_source_error_after_draining_spooled_items() -> None:
    async def source() -> AsyncIterator[str]:
        yield "ok"
        raise RuntimeError("boom")

    spooled = spool(source(), ahead=5)

    assert await anext(spooled) == "ok"
    with pytest.raises(RuntimeError, match="boom"):
        await anext(spooled)


async def test_sample_starts_at_the_first_value() -> None:
    async with sample(stream_from_iterable([11, 22, 33])) as latest:
        assert latest.current() == 11


async def test_sample_updated_returns_the_next_published_value() -> None:
    async with sample(stream_from_iterable([11, 22, 33])) as latest:
        assert await latest.updated() == 22


async def test_sample_tracks_the_latest_value() -> None:
    # The synchronous source drains in one go, so one update lands current on the last value.
    async with sample(stream_from_iterable([11, 22, 33])) as latest:
        await latest.updated()
        assert latest.current() == 33


async def test_sample_publish_skips_a_waiter_cancelled_before_it_resolves() -> None:
    # In the window between cancelling a waiter and its own deregistration, the
    # dead future is still in the set when the next publish runs: publish must skip
    # it (not crash on set_result) and still resolve the live waiter.
    queue: asyncio.Queue[int] = asyncio.Queue()
    queue.put_nowait(1)
    async with sample(stream_from_queue(queue)) as latest:
        doomed = asyncio.create_task(latest.updated())
        live = asyncio.create_task(latest.updated())
        await yield_once()  # one turn so both updated() calls register their waiters
        queue.put_nowait(2)  # queue the publish first, so the drain wakes before doomed...
        doomed.cancel()  # ...then cancel doomed: its future is now done but not yet deregistered

        assert await live == 2
        assert latest.current() == 2
        with pytest.raises(asyncio.CancelledError):
            await doomed


async def test_sample_cancels_a_pending_updated_waiter_on_exit() -> None:
    # A task still awaiting `updated` when the context closes must be cancelled,
    # not left hanging on a sample whose drain has stopped.
    queue: asyncio.Queue[int] = asyncio.Queue()
    queue.put_nowait(1)
    async with sample(stream_from_queue(queue)) as latest:
        waiting = asyncio.create_task(latest.updated())
        await yield_once()  # the task is parked awaiting the next publish

    with pytest.raises(asyncio.CancelledError):
        await waiting


async def test_updated_raises_the_source_error_to_a_pending_waiter() -> None:
    proceed = asyncio.Event()

    class SourceFailed(Exception):
        pass

    async def source() -> AsyncIterator[int]:
        yield 1
        await proceed.wait()
        raise SourceFailed("source failed")

    async def await_next_update() -> int:
        async with sample(source()) as latest:
            waiting = asyncio.create_task(latest.updated())
            await yield_once()  # the waiter registers; the drain is parked on `proceed`
            proceed.set()  # the source now raises on the next drain pull
            return await waiting  # the pending wait sees the source error, not a hang

    with pytest.raises(SourceFailed, match="source failed"):
        await await_next_update()


async def test_updated_fails_fast_after_the_source_has_already_failed() -> None:
    class SourceFailed(Exception):
        pass

    async def source() -> AsyncIterator[int]:
        yield 1
        raise SourceFailed("source failed")

    async def update_after_failure() -> int:
        async with sample(source()) as latest:
            await yield_once()  # let the drain pull again and fail, storing the error
            return await latest.updated()  # raises the stored error at once, never registering a doomed waiter

    with pytest.raises(SourceFailed, match="source failed"):
        await update_after_failure()


async def test_sample_rejects_an_empty_stream() -> None:
    with pytest.raises(ValueError, match="at least one value"):
        async with sample(stream_from_iterable([])):
            pass  # pragma: no cover - sample raises on enter, so the body never runs


async def test_ticks_fires_at_once_and_then_on_the_interval() -> None:
    # Firing immediately is what lets periodic work do its first sweep at boot rather
    # than one interval later, which for a sweep that catches up on missed deadlines is
    # the difference between starting and stalling.
    moments = [datetime(2026, 3, 14, 9, 30, tzinfo=UTC) + timedelta(seconds=n) for n in range(3)]
    handed = iter(moments)

    # The smallest positive interval, so the waiting is not what this measures: the moments
    # come from the clock it was handed, and only the *first* one proves it yields before
    # it sleeps at all.
    beating = ticks(timedelta(microseconds=1), now=lambda: next(handed))
    taken = [await anext(beating) for _ in moments]
    await beating.aclose()

    assert taken == moments, "each tick carries its own moment, so a consumer needs no clock"


@pytest.mark.parametrize("every", [timedelta(), timedelta(seconds=-1)])
async def test_ticks_refuses_an_interval_that_is_not_positive(every: timedelta) -> None:
    # There is no reading of a zero interval worth having: taken literally it is a loop
    # that yields as fast as its sink can consume, which pins a core to do housekeeping.
    # A duration read out of configuration whose setting was never set is how one arrives.
    beating = ticks(every)

    with pytest.raises(ValueError, match="every must be a positive duration"):
        await anext(beating)


async def test_ticks_waits_the_interval_between_events() -> None:
    # Two intervals for three events, because the first one does not wait. The bound is
    # deliberately loose: an event loop may fire a timer a fraction early, so asserting
    # the exact nominal floor measures the platform's timer precision rather than this,
    # and fails on a margin of microseconds. What is worth proving is that it waits at
    # all rather than yielding as fast as it is asked.
    started = monotonic()

    beating = ticks(timedelta(milliseconds=20))
    for _ in range(3):
        await anext(beating)
    await beating.aclose()

    assert monotonic() - started >= 0.03


async def test_a_sink_over_ticks_runs_once_per_event() -> None:
    # The shape the whole thing exists for: periodic work says only *what* happens, and
    # a stream says when, so the same sink runs off a clock here and off a fixed list of
    # instants in a test.
    swept: list[datetime] = []

    async def sweep(moment: datetime) -> None:
        swept.append(moment)

    moments = [datetime(2026, 3, 14, 9, 30, tzinfo=UTC), datetime(2026, 3, 14, 9, 31, tzinfo=UTC)]
    await from_sink(sweep)(stream_from_iterable(moments))

    assert swept == moments
