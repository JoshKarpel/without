---
title: Reintroduce the fan-out/fan-in wiring connectors
labels: [without]
---

## Summary

The fan-out/fan-in connectors `route`, `broadcast`, `distribute`, `tee`, and
`merge` are not part of `without.wiring`; they were removed as speculative surface.
Bring them back when a concrete use case needs them.

## Package(s)

`without` (core).

## Notes

**Why they were removed.** No shipped package used them; only the core's own
`test_wiring.py` imported them. They were speculative surface that carried real
queue-and-background-task complexity (bounded queues, `_Stop` sentinels,
`background_task` scoping), so they widened the narrow waist beyond what was
load-bearing. The implementations and tests are reproduced below so a
reintroduction does not have to reconstruct them.

**What they were.** `merge` (N→1 fan-in, completion order), `distribute` (one
stream across N competing workers, bounded concurrency), `tee` (fan a stream into
N independent copies), `broadcast` (`tee` then `merge` over N processors), `route`
(partition a stream by runtime type). All but `route` shared bounded-queue
backpressure; `route` failed loudly on an unmatched type.

**How to bring them back.** The trigger is a real intra-request fan-out/fan-in
need (e.g. broadcasting one input to several processors, or distributing work
across bounded workers within a connection). This also intersects the actor-model
question: the one un-clean spot in the model is the hand-rolled queue funnel
(`stream_from_queue` as a shared inbox); a **dynamic-merge** connector (a fan-in
over a *changing* set of sources, which the static `merge` did not cover) is the
primitive that would pull that spot back inside the stream model. Consider
designing dynamic-merge alongside the reintroduction rather than restoring the
static set verbatim.

## Implementation (for reference)

These lived in `without.wiring` and were re-exported from `without.__init__`. They
need these imports already present in `wiring.py`: `asyncio`,
`from collections.abc import AsyncIterator`, `from contextlib import
asynccontextmanager`, and `Processor` / `Stream` from `without.contracts`, plus
`background_task` from `without.tasks`.

```python
class _Stop:
    """Sentinel marking the end of an internal queue, distinct from any value."""


async def merge[T](*sources: Stream[T]) -> AsyncIterator[T]:
    """Interleave several sources into one stream, in completion order (fan-in).

    The N-to-1 dual of the fan-out connectors: where `distribute` and
    `broadcast` split or copy one stream across many consumers and merge the
    results back, `merge` folds many independent sources into a single stream.
    One forwarding task drains each source into a shared bounded outbox; the
    merged stream ends once every source has. The bound gives backpressure (a
    slow consumer stalls the forwarders rather than buffering an unbounded
    backlog), and the `finally` guarantees the stop sentinel is emitted even if
    a source raises, so the error surfaces here instead of hanging the consumer.
    """
    outbox: asyncio.Queue[T | _Stop] = asyncio.Queue(maxsize=len(sources) or 1)

    async def forward(source: Stream[T]) -> None:
        async for value in source:
            await outbox.put(value)

    async def run() -> None:
        try:
            async with asyncio.TaskGroup() as group:
                for source in sources:
                    group.create_task(forward(source))
        finally:
            await outbox.put(_Stop())

    async with background_task(run()):
        while not isinstance(item := await outbox.get(), _Stop):
            yield item


async def distribute[In, Out](
    source: Stream[In],
    processor: Processor[In, Out],
    workers: int,
) -> AsyncIterator[Out]:
    """Spread one stream across `workers` competing consumers, merging outputs.

    The distribute edge: each event is handled *once*, by whichever worker is
    free, not broadcast to all of them. This is the bounded-concurrency
    primitive: `workers` independent instances of `processor` (each with its
    own state) pull from a shared inbox, and their outputs merge into the single
    stream returned here, in completion order.

    Concurrency is capped at `workers` and backpressure is end-to-end: the
    queues are bounded, so a slow consumer stalls the workers, which stalls the
    feeder pulling from `source`. The source is never drained faster than the
    workers retire it.
    """
    if workers < 1:
        raise ValueError(f"workers must be at least 1, got {workers}")

    inbox: asyncio.Queue[In | _Stop] = asyncio.Queue(maxsize=workers)

    async def feed() -> None:
        try:
            async for event in source:
                await inbox.put(event)
        finally:
            for _ in range(workers):
                await inbox.put(_Stop())

    async def take() -> AsyncIterator[In]:
        while not isinstance(event := await inbox.get(), _Stop):
            yield event

    async with background_task(feed()):
        async for output in merge(*(processor(take()) for _ in range(workers))):
            yield output


@asynccontextmanager
async def tee[T](source: Stream[T], branches: int, buffer: int = 1) -> AsyncIterator[tuple[Stream[T], ...]]:
    """Fan a stream out into `branches` independent copies (the broadcast edge).

    Every branch sees every value, in order: the structural counterpart to
    `distribute` (which splits events *between* consumers). One pump reads the
    source once and pushes each value to every branch. Every branch MUST be
    consumed, and *concurrently*: a full branch queue blocks the pump, so
    draining one branch to exhaustion before starting another deadlocks.

    `buffer` is how many values each branch may run ahead, trading memory for
    slack between a fast and a slow branch: the default `1` keeps memory
    O(branches) and the slowest branch gating the rest, a larger value lets a
    fast branch pull ahead, and `0` is unbounded (no backpressure: a slow
    branch buffers without limit).
    """
    if branches < 1:
        raise ValueError(f"branches must be at least 1, got {branches}")
    if buffer < 0:
        raise ValueError(f"buffer must be at least 0, got {buffer}")

    queues: list[asyncio.Queue[T | _Stop]] = [asyncio.Queue(maxsize=buffer) for _ in range(branches)]

    async def pump() -> None:
        try:
            async for value in source:
                for queue in queues:
                    await queue.put(value)
        finally:
            for queue in queues:
                await queue.put(_Stop())

    async def drain(queue: asyncio.Queue[T | _Stop]) -> AsyncIterator[T]:
        while not isinstance(value := await queue.get(), _Stop):
            yield value

    async with background_task(pump()):
        yield tuple(drain(queue) for queue in queues)


async def broadcast[In, Out](source: Stream[In], *processors: Processor[In, Out]) -> AsyncIterator[Out]:
    """Feed every event to every processor (fan-out), merging their outputs.

    The processor-level convenience over `tee`: `broadcast` is `tee` then
    `merge`. Each processor sees the whole input stream and runs on its own
    branch; their outputs interleave into the single stream returned here. The
    fan-out twin of `distribute`.
    """
    if not processors:
        raise ValueError("broadcast requires at least one processor")

    async with tee(source, len(processors)) as branches:
        merged = merge(*(processor(branch) for processor, branch in zip(processors, branches, strict=True)))
        async for output in merged:
            yield output


def _route_for[T](
    value: T, types: tuple[type[T], ...], queues: list[asyncio.Queue[T | _Stop]]
) -> asyncio.Queue[T | _Stop]:
    for type_, queue in zip(types, queues, strict=True):
        if isinstance(value, type_):
            return queue
    raise TypeError(f"no route for value of type {type(value).__name__!r}")


@asynccontextmanager
async def route[T](source: Stream[T], *types: type[T]) -> AsyncIterator[tuple[Stream[T], ...]]:
    """Partition a stream by the runtime type of each value, one branch per type.

    Unlike `tee` (every branch sees every value), `route` sends each value to
    exactly one branch: the first `types` entry it is an instance of. This is
    how a processor that emits a heterogeneous union (say a response and a
    background-kickoff) splits its output to distinct sinks. A value matching no
    listed type raises rather than being silently dropped, so an unhandled
    variant fails loudly. Branches carry the source type; narrow each downstream.
    """
    if not types:
        raise ValueError("route requires at least one variant type")

    queues: list[asyncio.Queue[T | _Stop]] = [asyncio.Queue(maxsize=1) for _ in types]

    async def pump() -> None:
        try:
            async for value in source:
                await _route_for(value, types, queues).put(value)
        finally:
            for queue in queues:
                await queue.put(_Stop())

    async def drain(queue: asyncio.Queue[T | _Stop]) -> AsyncIterator[T]:
        while not isinstance(value := await queue.get(), _Stop):
            yield value

    async with background_task(pump()):
        yield tuple(drain(queue) for queue in queues)
```

## Tests (for reference)

```python
async def test_distribute_handles_every_event_exactly_once() -> None:
    async def square(event: int, _: None) -> Transition[None, int]:
        return Transition(state=None, output=event * event)

    events = [2, 3, 4, 5, 6, 7]
    outputs = await collect(distribute(stream(events), from_scan(None, square), workers=3))

    assert sorted(outputs) == sorted(value * value for value in events)


async def test_distribute_caps_concurrency_at_the_worker_count() -> None:
    in_flight = 0
    peak = 0
    release = asyncio.Event()

    async def hold_until_released(event: int, _: None) -> Transition[None, int]:
        nonlocal in_flight, peak
        in_flight += 1
        peak = max(peak, in_flight)
        await release.wait()
        in_flight -= 1
        return Transition(state=None, output=event)

    async def release_once_saturated() -> None:
        while peak < 4:
            await asyncio.sleep(0)
        release.set()

    worker = from_scan(None, hold_until_released)

    outputs = await asyncio.gather(
        collect(distribute(stream(range(20)), worker, workers=4)),
        release_once_saturated(),
    )

    assert sorted(outputs[0]) == list(range(20))
    assert peak == 4


async def test_merge_folds_every_source_into_one_stream() -> None:
    merged = merge(stream([1, 2, 3]), stream([10, 20]), stream([100]))

    assert sorted(await collect(merged)) == [1, 2, 3, 10, 20, 100]


async def test_tee_gives_every_branch_every_value_in_order() -> None:
    async with tee(stream([1, 2, 3]), branches=3) as branches:
        drained = await asyncio.gather(*(collect(branch) for branch in branches))

    assert drained == [[1, 2, 3], [1, 2, 3], [1, 2, 3]]


async def test_tee_buffer_lets_a_fast_branch_run_ahead_of_a_slow_one() -> None:
    async with tee(stream([1, 2, 3, 4]), branches=2, buffer=4) as (fast, slow):
        ahead = await collect(fast)
        behind = await collect(slow)

    assert ahead == [1, 2, 3, 4]
    assert behind == [1, 2, 3, 4]


async def test_broadcast_feeds_every_event_to_every_processor() -> None:
    async def double(event: int, _: None) -> Transition[None, str]:
        return Transition(state=None, output=f"double={event * 2}")

    async def negate(event: int, _: None) -> Transition[None, str]:
        return Transition(state=None, output=f"negate={-event}")

    outputs = await collect(broadcast(stream([5, 6]), from_scan(None, double), from_scan(None, negate)))

    assert sorted(outputs) == ["double=10", "double=12", "negate=-5", "negate=-6"]


@dataclass(frozen=True, slots=True)
class Response:
    body: str


@dataclass(frozen=True, slots=True)
class KickOff:
    job: str


async def test_route_sends_each_value_to_the_branch_for_its_type() -> None:
    events: list[Response | KickOff] = [
        Response("ok"),
        KickOff("reindex"),
        Response("created"),
    ]

    async with route(stream(events), Response, KickOff) as (responses, kickoffs):
        drained = await asyncio.gather(collect(responses), collect(kickoffs))

    assert drained == [[Response("ok"), Response("created")], [KickOff("reindex")]]


async def test_route_raises_on_a_value_matching_no_listed_type() -> None:
    with pytest.raises(TypeError):
        async with route(stream([Response("ok"), KickOff("nope")]), Response) as (responses,):
            await collect(responses)
```
