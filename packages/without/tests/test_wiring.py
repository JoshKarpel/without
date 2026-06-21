from without import Transition, from_reducer, pipe, sample
from without.testing import collect, stream, tick


async def test_pipe_feeds_every_output_into_the_processor() -> None:
    def double(event: int, _: None) -> Transition[None, int]:
        return Transition(state=None, outputs=(event * 2,))

    doubled = pipe(stream([6, 7, 8]), from_reducer(None, double))

    assert await collect(doubled) == [12, 14, 16]


async def test_sample_starts_at_the_first_value() -> None:
    async with sample(stream([11, 22, 33])) as latest:
        assert latest.current() == 11


async def test_sample_tracks_the_latest_value() -> None:
    async with sample(stream([11, 22, 33])) as latest:
        await tick()
        assert latest.current() == 33
