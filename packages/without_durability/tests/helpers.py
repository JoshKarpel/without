from __future__ import annotations

import asyncio
from dataclasses import dataclass
from dataclasses import field
from datetime import UTC
from datetime import datetime
from datetime import timedelta

from without_durability import MemoryCheckpointer
from without_durability import Pass
from without_durability import Recorded

STARTED_AT = datetime(2026, 3, 14, 9, 30, tzinfo=UTC)


@dataclass(slots=True)
class Clock:
    """A clock the test moves, so a three-day wait costs a line rather than three days."""

    at: datetime = STARTED_AT

    def __call__(self) -> datetime:
        return self.at

    def advance(self, by: timedelta) -> None:
        self.at += by


@dataclass(frozen=True, slots=True)
class ParkedWrites(MemoryCheckpointer):
    """
    The double, with a `record` that parks mid-write so a cancellation can land inside it.

    Every real store's `record` is a round trip to a server, which is a suspension point;
    the in-memory one's is not, so this is the one shape a test cannot reach by driving the
    double as it comes.
    """

    writing: asyncio.Event = field(default_factory=asyncio.Event)
    proceed: asyncio.Event = field(default_factory=asyncio.Event)
    # A store that is reachable and then is not, which is the other thing a write can do
    # while a step is being torn down around it.
    refuse: bool = False

    async def record(self, holder: Pass, key: str, value: object) -> Recorded:
        self.writing.set()
        await self.proceed.wait()
        if self.refuse:
            raise ConnectionError("the store was briefly unreachable")
        return await super().record(holder, key, value)


def as_text(recorded: object) -> str:
    """The text a step recorded, parsed rather than cast."""
    if not isinstance(
        recorded, str
    ):  # pragma: no cover - the arm that makes this a parser rather than a cast; no test feeds it a bad value
        raise TypeError(f"{recorded!r} is not the text this step recorded")
    return recorded
