from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

# A `timedelta` is the right type for a duration everywhere in this workspace, and the
# reason is exactly why these exist: it names its unit, so nothing downstream has to guess
# whether a bare number meant seconds or milliseconds. What it cannot do is say that a
# duration *survives* the boundary it is about to cross. A TCP keepalive knob carries whole
# seconds, an SSE `retry:` line carries whole milliseconds, and SQLite's `busy_timeout`
# carries whole milliseconds, so each one truncates whatever it is handed. Truncation is at
# its worst where it is least visible: half a millisecond of `retry:` becomes `retry: 0`,
# which does not mean "almost no wait", it means "reconnect immediately".
#
# So a boundary of that kind declares one of these instead, and what makes them worth
# having is what they *cannot* hold. Each is a count, not a duration: there is no argument
# to either constructor that names a finer unit, so a value too fine to cross is not a
# value the type can be asked to carry. `of` is the one way an arbitrary `timedelta`
# becomes one, and the only place the question "does this divide?" is ever asked.

__all__ = [
    "Milliseconds",
    "Seconds",
]


def _counted(duration: timedelta, unit: timedelta, units: str) -> int:
    count, remainder = divmod(duration, unit)
    if remainder:
        raise ValueError(f"a whole number of {units} cannot express {duration}")
    return count


@dataclass(frozen=True, slots=True)
class Seconds:
    """
    A duration a boundary carrying integer seconds can express exactly.

    The count itself, rather than a `timedelta` that happens to divide by one second: a
    duration finer than the boundary carries is not something this can be constructed
    from, so nothing downstream has a truncation left to do.

    ```python
    tcp_keepalive(idle=Seconds(60), interval=Seconds(10))
    ```

    `duration` is the `timedelta` back out, for arithmetic and for anything that takes a
    plain duration. `of` is the way in from one, and the only place the question of
    whether it divides is asked:

    ```python
    Seconds.of(settings.keepalive_idle)  # raises on a duration finer than a second
    ```
    """

    count: int

    @property
    def duration(self) -> timedelta:
        """The `timedelta` this count of seconds names."""
        return timedelta(seconds=self.count)

    @classmethod
    def of(cls, duration: timedelta) -> Seconds:
        """Parse a `timedelta` into a count of seconds, refusing a finer duration."""
        return cls(_counted(duration, timedelta(seconds=1), "seconds"))


@dataclass(frozen=True, slots=True)
class Milliseconds:
    """
    A duration a boundary carrying integer milliseconds can express exactly.

    The millisecond counterpart of `Seconds`, with the same shape: a count in, `duration`
    back out, and `of` to parse one from a `timedelta`.

    ```python
    yield Retry(Milliseconds.of(timedelta(seconds=30)))
    ```
    """

    count: int

    @property
    def duration(self) -> timedelta:
        """The `timedelta` this count of milliseconds names."""
        return timedelta(milliseconds=self.count)

    @classmethod
    def of(cls, duration: timedelta) -> Milliseconds:
        """Parse a `timedelta` into a count of milliseconds, refusing a finer duration."""
        return cls(_counted(duration, timedelta(milliseconds=1), "milliseconds"))
