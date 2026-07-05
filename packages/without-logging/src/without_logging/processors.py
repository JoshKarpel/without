from __future__ import annotations

from collections.abc import Awaitable
from collections.abc import Callable

from without.contracts import Processor
from without.contracts import from_map

from without_logging.record import Record


def at_least(level: int) -> Callable[[Record], Awaitable[bool]]:
    """
    A predicate matching records at `level` or more severe: the level threshold.

    Pair it with the core `from_selector` to keep only those records (or
    `from_filter` to drop them). It is `async` to match the one color of
    predicate `from_selector`/`from_filter` expect, though the decision itself
    just compares integers. Because a `Record.level` is a plain `int`, a `Level`
    member reads naturally as the argument (`at_least(Level.WARNING)`), but any
    numeric level works.
    """

    async def is_severe_enough(record: Record) -> bool:
        return record.level >= level

    return is_severe_enough


def add_fields(**fields: object) -> Processor[Record, Record]:
    """
    A processor that merges static `fields` onto every record: enrichment.

    Enrichment *is* expressible as a `from_map` (one record in, one enriched
    record out), so it uses the core builder directly. Dynamic enrichment
    (stamping, say, a request id sampled from a `Context`) is the same shape with
    the value read via `current()` inside the step.
    """

    async def enrich(record: Record) -> Record:
        return record.with_fields(**fields)

    return from_map(enrich)
