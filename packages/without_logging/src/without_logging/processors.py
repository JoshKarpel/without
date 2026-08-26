from __future__ import annotations

from collections.abc import Awaitable
from collections.abc import Callable

from without_streams.interfaces import Processor
from without_streams.interfaces import from_map

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
    record out), so it uses the core builder directly. Enriching from a *shared
    behavior* (a value the whole pipeline sees the latest of: the current config
    revision, a sampling rate) is the same shape, reading it via `current()`
    inside the step. Per-call-site context (a request or trace id, which is
    task-local) is *not* recoverable here: the pipeline runs in the sink task,
    having left the caller's context. Bind it at the edge instead with
    `bind`/`merge_context`, which read it while `emit` is still on the caller's
    task (see the guide).
    """

    async def enrich(record: Record) -> Record:
        return record.with_fields(**fields)

    return from_map(enrich)
