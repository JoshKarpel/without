from __future__ import annotations

from collections.abc import Iterator
from collections.abc import Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from types import MappingProxyType

from without_logging.record import Record

_bound: ContextVar[Mapping[str, object]] = ContextVar("without_logging_bound_context", default=MappingProxyType({}))


@contextmanager
def bind(**fields: object) -> Iterator[None]:
    """
    Bind `fields` onto every record logged within this block, on this task.

    The call-site half of structured context, the equivalent of structlog's
    `bind_contextvars`: `with bind(request_id=...)` stamps those fields onto every
    record logged inside the block. Scoping is explicit and lexical: the fields
    are in effect for the block and gone after it, leaving no shared place
    mutated. It writes a task-local `ContextVar`, so concurrent tasks each see
    only their own binds, and nested binds accumulate (an inner `bind` merges onto
    the outer and is undone on exit; a key set by both takes the inner value
    inside the inner block).

    Binding alone does nothing until the `capture` side reads it: pair this with
    `merge_context`, which merges the bound fields onto each record at the parse
    edge, where they are still live (the handler's `emit` runs synchronously on
    the logging call's task). A downstream processor cannot read them, having left
    the caller's context for the sink task.
    """
    merged = MappingProxyType({**_bound.get(), **fields})
    token = _bound.set(merged)
    try:
        yield
    finally:
        _bound.reset(token)


def merge_context(record: Record) -> Record:
    """
    Merge the context bound by `bind` onto a record: the read half of call-site context.

    A `Record -> Record` enrichment, *composed on top of* a parser rather than
    wrapping it: `capture`'s default parser applies it after `parse_record`, and a
    custom parser composes it the same way,
    `merge_context(parse_record(log_record))`. A field the log call set explicitly
    via `extra=` wins over a bound field of the same name (the more specific
    value), so `bind` supplies defaults, never overrides a per-call value.

    It reads the task-local `ContextVar` that `bind` writes, so it MUST run at the
    parse edge, in the handler's `emit` on the caller's task, not as a pipeline
    `Processor`: the pipeline runs in the sink task, having left that context,
    where the bind would read as empty.
    """
    bound = _bound.get()
    if not bound:
        return record
    return record.with_fields(**{key: value for key, value in bound.items() if key not in record.fields})
