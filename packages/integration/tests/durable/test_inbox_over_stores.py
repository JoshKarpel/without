from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from collections.abc import Callable

import pytest
from without_durability import INBOX
from without_durability import Checkpointer
from without_durability import Completed
from without_durability import Durable
from without_durability import Entry
from without_durability import Listening
from without_durability import Outcome
from without_durability import Run
from without_durability import claimed
from without_durability import resume

from .stores import durable  # noqa: F401 - the parametrized fixture every test here takes

# The inbox half of the `Checkpointer` and `Durable` contracts, over every store that
# implements them. Two of the requirements are invisible to the type checker and one of them
# is invisible to a careless test as well: `append` owes distinct, ordered keys under
# concurrency, entries owe their appearance in `load`, and `receive` owes a replay the same
# entries rather than whatever has arrived since. Nothing but this suite holds a store to
# any of it.
#
# The memory store is the control. It gets ordering from a plain dict and atomicity from
# having no `await` between its reads and its writes, so a failure there is a failure of
# this test rather than of a store.

pytestmark = pytest.mark.compose


async def passing[T](checkpointer: Checkpointer, workflow: str, body: Callable[[Run], Awaitable[T]]) -> Outcome[T]:
    """One claimed pass, released on the way out, which is what the worker does."""
    holder = await claimed(checkpointer, workflow)
    try:
        return await resume(holder, checkpointer, body)
    finally:
        await checkpointer.release(holder)


def reading(key: str, *, limit: int | None = None) -> Callable[[Run], Awaitable[tuple[Entry, ...]]]:
    """A workflow whose whole body is one `receive` from the top of the inbox."""

    async def body(run: Run) -> tuple[Entry, ...]:
        return await run.receive(key, limit=limit)

    return body


def said(outcome: Outcome[tuple[Entry, ...]]) -> list[object]:
    """The values a reading pass came back with, failing here if it stopped short."""
    assert isinstance(outcome, Completed), f"the pass did not finish: {outcome}"
    return [entry.value for entry in outcome.value]


async def test_appends_made_in_turn_sort_into_the_order_they_were_made(
    durable: Durable,  # noqa: F811
    workflow: str,
) -> None:
    # The order a consumer reads an inbox by. It is carried twice over, by the keys
    # sorting and by `load` returning the entries in that same order, and both are asserted
    # because a store can meet either alone: one that numbered keys from a counter it kept
    # apart from the load position would sort correctly and render backwards.
    checkpointer = durable.checkpointer
    messages = [f"message-{index:02d}" for index in range(8)]

    keys = [(await checkpointer.append(workflow, message)).key for message in messages]

    assert keys == sorted(keys), "the keys do not sort into the order they were assigned in"
    assert await checkpointer.load(workflow) == dict(zip(keys, messages, strict=True))


async def test_concurrent_appends_all_land_under_distinct_keys(
    durable: Durable,  # noqa: F811
    workflow: str,
) -> None:
    # The requirement `append` carries that `supply` does not: the store names the key, so
    # the store is what has to make two callers racing for the next one impossible. A store
    # computing the next number in one round trip and writing it in another passes every
    # sequential test here and loses a message the moment two callers overlap.
    #
    # Appending together rather than in turn is the whole of the arrangement. It does not
    # *guarantee* overlap on any one run, but a store with the race loses a message often
    # enough across the parametrization to be caught, where a sequential loop could not
    # catch it at all.
    #
    # What is deliberately not asserted is that the keys come back in the order the
    # coroutines were *listed* in. `gather` reports results in argument order and the
    # appends land in whatever order the store served them, so there is no append order
    # here for the keys to agree with: with genuinely concurrent callers, the store's
    # choice is the order, which is what the `load` assertion reads it back as.
    checkpointer = durable.checkpointer
    messages = [f"message-{index:02d}" for index in range(16)]

    appended = await asyncio.gather(*(checkpointer.append(workflow, message) for message in messages))

    keys = [entry.key for entry in appended]
    assert len(set(keys)) == len(keys), "two appends were handed the same key"
    assert {entry.value for entry in appended} == set(messages), "an append lost its value"

    recorded = await checkpointer.load(workflow)
    assert [key for key in recorded if key.startswith(INBOX)] == sorted(keys), (
        "the entries are not in `load`, or the load order and the key order disagree"
    )
    assert recorded == {entry.key: entry.value for entry in appended}


async def test_an_entry_is_an_ordinary_record_that_load_returns_in_place(
    durable: Durable,  # noqa: F811
    workflow: str,
) -> None:
    # Entries interleave with everything else the workflow has done rather than living in
    # a table of their own, which is what lets a consumer render a transcript straight out
    # of `load` and fork one by copying what `load` returns.
    checkpointer = durable.checkpointer
    holder = await claimed(checkpointer, workflow)
    try:
        first = await checkpointer.append(workflow, "before")
        await checkpointer.record(holder, "in-between", "a step")
        second = await checkpointer.append(workflow, "after")
    finally:
        await checkpointer.release(holder)

    assert list(await checkpointer.load(workflow)) == [first.key, "in-between", second.key]


async def test_a_receive_that_crashes_replays_to_the_same_entries(
    durable: Durable,  # noqa: F811
    workflow: str,
) -> None:
    # The property the step-wrapping exists for. Written carelessly this passes against an
    # implementation that re-reads the inbox live, because on a quiet store a live read and
    # a recorded one give the same answer; appending *between* the two passes is what makes
    # the two answers differ, so it is the whole test rather than a flourish.
    checkpointer = durable.checkpointer
    for message in ("one", "two"):
        await checkpointer.append(workflow, message)

    first = await passing(checkpointer, workflow, reading("heard"))
    assert said(first) == ["one", "two"]

    await checkpointer.append(workflow, "three")

    replayed = await passing(checkpointer, workflow, reading("heard"))
    assert said(replayed) == ["one", "two"], "the replay saw an entry the first pass could not have"


async def test_a_partial_take_leaves_the_rest_for_the_next_read(
    durable: Durable,  # noqa: F811
    workflow: str,
) -> None:
    # A consumer that treats the first new message as opening a unit of work and the rest
    # as belonging to it cannot advance its cursor past the lot, so `limit` is what lets it
    # take one and come back.
    checkpointer = durable.checkpointer
    for message in ("one", "two", "three"):
        await checkpointer.append(workflow, message)

    async def take_one_then_the_rest(run: Run) -> tuple[list[object], list[object]]:
        opened = await run.receive("opened", limit=1)
        rest = await run.receive("rest", after=opened[-1].key)
        return [entry.value for entry in opened], [entry.value for entry in rest]

    assert await passing(checkpointer, workflow, take_one_then_the_rest) == Completed(value=(["one"], ["two", "three"]))


async def test_a_receive_with_an_empty_inbox_leaves_the_workflow_listening(
    durable: Durable,  # noqa: F811
    workflow: str,
) -> None:
    checkpointer = durable.checkpointer

    assert await passing(checkpointer, workflow, reading("heard")) == Listening(key="heard")

    await checkpointer.append(workflow, "at last")

    assert said(await passing(checkpointer, workflow, reading("heard"))) == ["at last"]


async def test_deliver_makes_the_workflow_ready(
    durable: Durable,  # noqa: F811
    workflow: str,
) -> None:
    # The reason `deliver` exists rather than leaving a caller to append and then schedule:
    # a workflow parked as `Listening` is woken by nothing but the next message, so an
    # append that reaches the store without a wakeup is a message nobody will ever read.
    assert await passing(durable.checkpointer, workflow, reading("heard")) == Listening(key="heard")

    entry = await durable.deliver(workflow, "wake up")

    taken = await durable.scheduler.next_ready(within=durable.scheduler.lease)
    assert taken is not None, "the delivery did not make the workflow ready"
    assert taken.workflow == workflow
    assert (await durable.checkpointer.load(workflow))[entry.key] == "wake up"


async def test_entries_survive_being_copied_into_a_forked_workflow(
    durable: Durable,  # noqa: F811
    workflow: str,
) -> None:
    # Forking is why an entry is never consumed. A consumer that copies a prefix of one
    # workflow's records into a new id copies entries like anything else, with no rule
    # about what happens to the unread ones, because what a pass records is a *reference*
    # to an entry rather than a copy of its value.
    checkpointer = durable.checkpointer
    fork = f"{workflow}-fork"
    for message in ("one", "two"):
        await checkpointer.append(workflow, message)
    original = await checkpointer.load(workflow)

    for key, value in original.items():
        await checkpointer.supply(fork, key, value)

    assert await checkpointer.load(fork) == original
    assert said(await passing(checkpointer, fork, reading("heard"))) == ["one", "two"]
