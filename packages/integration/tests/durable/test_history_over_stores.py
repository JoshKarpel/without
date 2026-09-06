from __future__ import annotations

from collections.abc import Awaitable
from collections.abc import Callable
from datetime import UTC
from datetime import datetime
from datetime import timedelta

import pytest
from without_durability import Checkpointer
from without_durability import Durable
from without_durability import Pass
from without_durability import claimed

from .stores import durable  # noqa: F401 - the parametrized fixture every test here takes
from .stores import transacting  # noqa: F401 - `transact` over whichever store that handed out

# The timestamp half of the `Checkpointer` contract, over every store that implements it.
# `history` owes the records `load` owes, in the same order, each carried with the moment
# the store wrote it, and it runs here for the reason the ordering suite does: what a store
# stamps a record with is invisible in the signature, so nothing but a suite that holds all
# of them to it can say the five agree.
#
# What is *not* asserted is precision, and the omission is deliberate. Redis stamps whole
# milliseconds, SQLite whole milliseconds through `subsec`, and Postgres microseconds, so a
# suite pinning two records apart in time would be pinning the finest clock any of them
# happens to have rather than the contract, and would fail on the coarsest.

pytestmark = pytest.mark.compose

# Written in the reverse of their sorted order, for the reason the ordering suite writes
# them that way: a store returning rows by primary key would otherwise come back correct by
# coincidence.
KEYS = ("step-c", "step-b", "step-a")

# How far the store's clock may sit from this process's before the stamp stops being a wall
# clock at all. Wide, because these are two machines and the assertion is "this is a real
# moment now" rather than "these two clocks agree": a container's clock drifting by a second
# is ordinary, and a stamp that is a counter, a monotonic reading, or a zero is minutes or
# decades out rather than seconds.
LOOSELY = timedelta(minutes=5)


async def write_interleaved(
    checkpointer: Checkpointer,
    transact: Callable[[Pass, str, str], Awaitable[object]],
    holder: Pass,
    workflow: str,
) -> None:
    """Every key, rotating through all three writers, as the ordering suite does."""
    for index, key in enumerate(KEYS):
        match index % 3:
            case 0:
                await checkpointer.record(holder, key, f"{key}-value")
            case 1:
                await checkpointer.supply(workflow, key, f"{key}-value")
            case _:
                await transact(holder, key, f"{key}-value")


async def test_history_carries_every_record_with_the_moment_it_was_written(
    durable: Durable,  # noqa: F811
    transacting: Callable[[Pass, str, str], Awaitable[object]],  # noqa: F811
    workflow: str,
) -> None:
    checkpointer = durable.checkpointer
    holder = await claimed(checkpointer, workflow)
    try:
        await write_interleaved(checkpointer, transacting, holder, workflow)
    finally:
        await checkpointer.release(holder)

    history = await checkpointer.history(workflow)
    around = datetime.now(UTC)

    assert list(history) == list(KEYS), "the same order `load` owes, since it is the same records"
    assert {key: written.value for key, written in history.items()} == await checkpointer.load(workflow)
    for key, written in history.items():
        assert written.at.tzinfo is not None, f"{key} was stamped with a naive datetime"
        assert abs(written.at - around) < LOOSELY, f"{key} was stamped {written.at}, which is not a moment just now"
    assert [written.at for written in history.values()] == sorted(written.at for written in history.values()), (
        "records written in order carry times in that order"
    )


async def test_a_losing_write_moves_the_time_no_more_than_the_value(
    durable: Durable,  # noqa: F811
    workflow: str,
) -> None:
    # The third clause of the same rule the ordering suite pins the other two of:
    # first-writer-wins decides what a key holds and where it sits, and this says it decides
    # when the key was written too. A store stamping on every write would report a replayed
    # step as having run at the moment of the replay, which is the one reading of this
    # column nobody wants.
    checkpointer = durable.checkpointer
    holder = await claimed(checkpointer, workflow)
    try:
        await checkpointer.record(holder, "charged", "ch-first")
        first = (await checkpointer.history(workflow))["charged"]

        losing = await checkpointer.record(holder, "charged", "ch-second")
        assert losing.first is False

        # And from outside the pass, which is the write that reaches the store by a
        # different statement and so could stamp by a different rule.
        await checkpointer.supply(workflow, "charged", "ch-third")
    finally:
        await checkpointer.release(holder)

    assert (await checkpointer.history(workflow))["charged"] == first


async def test_a_workflow_that_has_recorded_nothing_has_no_history(
    durable: Durable,  # noqa: F811
    workflow: str,
) -> None:
    # Reading a workflow is not creating one, exactly as it is not for `load`: this is the
    # call a status view makes for an id nobody has ever submitted.
    assert await durable.checkpointer.history(workflow) == {}
