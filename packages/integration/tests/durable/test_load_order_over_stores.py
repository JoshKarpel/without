from __future__ import annotations

from collections.abc import Awaitable
from collections.abc import Callable

import pytest
from without_durability import Checkpointer
from without_durability import Durable
from without_durability import Pass
from without_durability import claimed

from .stores import durable  # noqa: F401 - the parametrized fixture every test here takes
from .stores import transacting  # noqa: F401 - `transact` over whichever store that handed out

# The ordering half of the `Checkpointer` contract, over every store that implements it.
# `load` owes its records in the order they were first recorded, and the point of running
# it here rather than in each store's own package is that the guarantee is invisible in the
# signature: `dict[str, object]` says nothing about order, so nothing but this suite holds
# an implementation to it.
#
# The memory store is the control. It gets the order from a plain dict and needs no
# machinery at all, so a failure there is a failure of this test rather than of a store.

pytestmark = pytest.mark.compose


# Enough keys that an accidental pass is unlikely, and few enough that the round trips are
# cheap. The order they are *written* in is deliberately the reverse of the order they sort
# in, which is the whole of what makes this test mean anything on the SQL stores: they
# return rows by primary key unless told otherwise, so keys generated in sorted order would
# come back in insertion order by coincidence and the test would pass against a store with
# no guarantee at all.
KEYS = tuple(f"step-{index:03d}" for index in range(24))[::-1]

# Long enough to push Redis past `hash-max-listpack-value`, which converts the steps hash
# from a listpack to a hashtable. That conversion is the entire reason this test exists for
# Redis: a listpack preserves insertion order, so a hash under the threshold answers
# correctly while the store carries no guarantee. The value is the lever rather than the
# field count because `hash-max-listpack-value` (64 bytes) is reached by one write where
# `hash-max-listpack-entries` needs hundreds, and because that second default has moved
# between releases (512 on Redis 8, and widely documented as 128), so a test sized against
# it silently stops exercising the path when the server changes underneath it.
#
# `test_redis_store.py` asserts the conversion actually happens. Here it is arranged and
# not checked, because a cross-store test that knew what a listpack was would be reaching
# into one implementation from the suite that exists to not do that.
PADDING = "x" * 80


def value_for(key: str) -> str:
    return f"{key}-{PADDING}"


async def write_interleaved(
    checkpointer: Checkpointer,
    transact: Callable[[Pass, str, str], Awaitable[object]],
    holder: Pass,
    workflow: str,
    keys: tuple[str, ...],
) -> None:
    """
    Write every key, rotating through all three writers a store has.

    Rotating rather than grouping is the point: a store assigns position on whichever
    statement or script performed the write, so a test driving only `record` leaves the
    other two write paths unmeasured, and those are the ones easiest to miss. `supply` is
    called from outside the pass on purpose, since that is how it is called in production.
    """
    for index, key in enumerate(keys):
        match index % 3:
            case 0:
                await checkpointer.record(holder, key, value_for(key))
            case 1:
                await checkpointer.supply(workflow, key, value_for(key))
            case _:
                await transact(holder, key, value_for(key))


async def test_load_returns_records_in_the_order_they_were_first_recorded(
    durable: Durable,  # noqa: F811
    transacting: Callable[[Pass, str, str], Awaitable[object]],  # noqa: F811
    workflow: str,
) -> None:
    checkpointer = durable.checkpointer
    holder = await claimed(checkpointer, workflow)
    try:
        await write_interleaved(checkpointer, transacting, holder, workflow, KEYS)
    finally:
        await checkpointer.release(holder)

    recorded = await checkpointer.load(workflow)

    assert list(recorded) == list(KEYS)
    assert recorded == {key: value_for(key) for key in KEYS}, "and the values survived alongside the order"


async def test_a_losing_write_moves_neither_the_value_nor_the_position(
    durable: Durable,  # noqa: F811
    transacting: Callable[[Pass, str, str], Awaitable[object]],  # noqa: F811
    workflow: str,
) -> None:
    # First-writer-wins already decides what a key holds. This is the other half of it: the
    # same writer decides where the key sits, so a write that loses changes neither. A
    # store that assigns position on every write rather than on the insert would pass every
    # other assertion here and silently move a key to the end the moment anything wrote it
    # twice, which is exactly what a replayed pass does.
    checkpointer = durable.checkpointer
    keys = KEYS[:3]
    holder = await claimed(checkpointer, workflow)
    try:
        await write_interleaved(checkpointer, transacting, holder, workflow, keys)

        first, second, third = keys

        losing = await checkpointer.record(holder, first, "a different value entirely")
        assert losing.first is False
        assert losing.value == value_for(first), "the loser was told what the winner stored"

        # The same value offered again, which `Recorded.first` counts as a win for *both*
        # passes: two that ran the same effect have nothing to disagree about. It is a
        # separate case because a store that conflated "won the tie" with "actually
        # inserted" would allocate a second position here while reporting no race at all.
        tied = await checkpointer.record(holder, first, value_for(first))
        assert tied.first is True

        await checkpointer.supply(workflow, second, "a different value entirely")
        await transacting(holder, third, "a different value entirely")
    finally:
        await checkpointer.release(holder)

    recorded = await checkpointer.load(workflow)

    assert list(recorded) == list(keys)
    assert recorded == {key: value_for(key) for key in keys}


async def test_each_workflow_is_ordered_on_its_own(
    durable: Durable,  # noqa: F811
    transacting: Callable[[Pass, str, str], Awaitable[object]],  # noqa: F811
    workflow: str,
) -> None:
    # Interleaved in time, so a store numbering its writes from one counter hands these two
    # workflows interleaved positions. That is fine and deliberately not asserted against:
    # the guarantee is each workflow's own order, which gaps in a shared sequence satisfy,
    # and a test demanding contiguous per-workflow numbers would be pinning an
    # implementation choice rather than the contract.
    checkpointer = durable.checkpointer
    other = f"{workflow}-other"
    mine, theirs = KEYS[:4], KEYS[4:8]

    holder = await claimed(checkpointer, workflow)
    neighbour = await claimed(checkpointer, other)
    try:
        for ours, yours in zip(mine, theirs, strict=True):
            await checkpointer.record(holder, ours, value_for(ours))
            await checkpointer.record(neighbour, yours, value_for(yours))
    finally:
        await checkpointer.release(holder)
        await checkpointer.release(neighbour)

    assert list(await checkpointer.load(workflow)) == list(mine)
    assert list(await checkpointer.load(other)) == list(theirs)
