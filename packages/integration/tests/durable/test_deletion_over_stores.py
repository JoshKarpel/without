from __future__ import annotations

from datetime import timedelta

import pytest
from without_durability import Durable
from without_durability import Fenced
from without_durability import claimed
from without_durability import now_utc

from .stores import durable  # noqa: F401 - the parametrized fixture every test here takes

# Deleting a workflow, over every store that implements it. It runs here rather than in each
# store's own package because the interesting half is not that the records go: it is what
# happens to the pass that was running and the wakeup that was queued when they did, and
# each store answers that by a different route. Redis raises a token in a hash and deletes
# stream entries one batch at a time; the sorted set and the two SQL stores collapse the
# whole question into one row per workflow; the in-memory double does it with three dicts.
#
# The memory store is the control. A failure there is a failure of this test rather than of
# a store, since it has no machinery to get wrong.

pytestmark = pytest.mark.compose

# Long enough for a delivery to be taken and short enough that a test asserting *nothing*
# arrives does not spend a second finding out. It is a read budget rather than a rate: the
# blocking queue answers the instant an entry lands, and the polling ones look every 50ms.
BRIEFLY = timedelta(milliseconds=250)

# Far enough ahead that any deadline written since would have matured, so a `wake_due` that
# still reports nothing is reporting that nothing was written rather than that nothing is
# ripe yet.
LATER = timedelta(days=1)


async def test_delete_removes_every_record_and_says_how_many(
    durable: Durable,  # noqa: F811
    workflow: str,
) -> None:
    checkpointer = durable.checkpointer
    holder = await claimed(checkpointer, workflow)
    await checkpointer.record(holder, "charged", "ch-1")
    await checkpointer.supply(workflow, "approved", True)
    await checkpointer.append(workflow, "a message")
    await checkpointer.release(holder)

    removed = await durable.delete(workflow)

    assert removed == 3, "the inbox entry counts, since an entry is an ordinary record"
    assert await checkpointer.load(workflow) == {}
    assert await checkpointer.history(workflow) == {}


async def test_deleting_a_workflow_nobody_has_run_removes_nothing(
    durable: Durable,  # noqa: F811
    workflow: str,
) -> None:
    # Deleting is not creating, and the count is what says so: a caller sweeping ids it is
    # not sure about reads zero rather than an error.
    assert await durable.delete(workflow) == 0


async def test_delete_leaves_every_other_workflow_alone(
    durable: Durable,  # noqa: F811
    workflow: str,
) -> None:
    # The stores that number their records off one shared counter, and the Redis queue that
    # holds every workflow's wakeups in one stream, both have to pick this workflow out of a
    # structure its neighbours share.
    checkpointer = durable.checkpointer
    neighbour = f"{workflow}-neighbour"
    await durable.arrive(workflow, "submitted", "mine")
    await durable.arrive(neighbour, "submitted", "theirs")

    await durable.delete(workflow)

    assert await checkpointer.load(neighbour) == {"submitted": "theirs"}
    taken = await durable.scheduler.next_ready(BRIEFLY)
    assert taken is not None
    assert taken.workflow == neighbour


async def test_a_pass_still_holding_a_deleted_workflow_is_fenced(
    durable: Durable,  # noqa: F811
    workflow: str,
) -> None:
    # The reason a delete raises the fencing token rather than removing it. This pass keeps
    # its `Pass` and believes it still owns the workflow; without the raise it would carry
    # on writing its remaining steps back into a workflow that has been deleted, one at a
    # time, with nothing to show that it happened.
    checkpointer = durable.checkpointer
    holder = await claimed(checkpointer, workflow)
    await checkpointer.record(holder, "charged", "ch-1")

    await durable.delete(workflow)

    with pytest.raises(Fenced):
        await checkpointer.record(holder, "shipped", "sh-1")
    assert await checkpointer.load(workflow) == {}


async def test_a_deleted_workflow_can_be_run_again_from_nothing(
    durable: Durable,  # noqa: F811
    workflow: str,
) -> None:
    # What the tombstone keeps is the ordering, not the claim: the id is free immediately,
    # and the workflow that takes it starts with an empty checkpoint.
    checkpointer = durable.checkpointer
    first = await claimed(checkpointer, workflow)
    await checkpointer.record(first, "charged", "ch-1")
    await durable.delete(workflow)

    second = await claimed(checkpointer, workflow)

    assert second.token > first.token, "the fence went up rather than back to the beginning"
    assert await checkpointer.load(workflow) == {}
    assert (await checkpointer.record(second, "charged", "ch-2")).first is True


async def test_a_cancelled_wakeup_never_reaches_a_worker(
    durable: Durable,  # noqa: F811
    workflow: str,
) -> None:
    await durable.arrive(workflow, "submitted", "an order")

    await durable.delete(workflow)

    assert await durable.scheduler.wake_due(now_utc() + LATER) == ()
    assert await durable.scheduler.next_ready(BRIEFLY) is None


async def test_a_pass_whose_delivery_was_cancelled_does_not_put_the_workflow_back(
    durable: Durable,  # noqa: F811
    workflow: str,
) -> None:
    # The half of a delete that sweeping the queue cannot reach, and the reason `wake_at` is
    # conditional on the delivery still being live. A worker answers for its delivery
    # *after* the pass, so a delete that lands mid-pass is followed a moment later by the
    # deadline that pass chose. Written unconditionally, that queues the workflow whose
    # records have just been discarded, and the next worker runs it from the top with
    # nothing recorded: the deletion undone by the very thing it interrupted.
    await durable.arrive(workflow, "submitted", "an order")
    delivery = await durable.scheduler.next_ready(BRIEFLY)
    assert delivery is not None
    assert delivery.workflow == workflow

    await durable.delete(workflow)
    # A deadline already in the past, so a store that wrote it would make the workflow
    # visible at once rather than leaving the assertion to a timeout.
    await durable.scheduler.wake_at(delivery, now_utc() - timedelta(seconds=1))

    assert await durable.scheduler.wake_due(now_utc() + LATER) == ()
    assert await durable.scheduler.next_ready(BRIEFLY) is None
