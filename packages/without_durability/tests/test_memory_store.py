from __future__ import annotations

from datetime import timedelta

from without_durability import MemoryCheckpointer
from without_durability import MemoryScheduler
from without_durability import Written
from without_durability import claimed

from .helpers import STARTED_AT
from .helpers import Clock

# The double's own properties, where the cross-store suite covers what it shares with the
# four shipped stores. What is left here is the two things a dict does differently: the
# clock it stamps records from is an argument rather than a server's, and its queue is a
# `deque` that can hold one workflow more than once where every shipped queue holds each
# workflow exactly once.

WORKFLOW = "wf-memory-1"
BRIEF = timedelta(milliseconds=10)


async def test_records_are_stamped_from_the_clock_the_store_was_given() -> None:
    # The other stores read their server's clock, which a test cannot move. Here it is an
    # argument, so a suite asserting on how long a workflow spent between two steps costs a
    # line rather than the interval itself.
    clock = Clock()
    checkpointer = MemoryCheckpointer(now=clock)
    holder = await claimed(checkpointer, WORKFLOW)

    await checkpointer.record(holder, "charged", "ch-1")
    clock.advance(timedelta(days=3))
    await checkpointer.record(holder, "settled", "st-1")

    assert await checkpointer.history(WORKFLOW) == {
        "charged": Written(value="ch-1", at=STARTED_AT),
        "settled": Written(value="st-1", at=STARTED_AT + timedelta(days=3)),
    }


async def test_cancelling_removes_every_copy_of_a_workflow_the_queue_holds() -> None:
    # A `deque` is the one queue here that can hold a workflow twice, since every shipped
    # store holds one entry or row per workflow and a second wakeup lands on the first.
    # Removing by value while iterating would leave the duplicate behind, and a delete that
    # left one wakeup standing runs the deleted workflow from the top.
    scheduler = MemoryScheduler()
    await scheduler.make_ready(WORKFLOW)
    await scheduler.make_ready("wf-spared")
    await scheduler.make_ready(WORKFLOW)

    await scheduler.cancel(WORKFLOW)

    assert list(scheduler.queue) == ["wf-spared"]
    taken = await scheduler.next_ready(BRIEF)
    assert taken is not None
    assert taken.workflow == "wf-spared"
    assert await scheduler.next_ready(BRIEF) is None
