from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest
from without_durability import Durable
from without_durability import Fenced
from without_durability import claimed

from .stores import durable  # noqa: F401 - the parametrized fixture every test here takes

# The two-deadline half of the `Checkpointer` contract, over every store that implements it.
# A claim lapses at the earlier of one `alive` past its holder's last word and its `budget`
# running out, and neither half is visible in a signature: `claim` returns a `Pass` whatever
# the deadlines mean underneath, so nothing but a suite holding all five stores to the same
# behaviour can say they agree.
#
# Real elapsed time rather than an injected clock, deliberately. Four of these stores measure
# a lease by their *server's* clock precisely so that a stalled claimant cannot argue with it,
# which leaves a suite no clock to move: the only honest way to ask whether a deadline has
# passed is to let it pass. So the windows here are as short as five stores over a container
# network can be held to, and every assertion is about which side of a deadline something
# landed on rather than about how long anything took.

pytestmark = pytest.mark.compose

# Short enough that waiting one out costs a fraction of a second, wide enough that a round
# trip to a container cannot be mistaken for an expiry.
BRIEF = timedelta(milliseconds=150)
# Long enough that nothing in a test expires by accident, so a claim that is gone is one
# something here let go of.
AMPLE = timedelta(seconds=30)


async def test_a_renewing_pass_keeps_a_claim_past_its_liveness_window(
    durable: Durable,  # noqa: F811 - the fixture imported above, taken by name
    workflow: str,
) -> None:
    # The property the whole split exists for: a pass that is still working holds its
    # workflow however short the liveness window is, because it keeps saying so. A single
    # lease sized for takeover latency would fence this slow-but-healthy pass and make it
    # perform its current step twice.
    checkpointer = durable.checkpointer
    holder = await claimed(checkpointer, workflow, AMPLE, BRIEF)

    for _ in range(4):
        await asyncio.sleep(BRIEF.total_seconds() / 3)
        assert await checkpointer.renew(holder, BRIEF), "the pass still holds it, so renewing succeeds"

    assert await checkpointer.claim(workflow, AMPLE, BRIEF) is None, (
        "and nobody else may take a workflow whose holder has been heard from"
    )


async def test_a_claim_lapses_when_its_holder_stops_saying_anything(
    durable: Durable,  # noqa: F811 - the fixture imported above, taken by name
    workflow: str,
) -> None:
    # The other side of the same window, and the reason it can be short: silence is what
    # frees a workflow, so how fast a dead worker's work is picked up is this number rather
    # than however long its longest step might have been.
    checkpointer = durable.checkpointer
    await claimed(checkpointer, workflow, AMPLE, BRIEF)

    await asyncio.sleep(BRIEF.total_seconds() * 3)

    assert await checkpointer.claim(workflow, AMPLE, BRIEF) is not None, (
        "a holder that went quiet for longer than its liveness window has lost the workflow"
    )


async def test_renewing_cannot_carry_a_claim_past_its_budget(
    durable: Durable,  # noqa: F811 - the fixture imported above, taken by name
    workflow: str,
) -> None:
    # What stops a hung pass from holding a workflow for ever. A step that will never return
    # goes on asking for the renewal perfectly well, since its loop is free and it is simply
    # not going to finish, so the budget has to be a deadline renewal cannot lift, and the
    # renewal has to *say* the budget is gone: the worker acts on the answer, and a renewal
    # that reported success on a lapsed claim would leave the hung pass running, holding
    # the only delivery its workflow has.
    checkpointer = durable.checkpointer
    holder = await claimed(checkpointer, workflow, BRIEF, BRIEF)

    await asyncio.sleep(BRIEF.total_seconds() * 3)

    assert not await checkpointer.renew(holder, AMPLE), "the budget has run out, so the renewal reports the claim gone"
    assert await checkpointer.claim(workflow, AMPLE, BRIEF) is not None, "and the workflow is free"


async def test_a_window_shorter_than_what_is_left_takes_nothing_away(
    durable: Durable,  # noqa: F811 - the fixture imported above, taken by name
    workflow: str,
) -> None:
    # A step naming a small `within` late in a pass, with most of the pass's budget still
    # standing. Setting the budget to the window would cut the unannotated steps behind it
    # down to that window, and a renewal could not lift it again; the budget a pass runs
    # under can only ever be too generous, which is the promise `extending` skips round
    # trips on.
    checkpointer = durable.checkpointer
    holder = await claimed(checkpointer, workflow, AMPLE, AMPLE)

    assert await checkpointer.extend(holder, BRIEF, AMPLE)
    await asyncio.sleep(BRIEF.total_seconds() * 3)

    assert await checkpointer.claim(workflow, AMPLE, BRIEF) is None, (
        "the budget it was claimed for still stands, so the workflow is still held"
    )


async def test_a_write_refused_at_the_fence_renews_nobody(
    durable: Durable,  # noqa: F811 - the fixture imported above, taken by name
    workflow: str,
) -> None:
    # A superseded pass's stray write, landing after the pass that superseded it has gone
    # quiet. The write is refused, and it has to renew *nothing* on the way out: a store
    # that renewed whoever holds the claim would keep a dead holder's claim alive for as
    # long as the corpse of the pass before it kept writing, and delay the takeover that
    # the holder's silence should have brought on.
    checkpointer = durable.checkpointer
    stalled = await claimed(checkpointer, workflow, AMPLE, BRIEF)
    await checkpointer.release(stalled)
    await claimed(checkpointer, workflow, AMPLE, BRIEF)
    await asyncio.sleep(BRIEF.total_seconds() * 2)  # the holder goes quiet for longer than its window

    with pytest.raises(Fenced):
        await checkpointer.record(stalled, "stray", "s-1")

    assert await checkpointer.claim(workflow, AMPLE, BRIEF) is not None, (
        "the holder's silence freed the workflow, and the refused write did not un-free it"
    )


async def test_a_step_that_says_it_needs_longer_is_granted_it(
    durable: Durable,  # noqa: F811 - the fixture imported above, taken by name
    workflow: str,
) -> None:
    # `Run.step(..., within=...)` reaching the store. The claim was taken for a budget that
    # this step is about to outlive, and saying so up front is what keeps the pass from being
    # fenced at the write that follows a slow effect.
    checkpointer = durable.checkpointer
    holder = await claimed(checkpointer, workflow, BRIEF, BRIEF)

    assert await checkpointer.extend(holder, AMPLE, AMPLE), "the pass still holds it, so the budget is granted"
    await asyncio.sleep(BRIEF.total_seconds() * 3)

    assert await checkpointer.claim(workflow, AMPLE, BRIEF) is None, (
        "and the workflow is held past the budget it was claimed under"
    )
    assert await checkpointer.record(holder, "charged", "ch-1"), "so the write after the slow step lands"


async def test_a_superseded_pass_is_told_it_no_longer_holds_the_workflow(
    durable: Durable,  # noqa: F811 - the fixture imported above, taken by name
    workflow: str,
) -> None:
    # Both report rather than raise, because the caller has a decision to make: a worker
    # learning this from its own renewal stops the pass it is running, where `Fenced` is the
    # answer to a write that pass will now never get to make.
    checkpointer = durable.checkpointer
    stalled = await claimed(checkpointer, workflow, AMPLE, BRIEF)
    await checkpointer.release(stalled)
    await claimed(checkpointer, workflow, AMPLE, BRIEF)

    assert not await checkpointer.renew(stalled, BRIEF)
    assert not await checkpointer.extend(stalled, AMPLE, AMPLE)


async def test_a_write_is_a_sign_of_life(
    durable: Durable,  # noqa: F811 - the fixture imported above, taken by name
    workflow: str,
) -> None:
    # Which is what makes a workflow of ordinary short steps renew itself for nothing, and
    # leaves the worker's own tick with the case it is really for: one step long enough that
    # no write falls inside a whole window.
    #
    # A wider window than the rest of this file uses, because this is the one test whose
    # margin is a *write* rather than a sleep: SQLite commits under `synchronous=FULL`, so a
    # single fsync on a loaded machine can be most of a `BRIEF`, and a claim that lapsed
    # would say the store failed to renew when all that happened was a slow disk. Six writes
    # spanning well past the window keep the assertion exactly as strong.
    window = BRIEF * 4
    checkpointer = durable.checkpointer
    holder = await claimed(checkpointer, workflow, AMPLE, window)

    for step in range(6):
        await asyncio.sleep(BRIEF.total_seconds())
        await checkpointer.record(holder, f"step-{step}", step)

    assert await checkpointer.claim(workflow, AMPLE, window) is None, (
        "a pass that has been writing has been heard from, so nothing may take its workflow"
    )


async def test_a_write_still_in_flight_does_not_take_a_released_workflow_back(
    durable: Durable,  # noqa: F811 - the fixture imported above, taken by name
    workflow: str,
) -> None:
    # The case that makes a write count as a sign of life dangerous unless it is capped. A
    # released pass keeps its token and is entitled to finish a write it had already started,
    # so that write renews a claim its holder has already given up; bringing the budget down
    # to now on release is what makes the renewal land on a deadline that has already passed.
    checkpointer = durable.checkpointer
    holder = await claimed(checkpointer, workflow, AMPLE, AMPLE)
    await checkpointer.release(holder)

    await checkpointer.record(holder, "charged", "ch-1")

    assert await checkpointer.claim(workflow, AMPLE, BRIEF) is not None, (
        "the workflow was handed back, and a write landing afterwards does not un-hand it"
    )


async def test_a_renewed_delivery_is_still_the_one_its_worker_may_answer_for(
    durable: Durable,  # noqa: F811 - the fixture imported above, taken by name
    workflow: str,
) -> None:
    # The queue half, and the reason `extend` hands a delivery back rather than nothing.
    # Three of these stores make the visibility a delivery was taken under *be* its receipt,
    # so renewing renames it, and a worker still holding the old name would find its own
    # `done` silently declined and the workflow redelivered for nothing.
    scheduler = durable.scheduler
    await scheduler.make_ready(workflow)
    taken = await scheduler.next_ready(BRIEF)
    assert taken is not None

    renewed = await scheduler.extend(taken, BRIEF)
    await scheduler.done(renewed)

    # Waiting the renewal out is what makes this an assertion about `done` rather than about
    # `extend`. A delivery `done` declined is not *gone*, it is merely invisible for as long
    # as the renewal bought, so asserting on an empty queue while that window is still open
    # passes whether or not the acknowledgement landed.
    await asyncio.sleep(BRIEF.total_seconds() * 3)

    assert await scheduler.next_ready(BRIEF) is None, (
        "answering for the renewed delivery finished the workflow, rather than hiding it for a while"
    )


async def test_renewing_a_delivery_the_store_no_longer_holds_says_nothing(
    durable: Durable,  # noqa: F811 - the fixture imported above, taken by name
    workflow: str,
) -> None:
    # A worker renewing on a tick has no way to know its delivery was cancelled a moment ago,
    # so this has to be quiet rather than an error: the workflow is gone, and putting its
    # wakeup back would be the resurrection that cancelling exists to rule out. What comes
    # back is what went in, since there is no new name to report.
    scheduler = durable.scheduler
    await scheduler.make_ready(workflow)
    taken = await scheduler.next_ready(BRIEF)
    assert taken is not None
    await scheduler.cancel(workflow)

    assert await scheduler.extend(taken, AMPLE) == taken

    assert await scheduler.next_ready(BRIEF) is None, "and the cancelled workflow stayed cancelled"
