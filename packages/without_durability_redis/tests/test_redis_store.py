from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator
from contextlib import suppress
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import cast
from uuid import uuid4

import pytest
from integration.durable import Order
from redis.asyncio import Redis
from redis.crc import key_slot
from redis.exceptions import ResponseError
from without_durability import INBOX_DIGITS
from without_durability import Contended
from without_durability import Fenced
from without_durability import Recorded
from without_durability import Run
from without_durability import claimed
from without_durability import now_utc
from without_durability_redis import LuaEffect
from without_durability_redis import RedisCheckpointer
from without_durability_redis import RedisSetScheduler
from without_durability_redis import RedisStreamScheduler
from without_durability_redis import trimming
from without_durability_redis.checkpointer import fenced
from without_durability_redis.units import milliseconds
from without_durability_redis.units import seconds
from without_streams import ticks

# `just test` starts the services in compose.yaml and publishes each address; these
# tests drive the real server it started rather than a fake, and skip when it did not
# (no container engine on this machine, or pytest run directly).
pytestmark = pytest.mark.compose

ORDER = Order(order_id="o-42", sku="gizmo", cents=1999)


def as_count(recorded: object) -> int:
    """What a `transact` effect records here: the ledger total after the reservation."""
    if not isinstance(
        recorded, int
    ):  # pragma: no cover - the arm that makes this a parser rather than a cast; no test feeds it a bad value
        raise TypeError(f"{recorded!r} is not the count this effect recorded")
    return recorded


@pytest.fixture
async def redis() -> AsyncIterator[Redis]:
    published = os.environ.get("WITHOUT_TESTS_REDIS")
    if not published:  # pragma: no cover - the arm that runs is the one where this whole file is uncovered
        pytest.skip("WITHOUT_TESTS_REDIS is unset: run `just test`, which starts the services in compose.yaml")

    # podman-compose reports the published port alone, docker compose the bind address
    # it is published on (`0.0.0.0:32768`), which is a wildcard a client cannot dial.
    # Either way the loopback address is where the port is reachable, so only the port
    # is taken.
    port = published.strip().rpartition(":")[2]
    # `decode_responses=True` is the app's call to make, and this app makes it: it owns
    # both ends of every key it touches, so nothing downstream has to ask whether a
    # value came back as bytes.
    client = Redis(host="127.0.0.1", port=int(port), decode_responses=True)
    try:
        yield client
    finally:
        await client.aclose()


@pytest.fixture
def workflow() -> str:
    # Every test gets its own idempotency key rather than flushing the database,
    # because the stack is shared by every worker in the session: a flush would pull
    # another test's checkpoint out from under it.
    return f"test-{uuid4().hex}"


async def test_only_one_of_many_processes_racing_for_a_workflow_gets_to_pass_over_it(
    redis: Redis,
    workflow: str,
) -> None:
    # The guarantee against the real server, where it has to hold across processes that
    # share nothing: every client registers the same script, and Redis is the only party
    # that sees all of them, which is why the check and the take have to happen there.
    racing = [RedisCheckpointer(redis=redis) for _ in range(8)]

    claims = await asyncio.gather(*(store.claim(workflow, timedelta(minutes=1)) for store in racing))
    won = [holder for holder in claims if holder is not None]

    assert len(won) == 1, "a claim is exclusive no matter how many clients ask at once"

    with pytest.raises(Contended):
        await claimed(racing[0], workflow)

    await racing[0].release(won[0])
    again = await claimed(racing[1], workflow)

    assert again.token > won[0].token, "and the next claim outranks the one before it"


async def test_a_write_from_a_pass_that_lost_the_workflow_is_refused_by_redis(
    redis: Redis,
    workflow: str,
) -> None:
    # What a lease alone cannot give: the stalled holder still believes it owns the
    # workflow, and only the store can tell it otherwise. The script compares the token
    # it was handed against the highest one issued, in the same step as the write.
    checkpointer = RedisCheckpointer(redis=redis)
    stalled = await claimed(checkpointer, workflow, timedelta(milliseconds=1))
    await asyncio.sleep(0.05)
    took_over = await claimed(checkpointer, workflow)

    with pytest.raises(Fenced, match="moved on while this pass held it"):
        await checkpointer.record(stalled, "paid", "pay-from-the-dead")

    assert await checkpointer.record(took_over, "paid", "pay-real") == Recorded(value="pay-real", first=True)
    assert await checkpointer.load(workflow) == {"paid": "pay-real"}


async def test_a_workflow_whose_keys_expired_still_outranks_a_pass_from_before(
    redis: Redis,
    workflow: str,
) -> None:
    # These keys expire, so a workflow quiet for longer than the ttl is forgotten. If its
    # id is then reused, a plain counter would hand the new incarnation token 1 while a
    # pass stalled since before the expiry still held token 3, and the corpse would
    # outrank the living. Seeding the token from the server clock is what closes that.
    checkpointer = RedisCheckpointer(redis=redis)
    before = await claimed(checkpointer, workflow)

    await redis.delete(checkpointer.pass_key(workflow), checkpointer.hash_key(workflow))
    # The seeding is the server's clock, so the guarantee is "a later claim gets a later
    # millisecond", and the deletion above has to actually take one. A real expiry takes
    # the `ttl`, which buys this by a margin of about a day; a test that deletes the keys
    # by hand has to buy it explicitly.
    await asyncio.sleep(0.005)
    after = await claimed(checkpointer, workflow)

    assert after.token > before.token, "the workflow was forgotten, but its fence did not rewind"

    with pytest.raises(Fenced):
        await checkpointer.record(before, "paid", "pay-from-a-previous-life")


async def test_a_step_already_recorded_is_never_overwritten_and_hands_back_the_winner(
    redis: Redis,
    workflow: str,
) -> None:
    checkpointer = RedisCheckpointer(redis=redis)
    holder = await claimed(checkpointer, workflow)

    assert await checkpointer.record(holder, "captured:piano", "cap-first") == Recorded(value="cap-first", first=True)
    assert await checkpointer.record(holder, "captured:piano", "cap-second") == Recorded(value="cap-first", first=False)
    assert await checkpointer.supply(workflow, "captured:piano", "cap-third") == "cap-first"
    assert await checkpointer.load(workflow) == {"captured:piano": "cap-first"}


async def test_a_result_the_codec_reshapes_still_counts_as_this_pass_s_own(
    redis: Redis,
    workflow: str,
) -> None:
    # A result crosses the codec both ways, so a pass that won outright can be handed back
    # something unequal: JSON has no tuple. `first` is this store's own answer rather than
    # that comparison, which is what stops `run_durably` reading a reshape as a race that
    # never happened.
    checkpointer = RedisCheckpointer(redis=redis)
    holder = await claimed(checkpointer, workflow)

    assert await checkpointer.record(holder, "bounds", (0, 2000)) == Recorded(value=[0, 2000], first=True)


async def test_two_passes_that_recorded_the_same_value_both_count_as_the_writer(
    redis: Redis,
    workflow: str,
) -> None:
    # A tie is not a race. Two passes that ran the same effect and produced the same
    # encoding have nothing to disagree about, so calling the second a loser would stop a
    # graph run over a difference that does not exist.
    checkpointer = RedisCheckpointer(redis=redis)
    holder = await claimed(checkpointer, workflow)

    await checkpointer.record(holder, "captured", "cap-1")

    assert await checkpointer.record(holder, "captured", "cap-1") == Recorded(value="cap-1", first=True)


async def test_a_workflows_two_keys_share_a_slot_so_a_script_may_touch_both(workflow: str) -> None:
    # The hash tag is not decoration: `record` reads the claim and writes the steps in
    # one script, which Redis Cluster refuses unless both keys hash to the same slot.
    # `key_slot` is the same function the cluster client routes with, so this answers the
    # question without needing a cluster (the compose stack is a single node).
    checkpointer = RedisCheckpointer(redis=Redis())

    steps, claim = checkpointer.hash_key(workflow), checkpointer.pass_key(workflow)

    assert steps == f"workflow:{{{workflow}}}"
    assert claim == f"{steps}:pass"
    assert key_slot(steps.encode()) == key_slot(claim.encode())
    assert key_slot(f"workflow:{workflow}".encode()) != key_slot(f"workflow:{workflow}:pass".encode()), (
        "and without the tag they would land on different nodes, which is what the tag is for"
    )


async def test_the_order_survives_the_hash_leaving_its_listpack_encoding(redis: Redis, workflow: str) -> None:
    # The cross-store suite asserts what `load` owes; this asserts that the arrangement it
    # relies on is real. Redis keeps a hash's insertion order only while it is
    # listpack-encoded, so a store carrying no order at all answers correctly under the
    # threshold, and a test that never crosses it proves nothing. Crossing it here, and
    # checking that the crossing happened, is what makes the ordering assertion meaningful.
    #
    # The value is the lever rather than the field count: `hash-max-listpack-value` is 64
    # bytes, so one write converts the hash, where `hash-max-listpack-entries` takes
    # hundreds and has moved between releases (512 on Redis 8, 128 in older documentation).
    # A test sized against that second default would quietly stop exercising this path when
    # the server changed underneath it.
    checkpointer = RedisCheckpointer(redis=redis)
    holder = await claimed(checkpointer, workflow)
    written = [f"step-{index:02d}" for index in range(8)][::-1]

    for step in written:
        await checkpointer.record(holder, step, "x" * 80)

    steps = checkpointer.hash_key(workflow)

    assert await redis.object("encoding", steps) == "hashtable", "or this test is not reaching the path it exists for"
    assert list(await redis.hgetall(steps)) != written, "and the server's own field order is no longer insertion order"
    assert list(await checkpointer.load(workflow)) == written


async def test_an_appended_entry_carries_the_same_position_its_key_is_built_from(redis: Redis, workflow: str) -> None:
    # `HLEN` does double duty in `APPEND`: it is the field's place in the load order and
    # the number the key is built from. Reading them back apart is what says they cannot
    # drift, since a script that took the position after the write would name the field
    # from one number and pack it with another, and `load` would then sort the inbox into
    # an order its own keys contradict.
    checkpointer = RedisCheckpointer(redis=redis)
    await checkpointer.record(await claimed(checkpointer, workflow), "a-step", "a value")

    entry = await checkpointer.append(workflow, "a message")

    packed = cast(str, await redis.hget(checkpointer.hash_key(workflow), entry.key))
    position, _at, _encoded = packed.split(":", 2)
    assert entry.key.endswith(position.zfill(INBOX_DIGITS))
    assert int(position) == 1, "the step written first took position zero"


async def test_a_field_carries_its_position_and_its_time_in_front_of_the_encoding(
    redis: Redis,
    workflow: str,
) -> None:
    # The packed format, read raw, because it is the one thing a hash cannot carry for
    # itself: a field has no metadata and `HGETALL` order is unspecified past the listpack
    # threshold, so both the position and the time have to be *in* the value. Reading the
    # field back apart is what says the two numbers are there and in that order, where
    # `history` alone would pass against a store that had guessed the time in Python.
    checkpointer = RedisCheckpointer(redis=redis)
    holder = await claimed(checkpointer, workflow)

    await checkpointer.record(holder, "charged", "ch-1")

    packed = cast(str, await redis.hget(checkpointer.hash_key(workflow), "charged"))
    position, at, encoded = packed.split(":", 2)
    stamped = datetime.fromtimestamp(int(at) / 1000, UTC)
    assert int(position) == 0
    assert encoded == checkpointer.codec.encode("ch-1"), "and the codec's own text is what is left after the two"
    # Loosely, because this is the server's clock rather than this process's: what is being
    # asserted is that the number is a moment now, not that two machines agree.
    assert abs(stamped - now_utc()) < timedelta(minutes=5)
    assert (await checkpointer.history(workflow))["charged"].at == stamped


async def test_a_store_error_that_is_not_the_fence_is_not_swallowed(redis: Redis, workflow: str) -> None:
    # `record` forgives exactly one error, the script's own refusal. Anything else is a
    # real problem with the store, and reading it as "another pass took over" would tell
    # a workflow to stand down when the truth is that its checkpoint is unusable.
    checkpointer = RedisCheckpointer(redis=redis)
    holder = await claimed(checkpointer, workflow)
    await redis.set(checkpointer.hash_key(workflow), "not a hash at all")

    with pytest.raises(ResponseError, match="WRONGTYPE"):
        await checkpointer.record(holder, "paid", "pay-1")


@pytest.mark.parametrize(
    ("message", "refusal"),
    [
        ("FENCED pass 3 superseded by 4", True),
        ("WRONGTYPE Operation against a key holding the wrong kind of value", False),
        # The reason the check is anchored rather than a search: the word can reach a
        # message as *data* (a workflow id, a step's own encoding quoted back by a script
        # error), and reading that as a fence would tell a healthy pass to stand down over
        # a store that is actually broken.
        ("ERR user_script:1: cannot encode the value 'FENCED' script: 3d4f", False),
    ],
)
def test_only_a_refusal_that_leads_with_the_code_is_read_as_a_fence(message: str, refusal: bool) -> None:
    assert fenced(ResponseError(message)) is refusal


async def test_a_second_worker_preparing_the_same_queue_is_not_an_error(redis: Redis, workflow: str) -> None:
    # Every worker prepares the queue at boot, so all but the first find the consumer
    # group already there. Redis reports that as an error; here it is the normal case.
    scheduler = RedisStreamScheduler(redis=redis, namespace=workflow)
    await scheduler.prepare()
    await scheduler.prepare()

    await scheduler.make_ready("wf-after-two-prepares")
    delivered = await scheduler.next_ready(timedelta(seconds=1))

    assert delivered is not None
    assert delivered.workflow == "wf-after-two-prepares"
    assert await scheduler.next_ready(timedelta(milliseconds=50)) is None, "and nothing is delivered twice"


async def test_trimming_drops_what_has_been_answered_for_and_keeps_the_rest(
    redis: Redis,
    workflow: str,
) -> None:
    # The bound on a stream that only ever grows. `XACK` clears the pending list and
    # leaves the entry, so without this the queue is correct and unbounded.
    scheduler = RedisStreamScheduler(redis=redis, namespace=workflow)
    await scheduler.prepare()
    for each in ("wf-a", "wf-b", "wf-c"):
        await scheduler.make_ready(each)

    answered = await scheduler.next_ready(timedelta(seconds=1))
    assert answered is not None
    await scheduler.done(answered)
    held = await scheduler.next_ready(timedelta(seconds=1))  # taken, and deliberately not acked

    assert await scheduler.trim() == 1, "one entry has been answered for"
    assert await redis.xlen(scheduler.ready_key) == 2

    assert held is not None
    await scheduler.done(held)

    assert await scheduler.trim() == 1, "and the one that was pending goes once it is acked"
    assert await redis.xlen(scheduler.ready_key) == 1, "the unread entry stays, which is the whole point"


async def test_trimming_a_queue_no_worker_has_read_yet_removes_nothing(
    redis: Redis,
    workflow: str,
) -> None:
    # The one hazard in `XTRIM ... ACKED`: with no consumer group it has no effect, so the
    # trim degrades to a plain `MAXLEN 0` and would delete work nobody has run. An order
    # submitted before the first worker boots is exactly that case, and `prepare` creating
    # its group from `0` is what makes such an order deliverable rather than stranded.
    scheduler = RedisStreamScheduler(redis=redis, namespace=workflow)
    await scheduler.make_ready("wf-submitted-before-any-worker")

    assert await scheduler.trim() == 0
    assert await redis.xlen(scheduler.ready_key) == 1, "the order survived the trimmer that ran before the worker"

    await scheduler.prepare()
    delivered = await scheduler.next_ready(timedelta(seconds=1))

    assert delivered is not None
    assert delivered.workflow == "wf-submitted-before-any-worker"


async def test_trimming_a_queue_that_does_not_exist_yet_is_not_an_error(redis: Redis, workflow: str) -> None:
    # A trimmer started beside the worker rather than after it, which is how it would
    # actually be deployed.
    assert await RedisStreamScheduler(redis=redis, namespace=workflow).trim() == 0


async def test_a_trim_error_that_is_not_a_missing_stream_is_not_swallowed(redis: Redis, workflow: str) -> None:
    scheduler = RedisStreamScheduler(redis=redis, namespace=workflow)
    await redis.set(scheduler.ready_key, "not a stream at all")

    with pytest.raises(ResponseError, match="WRONGTYPE"):
        await scheduler.trim()


async def test_the_trimmer_keeps_running_until_it_is_cancelled(redis: Redis, workflow: str) -> None:
    # Control plane, not data plane: its own loop on its own interval, so housekeeping
    # does not cost more the busier the queue gets.
    scheduler = RedisStreamScheduler(redis=redis, namespace=workflow)
    await scheduler.prepare()
    await scheduler.make_ready("wf-tidied")
    answered = await scheduler.next_ready(timedelta(seconds=1))
    assert answered is not None
    await scheduler.done(answered)

    async def tidy() -> None:
        await trimming(scheduler)(ticks(timedelta(milliseconds=10)))

    tidying = asyncio.create_task(tidy())
    try:
        async with asyncio.timeout(5):
            while await redis.xlen(scheduler.ready_key):
                await asyncio.sleep(0.01)
    finally:
        tidying.cancel()
        with suppress(asyncio.CancelledError):
            await tidying


async def test_an_effect_in_this_redis_is_performed_and_recorded_in_one_commit(
    redis: Redis,
    workflow: str,
) -> None:
    # Exactly-once, on Redis, which the usual framing says needs Postgres. It does not:
    # a Lua script is an atomic commit over Redis data, so a step whose effect *is* a
    # Redis write records itself in the same script. What actually bounds this is that
    # you can only transact within one datastore, and Postgres is only privileged because
    # that is usually where the data already is.
    checkpointer = RedisCheckpointer(redis=redis)
    holder = await claimed(checkpointer, workflow)
    # Tagged into the workflow's own slot, which is what "the same datastore" reduces to
    # once the datastore is partitioned.
    ledger = f"{checkpointer.hash_key(workflow)}:ledger"
    reserve = LuaEffect(
        source="return cjson.encode(redis.call('HINCRBY', KEYS[1], ARGV[1], tonumber(ARGV[2])))",
        keys=(ledger,),
        args=("piano", 1),
    )

    first = await Run(holder=holder, checkpointer=checkpointer, recorded={}).transact("reserved", reserve, as_count)
    again = await Run(holder=holder, checkpointer=checkpointer, recorded={}).transact("reserved", reserve, as_count)

    assert (first, again) == (1, 1), "the second pass read the record rather than reserving again"
    assert await redis.hget(ledger, "piano") == "1", "the stock moved once, however many passes reached the step"
    assert await checkpointer.load(workflow) == {"reserved": 1}
    assert key_slot(ledger.encode()) == key_slot(checkpointer.hash_key(workflow).encode())


async def test_a_transacted_effect_is_refused_from_a_superseded_pass(redis: Redis, workflow: str) -> None:
    checkpointer = RedisCheckpointer(redis=redis)
    stalled = await claimed(checkpointer, workflow, timedelta(milliseconds=1))
    await asyncio.sleep(0.05)
    await claimed(checkpointer, workflow)
    ledger = f"{checkpointer.hash_key(workflow)}:ledger"

    with pytest.raises(Fenced):
        await Run(holder=stalled, checkpointer=checkpointer, recorded={}).transact(
            "reserved",
            LuaEffect(
                source="return cjson.encode(redis.call('HINCRBY', KEYS[1], ARGV[1], 1))",
                keys=(ledger,),
                args=("piano",),
            ),
            as_count,
        )

    assert await redis.exists(ledger) == 0, "the fence is checked before the effect runs, not after"


async def test_a_transact_error_that_is_not_the_fence_is_not_swallowed(redis: Redis, workflow: str) -> None:
    # As for `record`: the script's own refusal is the one error this reads, and taking
    # a broken store for "another pass took over" would tell a workflow to stand down
    # when the truth is that its checkpoint is unusable.
    checkpointer = RedisCheckpointer(redis=redis)
    holder = await claimed(checkpointer, workflow)
    await redis.set(checkpointer.hash_key(workflow), "not a hash at all")

    with pytest.raises(ResponseError, match="WRONGTYPE"):
        await Run(holder=holder, checkpointer=checkpointer, recorded={}).transact(
            "reserved",
            LuaEffect(source="return cjson.encode(1)"),
            as_count,
        )


def scheduled(redis: Redis, workflow: str, lease: timedelta = timedelta(seconds=30)) -> RedisSetScheduler:
    return RedisSetScheduler(redis=redis, namespace=workflow, lease=lease)


BRIEFLY = timedelta(milliseconds=200)


async def test_a_workflow_taken_off_the_schedule_is_invisible_for_its_lease(redis: Redis, workflow: str) -> None:
    queue = scheduled(redis, workflow)
    await queue.make_ready(workflow)

    taken = await queue.next_ready(BRIEFLY)

    assert taken is not None
    assert await queue.next_ready(timedelta(milliseconds=50)) is None, "one taker at a time, as a consumer group is"


async def test_a_wakeup_arriving_mid_pass_survives_the_pass_that_was_running(redis: Redis, workflow: str) -> None:
    # The lost-wakeup bug this design has to answer for, and the reason `done` compares
    # scores. A stream cannot lose it (every `make_ready` is a new entry); a sorted set
    # holds one entry per workflow, so the wakeup lands on the entry the pass is holding
    # and removing that entry afterwards would throw it away.
    queue = scheduled(redis, workflow)
    await queue.make_ready(workflow)
    held = await queue.next_ready(BRIEFLY)
    assert held is not None

    await queue.make_ready(workflow)  # a confirmation, while the pass is still running
    await queue.done(held)

    assert await queue.next_ready(BRIEFLY) is not None, "the pass that finished did not clean up someone else's wakeup"


async def test_a_deadline_set_during_a_pass_outlives_that_passs_acknowledgement(
    redis: Redis,
    workflow: str,
) -> None:
    # The worker schedules and then acknowledges, in that order, so the acknowledgement
    # must not undo the scheduling. The score comparison is what allows those to stay two
    # calls instead of one.
    queue = scheduled(redis, workflow)
    await queue.make_ready(workflow)
    held = await queue.next_ready(BRIEFLY)
    assert held is not None

    await queue.wake_at(held, now_utc() + timedelta(milliseconds=150))
    await queue.done(held)  # a stale acknowledgement, which the deadline it wrote makes inert

    assert await queue.next_ready(timedelta(milliseconds=50)) is None, "not due yet"
    assert await queue.next_ready(timedelta(seconds=2)) is not None, "and still there when it is"


async def test_an_abandoned_workflow_comes_back_without_anyone_reclaiming_it(redis: Redis, workflow: str) -> None:
    # Two views of one schedule, differing only in how long each holds what it takes.
    # The short one stands in for the worker that dies; the patient one for whoever finds
    # the work afterwards, and its long lease is what keeps the last assertion about the
    # score comparison rather than about how fast the assertions above it ran.
    dying = scheduled(redis, workflow, lease=timedelta(milliseconds=50))
    surviving = scheduled(redis, workflow, lease=timedelta(seconds=30))
    await dying.make_ready(workflow)
    abandoned = await dying.next_ready(BRIEFLY)
    assert abandoned is not None

    taken_over = await surviving.next_ready(timedelta(seconds=2))

    assert taken_over is not None
    assert taken_over.receipt != abandoned.receipt, "a fresh lease, so the old holder no longer owns it"
    assert await dying.reclaim(timedelta()) is None, "and nothing had to go looking for it"

    await dying.done(abandoned)

    assert await surviving.next_ready(timedelta(milliseconds=50)) is None, (
        "the overrun worker finishing does not drop what its successor is holding"
    )


async def test_the_schedule_holds_one_entry_per_workflow_however_many_wakeups_arrive(
    redis: Redis,
    workflow: str,
) -> None:
    # Where the two queues differ in kind: a stream grows an entry per wakeup and needs
    # trimming, a sorted set holds the workflow once and needs none.
    queue = scheduled(redis, workflow)

    for _ in range(5):
        await queue.make_ready(workflow)

    assert await redis.zcard(queue.schedule_key) == 1
    assert await queue.wake_due(now_utc()) == (), "being due and being ready are the same score"
    await queue.prepare()  # nothing to create


async def test_a_queue_key_holding_something_other_than_a_stream_fails_loudly(redis: Redis, workflow: str) -> None:
    # `prepare` forgives exactly one error, the one that means "another worker got here
    # first". Anything else is a real problem with the queue and must not be swallowed.
    scheduler = RedisStreamScheduler(redis=redis, namespace=workflow)
    await redis.set(scheduler.ready_key, "not a stream at all")

    with pytest.raises(ResponseError, match="WRONGTYPE"):
        await scheduler.prepare()


async def test_a_sleeping_workflow_is_moved_to_the_stream_when_it_comes_due(
    redis: Redis,
    workflow: str,
) -> None:
    # The move that has to be one operation: off the sleepers and onto the stream. As two
    # calls, a timer that died in between would leave the workflow in neither structure
    # and asleep forever, which is why it is a script.
    scheduler = RedisStreamScheduler(redis=redis, namespace=workflow)
    await scheduler.prepare()
    await scheduler.make_ready("wf-sleeping")
    held = await scheduler.next_ready(BRIEFLY)
    assert held is not None
    await scheduler.wake_at(held, now_utc() + timedelta(seconds=30))

    assert await scheduler.wake_due(now_utc()) == (), "not due yet, so it stays on the sleepers"
    assert await redis.zcard(scheduler.sleeping_key) == 1

    assert await scheduler.wake_due(now_utc() + timedelta(minutes=1)) == ("wf-sleeping",)
    assert await redis.zcard(scheduler.sleeping_key) == 0, "and it is off the sleepers, not in both"

    delivered = await scheduler.next_ready(timedelta(seconds=1))

    assert delivered is not None
    assert delivered.workflow == "wf-sleeping"


async def test_a_discarded_workflow_loses_its_hash_and_keeps_a_token_that_outranks_it(
    redis: Redis,
    workflow: str,
) -> None:
    # The two keys go opposite ways, which is why `discard` is a script and not a `DEL` of
    # both. The steps hash goes entirely, so the next write to this id starts from a count
    # nothing has taken; the pass hash stays with its token raised, so a pass still holding
    # one is refused rather than outranking the workflow that takes the id next.
    checkpointer = RedisCheckpointer(redis=redis)
    holder = await claimed(checkpointer, workflow)
    await checkpointer.record(holder, "charged", "ch-1")

    assert await checkpointer.discard(workflow) == 1

    assert await redis.exists(checkpointer.hash_key(workflow)) == 0, "the fields are gone with the key, not one by one"
    superseded = int(cast(str, await redis.hget(checkpointer.pass_key(workflow), "token")))
    assert superseded > holder.token
    assert int(cast(str, await redis.hget(checkpointer.pass_key(workflow), "until"))) == 0, "and it is claimable again"
    assert await redis.ttl(checkpointer.pass_key(workflow)) > 0, "so the tombstone expires rather than being swept"


async def test_a_workflow_nobody_claimed_leaves_no_tombstone_when_it_is_discarded(
    redis: Redis,
    workflow: str,
) -> None:
    # A `Pass` is only ever handed out by a `claim` that wrote a token, so where there is no
    # token there is no pass to fence. Minting one here would leave a key behind for a
    # workflow that never ran, which is what an operator sweeping ids it is unsure about
    # would do to every one of them.
    checkpointer = RedisCheckpointer(redis=redis)
    await checkpointer.supply(workflow, "approved", True)

    assert await checkpointer.discard(workflow) == 1

    assert await redis.exists(checkpointer.pass_key(workflow)) == 0


async def test_cancelling_walks_the_stream_past_the_entries_it_is_not_looking_for(
    redis: Redis,
    workflow: str,
) -> None:
    # The expensive half of `cancel` on this queue, driven at a batch size of one so the
    # scan takes several round trips and lands on entries belonging to other workflows. A
    # stream is addressed by entry id and an id says only *when* an entry was appended, so
    # finding a workflow's entries means reading them; what this pins is that the walk makes
    # progress past a batch it deletes nothing from, which is where a cursor that was
    # inclusive rather than exclusive would loop forever.
    scheduler = RedisStreamScheduler(redis=redis, namespace=workflow, scan=1)
    await scheduler.prepare()
    for turn in range(3):
        await scheduler.make_ready("wf-doomed")
        await scheduler.make_ready(f"wf-spared-{turn}")

    await scheduler.cancel("wf-doomed")

    left = cast(list[tuple[str, dict[str, str]]], await redis.xrange(scheduler.ready_key))
    assert [fields["workflow"] for _entry, fields in left] == ["wf-spared-0", "wf-spared-1", "wf-spared-2"]


async def test_cancelling_takes_a_workflow_off_the_sleepers_as_well_as_the_stream(
    redis: Redis,
    workflow: str,
) -> None:
    # Two structures here where every other queue has one, so a cancel has to reach both: a
    # workflow waiting on a clock is in the sorted set and nowhere near the stream, and
    # leaving it there is a deleted workflow that wakes up on schedule.
    scheduler = RedisStreamScheduler(redis=redis, namespace=workflow)
    await scheduler.prepare()
    await scheduler.make_ready("wf-sleeping")
    held = await scheduler.next_ready(BRIEFLY)
    assert held is not None
    await scheduler.wake_at(held, now_utc() + timedelta(seconds=30))
    assert await redis.zcard(scheduler.sleeping_key) == 1

    await scheduler.cancel("wf-sleeping")

    assert await redis.zcard(scheduler.sleeping_key) == 0
    assert await scheduler.wake_due(now_utc() + timedelta(minutes=1)) == ()


async def test_cancelling_a_sorted_set_entry_removes_every_meaning_its_score_could_have(
    redis: Redis,
    workflow: str,
) -> None:
    # One `ZREM` where the stream needs a scan, which is the sorted set's side of the trade
    # that whole design makes: a workflow appears once, so queued, sleeping, and out with a
    # worker are one entry differing only in its score.
    queue = scheduled(redis, workflow)
    await queue.make_ready(workflow)
    held = await queue.next_ready(BRIEFLY)
    assert held is not None

    await queue.cancel(workflow)

    assert await redis.zcard(queue.schedule_key) == 0
    # And the pass still running cannot put it back, since the score it took is no longer
    # there to compare against.
    await queue.wake_at(held, now_utc() - timedelta(seconds=1))
    assert await redis.zcard(queue.schedule_key) == 0
    assert await queue.next_ready(timedelta(milliseconds=50)) is None


async def test_a_delivery_a_dead_worker_never_answered_for_is_taken_over(
    redis: Redis,
    workflow: str,
) -> None:
    # The crash the pending list exists to survive. `XREADGROUP` moves an entry into the
    # consumer's pending list rather than deleting it, so a worker that dies mid-pass
    # leaves the wakeup to be reclaimed instead of taking it to the grave.
    dying = RedisStreamScheduler(redis=redis, namespace=workflow)
    await dying.prepare()
    await dying.make_ready("wf-abandoned")
    abandoned = await dying.next_ready(timedelta(seconds=1))
    assert abandoned is not None  # taken, and deliberately never acknowledged

    surviving = RedisStreamScheduler(redis=redis, namespace=workflow)

    assert await surviving.next_ready(timedelta(milliseconds=50)) is None, "no new entries to read"

    taken_over = await surviving.reclaim(timedelta())

    assert taken_over is not None
    assert taken_over.receipt == abandoned.receipt, "the same delivery, taken over rather than duplicated"
    assert await surviving.reclaim(timedelta(minutes=1)) is None, "and nothing else is outstanding for long"


async def test_the_two_queue_keys_share_a_slot_so_the_timers_script_may_touch_both(workflow: str) -> None:
    # `wake_due` is a script over both of these, because taking a workflow off the
    # sleepers and appending it to the stream are durable only together. Redis Cluster
    # refuses a script whose keys hash to two slots, so untagged this is not a queue that
    # degrades on a cluster but one whose timer cannot run at all, taking the worker with
    # it: the timer and the pass loop share a task group.
    queue = RedisStreamScheduler(redis=Redis(), namespace=workflow)

    assert key_slot(queue.ready_key.encode()) == key_slot(queue.sleeping_key.encode())


@pytest.mark.parametrize(
    ("duration", "in_milliseconds", "in_seconds"),
    [
        (timedelta(microseconds=500), 1, 1),
        (timedelta(milliseconds=1500), 1500, 2),
        (timedelta(minutes=1), 60_000, 60),
        (timedelta(), 0, 0),
    ],
    ids=["under a unit", "part of a unit", "whole units", "none at all"],
)
def test_a_duration_is_rendered_without_becoming_one_of_redis_sentinels(
    duration: timedelta,
    in_milliseconds: int,
    in_seconds: int,
) -> None:
    # Every duration this store sends crosses the wire as a whole number of Redis's own
    # units, and each of those units has a zero that means something other than "no time
    # at all": `EXPIRE key 0` deletes the key, `BLOCK 0` blocks forever, and a lease of
    # zero milliseconds writes a claim that has already lapsed. Truncating would turn a
    # duration a caller was careful to make positive into whichever sentinel it landed on,
    # so anything positive rounds up to one unit.
    #
    # A genuine zero survives it, and has to: `reclaim`'s `idle` is a threshold rather
    # than an interval, and zero there means take over anything outstanding.
    assert (milliseconds(duration), seconds(duration)) == (in_milliseconds, in_seconds)


async def test_a_checkpoint_ttl_shorter_than_a_second_does_not_delete_what_it_expires(
    redis: Redis,
    workflow: str,
) -> None:
    # The same truncation, at its worst: `EXPIRE key 0` does not expire a key soon, it
    # deletes it now, so a sub-second TTL would erase every claim and every record as it
    # was written.
    checkpointer = RedisCheckpointer(redis=redis, ttl=timedelta(milliseconds=500))
    holder = await claimed(checkpointer, workflow)

    assert await checkpointer.record(holder, "paid", "pay-1") == Recorded(value="pay-1", first=True)
    assert await checkpointer.load(workflow) == {"paid": "pay-1"}


async def test_a_blocking_read_shorter_than_a_millisecond_still_comes_back(
    redis: Redis,
    workflow: str,
) -> None:
    # `BLOCK 0` is not a short wait but an unbounded one, and the bound exists precisely
    # so a cancelled worker can notice. Truncated, the most careful caller here (one
    # asking for the shortest possible wait) is the one that hangs forever.
    queue = RedisStreamScheduler(redis=redis, namespace=workflow)
    await queue.prepare()

    async with asyncio.timeout(2):
        assert await queue.next_ready(timedelta(microseconds=500)) is None


async def test_a_client_speaking_the_newer_protocol_reads_the_queue_the_same_way(
    redis: Redis,
    workflow: str,
) -> None:
    # `decode_responses` is a documented requirement; the protocol is not, and a client
    # built with `protocol=3` is an ordinary thing for an application to do. redis-py
    # hands back what the wire gave it, which for `XREADGROUP` is a mapping under RESP3
    # and a list of pairs under RESP2, so a worker that reads only one shape dies on its
    # first pull against a client it never said anything about.
    reached = redis.connection_pool.connection_kwargs
    client = Redis(host=reached["host"], port=reached["port"], decode_responses=True, protocol=3)
    try:
        queue = RedisStreamScheduler(redis=client, namespace=workflow)
        await queue.prepare()
        await queue.make_ready("wf-resp3")

        delivered = await queue.next_ready(timedelta(seconds=1))

        assert delivered is not None
        assert delivered.workflow == "wf-resp3"
        await queue.done(delivered)
    finally:
        await client.aclose()


async def test_a_deadline_landing_on_the_leases_own_millisecond_survives_the_acknowledgement(
    redis: Redis,
    workflow: str,
) -> None:
    # The receipt is a score, and a score is a number rather than a version, so a deadline
    # that happens to land on the millisecond this pass's lease expires is *the same
    # number*: the comparison in `done` passes and the acknowledgement removes the entry
    # the wakeup just wrote. Not a remote coincidence, either, since a workflow sleeping
    # for the lease under a scheduler holding the same lease lands a millisecond or two
    # away on every pass.
    queue = RedisSetScheduler(redis=redis, namespace=workflow)
    await queue.make_ready("wf-colliding")
    delivery = await queue.next_ready(BRIEFLY)
    assert delivery is not None
    colliding = datetime.fromtimestamp(float(delivery.receipt) / 1000, UTC)

    await queue.wake_at(delivery, colliding)
    await queue.done(delivery)  # a stale acknowledgement, arriving after the deadline was written

    assert await redis.zscore(queue.schedule_key, "wf-colliding") is not None, (
        "the acknowledgement must not undo a wakeup that arrived while the pass was ending"
    )


async def test_a_delivery_behind_a_workers_own_pending_entries_is_still_taken_over(
    redis: Redis,
    workflow: str,
) -> None:
    # `XAUTOCLAIM` bounds its own work: it scans about ten times `count` pending entries
    # and then stops, handing back where it got to. Asking from the beginning every time
    # therefore gives up after ten and reports that nothing was abandoned, while a dead
    # worker's deliveries sit behind them. The worker arranges that for itself, since a
    # pool of twenty holds twenty entries whose idle clocks it keeps resetting.
    abandoned = timedelta(milliseconds=500)
    dying = RedisStreamScheduler(redis=redis, namespace=workflow)
    await dying.prepare()
    for number in range(12):
        await dying.make_ready(f"wf-{number}")
        assert await dying.next_ready(BRIEFLY) is not None  # taken, and never acknowledged

    # Long enough that every one of them is idle by the threshold below, so what the
    # rescuer can reach is a question about the scan rather than about the clock.
    await asyncio.sleep(abandoned.total_seconds() * 1.2)
    surviving = RedisStreamScheduler(redis=redis, namespace=workflow)
    taken = [delivery.workflow for _ in range(10) if (delivery := await surviving.reclaim(abandoned)) is not None]

    assert len(taken) == 10, "the ten at the front come over one pull at a time, as a pool of twenty would take them"
    # Each of those ten reset its own idle clock as it was claimed, so the front of the
    # pending list is now this worker's and not idle enough to take again. What is left is
    # exactly the shape that made `reclaim` give up: two abandoned deliveries behind ten
    # entries a scan bounded at ten never reaches.
    behind = await surviving.reclaim(abandoned)

    assert behind is not None, "the deliveries behind this worker's own entries are still abandoned work"
    assert behind.workflow not in taken


async def test_a_wakeup_that_arrived_during_a_pass_is_not_overwritten_by_its_deadline(
    redis: Redis,
    workflow: str,
) -> None:
    # A workflow holds one member here, so a deadline written unconditionally lands on top
    # of a `make_ready` that arrived while the pass was ending, and a confirmation waits
    # out a settlement window it should have interrupted. The receipt is what tells the
    # two apart: anything that asked for another pass wrote a different score.
    queue = RedisSetScheduler(redis=redis, namespace=workflow)
    await queue.make_ready("wf-confirmed")
    held = await queue.next_ready(BRIEFLY)
    assert held is not None

    await queue.make_ready("wf-confirmed")  # the confirmation, arriving mid-pass
    await queue.wake_at(held, now_utc() + timedelta(days=3))

    assert await queue.next_ready(BRIEFLY) is not None, "the wakeup that arrived is still due now"
