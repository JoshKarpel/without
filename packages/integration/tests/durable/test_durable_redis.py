from __future__ import annotations

import asyncio
import json
import os
from collections.abc import AsyncIterator
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Mapping
from contextlib import suppress
from dataclasses import dataclass
from dataclasses import field
from datetime import timedelta
from uuid import uuid4

import httpx
import pytest
from integration.durable import Contended
from integration.durable import Fenced
from integration.durable import Order
from integration.durable import Payouts
from integration.durable import Reached
from integration.durable import RedisCheckpoints
from integration.durable import RedisSchedule
from integration.durable import Run
from integration.durable import Services
from integration.durable import Suspended
from integration.durable import Wakeups
from integration.durable import claimed
from integration.durable import fulfilment
from integration.durable import now_utc
from integration.durable import pay_out
from integration.durable import resume
from integration.durable import run_saga
from integration.durable import unwinding
from integration.durable.api import Payments
from integration.durable.api import payments_app
from integration.durable.wakeups import RedisWakeups
from integration.durable.worker import work
from redis.asyncio import Redis
from redis.crc import key_slot
from redis.exceptions import ResponseError
from without_dag import CompiledGraph
from without_http import serving

# `just test` starts the services in compose.yaml and publishes each address; these
# tests drive the real server it started rather than a fake, and skip when it did not
# (no podman on this machine, or pytest run directly).
pytestmark = pytest.mark.compose

ORDER = Order(order_id="o-42", sku="gizmo", cents=1999)


async def passing[T](
    checkpoints: RedisCheckpoints,
    workflow: str,
    body: Callable[[Run], Awaitable[T]],
) -> T:
    """One claimed pass, released on the way out, which is what the worker does."""
    holder = await claimed(checkpoints, workflow)
    try:
        return await resume(holder, checkpoints, body)
    finally:
        await checkpoints.release(holder)


async def saga[In, Out, Reaches, Undone](
    forward: CompiledGraph[In, Out],
    unwind: CompiledGraph[Reaches, Undone],
    reaches: Callable[[Mapping[str, object]], Reaches],
    checkpoints: RedisCheckpoints,
    workflow: str,
    value: In,
) -> Out:
    holder = await claimed(checkpoints, workflow)
    try:
        return await run_saga(forward, unwind, reaches, checkpoints, holder, value)
    finally:
        await checkpoints.release(holder)


@pytest.fixture
async def redis() -> AsyncIterator[Redis]:
    published = os.environ.get("WITHOUT_TESTS_REDIS")
    if not published:  # pragma: no cover - the arm that runs is the one where this whole file is uncovered
        pytest.skip("WITHOUT_TESTS_REDIS is unset: run `just test`, which starts the services in compose.yaml")

    # podman-compose reports the published port alone, docker compose the bind address
    # it is published on (`0.0.0.0:32768`), which is a wildcard a client cannot dial.
    # Either way the loopback address is where the port is reachable.
    host, _, port = published.strip().rpartition(":")
    # `decode_responses=True` is the app's call to make, and this app makes it: it owns
    # both ends of every key it touches, so nothing downstream has to ask whether a
    # value came back as bytes.
    client = Redis(host=host or "127.0.0.1", port=int(port), decode_responses=True)
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


@dataclass(slots=True)
class Gateway:
    calls: list[str] = field(default_factory=list)
    broken: set[str] = field(default_factory=set)

    async def perform(self, effect: str, result: str) -> str:
        self.calls.append(effect)
        if effect in self.broken:
            raise RuntimeError(f"{effect} is down")
        return result

    def services(self) -> Services:
        async def charge(order: Order) -> str:
            return await self.perform("charge", f"ch-{order.order_id}")

        async def reserve(order: Order) -> str:
            return await self.perform("reserve", f"rs-{order.sku}")

        async def ship(charge_id: str, reservation_id: str) -> str:
            return await self.perform("ship", f"tr-{charge_id}-{reservation_id}")

        async def refund(charge_id: str) -> str:
            return await self.perform("refund", f"rf-{charge_id}")

        async def release(reservation_id: str) -> str:
            return await self.perform("release", f"rl-{reservation_id}")

        return Services(charge=charge, reserve=reserve, ship=ship, refund=refund, release=release)


async def test_a_workflow_resumes_from_a_checkpoint_left_in_redis_by_a_dead_process(
    redis: Redis,
    workflow: str,
) -> None:
    # The whole point of the string keys, end to end: the first "process" leaves a
    # hash behind, and a second one that shares nothing with it but the workflow's
    # id picks the run up from that hash alone.
    checkpoints = RedisCheckpoints(redis=redis)
    crashed = Gateway(broken={"ship"})
    services = crashed.services()

    with pytest.raises(RuntimeError, match="ship is down"):
        await saga(fulfilment(services), unwinding(services), Reached.of, checkpoints, workflow, ORDER)

    # What an operator sees with `redis-cli`: one hash per workflow, one field per
    # completed step, each holding that step's result as JSON.
    assert set(await redis.hkeys(f"workflow:{{{workflow}}}")) == {"charged", "reserved"}
    assert await redis.hget(f"workflow:{{{workflow}}}", "charged") == '"ch-o-42"'
    assert await redis.ttl(f"workflow:{{{workflow}}}") > 0, "a checkpoint expires on its own rather than being swept"

    recovered = Gateway()
    receipt = await saga(
        fulfilment(recovered.services()),
        unwinding(recovered.services()),
        Reached.of,
        checkpoints,
        workflow,
        ORDER,
    )

    assert receipt["tracking"] == "tr-ch-o-42-rs-gizmo"
    assert recovered.calls == ["ship"], "the charge and the reservation were read back out of Redis"


async def test_a_compensation_is_recorded_under_its_own_key(redis: Redis, workflow: str) -> None:
    checkpoints = RedisCheckpoints(redis=redis)
    gateway = Gateway(broken={"ship"})
    services = gateway.services()

    with pytest.raises(RuntimeError, match="ship is down"):
        await saga(fulfilment(services), unwinding(services), Reached.of, checkpoints, workflow, ORDER)

    assert await checkpoints.load(f"{workflow}:unwind") == {
        "refunded": "rf-ch-o-42",
        "released": "rl-rs-gizmo",
        "unwound": {"refunded": "rf-ch-o-42", "released": "rl-rs-gizmo"},
    }
    assert sorted(gateway.calls[-2:]) == ["refund", "release"]


def paying(calls: list[str]) -> Payouts:
    """A payout's effects, recording each one so a test can see what a pass performed."""

    async def items(order_id: str) -> dict[str, int]:
        calls.append("items")
        return {"piano": 90_000, "stool": 4_000}

    async def capture(sku: str, amount: int) -> str:
        calls.append(f"capture:{sku}")
        return f"cap-{sku}"

    async def pay(order_id: str, total: int) -> str:
        calls.append("pay")
        return f"pay-{total}"

    return Payouts(items=items, capture=capture, pay=pay)


async def test_a_workflow_suspended_on_an_approval_resumes_when_another_process_records_it(
    redis: Redis,
    workflow: str,
) -> None:
    # The stepwise mechanism end to end, and the thing a signal usually needs a server
    # for: the pass that asked for the approval is gone, and what resumes the workflow
    # is one field written into its hash by something that shares nothing with it.
    suspended = RedisCheckpoints(redis=redis)
    asked: list[str] = []

    async def body(run: Run) -> dict[str, object]:
        return await pay_out(run, "ord-42", paying(asked), settling=timedelta(), approval_over=10_000)

    with pytest.raises(Suspended) as suspension:
        await passing(suspended, workflow, body)

    assert suspension.value.key == "approved-by"
    assert set(await redis.hkeys(f"workflow:{{{workflow}}}")) == {
        "items",
        "captured:piano",
        "captured:stool",
        "settling",
    }
    assert asked == ["items", "capture:piano", "capture:stool"], "the money moved, the payout did not"

    approvals = RedisCheckpoints(redis=redis)
    await approvals.supply(workflow, "approved-by", "auditor-7")

    answered: list[str] = []

    async def resumed(run: Run) -> dict[str, object]:
        return await pay_out(run, "ord-42", paying(answered), settling=timedelta(), approval_over=10_000)

    payout = await passing(RedisCheckpoints(redis=redis), workflow, resumed)

    assert payout["approved_by"] == "auditor-7"
    assert payout["captures"] == {"piano": "cap-piano", "stool": "cap-stool"}
    assert answered == ["pay"], "everything before the approval was read back out of Redis"


# The pair, end to end, over a real one-second wait: an API process that only writes,
# a worker process that only reads, and Redis holding everything between them. Its
# budget is generous because the workflow really does sleep for a second.
#
# Run against both queues, which is what makes "drop-in" a claim rather than an
# aspiration: the same API, the same worker, the same assertions, over a stream beside a
# sorted set and over one sorted set. Nothing above `Wakeups` can tell which it has.
@pytest.mark.timeout(30)
@pytest.mark.parametrize("queue", [RedisWakeups, RedisSchedule], ids=["stream", "schedule"])
async def test_an_order_submitted_to_the_api_is_carried_to_payout_by_the_worker(
    redis: Redis,
    workflow: str,
    queue: Callable[..., Wakeups],
) -> None:
    checkpoints = RedisCheckpoints(redis=redis)
    # The queue is namespaced per test so parallel runs do not pop each other's work,
    # which a deployment would not do: there, sharing one queue *is* how work spreads.
    wakeups = queue(redis=redis, namespace=workflow)
    worker = asyncio.create_task(work(checkpoints, wakeups))

    try:
        async with (
            serving(payments_app(Payments(checkpoints=checkpoints, wakeups=wakeups))) as server,
            httpx.AsyncClient(base_url=f"http://{server.host}:{server.port}") as client,
        ):
            submitted = await client.post(
                "/orders",
                json={"items": {"piano": 90_000, "stool": 4_000}},
                headers={"idempotency-key": workflow},
            )
            assert submitted.status_code == 202

            # The captures happen at once; the payout then waits out its settlement
            # window, and the worker is the only thing that knows the window exists.
            recorded = await until(client, workflow, lambda state: "settling" in state["recorded"])
            assert set(recorded) == {"order", "items", "captured:piano", "captured:stool", "settling"}

            # Past the deadline the timer makes it ready again, the pass runs, and it
            # stops on the confirmation, where nothing but a person can move it.
            await asyncio.sleep(1.5)
            held = await client.get(f"/orders/{workflow}")
            assert json.loads(held.text)["done"] is False

            confirmed = await client.post(f"/orders/{workflow}/confirmation", json={"approved_by": "auditor-7"})
            assert confirmed.status_code == 202

            paid = await until(client, workflow, lambda state: state["done"])

        assert paid["approved-by"] == "auditor-7"
        assert paid["paid"] == f"pay-{workflow}-94000"
    finally:
        worker.cancel()
        with suppress(asyncio.CancelledError):
            await worker


async def until(client: httpx.AsyncClient, workflow: str, reached: object) -> dict[str, object]:
    """Watch a workflow the way a UI would: poll its status until it says what we want."""
    while True:
        state = json.loads((await client.get(f"/orders/{workflow}")).text)
        if reached(state):  # type: ignore[operator]
            recorded: dict[str, object] = state["recorded"]
            return recorded
        await asyncio.sleep(0.05)


async def test_only_one_of_many_processes_racing_for_a_workflow_gets_to_pass_over_it(
    redis: Redis,
    workflow: str,
) -> None:
    # The guarantee against the real server, where it has to hold across processes that
    # share nothing: every client registers the same script, and Redis is the only party
    # that sees all of them, which is why the check and the take have to happen there.
    racing = [RedisCheckpoints(redis=redis) for _ in range(8)]

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
    checkpoints = RedisCheckpoints(redis=redis)
    stalled = await claimed(checkpoints, workflow, timedelta(milliseconds=1))
    await asyncio.sleep(0.05)
    took_over = await claimed(checkpoints, workflow)

    with pytest.raises(Fenced, match="moved on while this pass held it"):
        await checkpoints.record(stalled, "paid", "pay-from-the-dead")

    assert await checkpoints.record(took_over, "paid", "pay-real") == "pay-real"
    assert await checkpoints.load(workflow) == {"paid": "pay-real"}


async def test_a_step_already_recorded_is_never_overwritten_and_hands_back_the_winner(
    redis: Redis,
    workflow: str,
) -> None:
    checkpoints = RedisCheckpoints(redis=redis)
    holder = await claimed(checkpoints, workflow)

    assert await checkpoints.record(holder, "captured:piano", "cap-first") == "cap-first"
    assert await checkpoints.record(holder, "captured:piano", "cap-second") == "cap-first"
    assert await checkpoints.supply(workflow, "captured:piano", "cap-third") == "cap-first"
    assert await checkpoints.load(workflow) == {"captured:piano": "cap-first"}


async def test_a_workflows_two_keys_share_a_slot_so_a_script_may_touch_both(workflow: str) -> None:
    # The hash tag is not decoration: `record` reads the claim and writes the steps in
    # one script, which Redis Cluster refuses unless both keys hash to the same slot.
    # `key_slot` is the same function the cluster client routes with, so this answers the
    # question without needing a cluster (the compose stack is a single node).
    checkpoints = RedisCheckpoints(redis=Redis())

    steps, claim = checkpoints.hash_key(workflow), checkpoints.pass_key(workflow)

    assert steps == f"workflow:{{{workflow}}}"
    assert claim == f"{steps}:pass"
    assert key_slot(steps.encode()) == key_slot(claim.encode())
    assert key_slot(f"workflow:{workflow}".encode()) != key_slot(f"workflow:{workflow}:pass".encode()), (
        "and without the tag they would land on different nodes, which is what the tag is for"
    )


async def test_a_store_error_that_is_not_the_fence_is_not_swallowed(redis: Redis, workflow: str) -> None:
    # `record` forgives exactly one error, the script's own refusal. Anything else is a
    # real problem with the store, and reading it as "another pass took over" would tell
    # a workflow to stand down when the truth is that its checkpoint is unusable.
    checkpoints = RedisCheckpoints(redis=redis)
    holder = await claimed(checkpoints, workflow)
    await redis.set(checkpoints.hash_key(workflow), "not a hash at all")

    with pytest.raises(ResponseError, match="WRONGTYPE"):
        await checkpoints.record(holder, "paid", "pay-1")


async def test_a_second_worker_preparing_the_same_queue_is_not_an_error(redis: Redis, workflow: str) -> None:
    # Every worker prepares the queue at boot, so all but the first find the consumer
    # group already there. Redis reports that as an error; here it is the normal case.
    wakeups = RedisWakeups(redis=redis, namespace=workflow)
    await wakeups.prepare()
    await wakeups.prepare()

    await wakeups.make_ready("wf-after-two-prepares")
    delivered = await wakeups.next_ready(timedelta(seconds=1))

    assert delivered is not None
    assert delivered.workflow == "wf-after-two-prepares"
    assert await wakeups.next_ready(timedelta(milliseconds=50)) is None, "and nothing is delivered twice"


def scheduled(redis: Redis, workflow: str, lease: timedelta = timedelta(seconds=30)) -> RedisSchedule:
    return RedisSchedule(redis=redis, namespace=workflow, lease=lease)


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

    await queue.wake_at(workflow, now_utc() + timedelta(milliseconds=150))
    await queue.done(held)

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
    await redis.set(f"{workflow}:ready", "not a stream at all")
    wakeups = RedisWakeups(redis=redis, namespace=workflow)

    with pytest.raises(ResponseError, match="WRONGTYPE"):
        await wakeups.prepare()
