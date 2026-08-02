from __future__ import annotations

import asyncio
import json
import os
from collections.abc import AsyncIterator
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Mapping
from contextlib import suppress
from datetime import timedelta
from uuid import uuid4

import httpx
import pytest
from gateways import Gateway
from gateways import paying
from gateways import until
from integration.durable import Contended
from integration.durable import Fenced
from integration.durable import Order
from integration.durable import PostgresCheckpoints
from integration.durable import PostgresSchedule
from integration.durable import Reached
from integration.durable import Run
from integration.durable import Suspended
from integration.durable import claimed
from integration.durable import fulfilment
from integration.durable import now_utc
from integration.durable import pay_out
from integration.durable import resume
from integration.durable import run_saga
from integration.durable import unwinding
from integration.durable.api import Payments
from integration.durable.api import payments_app
from integration.durable.postgres import MIGRATION_LOCK
from integration.durable.postgres import migrate
from integration.durable.worker import work
from psycopg import AsyncCursor
from psycopg.rows import TupleRow
from psycopg_pool import AsyncConnectionPool
from without_dag import CompiledGraph
from without_http import serving

# `just test` starts the services in compose.yaml and publishes each address; these tests
# drive the real server it started rather than a fake, and skip when it did not (no
# podman on this machine, or pytest run directly).
pytestmark = pytest.mark.compose

ORDER = Order(order_id="o-42", sku="gizmo", cents=1999)
BRIEFLY = timedelta(milliseconds=200)

# An *application* table, which is the only kind that makes `transact` worth having: the
# whole claim is that a step's own business write and its checkpoint commit together, and
# a step that writes to another workflow table would not be showing that.
LEDGER = "CREATE TABLE IF NOT EXISTS stock_ledger (sku text PRIMARY KEY, reserved integer NOT NULL)"


@pytest.fixture
async def pool() -> AsyncIterator[AsyncConnectionPool]:
    published = os.environ.get("WITHOUT_TESTS_POSTGRES")
    if not published:  # pragma: no cover - the arm that runs is the one where this whole file is uncovered
        pytest.skip("WITHOUT_TESTS_POSTGRES is unset: run `just test`, which starts the services in compose.yaml")

    # podman-compose reports the published port alone, docker compose the bind address it
    # is published on (`0.0.0.0:32768`), which is a wildcard a client cannot dial. Either
    # way the loopback address is where the port is reachable.
    host, _, port = published.strip().rpartition(":")
    dsn = f"postgresql://postgres:without@{host or '127.0.0.1'}:{int(port)}/without"
    connections = AsyncConnectionPool(dsn, min_size=1, max_size=8, open=False)
    await connections.open(wait=True)
    try:
        # Idempotent and safe against every other test doing it at the same instant, which
        # is what the advisory lock in it is for. A deployment runs it once at startup.
        await migrate(connections)
        yield connections
    finally:
        await connections.close()


@pytest.fixture
async def ledger(pool: AsyncConnectionPool) -> str:
    async with pool.connection() as connection:
        await connection.execute("SELECT pg_advisory_xact_lock(%s)", (MIGRATION_LOCK,))
        await connection.execute(LEDGER)
    return "stock_ledger"


@pytest.fixture
def workflow() -> str:
    # Every test gets its own idempotency key rather than truncating the tables, because
    # the database is shared by every worker in the session: a truncate would pull another
    # test's checkpoint out from under it.
    return f"test-{uuid4().hex}"


def reserving(sku: str, quantity: int) -> Callable[[AsyncCursor[TupleRow]], Awaitable[object]]:
    """An effect in this same database: move stock, and report where it landed."""

    async def effect(cursor: AsyncCursor[TupleRow]) -> object:
        await cursor.execute(
            "INSERT INTO stock_ledger (sku, reserved) VALUES (%s, %s) "
            "ON CONFLICT (sku) DO UPDATE SET reserved = stock_ledger.reserved + EXCLUDED.reserved "
            "RETURNING reserved",
            (sku, quantity),
        )
        reserved = await cursor.fetchone()
        assert reserved is not None
        return reserved[0]

    return effect


async def reserved(pool: AsyncConnectionPool, sku: str) -> int | None:
    async with pool.connection() as connection, connection.cursor() as cursor:
        await cursor.execute("SELECT reserved FROM stock_ledger WHERE sku = %s", (sku,))
        found = await cursor.fetchone()
    return None if found is None else int(found[0])


async def passing[T](
    checkpoints: PostgresCheckpoints,
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
    checkpoints: PostgresCheckpoints,
    workflow: str,
    value: In,
) -> Out:
    holder = await claimed(checkpoints, workflow)
    try:
        return await run_saga(forward, unwind, reaches, checkpoints, holder, value)
    finally:
        await checkpoints.release(holder)


async def test_a_workflow_resumes_from_a_checkpoint_left_in_postgres_by_a_dead_process(
    pool: AsyncConnectionPool,
    workflow: str,
) -> None:
    # The same end-to-end resumption the Redis store is asked for, over rows instead of a
    # hash: the first "process" leaves a table behind, and a second one that shares
    # nothing with it but the workflow's id picks the run up from those rows alone.
    checkpoints = PostgresCheckpoints(pool=pool)
    crashed = Gateway(broken={"ship"})
    services = crashed.services()

    with pytest.raises(RuntimeError, match="ship is down"):
        await saga(fulfilment(services), unwinding(services), Reached.of, checkpoints, workflow, ORDER)

    # What an operator sees with `psql`: one row per completed step, each holding that
    # step's result as jsonb, which is queryable rather than merely readable.
    async with pool.connection() as connection, connection.cursor() as cursor:
        await cursor.execute(
            "SELECT step, value FROM workflow_checkpoint WHERE workflow = %s ORDER BY step",
            (workflow,),
        )
        assert await cursor.fetchall() == [("charged", "ch-o-42"), ("reserved", "rs-gizmo")]

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
    assert recovered.calls == ["ship"], "the charge and the reservation were read back out of Postgres"


async def test_a_compensation_is_recorded_under_its_own_key(pool: AsyncConnectionPool, workflow: str) -> None:
    checkpoints = PostgresCheckpoints(pool=pool)
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


async def test_a_workflow_suspended_on_an_approval_resumes_when_another_process_records_it(
    pool: AsyncConnectionPool,
    workflow: str,
) -> None:
    # The stepwise mechanism end to end, and the thing a signal usually needs a server
    # for: the pass that asked for the approval is gone, and what resumes the workflow is
    # one row written into its checkpoint by something that shares nothing with it.
    suspended = PostgresCheckpoints(pool=pool)
    asked: list[str] = []

    async def body(run: Run) -> dict[str, object]:
        return await pay_out(run, "ord-42", paying(asked), settling=timedelta(), approval_over=10_000)

    with pytest.raises(Suspended) as suspension:
        await passing(suspended, workflow, body)

    assert suspension.value.key == "approved-by"
    assert set(await suspended.load(workflow)) == {"items", "captured:piano", "captured:stool", "settling"}
    assert asked == ["items", "capture:piano", "capture:stool"], "the money moved, the payout did not"

    await PostgresCheckpoints(pool=pool).supply(workflow, "approved-by", "auditor-7")

    answered: list[str] = []

    async def resumed(run: Run) -> dict[str, object]:
        return await pay_out(run, "ord-42", paying(answered), settling=timedelta(), approval_over=10_000)

    payout = await passing(PostgresCheckpoints(pool=pool), workflow, resumed)

    assert payout["approved_by"] == "auditor-7"
    assert payout["captures"] == {"piano": "cap-piano", "stool": "cap-stool"}
    assert answered == ["pay"], "everything before the approval was read back out of Postgres"


# The pair over one Postgres, which is the strongest form of the claim this store makes:
# the API writes, the worker reads, the checkpoint and the queue are tables in the same
# database, and there is no second system anywhere in the deployment. Its budget is
# generous because the workflow really does sleep for a second.
@pytest.mark.timeout(30)
async def test_an_order_submitted_to_the_api_is_carried_to_payout_by_the_worker(
    pool: AsyncConnectionPool,
    workflow: str,
) -> None:
    checkpoints = PostgresCheckpoints(pool=pool)
    # The queue is namespaced per test so parallel runs do not take each other's work,
    # which a deployment would not do: there, sharing one queue *is* how work spreads.
    wakeups = PostgresSchedule(pool=pool, namespace=workflow)
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

            # Past the deadline the workflow becomes visible on its own, the pass runs,
            # and it stops on the confirmation, where nothing but a person can move it.
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


async def test_only_one_of_many_processes_racing_for_a_workflow_gets_to_pass_over_it(
    pool: AsyncConnectionPool,
    workflow: str,
) -> None:
    # The guarantee against the real server, where it has to hold across processes that
    # share nothing. Postgres is the only party that sees all of them, and the whole check
    # is the `WHERE` on the upsert's `DO UPDATE`: a conflicting row whose lease has not
    # elapsed fails it, so no row comes back and that claimant lost.
    racing = [PostgresCheckpoints(pool=pool) for _ in range(8)]

    claims = await asyncio.gather(*(store.claim(workflow, timedelta(minutes=1)) for store in racing))
    won = [holder for holder in claims if holder is not None]

    assert len(won) == 1, "a claim is exclusive no matter how many clients ask at once"

    with pytest.raises(Contended):
        await claimed(racing[0], workflow)

    await racing[0].release(won[0])
    again = await claimed(racing[1], workflow)

    assert again.token > won[0].token, "and the next claim outranks the one before it"


async def test_a_write_from_a_pass_that_lost_the_workflow_is_refused_by_postgres(
    pool: AsyncConnectionPool,
    workflow: str,
) -> None:
    # What a lease alone cannot give: the stalled holder still believes it owns the
    # workflow, and only the store can tell it otherwise. The statement locks the claim
    # row, compares the token it was handed against the newest one, and writes nothing.
    checkpoints = PostgresCheckpoints(pool=pool)
    stalled = await claimed(checkpoints, workflow, timedelta(milliseconds=1))
    await asyncio.sleep(0.05)
    took_over = await claimed(checkpoints, workflow)

    with pytest.raises(Fenced, match="moved on while this pass held it"):
        await checkpoints.record(stalled, "paid", "pay-from-the-dead")

    assert await checkpoints.record(took_over, "paid", "pay-real") == "pay-real"
    assert await checkpoints.load(workflow) == {"paid": "pay-real"}


async def test_a_step_already_recorded_is_never_overwritten_and_hands_back_the_winner(
    pool: AsyncConnectionPool,
    workflow: str,
) -> None:
    checkpoints = PostgresCheckpoints(pool=pool)
    holder = await claimed(checkpoints, workflow)

    assert await checkpoints.record(holder, "captured:piano", "cap-first") == "cap-first"
    assert await checkpoints.record(holder, "captured:piano", "cap-second") == "cap-first"
    assert await checkpoints.supply(workflow, "captured:piano", "cap-third") == "cap-first"
    assert await checkpoints.load(workflow) == {"captured:piano": "cap-first"}


async def test_a_workflow_id_needs_no_contract_because_it_is_never_part_of_a_key(
    pool: AsyncConnectionPool,
    workflow: str,
) -> None:
    # The Redis store asks an id not to carry braces, because they delimit its cluster
    # hash tag and an id that brings its own splits the key structure. Here the id is a
    # query parameter, so there is nothing for it to break, which is the tell that the
    # contract was about building keys by interpolation rather than about workflow ids.
    awkward = f"{{{workflow}}} 'quoted' -- ;"
    checkpoints = PostgresCheckpoints(pool=pool)
    holder = await claimed(checkpoints, awkward)

    assert await checkpoints.record(holder, "paid", "pay-1") == "pay-1"
    assert await checkpoints.load(awkward) == {"paid": "pay-1"}
    assert await checkpoints.load(workflow) == {}, "and it addressed only itself"


async def test_a_step_that_records_json_null_is_not_a_step_that_never_ran(
    pool: AsyncConnectionPool,
    workflow: str,
) -> None:
    # `NOT NULL` on a jsonb column is what keeps these two apart: a step legitimately
    # returning `None` records the JSON value `null`, and reading it back as "no row"
    # would make a resumed pass run that step again.
    checkpoints = PostgresCheckpoints(pool=pool)
    holder = await claimed(checkpoints, workflow)

    assert await checkpoints.record(holder, "notified", None) is None
    assert await checkpoints.load(workflow) == {"notified": None}


async def test_an_effect_in_this_postgres_is_performed_and_recorded_in_one_commit(
    pool: AsyncConnectionPool,
    workflow: str,
    ledger: str,
) -> None:
    # Exactly-once, and this is the row the whole `Effect` parameter exists for: the
    # effect writes to an *application* table, in the transaction that writes the
    # checkpoint, so there is no window between the work and its receipt for a crash to
    # occupy. It is the same guarantee the Lua script gives over Redis data; what differs
    # is that here the data the step touches is usually already in this database.
    checkpoints = PostgresCheckpoints(pool=pool)
    holder = await claimed(checkpoints, workflow)
    reserve = reserving(workflow, 1)

    first = await Run(holder=holder, checkpoints=checkpoints, recorded={}).transact("reserved", reserve)
    again = await Run(holder=holder, checkpoints=checkpoints, recorded={}).transact("reserved", reserve)

    assert (first, again) == (1, 1), "the second pass read the record rather than reserving again"
    assert await reserved(pool, workflow) == 1, "the stock moved once, however many passes reached the step"
    assert await checkpoints.load(workflow) == {"reserved": 1}


async def test_a_transacted_effect_is_refused_from_a_superseded_pass(
    pool: AsyncConnectionPool,
    workflow: str,
    ledger: str,
) -> None:
    checkpoints = PostgresCheckpoints(pool=pool)
    stalled = await claimed(checkpoints, workflow, timedelta(milliseconds=1))
    await asyncio.sleep(0.05)
    await claimed(checkpoints, workflow)

    with pytest.raises(Fenced):
        await Run(holder=stalled, checkpoints=checkpoints, recorded={}).transact("reserved", reserving(workflow, 1))

    assert await reserved(pool, workflow) is None, "the fence is checked before the effect runs, not after"


async def test_an_effect_that_fails_leaves_neither_the_work_nor_the_record(
    pool: AsyncConnectionPool,
    workflow: str,
    ledger: str,
) -> None:
    # The other half of "one commit", and the half a second round trip cannot offer: the
    # effect's write and the checkpoint row are rolled back together, so a step that gets
    # halfway leaves nothing for the next pass to be confused by.
    checkpoints = PostgresCheckpoints(pool=pool)
    holder = await claimed(checkpoints, workflow)

    async def half_way(cursor: AsyncCursor[TupleRow]) -> object:
        await reserving(workflow, 5)(cursor)
        raise RuntimeError("the warehouse said no")

    with pytest.raises(RuntimeError, match="the warehouse said no"):
        await Run(holder=holder, checkpoints=checkpoints, recorded={}).transact("reserved", half_way)

    assert await reserved(pool, workflow) is None
    assert await checkpoints.load(workflow) == {}, "and the step is unrecorded, so the next pass may retry it"


def scheduled(pool: AsyncConnectionPool, workflow: str, lease: timedelta = timedelta(seconds=30)) -> PostgresSchedule:
    return PostgresSchedule(pool=pool, namespace=workflow, lease=lease)


async def test_a_workflow_taken_off_the_schedule_is_invisible_for_its_lease(
    pool: AsyncConnectionPool,
    workflow: str,
) -> None:
    queue = scheduled(pool, workflow)
    await queue.make_ready(workflow)

    taken = await queue.next_ready(BRIEFLY)

    assert taken is not None
    assert await queue.next_ready(timedelta(milliseconds=50)) is None, "one taker at a time, as a consumer group is"


async def test_two_workers_polling_one_queue_take_different_workflows(
    pool: AsyncConnectionPool,
    workflow: str,
) -> None:
    # What a table gives that a sorted set needs a script for: `FOR UPDATE SKIP LOCKED`
    # means the second poller passes over the row the first is updating rather than
    # queueing behind it, so a pool of workers fans out over one queue.
    queue = scheduled(pool, workflow)
    for each in ("wf-a", "wf-b", "wf-c"):
        await queue.make_ready(each)

    taken = await asyncio.gather(*(queue.next_ready(BRIEFLY) for _ in range(3)))
    workflows = [delivery.workflow for delivery in taken if delivery is not None]

    assert sorted(workflows) == ["wf-a", "wf-b", "wf-c"], "three pollers, three workflows, none taken twice"


async def test_a_wakeup_arriving_mid_pass_survives_the_pass_that_was_running(
    pool: AsyncConnectionPool,
    workflow: str,
) -> None:
    # The lost-wakeup bug this design has to answer for, and the reason `done` compares
    # visibilities. A table holds one row per workflow, so the wakeup lands on the row the
    # pass is holding and removing that row afterwards would throw it away.
    queue = scheduled(pool, workflow)
    await queue.make_ready(workflow)
    held = await queue.next_ready(BRIEFLY)
    assert held is not None

    await queue.make_ready(workflow)  # a confirmation, while the pass is still running
    await queue.done(held)

    assert await queue.next_ready(BRIEFLY) is not None, "the pass that finished did not clean up someone else's wakeup"


async def test_a_deadline_set_during_a_pass_outlives_that_passs_acknowledgement(
    pool: AsyncConnectionPool,
    workflow: str,
) -> None:
    # The worker schedules and then acknowledges, in that order, so the acknowledgement
    # must not undo the scheduling. The comparison is what allows those to stay two calls.
    queue = scheduled(pool, workflow)
    await queue.make_ready(workflow)
    held = await queue.next_ready(BRIEFLY)
    assert held is not None

    await queue.wake_at(workflow, now_utc() + timedelta(milliseconds=150))
    await queue.done(held)

    assert await queue.next_ready(timedelta(milliseconds=50)) is None, "not due yet"
    assert await queue.next_ready(timedelta(seconds=2)) is not None, "and still there when it is"


async def test_an_abandoned_workflow_comes_back_without_anyone_reclaiming_it(
    pool: AsyncConnectionPool,
    workflow: str,
) -> None:
    # Two views of one queue, differing only in how long each holds what it takes. The
    # short one stands in for the worker that dies; the patient one for whoever finds the
    # work afterwards, and its long lease is what keeps the last assertion about the
    # comparison rather than about how fast the assertions above it ran.
    dying = scheduled(pool, workflow, lease=timedelta(milliseconds=50))
    surviving = scheduled(pool, workflow, lease=timedelta(seconds=30))
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


async def test_the_queue_holds_one_row_per_workflow_however_many_wakeups_arrive(
    pool: AsyncConnectionPool,
    workflow: str,
) -> None:
    # Where this queue and the stream differ in kind: a stream grows an entry per wakeup
    # and needs trimming, a table holds the workflow once and needs none.
    queue = scheduled(pool, workflow)

    for _ in range(5):
        await queue.make_ready(workflow)

    async with pool.connection() as connection, connection.cursor() as cursor:
        await cursor.execute("SELECT count(*) FROM workflow_queue WHERE namespace = %s", (workflow,))
        assert await cursor.fetchone() == (1,)

    assert await queue.wake_due(now_utc()) == (), "being due and being ready are the same visibility"
    await queue.prepare()  # already migrated, and running it again is a no-op
