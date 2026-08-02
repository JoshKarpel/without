from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator
from collections.abc import Awaitable
from collections.abc import Callable
from datetime import timedelta
from uuid import uuid4

import pytest
from integration.durable import Order
from psycopg import AsyncCursor
from psycopg import DataError
from psycopg.rows import TupleRow
from psycopg_pool import AsyncConnectionPool
from without_durability import Contended
from without_durability import Fenced
from without_durability import Run
from without_durability import claimed
from without_durability import now_utc
from without_durability_postgres import PostgresCheckpointer
from without_durability_postgres import PostgresDurable
from without_durability_postgres import PostgresScheduler
from without_durability_postgres.store import MIGRATION_LOCK
from without_durability_postgres.store import migrate

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


def durably(pool: AsyncConnectionPool, workflow: str) -> PostgresDurable:
    """Both stores over one pool, which is what makes `arrive` a single commit."""
    return PostgresDurable(
        checkpointer=PostgresCheckpointer(pool=pool),
        scheduler=PostgresScheduler(pool=pool, namespace=workflow),
    )


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


async def test_only_one_of_many_processes_racing_for_a_workflow_gets_to_pass_over_it(
    pool: AsyncConnectionPool,
    workflow: str,
) -> None:
    # The guarantee against the real server, where it has to hold across processes that
    # share nothing. Postgres is the only party that sees all of them, and the whole check
    # is the `WHERE` on the upsert's `DO UPDATE`: a conflicting row whose lease has not
    # elapsed fails it, so no row comes back and that claimant lost.
    racing = [PostgresCheckpointer(pool=pool) for _ in range(8)]

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
    checkpointer = PostgresCheckpointer(pool=pool)
    stalled = await claimed(checkpointer, workflow, timedelta(milliseconds=1))
    await asyncio.sleep(0.05)
    took_over = await claimed(checkpointer, workflow)

    with pytest.raises(Fenced, match="moved on while this pass held it"):
        await checkpointer.record(stalled, "paid", "pay-from-the-dead")

    assert await checkpointer.record(took_over, "paid", "pay-real") == "pay-real"
    assert await checkpointer.load(workflow) == {"paid": "pay-real"}


async def test_a_step_already_recorded_is_never_overwritten_and_hands_back_the_winner(
    pool: AsyncConnectionPool,
    workflow: str,
) -> None:
    checkpointer = PostgresCheckpointer(pool=pool)
    holder = await claimed(checkpointer, workflow)

    assert await checkpointer.record(holder, "captured:piano", "cap-first") == "cap-first"
    assert await checkpointer.record(holder, "captured:piano", "cap-second") == "cap-first"
    assert await checkpointer.supply(workflow, "captured:piano", "cap-third") == "cap-first"
    assert await checkpointer.load(workflow) == {"captured:piano": "cap-first"}


async def test_a_workflow_id_needs_no_contract_because_it_is_never_part_of_a_key(
    pool: AsyncConnectionPool,
    workflow: str,
) -> None:
    # The Redis store asks an id not to carry braces, because they delimit its cluster
    # hash tag and an id that brings its own splits the key structure. Here the id is a
    # query parameter, so there is nothing for it to break, which is the tell that the
    # contract was about building keys by interpolation rather than about workflow ids.
    awkward = f"{{{workflow}}} 'quoted' -- ;"
    checkpointer = PostgresCheckpointer(pool=pool)
    holder = await claimed(checkpointer, awkward)

    assert await checkpointer.record(holder, "paid", "pay-1") == "pay-1"
    assert await checkpointer.load(awkward) == {"paid": "pay-1"}
    assert await checkpointer.load(workflow) == {}, "and it addressed only itself"


async def test_a_step_that_records_json_null_is_not_a_step_that_never_ran(
    pool: AsyncConnectionPool,
    workflow: str,
) -> None:
    # `NOT NULL` on a jsonb column is what keeps these two apart: a step legitimately
    # returning `None` records the JSON value `null`, and reading it back as "no row"
    # would make a resumed pass run that step again.
    checkpointer = PostgresCheckpointer(pool=pool)
    holder = await claimed(checkpointer, workflow)

    assert await checkpointer.record(holder, "notified", None) is None
    assert await checkpointer.load(workflow) == {"notified": None}


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
    checkpointer = PostgresCheckpointer(pool=pool)
    holder = await claimed(checkpointer, workflow)
    reserve = reserving(workflow, 1)

    first = await Run(holder=holder, checkpointer=checkpointer, recorded={}).transact("reserved", reserve)
    again = await Run(holder=holder, checkpointer=checkpointer, recorded={}).transact("reserved", reserve)

    assert (first, again) == (1, 1), "the second pass read the record rather than reserving again"
    assert await reserved(pool, workflow) == 1, "the stock moved once, however many passes reached the step"
    assert await checkpointer.load(workflow) == {"reserved": 1}


async def test_a_transacted_effect_is_refused_from_a_superseded_pass(
    pool: AsyncConnectionPool,
    workflow: str,
    ledger: str,
) -> None:
    checkpointer = PostgresCheckpointer(pool=pool)
    stalled = await claimed(checkpointer, workflow, timedelta(milliseconds=1))
    await asyncio.sleep(0.05)
    await claimed(checkpointer, workflow)

    with pytest.raises(Fenced):
        await Run(holder=stalled, checkpointer=checkpointer, recorded={}).transact("reserved", reserving(workflow, 1))

    assert await reserved(pool, workflow) is None, "the fence is checked before the effect runs, not after"


async def test_an_effect_that_fails_leaves_neither_the_work_nor_the_record(
    pool: AsyncConnectionPool,
    workflow: str,
    ledger: str,
) -> None:
    # The other half of "one commit", and the half a second round trip cannot offer: the
    # effect's write and the checkpoint row are rolled back together, so a step that gets
    # halfway leaves nothing for the next pass to be confused by.
    checkpointer = PostgresCheckpointer(pool=pool)
    holder = await claimed(checkpointer, workflow)

    async def half_way(cursor: AsyncCursor[TupleRow]) -> object:
        await reserving(workflow, 5)(cursor)
        raise RuntimeError("the warehouse said no")

    with pytest.raises(RuntimeError, match="the warehouse said no"):
        await Run(holder=holder, checkpointer=checkpointer, recorded={}).transact("reserved", half_way)

    assert await reserved(pool, workflow) is None
    assert await checkpointer.load(workflow) == {}, "and the step is unrecorded, so the next pass may retry it"


async def test_a_value_arriving_is_recorded_and_queued_in_one_commit(
    pool: AsyncConnectionPool,
    workflow: str,
) -> None:
    # The transition `Durable` exists to name. Over a split store this is two writes with
    # a crash window between them; here it is one transaction, so there is no state where
    # the order is recorded and nothing will ever run it.
    durable = durably(pool, workflow)

    assert await durable.arrive(workflow, "order", {"piano": 90_000}) == {"piano": 90_000}
    assert await durable.checkpointer.load(workflow) == {"order": {"piano": 90_000}}

    taken = await durable.scheduler.next_ready(BRIEFLY)

    assert taken is not None
    assert taken.workflow == workflow, "recorded and runnable, from one call"


async def test_a_second_arrival_under_one_key_hands_back_the_first(
    pool: AsyncConnectionPool,
    workflow: str,
) -> None:
    # `arrive` keeps `supply`'s first-writer-wins, which is what makes a resubmitted
    # order harmless: the workflow keeps spending the basket it already started on.
    durable = durably(pool, workflow)

    await durable.arrive(workflow, "order", {"piano": 90_000})

    assert await durable.arrive(workflow, "order", {"stool": 4_000}) == {"piano": 90_000}


async def test_an_arrival_that_fails_between_its_two_writes_leaves_neither(
    pool: AsyncConnectionPool,
    workflow: str,
) -> None:
    # The property one commit buys, shown by breaking the second write: the queue's
    # namespace carries a NUL byte, which a Postgres text field cannot hold. The
    # checkpoint statement has already run in that transaction, and the rollback takes it
    # with it. A split store cannot offer this, which is why its `arrive` records first:
    # it is left holding the recoverable half rather than neither.
    checkpointer = PostgresCheckpointer(pool=pool)
    broken = PostgresDurable(checkpointer=checkpointer, scheduler=PostgresScheduler(pool=pool, namespace="ns\x00bad"))

    with pytest.raises(DataError):
        await broken.arrive(workflow, "order", {"piano": 90_000})

    assert await checkpointer.load(workflow) == {}, "the record went back with the queue write"


async def test_two_stores_on_different_pools_are_refused_at_construction(pool: AsyncConnectionPool) -> None:
    # The invariant that makes `arrive` one commit, checked rather than documented: two
    # pools are two transactions however similar their connection strings look. It is the
    # same question `LuaEffect` asks with its hash tag, and SQL hides it rather than
    # answering it.
    elsewhere = AsyncConnectionPool(pool.conninfo, min_size=1, max_size=1, open=False)
    await elsewhere.open(wait=True)
    try:
        with pytest.raises(ValueError, match="must share one pool"):
            PostgresDurable(
                checkpointer=PostgresCheckpointer(pool=pool),
                scheduler=PostgresScheduler(pool=elsewhere),
            )
    finally:
        await elsewhere.close()


def scheduled(pool: AsyncConnectionPool, workflow: str, lease: timedelta = timedelta(seconds=30)) -> PostgresScheduler:
    return PostgresScheduler(pool=pool, namespace=workflow, lease=lease)


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
