from __future__ import annotations

import asyncio
import sqlite3
from collections.abc import AsyncIterator
from datetime import timedelta
from pathlib import Path

import pytest
from without_durability import Contended
from without_durability import Fenced
from without_durability import Recorded
from without_durability import Run
from without_durability import claimed
from without_durability import now_utc
from without_durability_sqlite import Database
from without_durability_sqlite import SqliteCheckpointer
from without_durability_sqlite import SqliteDurable
from without_durability_sqlite import SqliteEffect
from without_durability_sqlite import SqliteScheduler
from without_durability_sqlite import connect
from without_durability_sqlite import migrate

# No `compose` mark and no service: the whole point of this store is that there is
# nothing to start. The tests run against a real file in a temporary directory, which is
# the deployment rather than a stand-in for one.

WORKFLOW = "wf-sqlite-1"
BRIEFLY = timedelta(milliseconds=200)

LEDGER = "CREATE TABLE IF NOT EXISTS stock_ledger (sku TEXT PRIMARY KEY, reserved INTEGER NOT NULL)"


def as_count(recorded: object) -> int:
    """What a `transact` effect records here: the ledger total after the reservation."""
    if not isinstance(
        recorded, int
    ):  # pragma: no cover - the arm that makes this a parser rather than a cast; no test feeds it a bad value
        raise TypeError(f"{recorded!r} is not the count this effect recorded")
    return recorded


@pytest.fixture
async def database(tmp_path: Path) -> AsyncIterator[Database]:
    opened = connect(tmp_path / "workflows.db")
    try:
        await migrate(opened)
        await opened.run(lambda connection: connection.execute(LEDGER))
        yield opened
    finally:
        opened.connection.close()


@pytest.fixture
def checkpointer(database: Database) -> SqliteCheckpointer:
    return SqliteCheckpointer(database=database)


def reserving(sku: str, quantity: int) -> SqliteEffect:
    """An effect in this same file: move stock, and report where it landed."""

    def effect(cursor: sqlite3.Cursor) -> object:
        row = cursor.execute(
            "INSERT INTO stock_ledger (sku, reserved) VALUES (?, ?) "
            "ON CONFLICT (sku) DO UPDATE SET reserved = stock_ledger.reserved + excluded.reserved "
            "RETURNING reserved",
            (sku, quantity),
        ).fetchone()
        return row[0]

    return effect


async def reserved(database: Database, sku: str) -> int | None:
    row = await database.run(
        lambda connection: connection.execute("SELECT reserved FROM stock_ledger WHERE sku = ?", (sku,)).fetchone()
    )
    return None if row is None else int(row[0])


async def test_a_file_left_by_one_process_is_where_another_picks_the_workflow_up(
    tmp_path: Path,
    database: Database,
    checkpointer: SqliteCheckpointer,
) -> None:
    # The claim the whole package makes, and the only one a single process can show:
    # what is recorded is on disk, so a store opened afresh over the same path sees it.
    holder = await claimed(checkpointer, WORKFLOW)
    await checkpointer.record(holder, "charged", "ch-1")
    await checkpointer.release(holder)

    reopened = connect(tmp_path / "workflows.db")
    try:
        assert await SqliteCheckpointer(database=reopened).load(WORKFLOW) == {"charged": "ch-1"}
    finally:
        reopened.connection.close()


async def test_only_one_of_many_passes_racing_for_a_workflow_gets_to_run_one(
    checkpointer: SqliteCheckpointer,
) -> None:
    claims = await asyncio.gather(*(checkpointer.claim(WORKFLOW, timedelta(minutes=1)) for _ in range(8)))
    won = [holder for holder in claims if holder is not None]

    assert len(won) == 1, "a claim is exclusive no matter how many ask at once"

    with pytest.raises(Contended):
        await claimed(checkpointer, WORKFLOW)

    await checkpointer.release(won[0])
    again = await claimed(checkpointer, WORKFLOW)

    assert again.token > won[0].token, "and the next claim outranks the one before it"


async def test_a_write_from_a_pass_that_lost_the_workflow_is_refused(checkpointer: SqliteCheckpointer) -> None:
    stalled = await claimed(checkpointer, WORKFLOW, timedelta(milliseconds=1))
    await asyncio.sleep(0.05)
    took_over = await claimed(checkpointer, WORKFLOW)

    with pytest.raises(Fenced, match="moved on while this pass held it"):
        await checkpointer.record(stalled, "paid", "pay-from-the-dead")

    assert await checkpointer.record(took_over, "paid", "pay-real") == Recorded(value="pay-real", first=True)
    assert await checkpointer.load(WORKFLOW) == {"paid": "pay-real"}


async def test_a_step_already_recorded_is_never_overwritten_and_hands_back_the_winner(
    checkpointer: SqliteCheckpointer,
) -> None:
    holder = await claimed(checkpointer, WORKFLOW)

    assert await checkpointer.record(holder, "captured", "cap-first") == Recorded(value="cap-first", first=True)
    assert await checkpointer.record(holder, "captured", "cap-second") == Recorded(value="cap-first", first=False)
    assert await checkpointer.supply(WORKFLOW, "captured", "cap-third") == "cap-first"
    assert await checkpointer.load(WORKFLOW) == {"captured": "cap-first"}


async def test_a_result_the_codec_reshapes_still_counts_as_this_pass_s_own(
    checkpointer: SqliteCheckpointer,
) -> None:
    # A result crosses the codec both ways, so a pass that won outright can be handed back
    # something unequal: JSON has no tuple. `first` is this store's own answer rather than
    # that comparison, which is what stops `run_durably` reading a reshape as a race that
    # never happened.
    holder = await claimed(checkpointer, WORKFLOW)

    assert await checkpointer.record(holder, "bounds", (0, 2000)) == Recorded(value=[0, 2000], first=True)


async def test_two_passes_that_recorded_the_same_value_both_count_as_the_writer(
    checkpointer: SqliteCheckpointer,
) -> None:
    # A tie is not a race. Two passes that ran the same effect and produced the same
    # encoding have nothing to disagree about, so calling the second a loser would stop a
    # graph run over a difference that does not exist.
    holder = await claimed(checkpointer, WORKFLOW)

    await checkpointer.record(holder, "captured", "cap-1")

    assert await checkpointer.record(holder, "captured", "cap-1") == Recorded(value="cap-1", first=True)


async def test_a_step_that_records_json_null_is_not_a_step_that_never_ran(
    checkpointer: SqliteCheckpointer,
) -> None:
    # `NOT NULL` on the value column keeps these apart: a step legitimately returning
    # `None` records the JSON value `null`, and reading it back as "no row" would make a
    # resumed pass run that step again.
    holder = await claimed(checkpointer, WORKFLOW)

    assert await checkpointer.record(holder, "notified", None) == Recorded(value=None, first=True)
    assert await checkpointer.load(WORKFLOW) == {"notified": None}


async def test_an_effect_in_this_file_is_performed_and_recorded_in_one_commit(
    database: Database,
    checkpointer: SqliteCheckpointer,
) -> None:
    # Exactly-once, with no server involved. The effect writes an application table in
    # the same transaction as the checkpoint, which is the guarantee DBOS gets from
    # Postgres, for an application that never needed Postgres.
    holder = await claimed(checkpointer, WORKFLOW)
    reserve = reserving("piano", 1)

    first = await Run(holder=holder, checkpointer=checkpointer, recorded={}).transact("reserved", reserve, as_count)
    again = await Run(holder=holder, checkpointer=checkpointer, recorded={}).transact("reserved", reserve, as_count)

    assert (first, again) == (1, 1), "the second pass read the record rather than reserving again"
    assert await reserved(database, "piano") == 1, "the stock moved once, however many passes reached the step"
    assert await checkpointer.load(WORKFLOW) == {"reserved": 1}


async def test_a_transacted_effect_is_refused_from_a_superseded_pass(
    database: Database,
    checkpointer: SqliteCheckpointer,
) -> None:
    stalled = await claimed(checkpointer, WORKFLOW, timedelta(milliseconds=1))
    await asyncio.sleep(0.05)
    await claimed(checkpointer, WORKFLOW)

    with pytest.raises(Fenced):
        await Run(holder=stalled, checkpointer=checkpointer, recorded={}).transact(
            "reserved", reserving("piano", 1), as_count
        )

    assert await reserved(database, "piano") is None, "the fence is checked before the effect runs, not after"


async def test_an_effect_that_fails_leaves_neither_the_work_nor_the_record(
    database: Database,
    checkpointer: SqliteCheckpointer,
) -> None:
    # What `BEGIN IMMEDIATE` buys: the effect's write and the checkpoint row roll back
    # together, so a step that gets halfway leaves nothing for the next pass to trip on.
    holder = await claimed(checkpointer, WORKFLOW)

    def half_way(cursor: sqlite3.Cursor) -> object:
        reserving("piano", 5)(cursor)
        raise RuntimeError("the warehouse said no")

    with pytest.raises(RuntimeError, match="the warehouse said no"):
        await Run(holder=holder, checkpointer=checkpointer, recorded={}).transact("reserved", half_way, as_count)

    assert await reserved(database, "piano") is None
    assert await checkpointer.load(WORKFLOW) == {}, "and the step is unrecorded, so the next pass may retry it"


async def test_a_value_arriving_is_recorded_and_queued_in_one_commit(database: Database) -> None:
    durable = SqliteDurable(SqliteCheckpointer(database=database), SqliteScheduler(database=database))

    assert await durable.arrive(WORKFLOW, "order", {"piano": 90_000}) == {"piano": 90_000}
    assert await durable.checkpointer.load(WORKFLOW) == {"order": {"piano": 90_000}}

    taken = await durable.scheduler.next_ready(BRIEFLY)

    assert taken is not None
    assert taken.workflow == WORKFLOW, "recorded and runnable, from one call"


async def test_two_stores_on_different_databases_are_refused_at_construction(
    tmp_path: Path,
    database: Database,
) -> None:
    # Two SQLite files are two datastores however adjacent they sit on disk, and `arrive`
    # is only one commit when both stores are looking at the same one.
    elsewhere = connect(tmp_path / "somewhere-else.db")
    try:
        with pytest.raises(ValueError, match="must share one database"):
            SqliteDurable(SqliteCheckpointer(database=database), SqliteScheduler(database=elsewhere))
    finally:
        elsewhere.connection.close()


async def test_a_workflow_taken_off_the_schedule_is_invisible_for_its_lease(database: Database) -> None:
    queue = SqliteScheduler(database=database)
    await queue.make_ready(WORKFLOW)

    taken = await queue.next_ready(BRIEFLY)

    assert taken is not None
    assert await queue.next_ready(timedelta(milliseconds=50)) is None, "one taker at a time"


async def test_a_wakeup_arriving_mid_pass_survives_the_pass_that_was_running(database: Database) -> None:
    # The lost-wakeup bug every visibility-scored queue has to answer for: one row per
    # workflow, so a wakeup lands on the row the pass is holding and removing that row
    # afterwards would throw it away.
    queue = SqliteScheduler(database=database)
    await queue.make_ready(WORKFLOW)
    held = await queue.next_ready(BRIEFLY)
    assert held is not None

    await queue.make_ready(WORKFLOW)  # a confirmation, while the pass is still running
    await queue.done(held)

    assert await queue.next_ready(BRIEFLY) is not None, "the pass that finished did not clean up someone else's wakeup"


async def test_a_deadline_set_during_a_pass_outlives_that_passs_acknowledgement(database: Database) -> None:
    queue = SqliteScheduler(database=database)
    await queue.make_ready(WORKFLOW)
    held = await queue.next_ready(BRIEFLY)
    assert held is not None

    await queue.wake_at(WORKFLOW, now_utc() + timedelta(milliseconds=150))
    await queue.done(held)

    assert await queue.next_ready(timedelta(milliseconds=50)) is None, "not due yet"
    assert await queue.next_ready(timedelta(seconds=2)) is not None, "and still there when it is"


async def test_an_abandoned_workflow_comes_back_without_anyone_reclaiming_it(database: Database) -> None:
    dying = SqliteScheduler(database=database, lease=timedelta(milliseconds=50))
    surviving = SqliteScheduler(database=database, lease=timedelta(seconds=30))
    await dying.make_ready(WORKFLOW)
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


async def test_the_queue_holds_one_row_per_workflow_however_many_wakeups_arrive(database: Database) -> None:
    queue = SqliteScheduler(database=database)

    for _ in range(5):
        await queue.make_ready(WORKFLOW)

    rows = await database.run(lambda connection: connection.execute("SELECT count(*) FROM workflow_queue").fetchone())

    assert rows == (1,)
    assert await queue.wake_due(now_utc()) == (), "being due and being ready are the same visibility"
    await queue.prepare()  # already migrated, and running it again is a no-op
