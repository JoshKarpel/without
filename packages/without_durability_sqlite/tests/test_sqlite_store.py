from __future__ import annotations

import asyncio
import sqlite3
import threading
import time
from collections.abc import AsyncIterator
from datetime import timedelta
from pathlib import Path

import pytest
from without_async.testing import yield_once
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
        await opened.aclose()


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
        await reopened.aclose()


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


async def test_a_message_delivered_is_appended_and_queued_in_one_commit(database: Database) -> None:
    durable = SqliteDurable(SqliteCheckpointer(database=database), SqliteScheduler(database=database))

    entry = await durable.deliver(WORKFLOW, {"said": "hello"})

    assert entry.value == {"said": "hello"}
    assert await durable.checkpointer.load(WORKFLOW) == {entry.key: {"said": "hello"}}

    taken = await durable.scheduler.next_ready(BRIEFLY)

    assert taken is not None
    assert taken.workflow == WORKFLOW, "appended and runnable, from one call"


async def test_an_appended_key_is_named_from_the_row_the_insert_is_about_to_take(database: Database) -> None:
    # The number in the key is the table's own `seq` counter rather than a count of this
    # workflow's rows, which is what makes `APPEND` a single statement. It is global, so a
    # second workflow's appends leave gaps in the first one's keys; the contract asks only
    # that each workflow's own keys sort into append order, which this pins.
    checkpointer = SqliteCheckpointer(database=database)
    other = f"{WORKFLOW}-other"

    first = await checkpointer.append(WORKFLOW, "first")
    theirs = await checkpointer.append(other, "somebody else's")
    second = await checkpointer.append(WORKFLOW, "second")

    assert [first.key, theirs.key, second.key] == sorted([first.key, theirs.key, second.key])
    assert list(await checkpointer.load(WORKFLOW)) == [first.key, second.key], "with a gap where theirs took a number"
    assert list(await checkpointer.load(other)) == [theirs.key]


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
        await elsewhere.aclose()


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

    await queue.wake_at(held, now_utc() + timedelta(milliseconds=150))
    await queue.done(held)  # a stale acknowledgement, which the deadline it wrote makes inert

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


async def test_a_cancelled_caller_does_not_hand_the_connection_to_the_next_one(
    database: Database,
    checkpointer: SqliteCheckpointer,
) -> None:
    # A thread is not cancellable, so the caller unwinds while the statement runs on.
    # Releasing the connection there would put the next caller *inside* the transaction
    # still open on it: its write would be committed or rolled back with somebody else's,
    # which is exactly the guarantee this store exists to make.
    holder = await claimed(checkpointer, WORKFLOW)
    # A `threading.Event`, because it is waited on from the loop and set from the pool
    # thread: the point of the test is that those are two threads inside one connection.
    inside = threading.Event()

    def slow_and_doomed(cursor: sqlite3.Cursor) -> object:
        cursor.execute("INSERT INTO stock_ledger (sku, reserved) VALUES ('widget', 1)")
        inside.set()
        time.sleep(BRIEFLY.total_seconds())
        raise RuntimeError("this transaction rolls back, taking its own writes with it")

    doomed = asyncio.ensure_future(checkpointer.transact(holder, "reserved", slow_and_doomed))
    await asyncio.to_thread(inside.wait)
    doomed.cancel()
    with pytest.raises(asyncio.CancelledError):
        await doomed

    # The write that follows is the one at risk: it must be its own transaction, not a
    # passenger in the doomed one.
    await checkpointer.record(holder, "charged", "ch-1")

    assert await checkpointer.load(WORKFLOW) == {"charged": "ch-1"}, "the second write survived the first's rollback"
    assert await reserved(database, "widget") is None, "and the rolled-back effect left nothing behind"


async def test_closing_waits_out_the_statement_a_cancelled_caller_left_running(
    database: Database,
    checkpointer: SqliteCheckpointer,
) -> None:
    # The other end of the same fact, and the reason `aclose` exists rather than callers
    # reaching for `connection.close()`. Closing frees the connection and finalizes its
    # statements under any thread still executing one, which segfaults the process rather
    # than raising, and a cancelled caller is exactly what leaves such a thread behind.
    # So the close has to wait for the *thread*, not for the caller that walked away.
    holder = await claimed(checkpointer, WORKFLOW)
    inside = threading.Event()
    finish = threading.Event()

    def parked(cursor: sqlite3.Cursor) -> object:
        inside.set()
        # Bounded only as a backstop for the thread itself: every outcome of the test
        # sets `finish` in the `finally` below, and a genuinely hung `aclose` is ended
        # by the global thread-method timeout, not by this wait.
        finish.wait(timeout=5)
        return cursor.execute("SELECT 1").fetchone()[0]

    abandoned = asyncio.ensure_future(checkpointer.transact(holder, "reserved", parked))
    await asyncio.to_thread(inside.wait)
    abandoned.cancel()
    with pytest.raises(asyncio.CancelledError):
        await abandoned

    closing = asyncio.ensure_future(database.aclose())
    try:
        await yield_once()  # far enough to park on the guard, which the running thread holds
        assert not closing.done(), "the close must wait out the statement, not race it"
    finally:
        # However the assertion lands, release the parked thread: left holding the guard,
        # it would turn a plain failure into the fixture teardown hanging on `aclose`
        # until the global thread-method timeout kills the whole worker.
        finish.set()
        await closing


async def test_a_wakeup_that_arrived_during_a_pass_is_not_overwritten_by_its_deadline(database: Database) -> None:
    # A workflow holds one row here, so a deadline written unconditionally lands on top of
    # a `make_ready` that arrived while the pass was ending, and a confirmation waits out
    # a settlement window it should have interrupted. The receipt is what tells the two
    # apart: anything that asked for another pass wrote a different visibility.
    queue = SqliteScheduler(database=database)
    await queue.make_ready(WORKFLOW)
    held = await queue.next_ready(BRIEFLY)
    assert held is not None

    await queue.make_ready(WORKFLOW)  # the confirmation, arriving mid-pass
    await queue.wake_at(held, now_utc() + timedelta(days=3))

    assert await queue.next_ready(BRIEFLY) is not None, "the wakeup that arrived is still due now"


async def test_a_failure_after_the_transaction_is_already_gone_is_reported_as_itself(database: Database) -> None:
    # SQLite ends the transaction itself on a full disk, an I/O error, an interrupt, or
    # being out of memory, so by the time the handler runs there may be nothing left to
    # roll back. An unconditional `ROLLBACK` then raises "cannot rollback - no transaction
    # is active" *over* the error that caused it, and the caller is told the wrong thing
    # about its own failure. An effect that ends the transaction itself reaches the same
    # state deterministically, and on every platform.
    checkpointer = SqliteCheckpointer(database=database)
    holder = await claimed(checkpointer, WORKFLOW)

    def ends_the_transaction(cursor: sqlite3.Cursor) -> object:
        cursor.execute("ROLLBACK")
        raise RuntimeError("the carrier refused the parcel")

    with pytest.raises(RuntimeError, match="the carrier refused the parcel"):
        await checkpointer.transact(holder, "shipped", ends_the_transaction)

    assert await checkpointer.load(WORKFLOW) == {}
    assert await checkpointer.record(holder, "charged", "ch-1") == Recorded(value="ch-1", first=True), (
        "and the connection is usable, rather than stuck in a transaction nobody can end"
    )


async def test_a_split_deployment_reaches_the_two_halves_of_a_delete_separately(database: Database) -> None:
    # `SqliteDurable.delete` is one commit because both stores are one file. A deployment
    # keeping this checkpoint beside a queue in something else holds a `SplitDurable`
    # instead and reaches the halves one at a time, so each has to stand on its own: the
    # queue row goes whatever its visibility currently means, and the discard both forgets
    # the records and raises the fence over the pass that was mid-flight.
    checkpointer = SqliteCheckpointer(database=database)
    queue = SqliteScheduler(database=database)
    holder = await claimed(checkpointer, WORKFLOW)
    await checkpointer.record(holder, "charged", "ch-1")
    await queue.make_ready(WORKFLOW)

    await queue.cancel(WORKFLOW)

    assert await checkpointer.discard(WORKFLOW) == 1
    assert await checkpointer.load(WORKFLOW) == {}
    assert await queue.next_ready(BRIEFLY) is None
    with pytest.raises(Fenced):
        await checkpointer.record(holder, "shipped", "sh-1")
