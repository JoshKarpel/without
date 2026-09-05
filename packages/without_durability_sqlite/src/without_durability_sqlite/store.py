# The same three tables again, in one file on one machine, with no server and no
# third-party driver. It is the smallest thing that still meets every requirement in
# `without_durability.interfaces`, and putting it beside the Redis and Postgres stores is the
# clearest statement of what the interface is for: a durable workflow does not need a cluster,
# a database server, or a dependency.
#
# What SQLite settles that the others have to arrange:
#
#   - There is one writer at a time, by construction. `BEGIN IMMEDIATE` takes the write
#     lock for the whole transaction, so the fence check and the write it guards cannot
#     be interleaved with anything. Postgres needs `FOR UPDATE` on the claim row to get
#     that, because there readers and writers run concurrently and a statement's snapshot
#     can be stale; Redis needs a Lua script. Here the transaction *is* the exclusion.
#   - There is nothing to co-locate. `transact` and `arrive` reach the whole datastore
#     because the datastore is a file, so the question the other two stores have to keep
#     asking (are these two writes in one local commit?) has one answer and it is yes.
#
# What it costs is the shape of the whole thing: one machine. Every process sharing this
# store shares a filesystem, which means the exclusion holds across the processes on one
# box and not across a fleet. That is not a defect to apologise for, it is the deployment
# this store is for: a CLI that resumes, a desktop app, an agent on a laptop, a single
# node that would rather not run Postgres to remember what it was doing.
#
# `sqlite3` is a blocking API, so every call here hops to a thread, and that hop is what
# creates the concurrency this store has to answer for. A single-threaded event loop does
# not serialize these: `asyncio.to_thread` exists to get the work *off* that thread, so
# twenty passes are twenty pool workers inside one connection at once. One `asyncio.Lock`
# puts them back in a queue.
#
# What that lock is for is worth stating exactly, because SQLite's own answer sounds like
# it covers the case and does not. The library is built serialized here (`THREADSAFE=1`,
# `sqlite3.threadsafety == 3`), so concurrent use of one connection is already safe from
# corruption; what it promises is that the calls behave "as if they had all been made in
# the same order from a single thread", which is *linearization, not isolation*. A
# transaction is connection state, so a second caller landing mid-`BEGIN IMMEDIATE` joins
# that transaction rather than waiting for it: its write succeeds, reads back, and then
# disappears when the other caller rolls back. That is the failure the lock removes, and
# no threading mode removes it.
#
# What the lock costs is the other half of WAL. WAL exists so one writer runs alongside
# many readers, and one connection gives that up: `load` and `next_ready` queue behind
# whatever commit is in flight, `synchronous=FULL` fsync included. Buying it back means
# more connections (a reader pool, or one per thread) rather than a different lock, which
# is a bigger store than this one.
#
# Requires SQLite 3.42 or newer (2023-05-16), which is where the `subsec` modifier
# arrives. Every clock read below is `unixepoch('now', 'subsec')`, and without `subsec`
# that is whole seconds: a lease and a visibility would round to the same second, so two
# workers polling within one second of each other could both find a row visible. It is
# not a floor `requires-python` can enforce, because Python bundles a recent SQLite on
# Windows and macOS but on Linux `sqlite3` links whatever `libsqlite3` the distribution
# ships. `sqlite3.sqlite_version` is what a deployment should check.

from __future__ import annotations

import asyncio
import sqlite3
from collections.abc import Callable
from contextlib import closing
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from time import monotonic
from typing import cast

from without_async import Milliseconds
from without_durability.codec import JSON
from without_durability.codec import CheckpointCodec
from without_durability.interfaces import LEASE
from without_durability.interfaces import Delivery
from without_durability.interfaces import Fenced
from without_durability.interfaces import Pass
from without_durability.interfaces import Recorded
from without_durability.interfaces import check_duration
from without_durability.stepwise import now_utc

# How often a worker with nothing to do asks again, which is the price of having no
# blocking read. The *lease* is not restated here: it is `interfaces.LEASE`, because unlike the
# poll interval it has to agree with something outside this store (the checkpoint claim
# the worker takes for exactly as long).
POLL = timedelta(milliseconds=50)

# How long a process finding the write lock taken waits for it, in the unit
# `PRAGMA busy_timeout` carries. A frozen count is a value, so one serves every caller.
BUSY_TIMEOUT = Milliseconds(5_000)

# `value` is TEXT rather than a richer type, which is the same shape the Redis store's
# hash field has and leaves the same question open: what goes *in* the text is the
# store's injected `CheckpointCodec`, defaulting to JSON because that is what makes a
# checkpoint readable by anything that can open the file.
#
# `NOT NULL` on `value` keeps "no row" and "a row holding JSON null" distinguishable, so
# a step that legitimately records `None` is not read back as a step that never ran.
#
# The claim and queue tables are `WITHOUT ROWID` because each is addressed by its primary
# key and never by a rowid, so the extra indirection would be pure overhead. The checkpoint
# table pays that indirection deliberately, because its rowid is what `load`'s ordering
# guarantee rests on: rowids are handed out in insertion order and the conflict update
# below never touches one, so ordering by it is the order the steps were first recorded in.
# Nothing else here is a candidate. A `WITHOUT ROWID` table has no such column, and the
# primary key would order by step name, which is a different question with a plausible
# enough answer to pass a careless test.
SCHEMA = """
CREATE TABLE IF NOT EXISTS workflow_checkpoint (
    workflow TEXT NOT NULL,
    step TEXT NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY (workflow, step)
);

CREATE TABLE IF NOT EXISTS workflow_claim (
    workflow TEXT PRIMARY KEY,
    token INTEGER NOT NULL,
    held_until REAL NOT NULL
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS workflow_queue (
    namespace TEXT NOT NULL,
    workflow TEXT NOT NULL,
    visible_at REAL NOT NULL,
    PRIMARY KEY (namespace, workflow)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS workflow_queue_visible_at ON workflow_queue (namespace, visible_at);
"""

# Take the workflow if nobody holds it, and stamp the taking with the next number up.
# Identical in shape to the Postgres statement, down to the clause that does the work:
# the `WHERE` on `DO UPDATE` is the "is it free" check, and a conflicting row whose lease
# has not elapsed fails it, so nothing is written and `RETURNING` yields no row.
#
# The clock is the database's, which here is a formality rather than a guarantee. Redis
# and Postgres read their server's clock because the claimant is a different machine and
# a lease compared against the caller's own clock is only as good as the agreement
# between the two. SQLite *is* the caller's machine, so that argument does not apply;
# keeping the clock in SQL anyway costs nothing and keeps the three stores reading alike.
CLAIM = """
INSERT INTO workflow_claim (workflow, token, held_until)
VALUES (:workflow, 1, unixepoch('now', 'subsec') + :lease)
ON CONFLICT (workflow) DO UPDATE
    SET token = workflow_claim.token + 1, held_until = unixepoch('now', 'subsec') + :lease
    WHERE workflow_claim.held_until <= unixepoch('now', 'subsec')
RETURNING token
"""

# The fenced, conditional write, as one statement. The Postgres version wraps its fence
# read in a `FOR UPDATE` CTE so a claim committing mid-statement cannot go unseen; here
# the statement is its own transaction and SQLite admits one writer, so selecting the
# claim row inline is already serialized against every other write.
#
# `DO UPDATE SET value = the value already there` is a write that changes nothing and
# therefore returns the row that was already stored, which is how a caller that lost the
# race learns the winner's value instead of carrying on with its own.
#
# The second returned column is who won, which the caller cannot work out afterwards (see
# `Recorded`). Comparing the stored *text* against the text this call offered answers it
# in the statement, where both are in hand, and two passes that ran the same effect and
# encoded it identically both count as having won: there is nothing to disagree about.
RECORD = """
INSERT INTO workflow_checkpoint (workflow, step, value)
SELECT :workflow, :step, :value FROM workflow_claim
WHERE workflow = :workflow AND token <= :token
ON CONFLICT (workflow, step) DO UPDATE SET value = workflow_checkpoint.value
RETURNING value, value = :value
"""

# The same conditional write without the fence, for a value that comes from outside any
# pass. Deliberately not gated on a claim: an approval must not fail because a worker
# happens to be mid-pass, and first-writer-wins is the whole guarantee it needs.
SUPPLY = """
INSERT INTO workflow_checkpoint (workflow, step, value)
VALUES (:workflow, :step, :value)
ON CONFLICT (workflow, step) DO UPDATE SET value = workflow_checkpoint.value
RETURNING value
"""

FENCE = "SELECT token FROM workflow_claim WHERE workflow = ?"
ALREADY = "SELECT value FROM workflow_checkpoint WHERE workflow = ? AND step = ?"
WRITE = "INSERT INTO workflow_checkpoint (workflow, step, value) VALUES (?, ?, ?)"
# `ORDER BY rowid` is the whole of the ordering guarantee, and it is load-bearing rather
# than a formality: `WHERE workflow = ?` is served by the primary key's index, so without
# it the rows come back sorted by step name.
LOAD = "SELECT step, value FROM workflow_checkpoint WHERE workflow = ? ORDER BY rowid"
# Hand the workflow back early, but keep the token, so the next claim gets the next
# number up and a pass that comes back from the dead still loses.
RELEASE = "UPDATE workflow_claim SET held_until = unixepoch('now', 'subsec') WHERE workflow = ? AND token = ?"

# Take the oldest visible workflow and push it a lease into the future. There is no
# `SKIP LOCKED` here and none is wanted: it exists so one poller does not queue behind
# another's row lock, and SQLite has no concurrent writers to step over.
TAKE = """
UPDATE workflow_queue SET visible_at = unixepoch('now', 'subsec') + :lease
WHERE (namespace, workflow) = (
    SELECT namespace, workflow FROM workflow_queue
    WHERE namespace = :namespace AND visible_at <= unixepoch('now', 'subsec')
    ORDER BY visible_at LIMIT 1
)
RETURNING workflow, visible_at
"""

# Make the workflow visible at `visible_at`, whatever it was waiting for before. A plain
# upsert rather than a conditional one, including over a pass in flight: landing on top
# of a running pass's lease is what keeps the wakeup alive, since that pass will then
# decline to remove the row.
SCHEDULE = """
INSERT INTO workflow_queue (namespace, workflow, visible_at)
VALUES (:namespace, :workflow, :visible_at)
ON CONFLICT (namespace, workflow) DO UPDATE SET visible_at = excluded.visible_at
"""

# Finish, but only if nothing asked for another pass meanwhile. Anything that did wrote a
# different `visible_at`, so the equality is the whole check.
FINISH = "DELETE FROM workflow_queue WHERE namespace = ? AND workflow = ? AND visible_at = ?"

# Suspend until a deadline, under the same comparison and for the same reason. A workflow
# holds one row here, so writing the deadline unconditionally would land on top of a
# `make_ready` that arrived while the pass was ending and push a confirmation out to a
# deadline that may be days away. The deadline is in the checkpoint either way, so the
# pass that runs sooner writes it again.
SUSPEND = "UPDATE workflow_queue SET visible_at = ? WHERE namespace = ? AND workflow = ? AND visible_at = ?"

# What an effect is for a store whose datastore is a SQLite file: a callback handed a
# cursor already inside `transact`'s transaction.
#
# It is *not* async, and that is the difference from the Postgres store's rather than an
# oversight. The whole transaction runs on one worker thread, so an effect is ordinary
# blocking code there and awaiting inside it would be both impossible and pointless.
# Whatever it returns is recorded as the step's value, so it MUST be something the store's
# codec encodes, and it MUST confine itself to the cursor it is handed: opening another
# connection puts the work outside the transaction and gives back exactly the at-least-once
# gap `transact` closes. Unlike Redis's `LuaEffect` it returns an ordinary Python value
# rather than an encoding, because it runs in this process where the codec is.
type SqliteEffect = Callable[[sqlite3.Cursor], object]


@dataclass(frozen=True, slots=True)
class Database:
    """
    One SQLite connection and the lock that keeps one caller in it at a time.

    The analogue of the Postgres store's connection pool, and the opposite shape for the
    opposite reason: a pool exists so several statements run at once, and this exists so
    they do not. Not because a connection would corrupt (SQLite is built serialized here,
    so it would not), but because a transaction belongs to the *connection*: without this,
    a caller arriving mid-`BEGIN IMMEDIATE` writes into somebody else's transaction and
    loses its write to that transaction's rollback. See the note at the top of this
    module for why the event loop's single thread does not already prevent that.

    Build it with `connect`, which applies the pragmas that make this durable rather than
    merely persistent. Share one between the checkpoint store and the queue: that is what
    makes `SqliteDurable.arrive` a single commit, and it is checked rather than assumed.
    Close it with `aclose`, never `connection.close()`, for the reason given there.
    """

    connection: sqlite3.Connection
    guard: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False, compare=False)

    async def run[T](self, work: Callable[[sqlite3.Connection], T]) -> T:
        """
        Do `work` against the connection, on a thread, with nobody else inside it.

        Cancellation is where "nobody else" has to be arranged rather than assumed, and
        it is the reason this is not simply `async with self.guard`. A thread is not
        cancellable: cancelling the caller unwinds this coroutine at once while the
        thread runs on, so releasing the guard on the way out would hand the connection
        to the next caller while the last one is still inside it. That is not a
        theoretical race. The statement in flight may be a `BEGIN IMMEDIATE`
        transaction, and a write that lands in somebody else's open transaction is
        committed or rolled back with it: `record` returns, a read sees the row, and the
        rollback takes it away again, which is precisely the guarantee this store
        exists to make.

        So the guard is released by the *thread finishing* rather than by this coroutine
        returning. The work is a task, the caller awaits a shield of it (so cancelling
        the caller leaves the task alone), and a done-callback lets go of the connection
        when it is genuinely free. A cancelled caller still unwinds immediately; what it
        no longer does is take the connection with it.

        What the shield adds beyond that is the reporting. A statement that fails after
        its caller has gone has nobody left to raise to, and `shield` hands it to the
        loop's exception handler rather than dropping it, so a write that failed on the
        way out of a process is in the log instead of nowhere.
        """
        await self.guard.acquire()
        running = asyncio.ensure_future(asyncio.to_thread(work, self.connection))
        running.add_done_callback(lambda _finished: self.guard.release())
        return await asyncio.shield(running)

    async def aclose(self) -> None:
        """
        Close the connection once nobody is inside it.

        The other half of `run`'s handshake, and the reason a caller must never reach
        for `connection.close()` itself. `sqlite3.close()` frees the connection and
        finalizes its statements; a thread still executing one is then reading freed
        memory, which segfaults the process rather than raising. `run` makes that
        reachable by design, since a cancelled caller unwinds while its thread runs on,
        so a shutdown that follows a cancellation is exactly when the two meet: the
        worker's task is cancelled, the statement it left behind is still in flight, and
        the close lands on top of it.

        Taking the guard is what waits that out, because `run` releases it from the
        thread rather than from its caller. That wait is unbounded: an effect hung
        inside a `run` thread holds the guard until it returns, and cancelling this
        coroutine while it is parked on the guard abandons the close, leaving the
        connection open. The guard is then let go again, so a `run` arriving after
        this fails on a closed connection, which is the loud version of a bug that
        would otherwise be silent.

        The close itself goes to a thread like every other driver call: under WAL it
        runs the final checkpoint (and, with `synchronous=FULL`, an fsync), which is
        real disk I/O that does not belong on the event loop.
        """
        async with self.guard:
            await asyncio.to_thread(self.connection.close)


def connect(path: Path | str, *, timeout: Milliseconds = BUSY_TIMEOUT) -> Database:
    """
    Open the database this store runs on, configured for durability rather than speed.

    - `journal_mode=WAL` so a reader does not block the writer, which is what lets a
      status query run while a pass is mid-transaction.
    - `synchronous=FULL` because this store's entire claim is that `record` returning
      means the value survives. `NORMAL` is the usual advice under WAL and it trades
      exactly that away: a commit can be lost on power loss or an OS crash. Everything
      `run_durably` reasons about assumes the commit held, so this pays the fsync.
    - `busy_timeout` so a second process finding the write lock taken waits for it rather
      than failing immediately, which is the ordinary case when two processes share the
      file.

    `autocommit=True` leaves transaction control here rather than in the driver: every
    statement below is either atomic on its own or wrapped in an explicit
    `BEGIN IMMEDIATE`, and nothing is left to a hidden implicit transaction.
    """
    connection = sqlite3.connect(path, autocommit=True, check_same_thread=False)
    connection.execute("PRAGMA journal_mode = WAL")
    connection.execute("PRAGMA synchronous = FULL")
    connection.execute(f"PRAGMA busy_timeout = {timeout.count}")
    return Database(connection=connection)


def transacted[T](connection: sqlite3.Connection, work: Callable[[sqlite3.Cursor], T]) -> T:
    """
    Run `work` between `BEGIN IMMEDIATE` and `COMMIT`, rolling back if it raises.

    `IMMEDIATE` rather than the default deferred begin, and the difference is the whole
    of the exclusion: a deferred transaction takes the write lock at its first write, so
    a fence *read* before it would not be protected and could be overtaken. Taking the
    lock up front makes the read and the write it guards one step.

    The commit is inside the `try` because committing is one of the things that can fail:
    a deferred constraint is checked there, so a `COMMIT` can raise with the transaction
    still open. Left outside, nothing rolls that back, and the transaction stays open on a
    connection every later caller shares: their writes join it, return saying they are
    durable, and are lost when it is finally discarded, while the write lock is held
    against every other process on the machine for as long as this one lives. That is
    precisely the failure `Database.run`'s guard exists to prevent, arriving by the one
    door the guard does not cover.

    The rollback is conditional for the mirror-image reason: SQLite rolls back by itself
    on a full disk, an I/O error, an interrupt, or being out of memory, so an
    unconditional `ROLLBACK` raises "cannot rollback - no transaction is active" *over*
    the error that caused it, and the caller is told the wrong thing about its own
    failure. Asking the connection is how to tell which of the two happened.
    """
    connection.execute("BEGIN IMMEDIATE")
    try:
        with closing(connection.cursor()) as cursor:
            done = work(cursor)
        connection.execute("COMMIT")
    except BaseException:
        if connection.in_transaction:
            connection.execute("ROLLBACK")
        raise
    return done


async def migrate(database: Database) -> None:
    """
    Create the three tables, from every process, as often as it likes.

    No advisory lock and no race to guard against, unlike the Postgres migration: SQLite
    runs the whole script in one exclusive transaction, so a second process either waits
    for it or finds the tables already there.

    Schema migration as a whole is not what this is. There is no versioning and no path
    from one shape of these tables to another; `user_version` is where SQLite keeps that,
    and a deployment that needs it should use it.
    """
    await database.run(lambda connection: connection.executescript(SCHEMA))


@dataclass(frozen=True, slots=True)
class SqliteCheckpointer:
    """
    A workflow's completed steps as rows in one file, and its claim as a row beside them.

    The `Checkpointer` implementation for a deployment that is one machine, and the one
    that needs nothing installed. It meets the same requirements as the others by the
    simplest route any of them take: SQLite admits one writer, so a single statement or a
    single `BEGIN IMMEDIATE` transaction is already all the exclusion this needs.

    `SqliteEffect` is a callback over the open transaction's cursor, so a step whose
    effect is a write to *this* file happens exactly once. Since the file is the whole
    datastore, that covers every table an application on this machine keeps here, which
    is a broader reach than it sounds: it is the same guarantee DBOS gets from Postgres,
    for an application that never needed Postgres.

    A workflow id carries no constraints at all: it is bound as a query parameter, never
    parsed as key structure. Nothing here derives one id from another either, so an
    application is free to name a workflow's sibling (a saga's rollback, say) however it
    likes out of its own namespace.

    `codec` is how a step's result becomes the `TEXT` in a row and comes back, defaulting
    to the stdlib's JSON. Swap it to widen what a step may return or to speed the encoding
    up; what it MUST keep is the round trip, since a resumed pass reads what it produced.
    """

    database: Database
    codec: CheckpointCodec[str] = JSON

    async def load(self, workflow: str) -> dict[str, object]:
        rows = await self.database.run(lambda connection: connection.execute(LOAD, (workflow,)).fetchall())
        return {step: self.codec.decode(encoded) for step, encoded in rows}

    async def claim(self, workflow: str, lease: timedelta) -> Pass | None:
        taken = await self.database.run(
            lambda connection: connection.execute(
                CLAIM,
                {"workflow": workflow, "lease": lease.total_seconds()},
            ).fetchone()
        )
        if taken is None:
            return None
        return Pass(workflow=workflow, token=int(taken[0]))

    async def record(self, holder: Pass, key: str, value: object) -> Recorded:
        encoded = self.codec.encode(value)
        stored = await self.database.run(
            lambda connection: connection.execute(
                RECORD,
                {
                    "workflow": holder.workflow,
                    "step": key,
                    "value": encoded,
                    "token": holder.token,
                },
            ).fetchone()
        )
        if stored is None:
            # The statement wrote nothing, which happens for exactly one reason: the
            # `WHERE` that guards the insert compared this pass's token against the fence
            # and refused it. (A missing claim row would land here too, and a `Pass` is
            # only ever handed out by a `claim` that wrote one.)
            raise Fenced(f"{holder.workflow!r} moved on while this pass held it")
        return Recorded(value=self.codec.decode(stored[0]), first=bool(stored[1]))

    async def transact(self, holder: Pass, key: str, effect: SqliteEffect) -> object:
        """
        Run `effect` and record it in one transaction, so the step happens once.

        The order is the other stores': fence first, because a superseded pass must not
        act; then the *existence* check, because a step already recorded must not run
        again, which is what makes a replay perform nothing at all; then the effect; then
        the record. `BEGIN IMMEDIATE` holds the write lock across all four, so no other
        writer can land between them and any exception rolls back the effect along with
        its record.

        The effect's result is written and read back through the codec rather than
        returned as it came, so it round-trips exactly as a later pass will see it.
        """

        def one_commit(cursor: sqlite3.Cursor) -> object:
            fence = cursor.execute(FENCE, (holder.workflow,)).fetchone()
            if fence is None or holder.token < fence[0]:
                raise Fenced(f"{holder.workflow!r} moved on while this pass held it")
            recorded = cursor.execute(ALREADY, (holder.workflow, key)).fetchone()
            if recorded is not None:
                return self.codec.decode(recorded[0])
            written = self.codec.encode(effect(cursor))
            cursor.execute(WRITE, (holder.workflow, key, written))
            return self.codec.decode(written)

        return await self.database.run(lambda connection: transacted(connection, one_commit))

    async def supply(self, workflow: str, key: str, value: object) -> object:
        stored = await self.database.run(
            lambda connection: connection.execute(
                SUPPLY,
                {"workflow": workflow, "step": key, "value": self.codec.encode(value)},
            ).fetchone()
        )
        return self.codec.decode(cast(tuple[str], stored)[0])

    async def release(self, holder: Pass) -> None:
        await self.database.run(lambda connection: connection.execute(RELEASE, (holder.workflow, holder.token)))


@dataclass(frozen=True, slots=True)
class SqliteScheduler:
    """
    `Scheduler` as one table, each row scored by when its workflow becomes visible.

    A drop-in for every other queue here, and modelled on the same visibility scheme:
    queued now is a `visible_at` in the past, sleeping is one in the future, and being
    worked on is one a lease ahead, so `wake_due`, `reclaim`, and `prepare`'s queue half
    all have nothing to do.

    It polls, like the other visibility-scored queues, so the poll interval is a floor
    under how fast anything starts. SQLite offers no blocking read and no notification a
    process outside this one can wait on, so unlike the Postgres store there is not even
    a `LISTEN`/`NOTIFY` left on the table: within one process an `asyncio.Event` would do
    it, across processes on one machine it would take a filesystem watch, and neither is
    here.
    """

    database: Database
    namespace: str = "workflow"
    # How long a taken workflow stays invisible, and so how long after a process dies
    # before another picks its workflow up. `worker.work` reads it and claims the workflow
    # for the same span, which is the whole reason it is one number: a workflow that
    # becomes visible before its claim lapses is taken by a worker that cannot write to it
    # yet. This is the knob for a deployment whose passes take longer than a minute.
    lease: timedelta = LEASE
    poll: timedelta = POLL
    # Only `make_ready` reads it: "visible now" is the one time a caller names, where the
    # lease is measured by the database (in `TAKE`) and a deadline was chosen by the
    # workflow itself. Injected so a test can place a wakeup in a clock it controls.
    now: Callable[[], datetime] = now_utc
    # The two durations as the numbers SQLite and `asyncio.sleep` want, rendered once
    # rather than per iteration of `next_ready`'s poll loop, which is the one place here
    # that runs more than once per unit of work.
    lease_seconds: float = field(init=False, repr=False, compare=False)
    poll_seconds: float = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        check_duration("a lease", self.lease)
        check_duration("a poll interval", self.poll)
        object.__setattr__(self, "lease_seconds", self.lease.total_seconds())
        object.__setattr__(self, "poll_seconds", self.poll.total_seconds())

    async def prepare(self) -> None:
        """Create the tables, which every worker does at boot and all but the first find done."""
        await migrate(self.database)

    async def make_ready(self, workflow: str) -> None:
        await self.schedule(workflow, self.now())

    async def wake_at(self, delivery: Delivery, when: datetime) -> None:
        """
        Suspend the workflow until `when`, unless something asked for a pass meanwhile.

        The receipt is the visibility this pass took, so anything that rescheduled the
        workflow since (a confirmation, another worker taking over an overrun) wrote a
        different one and this leaves it be. Which is the right answer rather than a
        concession: the deadline lives in the workflow's checkpoint, so the pass that runs
        sooner reaches the same `sleep` and writes it again.
        """
        await self.database.run(
            lambda connection: connection.execute(
                SUSPEND,
                (when.timestamp(), self.namespace, delivery.workflow, float(delivery.receipt)),
            )
        )

    async def schedule(self, workflow: str, visible_at: datetime) -> None:
        await self.database.run(
            lambda connection: connection.execute(
                SCHEDULE,
                {"namespace": self.namespace, "workflow": workflow, "visible_at": visible_at.timestamp()},
            )
        )

    async def wake_due(self, now: datetime) -> tuple[str, ...]:
        """Nothing to do: a workflow whose `visible_at` has passed is already visible."""
        return ()

    async def next_ready(self, within: timedelta) -> Delivery | None:
        """The next visible workflow, waiting up to `within` for one to appear."""
        deadline = monotonic() + within.total_seconds()
        while True:
            taken = await self.database.run(
                lambda connection: connection.execute(
                    TAKE,
                    {"namespace": self.namespace, "lease": self.lease_seconds},
                ).fetchone()
            )
            if taken is not None:
                workflow, visible_at = taken
                # The receipt is the visibility this take wrote, rendered so it is a value
                # rather than a place: `done` compares it back and declines to remove a row
                # anything else has since rescheduled.
                return Delivery(workflow=workflow, receipt=repr(float(visible_at)))
            remaining = deadline - monotonic()
            if remaining <= 0:
                return None
            await asyncio.sleep(min(self.poll_seconds, remaining))

    async def reclaim(self, idle: timedelta) -> Delivery | None:
        """Nothing to take over by hand: an abandoned workflow becomes visible on its own."""
        return None

    async def done(self, delivery: Delivery) -> None:
        """
        Drop the workflow, unless something asked for another pass while this one ran.

        The receipt is the visibility this pass took, so anything that rescheduled the
        workflow meanwhile (a confirmation, this pass's own `wake_at`, another worker
        taking over an overrun) wrote a different one and this leaves it alone.
        """
        await self.database.run(
            lambda connection: connection.execute(
                FINISH,
                (self.namespace, delivery.workflow, float(delivery.receipt)),
            )
        )


@dataclass(frozen=True, slots=True)
class SqliteDurable:
    """
    A `Durable` whose two stores are one file, so `arrive` is a single commit.

    The strongest form of the guarantee, reached by the least machinery: there is nothing
    to co-locate, no pool to share by accident, and no sharding to grow into. The two
    stores MUST hold the same `Database`, checked at construction, which here is less a
    warning about distributed transactions than a way of saying that two SQLite files are
    two datastores however adjacent they sit on disk.
    """

    checkpointer: SqliteCheckpointer
    scheduler: SqliteScheduler

    def __post_init__(self) -> None:
        if self.checkpointer.database is not self.scheduler.database:
            raise ValueError("a SqliteDurable's two stores must share one database, or `arrive` is not one commit")

    async def arrive(self, workflow: str, key: str, value: object) -> object:
        """Record the value and make the workflow ready, together or not at all."""
        visible_at = self.scheduler.now().timestamp()

        def one_commit(cursor: sqlite3.Cursor) -> object:
            stored = cursor.execute(
                SUPPLY,
                {"workflow": workflow, "step": key, "value": self.checkpointer.codec.encode(value)},
            ).fetchone()
            cursor.execute(
                SCHEDULE,
                {"namespace": self.scheduler.namespace, "workflow": workflow, "visible_at": visible_at},
            )
            return self.checkpointer.codec.decode(cast(tuple[str], stored)[0])

        return await self.checkpointer.database.run(lambda connection: transacted(connection, one_commit))
