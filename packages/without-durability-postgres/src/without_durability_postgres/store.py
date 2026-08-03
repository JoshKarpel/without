# The same two interfaces as the Redis store, over one Postgres. It is the other half of that
# argument, and putting the two side by side is the point: `Checkpointer` and `Scheduler`
# state the guarantees, and a store says how it reaches them, so a family of stores is
# not one good implementation and one compromise.
#
# What is worth reading this file *for* is how little of it is mechanism. Every write the
# Redis store needs a Lua script for is one statement here, or one transaction, and
# neither is a thing this app supplies: a transaction is what a relational database is
# for. Redis needs scripts because it has no way to say "check this, then write that, and
# let nobody in between"; SQL says it by default. So the interesting comparison is not
# "which store is better" but *where the atomic unit came from*, and here it came with
# the database.
#
# Three tables, one database:
#
#   workflow_checkpoint   one row per (workflow, step), the value as jsonb
#   workflow_claim        one row per workflow: whose pass it is, and until when
#   workflow_queue        one row per (namespace, workflow), scored by when it is visible
#
# The third is what makes this a real alternative rather than half of one, and what
# `PostgresDurable` spends: the queue write and the checkpoint write are one commit,
# which is the reason "you need no second system" is a claim Postgres can make and
# Redis-plus-something cannot.
#
# Two consequences fall out of SQL that are worth naming, because both were live
# questions in the Redis store and neither survives the move.
#
# A workflow id is a *parameter* here, never part of a key, so the constraints the Redis
# store asks of one (no braces, bounded length) have nothing to attach to. That is the
# tell the Redis store predicted: it was a property of building keys by concatenation,
# not of workflow ids.
#
# Nothing expires. Redis re-arms a TTL on every write, which sweeps finished workflows
# for free and costs the sharp edge that a workflow suspended longer than the TTL loses
# its checkpoint while its wakeup survives. Here the rows stay until something deletes
# them, so that failure is gone and a control-plane sweep is now homework. It also lets
# the fencing token be an ordinary counter rather than a hybrid logical clock: a token
# can only rewind if a claim row disappears, and here that happens only if a sweep
# deletes it, which is a policy this app chooses rather than a lifetime the store
# imposes.
#
# Namespacing is the connection's job, not the key's. A table name is already scoped by
# its schema and database, so two deployments sharing a server are two databases (or two
# `search_path`s in the DSN) rather than two prefixes. The queue keeps a `namespace`
# *column* because there the namespace separates queues rather than deployments, and as a
# column it is data, which is the same move as the workflow id.

from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from collections.abc import Callable
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from datetime import timedelta
from time import monotonic
from typing import cast

from psycopg import AsyncCursor
from psycopg.rows import TupleRow
from psycopg_pool import AsyncConnectionPool
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
# blocking read. It is restated rather than imported from the Redis store so that running
# this one pulls in no Redis client at all, which is the whole shape of the offer. The
# *lease* is not restated: it is `interfaces.LEASE`, because unlike the poll interval it has
# to agree with something outside this store (the checkpoint claim the worker takes for
# exactly as long).
POLL = timedelta(milliseconds=50)

# One DDL for one database, because it *is* one database. `value` is `jsonb` rather than
# `json` so it is stored parsed, which is what buys the indexing and the operators that
# let an operator with `psql` query a workflow's history rather than only read it.
#
# That is a *storage* decision, and it is worth separating from the codec, which is the
# boundary decision the caller injects. The column says the bytes are a JSON document and
# normalizes them; the codec says how a Python value becomes that document and comes back.
# So every value crosses the boundary as text with an explicit `::jsonb` going in and a
# `::text` coming out, rather than letting psycopg's adapter be a second, invisible codec
# underneath the injected one. The cost of the column type is that a `PostgresCheckpointer`
# constrains its codec to produce JSON *text*, which is a real narrowing next to Redis and
# SQLite; what still varies is the library and the value mapping, which is where the
# interesting codecs differ anyway.
#
# `NOT NULL` on `value` is not decoration: it is what keeps "no row" and "a row holding
# JSON null" distinguishable, so a step that legitimately records `None` is not read back
# as a step that never ran.
#
# The index is the one query that matters for throughput, `next_ready`'s scan for the
# oldest visible row in a namespace. The other two tables are read by primary key.
SCHEMA = """
CREATE TABLE IF NOT EXISTS workflow_checkpoint (
    workflow text NOT NULL,
    step text NOT NULL,
    value jsonb NOT NULL,
    PRIMARY KEY (workflow, step)
);

CREATE TABLE IF NOT EXISTS workflow_claim (
    workflow text PRIMARY KEY,
    token bigint NOT NULL,
    held_until timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS workflow_queue (
    namespace text NOT NULL,
    workflow text NOT NULL,
    visible_at timestamptz NOT NULL,
    PRIMARY KEY (namespace, workflow)
);

CREATE INDEX IF NOT EXISTS workflow_queue_visible_at ON workflow_queue (namespace, visible_at);
"""

# An arbitrary constant, and the only thing about it that matters is that every process
# running this migration picks the same one. `CREATE TABLE IF NOT EXISTS` is not safe
# against itself: two of them racing on a fresh database is a duplicate-key error in the
# catalog rather than a no-op, and every worker runs the migration at boot.
MIGRATION_LOCK = 0x77_0F_10_2026

# Take the workflow if nobody holds it, and stamp the taking with the next number up.
#
# One statement, and every part of the Lua script it replaces is a clause of it. The
# `WHERE` on `DO UPDATE` is the "is it free" check: a conflicting row whose lease has not
# elapsed fails the predicate, so the update does not happen and `RETURNING` yields no
# row, which is how a lost race is reported. The insert arm covers a workflow nobody has
# ever claimed, and Postgres serializes two of those against each other on the primary
# key, so the loser waits and then takes the `DO UPDATE` path rather than both winning.
#
# The clock is `now()`, which is the server's and is the transaction's start time. The
# reasoning is the Redis store's: a lease compared against the claimant's own clock is
# only as good as the agreement between the two, which is exactly what fails when a
# machine is unhealthy enough to stall mid-pass.
CLAIM = """
INSERT INTO workflow_claim AS held (workflow, token, held_until)
VALUES (%(workflow)s, 1, now() + %(lease)s)
ON CONFLICT (workflow) DO UPDATE
    SET token = held.token + 1, held_until = now() + %(lease)s
    WHERE held.held_until <= now()
RETURNING token
"""

# The fenced, conditional write, and the whole of `record` in one statement.
#
# The `FOR UPDATE` is doing real work rather than being belt-and-braces. Without it the
# fence is read from the statement's snapshot, so a claim committing a microsecond after
# the statement began would go unseen and a superseded pass's write would land. Taking
# the row lock makes this statement queue behind any claim in flight and then re-read the
# row it locked, so the token compared against is the newest one.
#
# The rest is `HSETNX` and its read-back, as one upsert. `DO UPDATE SET value = the value
# already there` is a write that changes nothing and therefore returns the row that was
# already stored, which is how a caller that lost the race learns the winner's value
# instead of carrying on with its own. A plain `DO NOTHING` would return no row at all
# and force a second read that a concurrent inserter could still beat.
#
# The second returned column is who won, which the caller cannot work out afterwards (see
# `Recorded`). Here the comparison is between `jsonb` values rather than text, which is
# the stronger of the two: it is semantic, so two encoders that order an object's keys
# differently still agree.
#
#   returns  the value stored after the call and whether it is this call's, or no row at
#            all when the pass is fenced
RECORD = """
WITH fence AS (
    SELECT token FROM workflow_claim WHERE workflow = %(workflow)s FOR UPDATE
)
INSERT INTO workflow_checkpoint AS recorded (workflow, step, value)
SELECT %(workflow)s, %(step)s, %(value)s::jsonb FROM fence WHERE fence.token <= %(token)s
ON CONFLICT (workflow, step) DO UPDATE SET value = recorded.value
RETURNING recorded.value::text, recorded.value = %(value)s::jsonb
"""

# The same conditional write without the fence, for a value that comes from outside any
# pass. Deliberately not gated on a claim: an approval must not fail because a worker
# happens to be mid-pass, and first-writer-wins is the whole guarantee it needs.
SUPPLY = """
INSERT INTO workflow_checkpoint AS recorded (workflow, step, value)
VALUES (%(workflow)s, %(step)s, %(value)s::jsonb)
ON CONFLICT (workflow, step) DO UPDATE SET value = recorded.value
RETURNING recorded.value::text
"""

# The three statements `transact` runs between `BEGIN` and `COMMIT`, with the effect's own
# work in the middle. They are separate strings rather than one because the effect is
# arbitrary application SQL that this store cannot see, which is precisely what makes the
# transaction worth having.
FENCE = "SELECT token FROM workflow_claim WHERE workflow = %s FOR UPDATE"
ALREADY = "SELECT value::text FROM workflow_checkpoint WHERE workflow = %s AND step = %s"
WRITE = "INSERT INTO workflow_checkpoint (workflow, step, value) VALUES (%s, %s, %s::jsonb) RETURNING value::text"

LOAD = "SELECT step, value::text FROM workflow_checkpoint WHERE workflow = %s"
# Hand the workflow back early, but keep the token, so the next claim gets the next
# number up and a pass that comes back from the dead still loses. Conditional on the
# token for the same reason `release` is in the Redis store: a superseded pass letting go
# must not hand away a claim someone else is holding.
RELEASE = "UPDATE workflow_claim SET held_until = now() WHERE workflow = %s AND token = %s"

# What an effect is for a store whose datastore is a Postgres database: an async callback
# handed a cursor that is already inside `transact`'s transaction. The Redis store's is a
# Lua script and the in-memory double's is a function over its own dict; nothing is shared
# between the three but the position in `transact`.
#
# A callback rather than a statement-and-parameters pair, because the transaction is the
# unit and a caller may need several statements in it, may need to read before it writes,
# and may want ordinary Python between them. Whatever it returns is recorded as the step's
# value, so it MUST be something the store's codec encodes, and it MUST confine itself to
# the cursor it is handed: opening another connection puts the work outside the transaction
# and gives back exactly the at-least-once gap `transact` exists to close. Unlike Redis's
# `LuaEffect` it returns an ordinary Python value rather than an encoding, because it runs
# in this process where the codec is.
type SqlEffect = Callable[[AsyncCursor[TupleRow]], Awaitable[object]]


async def migrate(pool: AsyncConnectionPool) -> None:
    """
    Create the three tables, from every process, as often as it likes.

    Idempotent by `IF NOT EXISTS` and safe against itself by the advisory lock, which is
    the part that is easy to skip: concurrent `CREATE TABLE IF NOT EXISTS` is a
    duplicate-key error on the system catalog rather than a no-op, and a fleet of workers
    booting together is exactly a race. `pg_advisory_xact_lock` is held to the end of the
    surrounding transaction and released by the commit, so there is nothing to unlock.

    Schema migration as a whole is not what this is. There is no versioning and no path
    from one shape of these tables to another, which is the ordinary thing a deployment
    would want and the ordinary tool (Alembic, sqitch, plain numbered SQL files) is where
    it belongs.
    """
    async with pool.connection() as connection:
        await connection.execute("SELECT pg_advisory_xact_lock(%s)", (MIGRATION_LOCK,))
        await connection.execute(SCHEMA)


@dataclass(frozen=True, slots=True)
class PostgresCheckpointer:
    """
    A workflow's completed steps as rows in one table, and its claim as a row in another.

    The `Checkpointer` implementation for the deployment that already has a Postgres, and
    the one that can co-commit with the application's own tables, which is the capability
    the whole `Effect` parameter exists for. `SqlEffect` is a callback over `transact`'s
    open transaction, so a step whose effect is a write to *this* database happens exactly
    once rather than at least once.

    It holds a pool rather than a connection, because a pass is one short transaction and
    several passes run at once: a worker with a pool of ten runs ten passes without them
    queueing behind each other, and `next_ready`'s poll is not blocking a connection while
    it waits. Call `migrate` once against the same pool before anything else, at the
    entrypoint that built it.

    The durability question `RedisCheckpointer` has to hedge on does not arise here.
    `record` returning means the transaction committed, and a default Postgres has
    `synchronous_commit` on, so the write is on disk and survives a crash of the server
    rather than only of the client. That is exactly what `run_durably`'s reasoning about
    the window between an effect and its record assumes.

    A workflow id carries no constraints here at all, since it is bound as a query parameter
    rather than parsed as key structure. Nothing here derives one id from another either,
    so an application is free to name a workflow's sibling (a saga's rollback, say)
    however it likes out of its own namespace.

    `codec` is how a step's result becomes the document in a `jsonb` column and comes
    back, defaulting to the stdlib's JSON. The column type narrows what a codec here may
    be in a way it does not for the other two stores: it MUST render JSON *text*, because
    that is what `jsonb` will accept. What that still leaves free is the library and the
    value mapping, which is the part worth changing. What it MUST keep, as everywhere, is
    the round trip.
    """

    pool: AsyncConnectionPool
    codec: CheckpointCodec[str] = JSON

    async def load(self, workflow: str) -> dict[str, object]:
        async with self.pool.connection() as connection, connection.cursor() as cursor:
            await cursor.execute(LOAD, (workflow,))
            return {step: self.codec.decode(encoded) for step, encoded in await cursor.fetchall()}

    async def claim(self, workflow: str, lease: timedelta) -> Pass | None:
        async with self.pool.connection() as connection, connection.cursor() as cursor:
            await cursor.execute(CLAIM, {"workflow": workflow, "lease": lease})
            taken = await cursor.fetchone()
        if taken is None:
            return None
        return Pass(workflow=workflow, token=cast(int, taken[0]))

    async def record(self, holder: Pass, key: str, value: object) -> Recorded:
        async with self.pool.connection() as connection, connection.cursor() as cursor:
            await cursor.execute(
                RECORD,
                {
                    "workflow": holder.workflow,
                    "step": key,
                    "value": self.codec.encode(value),
                    "token": holder.token,
                },
            )
            stored = await cursor.fetchone()
        if stored is None:
            # The statement wrote nothing, which happens for exactly one reason: the
            # `WHERE` that guards the insert compared this pass's token against the fence
            # and refused it. (A missing claim row would land here too, and a `Pass` is
            # only ever handed out by a `claim` that wrote one.)
            raise Fenced(f"{holder.workflow!r} moved on while this pass held it")
        return Recorded(value=self.codec.decode(cast(str, stored[0])), first=cast(bool, stored[1]))

    async def transact(self, holder: Pass, key: str, effect: SqlEffect) -> object:
        """
        Run `effect` and record it in one transaction, so the step happens once.

        The order is the Lua script's, for the same reasons: fence first, because a
        superseded pass must not act; then the *existence* check, because a step already
        recorded must not run again, which is what makes a replay perform nothing at all;
        then the effect; then the record. What differs is that none of it needed a
        mechanism. `BEGIN` and `COMMIT` are the atomicity, the connection pool's context
        manager is what issues them, and an exception anywhere inside (the fence, the
        effect's own SQL, a constraint the effect violated) rolls the whole thing back
        including the record.

        The effect's result is written and read back through the column rather than
        returned as it came, so it round-trips through the codec exactly as a later pass
        will see it. A step that returns something `jsonb` renders differently (a tuple,
        which comes back a list) then does so on the first pass rather than surprising the
        second.
        """
        async with self.pool.connection() as connection, connection.cursor() as cursor:
            await cursor.execute(FENCE, (holder.workflow,))
            fence = await cursor.fetchone()
            if fence is None or holder.token < fence[0]:
                raise Fenced(f"{holder.workflow!r} moved on while this pass held it")
            await cursor.execute(ALREADY, (holder.workflow, key))
            recorded = await cursor.fetchone()
            if recorded is not None:
                return self.codec.decode(cast(str, recorded[0]))
            await cursor.execute(WRITE, (holder.workflow, key, self.codec.encode(await effect(cursor))))
            return self.codec.decode(cast(tuple[str], await cursor.fetchone())[0])

    async def supply(self, workflow: str, key: str, value: object) -> object:
        async with self.pool.connection() as connection, connection.cursor() as cursor:
            await cursor.execute(SUPPLY, {"workflow": workflow, "step": key, "value": self.codec.encode(value)})
            return self.codec.decode(cast(tuple[str], await cursor.fetchone())[0])

    async def release(self, holder: Pass) -> None:
        async with self.pool.connection() as connection:
            await connection.execute(RELEASE, (holder.workflow, holder.token))


# Take the oldest workflow that is visible and push it a lease into the future, in one
# statement, so two workers polling at the same instant cannot both take it.
#
# `FOR UPDATE SKIP LOCKED` is the whole of the distribution: the row the first worker is
# updating is locked, and the second does not queue behind it but passes over it to the
# next visible row. Without `SKIP LOCKED` a pool of workers polling one queue serializes
# on its head; with it, they fan out. It is also why nothing here needs a consumer group.
#
# The new `visible_at` is returned because it *is* the receipt, which is the trick
# `RedisSetScheduler` documents at length: a workflow appears once, so a wakeup arriving
# mid-pass lands on top of the entry that pass is holding, and finishing has to be
# conditional on the value being unchanged or it throws the wakeup away.
#
#   returns  the workflow and its new visibility, or no row when nothing is visible yet
TAKE = """
UPDATE workflow_queue AS entry
SET visible_at = now() + %(lease)s
FROM (
    SELECT workflow FROM workflow_queue
    WHERE namespace = %(namespace)s AND visible_at <= now()
    ORDER BY visible_at
    FOR UPDATE SKIP LOCKED
    LIMIT 1
) AS due
WHERE entry.namespace = %(namespace)s AND entry.workflow = due.workflow
RETURNING entry.workflow, entry.visible_at
"""

# Make the workflow visible at `visible_at`, whatever it was waiting for before. A plain
# upsert rather than a conditional one, including over a pass in flight: landing on top of
# a running pass's lease is what keeps the wakeup alive, since that pass will now decline
# to remove the row.
SCHEDULE = """
INSERT INTO workflow_queue (namespace, workflow, visible_at)
VALUES (%(namespace)s, %(workflow)s, %(visible_at)s)
ON CONFLICT (namespace, workflow) DO UPDATE SET visible_at = EXCLUDED.visible_at
"""

# Finish, but only if nothing asked for another pass in the meantime. Anything that did
# wrote a different `visible_at`, so the equality is the whole check.
FINISH = "DELETE FROM workflow_queue WHERE namespace = %s AND workflow = %s AND visible_at = %s"


@dataclass(frozen=True, slots=True)
class PostgresScheduler:
    """
    `Scheduler` as one table, each row scored by when its workflow becomes visible.

    A drop-in for either Redis queue: the same protocol, the same worker, the same API.
    It is modelled on the sorted-set one rather than on the stream, so queued now is a
    `visible_at` in the past, sleeping is one in the future, and being worked on is one a
    lease ahead, which leaves `wake_due`, `reclaim`, and `prepare`'s queue half with
    nothing to do.

    What Postgres adds over the sorted set is `SKIP LOCKED`, which is what lets several
    workers poll one queue without serializing on its head, and what a `ZRANGEBYSCORE` in
    a Lua script gets instead by being the only thing running.

    What it does not add is the blocking read. This polls on `poll`, so an idle worker
    costs a round trip per interval and a submitted order waits up to one interval to be
    picked up. Postgres can close that (`LISTEN`/`NOTIFY` on a dedicated connection, woken
    by a trigger or by the writer) and this does not, which is the honest state of it
    rather than a claim that a table cannot wait.

    `namespace` separates queues rather than deployments, and it is a column rather than
    part of a table name, so a queue name is data here as a workflow id is.
    """

    pool: AsyncConnectionPool
    namespace: str = "workflow"
    # How long a taken workflow stays invisible, and so how long after a worker dies
    # before someone else picks its workflow up. `worker.work` reads it and claims the
    # workflow for the same span, which is the whole reason it is one number: a workflow
    # that becomes visible before its claim lapses is taken by a worker that cannot write
    # to it yet. This is the knob for a deployment whose passes take longer than a minute.
    lease: timedelta = LEASE
    poll: timedelta = POLL
    # Only `make_ready` reads it: "visible now" is the one time a caller names, where the
    # lease is measured by the server (in `TAKE`) and a deadline was chosen by the
    # workflow itself. Injected so a test can place a wakeup in a clock it controls.
    now: Callable[[], datetime] = now_utc
    # The poll interval as the number `asyncio.sleep` wants, rendered once rather than per
    # iteration of `next_ready`'s loop, which is the one place here that runs more than
    # once per unit of work. `lease` stays a `timedelta`, since psycopg adapts it directly
    # into the `interval` the statement wants.
    poll_seconds: float = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        check_duration("a lease", self.lease)
        check_duration("a poll interval", self.poll)
        object.__setattr__(self, "poll_seconds", self.poll.total_seconds())

    async def prepare(self) -> None:
        """
        Create the tables, which every worker does at boot and all but the first find done.

        It creates the *checkpoint* tables too, because there is one database and one DDL
        for it. That is a little more than this interface is asked for, and it is the right
        place anyway: the worker already calls `prepare` before reading a queue, so a
        deployment gets its schema from the same call whichever queue it runs, and an
        entrypoint that would rather be explicit calls `migrate` itself.
        """
        await migrate(self.pool)

    async def make_ready(self, workflow: str) -> None:
        await self.schedule(workflow, self.now())

    async def wake_at(self, workflow: str, when: datetime) -> None:
        await self.schedule(workflow, when)

    async def schedule(self, workflow: str, visible_at: datetime) -> None:
        async with self.pool.connection() as connection:
            await connection.execute(
                SCHEDULE,
                {"namespace": self.namespace, "workflow": workflow, "visible_at": visible_at},
            )

    async def wake_due(self, now: datetime) -> tuple[str, ...]:
        """Nothing to do: a workflow whose `visible_at` has passed is already visible."""
        return ()

    async def next_ready(self, within: timedelta) -> Delivery | None:
        """
        The next visible workflow, waiting up to `within` for one to appear.

        Polling, because nothing here is listening. `within` bounds how long a cancelled
        worker sits in this call before it can notice, but unlike a blocking read it is
        spent in round trips rather than in one parked call, which is the cost of the
        design and the reason `poll` is a knob.
        """
        deadline = monotonic() + within.total_seconds()
        while True:
            async with self.pool.connection() as connection, connection.cursor() as cursor:
                await cursor.execute(TAKE, {"namespace": self.namespace, "lease": self.lease})
                taken = await cursor.fetchone()
            if taken is not None:
                workflow, visible_at = taken
                # The receipt is the visibility this take wrote, rendered so it is a value
                # rather than a place: `done` compares it back and declines to remove a row
                # anything else has since rescheduled.
                return Delivery(workflow=workflow, receipt=cast(datetime, visible_at).isoformat())
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
        taking over an overrun) wrote a different one and this leaves it alone. That is why
        a worker may call `wake_at` and then `done` in that order without the second
        undoing the first.
        """
        async with self.pool.connection() as connection:
            await connection.execute(
                FINISH,
                (self.namespace, delivery.workflow, datetime.fromisoformat(delivery.receipt)),
            )


@dataclass(frozen=True, slots=True)
class PostgresDurable:
    """
    A `Durable` whose two stores are one database, so `arrive` is a single commit.

    This is the row `SplitDurable` cannot fill in. Recording the value a workflow is
    waiting on and making the workflow runnable are two writes with a crash window
    between them everywhere else; here they are two statements in one transaction, so the
    window does not exist. That is the same capability `transact` offers a step, arriving
    at the interface above rather than inside a pass, and it is available for the same reason:
    both things live in one datastore.

    Which is why the two stores MUST share a pool, checked at construction rather than
    documented. It is the exact question `LuaEffect` asks with its hash tag, and it does
    not stop being asked because SQL hides it: a checkpoint and a queue in two Postgres
    databases are two datastores, and a transaction across them is a distributed
    transaction whatever the connection string suggests. Sharded Postgres asks it again
    at the next level down, where the answer is that both tables must be distributed by
    the workflow id and co-located, or the "one commit" here becomes a two-phase commit
    across nodes.
    """

    checkpointer: PostgresCheckpointer
    scheduler: PostgresScheduler

    def __post_init__(self) -> None:
        if self.checkpointer.pool is not self.scheduler.pool:
            raise ValueError("a PostgresDurable's two stores must share one pool, or `arrive` is not one commit")

    async def arrive(self, workflow: str, key: str, value: object) -> object:
        """
        Record the value and make the workflow ready, together or not at all.

        The order within the transaction does not matter, which is the point: a commit
        has no halfway. What does matter is that both statements go through the *same*
        cursor, since a second connection would be a second transaction wearing the same
        method's name.
        """
        codec = self.checkpointer.codec
        async with self.checkpointer.pool.connection() as connection, connection.cursor() as cursor:
            await cursor.execute(SUPPLY, {"workflow": workflow, "step": key, "value": codec.encode(value)})
            stored = cast(tuple[str], await cursor.fetchone())
            await cursor.execute(
                SCHEDULE,
                {"namespace": self.scheduler.namespace, "workflow": workflow, "visible_at": self.scheduler.now()},
            )
            return codec.decode(stored[0])
