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
from without_durability.interfaces import INBOX
from without_durability.interfaces import INBOX_DIGITS
from without_durability.interfaces import LEASE
from without_durability.interfaces import Delivery
from without_durability.interfaces import Entry
from without_durability.interfaces import Fenced
from without_durability.interfaces import Pass
from without_durability.interfaces import Recorded
from without_durability.interfaces import Written
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
# `seq` is what `load`'s ordering guarantee rests on. The default is evaluated on insert
# and left alone by the conflict update below, which is exactly the property the guarantee
# needs: the writer that first recorded a step decides where it sits, and a later write
# that loses moves neither the value nor the position.
#
# It is deliberately not scoped to the workflow. Numbering per workflow would mean reading
# the current maximum on every write, and the contract is only about the order *within* one
# workflow, which a shared sequence satisfies with gaps.
#
# There is no physical order to fall back on, which is worth stating because a heap scan
# looks like insertion order right up until it is not: the conflict update is a real MVCC
# update, so it writes a new tuple version and moves the row.
#
# `written_at` is what `history` reads, and its `DEFAULT` is doing the same work `seq`'s
# does: evaluated on insert, and left alone by every conflict update below, so a losing
# write moves the value, the position, and the time equally not at all. The clock is
# `clock_timestamp()` and not the `now()` every other statement here reads, which is the
# one place this file wants a time that is not the transaction's start: `transact` runs
# its effect *inside* the transaction, so a step that spent ten seconds at a gateway would
# be stamped ten seconds before it landed, and could carry an earlier time than a `supply`
# that committed while it ran and took a lower `seq`, which is `history` returning its
# records in one order and their times in another. Both are the *server's* clock, which is
# what makes two records' times comparable across the machines that wrote them, and is the
# same clock the claim's lease is measured by.
#
# `workflow_seq` is a named sequence with a `DEFAULT` rather than an identity column,
# because `append` has to mint a key from the *same* number that becomes the row's
# position, and an identity column is a number no statement is allowed to see. Two
# sequences cannot do it: the inbox key and the position would be drawn separately, so two
# concurrent appends could take them in opposite orders and `load` would render the pair
# backwards against keys that sort the other way. One number is both, so the two orders
# cannot disagree. `nextval` is atomic and never hands the same number out twice, which is
# what makes an inbox key safe to mint under concurrent writers where `MAX(...) + 1` inside
# a statement is a race two inserts can both win, with the loser's message vanishing into
# first-writer-wins and no error to show for it. It is shared across workflows and it skips
# numbers on rollback, so any one workflow's keys have gaps; the contract asks only that
# they sort into append order within a workflow, which is exactly what a shared counter
# gives.
#
# The index is the one query that matters for throughput, `next_ready`'s scan for the
# oldest visible row in a namespace. The other two tables are read by primary key.
SCHEMA = """
CREATE SEQUENCE IF NOT EXISTS workflow_seq;

CREATE TABLE IF NOT EXISTS workflow_checkpoint (
    workflow text NOT NULL,
    step text NOT NULL,
    value jsonb NOT NULL,
    seq bigint NOT NULL DEFAULT nextval('workflow_seq'),
    written_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (workflow, step)
);

CREATE TABLE IF NOT EXISTS workflow_claim (
    workflow text PRIMARY KEY,
    token bigint NOT NULL,
    -- The budget: the latest this claim can lapse at, whatever its holder does.
    held_until timestamptz NOT NULL,
    -- When it lapses if nothing more is heard, which is what `CLAIM` tests. Every statement
    -- that writes it holds it at or below `held_until` with a `LEAST`, so a sign of life
    -- cannot carry a pass past its budget or take a workflow back after a `RELEASE`.
    alive_until timestamptz NOT NULL,
    -- What one sign of life is worth, carried on the row because `RECORD` is one of them
    -- and is not told: a write is the plainest word from a pass there is, and the statement
    -- making it has only the workflow to go on. On the row rather than in a store-wide
    -- setting so a workflow claimed with a short window cannot quietly renew on a long one.
    alive_for interval NOT NULL
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
INSERT INTO workflow_claim AS held (workflow, token, held_until, alive_until, alive_for)
VALUES (
    %(workflow)s, 1,
    now() + %(budget)s, LEAST(now() + %(alive)s, now() + %(budget)s), %(alive)s
)
ON CONFLICT (workflow) DO UPDATE
    SET token = held.token + 1,
        held_until = now() + %(budget)s,
        alive_until = LEAST(now() + %(alive)s, now() + %(budget)s),
        alive_for = %(alive)s
    WHERE held.alive_until <= now()
RETURNING token
"""

# A fresh budget, and a sign of life with it: what a step that declared a `within` spends
# before it runs its effect.
#
# Conditional on the token rather than on the deadline, because a claim that has *lapsed*
# but not been taken is still this pass's to stretch: nobody else has raised the fence, so
# nothing has gone wrong that refusing here would repair. The token is the only thing that
# says somebody else owns the workflow now.
#
# `GREATEST` against the budget already standing, so a step naming a window shorter than
# what is left takes nothing away from the unannotated steps behind it: a budget can only
# ever be too generous from here, which is the promise `extending` skips round trips on.
EXTEND = """
UPDATE workflow_claim
SET held_until = GREATEST(held_until, now() + %(budget)s),
    alive_until = LEAST(now() + %(alive)s, GREATEST(held_until, now() + %(budget)s)),
    alive_for = %(alive)s
WHERE workflow = %(workflow)s AND token <= %(token)s
"""

# A sign of life and nothing else: the worker's tick, which says this pass is still running
# without saying it may run any longer than it was already granted. `LEAST` against the
# budget is what makes that true rather than merely intended.
#
# Refused once the budget has run out as well as below the fence, because the answer is
# what the worker acts on. A renewal that reported success on a lapsed claim would keep a
# hung pass running, holding the only delivery for its workflow, for as long as nothing
# else happened to take it.
RENEW = """
UPDATE workflow_claim
SET alive_until = LEAST(now() + %(alive)s, held_until), alive_for = %(alive)s
WHERE workflow = %(workflow)s AND token <= %(token)s AND held_until > now()
"""

# The fenced, conditional write, and the whole of `record` in one statement.
#
# The fence CTE is an `UPDATE` because the write is also a sign of life, and the cheapest
# place to say so is the statement that was already taking a row lock on the claim. That is
# what makes a workflow of ordinary short steps renew itself for nothing, and leaves the
# worker's tick with the case it is really for: one step long enough that no write falls
# inside a whole lease.
#
# The lock is doing real work rather than being belt-and-braces. Without it the fence is
# read from the statement's snapshot, so a claim committing a microsecond after the
# statement began would go unseen and a superseded pass's write would land. Taking the row
# lock makes this statement queue behind any claim in flight and then re-read the row it
# locked, so the token compared against is the newest one. An `UPDATE` gives that the same
# way `SELECT ... FOR UPDATE` did, re-evaluating its `WHERE` against the committed row
# version and returning what it re-read; confirmed against a real server rather than
# assumed, since the whole fence rests on it.
#
# The token is in the CTE's `WHERE` rather than only in the insert's, so a refused write
# renews nothing: a superseded pass's stray writes would otherwise keep the *winner's* claim
# alive after the winner had died, delaying the takeover that its silence should have
# brought on. The `LEAST` covers the other stray write, after a `RELEASE`: the budget is
# already `now()` then, so a write still in flight renews to `now()` and takes nothing back
# from whoever has claimed the workflow since.
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
    UPDATE workflow_claim
    SET alive_until = LEAST(now() + alive_for, held_until)
    WHERE workflow = %(workflow)s AND token <= %(token)s
    RETURNING token
)
INSERT INTO workflow_checkpoint AS recorded (workflow, step, value)
SELECT %(workflow)s, %(step)s, %(value)s::jsonb FROM fence
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

# `supply` under a key this statement mints instead of one the caller brought: the append
# that puts a message in a workflow's inbox.
#
# The CTE is what draws one number and spends it twice, as the key and as the row's
# position, which is the whole of why the key order and the load order agree under
# concurrency. `nextval` in the `SELECT` list is evaluated once for the one row it
# produces, and both columns then read that value rather than calling the sequence again.
# Supplying `seq` explicitly is the reason the column carries a `DEFAULT` rather than being
# an identity: every other insert here leaves it out and takes the default.
#
# No `ON CONFLICT` clause, deliberately. `nextval` never repeats, so the key is fresh by
# construction and a conflict would mean the numbering is broken; a duplicate-key error is
# the loud version of that, where an upsert would quietly hand back somebody else's
# message.
APPEND = f"""
WITH minted AS (SELECT nextval('workflow_seq') AS seq)
INSERT INTO workflow_checkpoint AS entry (workflow, step, value, seq)
SELECT
    %(workflow)s,
    '{INBOX}' || lpad(minted.seq::text, {INBOX_DIGITS}, '0'),
    %(value)s::jsonb,
    minted.seq
FROM minted
RETURNING entry.step, entry.value::text
"""

# The four statements `transact` runs between `BEGIN` and `COMMIT`, with the effect's own
# work in the middle. They are separate strings rather than one because the effect is
# arbitrary application SQL that this store cannot see, which is precisely what makes the
# transaction worth having.
#
# The fence is read twice, and the split is where the row lock goes. `FENCE` is a plain
# read before the effect, so a pass already superseded performs nothing; it takes no lock,
# so the claim row stays free while the effect runs, and the worker's `RENEW` on another
# connection lands instead of queueing behind the transaction for the whole effect. `WROTE`
# is the locked re-read *after* the effect, in the statement that renews for the reason
# `RECORD`'s does: it queues behind any claim in flight and re-evaluates against the
# committed row, so a pass superseded while its effect ran is refused here and the effect
# rolls back with the transaction. The lock is then held for one statement's worth rather
# than for the effect, which is the same window `RECORD` holds it for.
#
# `clock_timestamp()` rather than `now()`, and this is the one claim statement that needs
# it: `now()` is the transaction's start, and a sign of life stamped from before a long
# effect began could already be in the past by the time it commits, which would say a
# pass had gone quiet at the moment it was speaking.
FENCE = "SELECT token FROM workflow_claim WHERE workflow = %s"
WROTE = """
UPDATE workflow_claim SET alive_until = LEAST(clock_timestamp() + alive_for, held_until)
WHERE workflow = %s AND token <= %s
RETURNING token
"""
ALREADY = "SELECT value::text FROM workflow_checkpoint WHERE workflow = %s AND step = %s"
# `ON CONFLICT DO NOTHING` rather than a plain insert, because `supply` is deliberately not
# gated on the claim and so is the one writer this transaction's fence does not exclude. An
# approval landing between the `ALREADY` read and this write would otherwise turn a step
# into a duplicate-key error, which `transact` MUST not answer with: the step is recorded,
# so the contract is to hand back what is recorded. Returning no row says that happened,
# and the caller rolls the effect back rather than committing work whose record belongs to
# somebody else.
WRITE = """
INSERT INTO workflow_checkpoint (workflow, step, value) VALUES (%s, %s, %s::jsonb)
ON CONFLICT (workflow, step) DO NOTHING
RETURNING value::text
"""

LOAD = "SELECT step, value::text FROM workflow_checkpoint WHERE workflow = %s ORDER BY seq"
HISTORY = "SELECT step, value::text, written_at FROM workflow_checkpoint WHERE workflow = %s ORDER BY seq"

# Forget every record a workflow has. Paired with `SUPERSEDE` below and never run without
# it, which is what the transaction in `discard` is for.
DISCARD = "DELETE FROM workflow_checkpoint WHERE workflow = %s"

# Take the fencing token *up*, so a pass still holding one is refused at its next write.
#
# An `UPDATE` rather than the upsert `CLAIM` is, and the difference is what it declines to
# do: a workflow with no claim row has no `Pass` outstanding, since a `Pass` is only ever
# handed out by a `claim` that wrote one, so there is nothing to fence and a row minted
# here would be a tombstone for a workflow nobody ever claimed. `held_until = now()` hands
# the workflow back at the same time, so it is claimable again immediately: what is kept is
# the ordering, not the claim.
SUPERSEDE = """
UPDATE workflow_claim SET token = token + 1, held_until = now(), alive_until = now()
WHERE workflow = %s
"""
# Hand the workflow back early, but keep the token, so the next claim gets the next
# number up and a pass that comes back from the dead still loses. Conditional on the
# token for the same reason `release` is in the Redis store: a superseded pass letting go
# must not hand away a claim someone else is holding.
#
# Both deadlines, and the budget is the load-bearing one: a write this pass had already
# started is entitled to land (it keeps its token), and bringing `held_until` down to now
# is what stops that write's own renewal from claiming the workflow straight back, since
# every renewal is a `LEAST` against it.
RELEASE = """
UPDATE workflow_claim SET held_until = now(), alive_until = now()
WHERE workflow = %s AND token = %s
"""

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


class Supplied(Exception):
    """
    Something outside the pass recorded this step first, so the effect must not stand.

    Control flow rather than a failure, and it never leaves `transact`: raising is how the
    effect's transaction is rolled back, since the value to return is a value the *other*
    writer committed and reading it is a separate transaction's job.
    """


async def migrate(pool: AsyncConnectionPool) -> None:
    """
    Create the three tables and the sequence behind `seq`, from every process, as often as it likes.

    Idempotent by `IF NOT EXISTS` and safe against itself by the advisory lock, which is
    the part that is easy to skip: concurrent `CREATE TABLE IF NOT EXISTS` is a
    duplicate-key error on the system catalog rather than a no-op (and `CREATE SEQUENCE IF
    NOT EXISTS` is the same), and a fleet of workers booting together is exactly a race. `pg_advisory_xact_lock` is held to the end of the
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

    The column narrows the *values* too, and this is the one place where "store it as
    `jsonb`" is not free. `jsonb` holds a parsed document rather than the text it was
    given, so what comes back is `jsonb`'s rendering of the value rather than the codec's,
    and three things change with it:

    - a number goes through `numeric`, so a step returning `1e16` is read back as the
      integer `10000000000000000`. Above 2^53 it is not even the same number, since
      `json.dumps` writes the shortest decimal that round-trips *as a float* and `numeric`
      keeps that decimal exactly: `2.024478232766865e+16` returns as
      `20244782327668650`, which is a different value and not merely a different type.
    - keys are reordered by `jsonb`'s own rule (length, then bytes), so a mapping comes
      back in an order the codec did not choose. Equality survives it; iteration order
      does not, so a workflow that iterates a recorded mapping should sort it.
    - a string `jsonb` cannot hold is refused outright rather than narrowed: a `NUL`
      escape or a lone surrogate is valid JSON and valid to every other store here, and
      `record` raises on the cast.

    Nothing about the codec can repair any of it, since it happens after `encode` and
    before `decode`. So the round trip a step result MUST survive here is `jsonb`'s and
    not only JSON's. `run_durably` catches the first of the three rather than a comment,
    by comparing what a node returned against what the store reads back, type included,
    on the pass that wrote it; `Run.step`'s parser is where a stepwise workflow says what
    it expects.
    """

    pool: AsyncConnectionPool
    codec: CheckpointCodec[str] = JSON

    async def load(self, workflow: str) -> dict[str, object]:
        async with self.pool.connection() as connection, connection.cursor() as cursor:
            await cursor.execute(LOAD, (workflow,))
            return {step: self.codec.decode(encoded) for step, encoded in await cursor.fetchall()}

    async def history(self, workflow: str) -> dict[str, Written]:
        """The same records `load` returns, each with the moment the server wrote it."""
        async with self.pool.connection() as connection, connection.cursor() as cursor:
            await cursor.execute(HISTORY, (workflow,))
            return {
                step: Written(value=self.codec.decode(encoded), at=written_at)
                for step, encoded, written_at in await cursor.fetchall()
            }

    async def claim(self, workflow: str, budget: timedelta, alive: timedelta) -> Pass | None:
        async with self.pool.connection() as connection, connection.cursor() as cursor:
            await cursor.execute(CLAIM, {"workflow": workflow, "budget": budget, "alive": alive})
            taken = await cursor.fetchone()
        if taken is None:
            return None
        return Pass(workflow=workflow, token=cast(int, taken[0]))

    async def extend(self, holder: Pass, budget: timedelta, alive: timedelta) -> bool:
        async with self.pool.connection() as connection, connection.cursor() as cursor:
            await cursor.execute(
                EXTEND,
                {"workflow": holder.workflow, "token": holder.token, "budget": budget, "alive": alive},
            )
            # No row touched means the `WHERE` refused the token, which is the only way
            # this misses: a `Pass` exists because a `claim` wrote the row it names.
            return cursor.rowcount == 1

    async def renew(self, holder: Pass, alive: timedelta) -> bool:
        async with self.pool.connection() as connection, connection.cursor() as cursor:
            await cursor.execute(RENEW, {"workflow": holder.workflow, "token": holder.token, "alive": alive})
            return cursor.rowcount == 1

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

        The claim row is *not* locked while the effect runs, and that is deliberate. The
        fence is read plainly before the effect, so a superseded pass performs nothing, and
        re-read under the row lock after it (`WROTE`), so a pass superseded meanwhile is
        refused and the effect rolls back with the transaction. Holding the lock across the
        effect instead would queue the worker's own renewal behind it, so a long effect
        would run with nothing renewing the delivery and be taken over on commit for having
        gone quiet; it would also make `claim` wait out the effect on a pinned connection
        rather than being told the workflow is held. What keeps another pass out while the
        effect runs is the claim's liveness, renewed by the worker's tick, and what keeps
        the effect from landing twice if that lapses is the step row itself: a second
        transaction's `WRITE` conflicts with the first's and rolls its effect back.

        The fence excludes every other *pass*, and one writer is left over: `supply` is
        ungated on purpose, so an approval can land under this key between the read and the
        write. That is what the retry below is for. The insert declines to overwrite, the
        transaction rolls back so the effect goes with it, and the value that did land is
        read and returned, which is what "the recorded value without re-running" means when
        the recording was somebody else's. Rare enough to pay a second transaction for, and
        it costs nothing on the path that wins.
        """
        try:
            async with self.pool.connection() as connection, connection.cursor() as cursor:
                await cursor.execute(FENCE, (holder.workflow,))
                fence = await cursor.fetchone()
                if fence is None or holder.token < fence[0]:
                    raise Fenced(f"{holder.workflow!r} moved on while this pass held it")
                await cursor.execute(ALREADY, (holder.workflow, key))
                recorded = await cursor.fetchone()
                if recorded is not None:
                    return self.codec.decode(cast(str, recorded[0]))
                encoded = self.codec.encode(await effect(cursor))
                await cursor.execute(WROTE, (holder.workflow, holder.token))
                if await cursor.fetchone() is None:
                    raise Fenced(f"{holder.workflow!r} moved on while this pass held it")
                await cursor.execute(WRITE, (holder.workflow, key, encoded))
                written = await cursor.fetchone()
                if written is None:
                    raise Supplied
                return self.codec.decode(cast(str, written[0]))
        except Supplied:
            pass
        async with self.pool.connection() as connection, connection.cursor() as cursor:
            await cursor.execute(ALREADY, (holder.workflow, key))
            landed = await cursor.fetchone()
        if landed is None:
            # The conflict was with the effect's *own* uncommitted insert, so the rollback
            # took that away too and there is nothing to read back. Said plainly here,
            # because the alternative is a `NoneType` error from a line that looks like
            # ordinary decoding, and because the cure is on the caller's side: the
            # application's tables are what an effect writes, and the step's own row is
            # the store's to write from what the effect returned.
            raise ValueError(
                f"the effect for {key!r} wrote that step's own checkpoint row, so its transaction "
                f"could not record the step and was rolled back; an effect writes the application's tables"
            )
        return self.codec.decode(cast(str, landed[0]))

    async def supply(self, workflow: str, key: str, value: object) -> object:
        async with self.pool.connection() as connection, connection.cursor() as cursor:
            await cursor.execute(SUPPLY, {"workflow": workflow, "step": key, "value": self.codec.encode(value)})
            return self.codec.decode(cast(tuple[str], await cursor.fetchone())[0])

    async def append(self, workflow: str, value: object) -> Entry:
        """File `value` in this workflow's inbox, under the next key the sequence hands out."""
        async with self.pool.connection() as connection, connection.cursor() as cursor:
            await cursor.execute(APPEND, {"workflow": workflow, "value": self.codec.encode(value)})
            key, encoded = cast(tuple[str, str], await cursor.fetchone())
            return Entry(key=key, value=self.codec.decode(encoded))

    async def discard(self, workflow: str) -> int:
        """
        Forget every record this workflow has, and raise its fence, in one transaction.

        One commit rather than two statements, because the two are only right together: a
        crash between them either leaves the records deleted with the fence unraised, so
        the pass that was mid-flight writes them back one at a time, or the reverse, which
        fences a live pass for a deletion that never happened.

        What is left behind is one claim row carrying a number. Nothing here sweeps it, in
        keeping with the rest of this store, where nothing expires and a control-plane
        sweep is the deployment's homework.
        """
        async with self.pool.connection() as connection, connection.cursor() as cursor:
            await cursor.execute(SUPERSEDE, (workflow,))
            await cursor.execute(DISCARD, (workflow,))
            return cursor.rowcount

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
# `AS MATERIALIZED` is what makes `LIMIT 1` mean one row, and it is load-bearing rather
# than a hint. A `LIMIT` bounds what a sub-select *returns*, not how many times the planner
# may evaluate it: written as a plain sub-select in the `FROM`, it can land on the inner
# side of a nested loop and be rescanned per outer row, and a rescanned `SKIP LOCKED` scan
# does not repeat itself, since it passes over the rows this same statement has already
# locked. Each rescan would then yield a *different* workflow, and the statement would
# lease several while `next_ready` reads one row and drops the rest, leaving the others
# invisible for a full lease with no delivery in anyone's hands.
#
# Which plan a version of Postgres picks for which statistics is not a thing this store
# should have an opinion about, and that is the argument for materializing rather than for
# trusting the shape: the CTE is evaluated once, before the update, so the count is a
# property of the statement instead of a property of the plan. It is also the idiom the
# queue-in-Postgres pattern is usually written with, for this reason.
#
# The new `visible_at` is returned because it *is* the receipt, which is the trick
# `RedisSetScheduler` documents at length: a workflow appears once, so a wakeup arriving
# mid-pass lands on top of the entry that pass is holding, and finishing has to be
# conditional on the value being unchanged or it throws the wakeup away.
#
#   returns  the workflow and its new visibility, or no row when nothing is visible yet
TAKE = """
WITH due AS MATERIALIZED (
    SELECT workflow FROM workflow_queue
    WHERE namespace = %(namespace)s AND visible_at <= now()
    ORDER BY visible_at
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
UPDATE workflow_queue AS entry
SET visible_at = now() + %(lease)s
FROM due
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

# Withdraw the workflow's right to run, whatever its row currently means. Unconditional
# where `FINISH` compares the receipt, which is the difference between finishing a pass
# (leave anything that asked for another) and cancelling the workflow (leave nothing).
CANCEL = "DELETE FROM workflow_queue WHERE namespace = %s AND workflow = %s"

# Suspend until a deadline, under the same comparison and for the same reason. A workflow
# holds one row here, so writing the deadline unconditionally would land on top of a
# `make_ready` that arrived while the pass was ending and push a confirmation out to a
# deadline that may be days away. The row anything else wrote carries a different
# `visible_at`, and this leaves it alone: the sooner wakeup is the one that was wanted,
# and the deadline is in the checkpoint, so the pass that runs writes it again.
SUSPEND = """
UPDATE workflow_queue SET visible_at = %(when)s
WHERE namespace = %(namespace)s AND workflow = %(workflow)s AND visible_at = %(receipt)s
"""

# Keep a delivery this worker's for another `within`, under the same comparison as
# `SUSPEND` and for the same reason: a `make_ready` that landed since would have written a
# different `visible_at`, and pushing the visibility out on top of it would bury a wakeup
# that has already arrived.
#
# It returns the new visibility because that *is* the new receipt, which is the price of
# the trick that makes this table a queue. A worker that went on holding the old one would
# find its own `FINISH` refused by the equality above and the workflow redelivered for
# nothing, so the rename is reported rather than left to be discovered.
#
#   returns  the new receipt, or no row when this delivery is no longer this worker's
RENEW_DELIVERY = """
UPDATE workflow_queue SET visible_at = now() + %(within)s
WHERE namespace = %(namespace)s AND workflow = %(workflow)s AND visible_at = %(receipt)s
RETURNING visible_at
"""


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

    async def wake_at(self, delivery: Delivery, when: datetime) -> None:
        """
        Suspend the workflow until `when`, unless something asked for a pass meanwhile.

        The receipt is the visibility this pass took, so anything that rescheduled the
        workflow since (a confirmation, another worker taking over an overrun) wrote a
        different one and this leaves it be. Which is the right answer rather than a
        concession: the deadline lives in the workflow's checkpoint, so the pass that runs
        sooner reaches the same `sleep` and writes it again.
        """
        async with self.pool.connection() as connection:
            await connection.execute(
                SUSPEND,
                {
                    "namespace": self.namespace,
                    "workflow": delivery.workflow,
                    "receipt": datetime.fromisoformat(delivery.receipt),
                    "when": when,
                },
            )

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

    async def extend(self, delivery: Delivery, within: timedelta) -> Delivery:
        """
        Push this delivery's visibility out, and say what it is called now.

        The new visibility is the new receipt, since this table's receipt *is* its
        visibility, so the caller is handed a delivery to use from here rather than left to
        work out that the one it holds has been renamed.

        No row means this delivery is no longer this worker's: taken over, cancelled, or
        rescheduled by a wakeup that arrived mid-pass, all of which wrote a `visible_at`
        that is not the one it took. The answer to every one of them is to hand back what
        came in and let whoever now owns the row have it, exactly as `wake_at` and `done`
        already do.
        """
        async with self.pool.connection() as connection, connection.cursor() as cursor:
            await cursor.execute(
                RENEW_DELIVERY,
                {
                    "namespace": self.namespace,
                    "workflow": delivery.workflow,
                    "receipt": datetime.fromisoformat(delivery.receipt),
                    "within": within,
                },
            )
            renewed = await cursor.fetchone()
        if renewed is None:
            return delivery
        return Delivery(workflow=delivery.workflow, receipt=cast(datetime, renewed[0]).isoformat())

    async def cancel(self, workflow: str) -> None:
        """
        Drop the workflow's row, whichever of the three things its `visible_at` means.

        One `DELETE` covers queued, sleeping, and out with a worker, because this table
        holds one row per workflow and the visibility is the only thing that differs
        between them. That is the same collapse that leaves `wake_due` and `reclaim` with
        nothing to do.

        The half of `cancel` a queue sweep cannot reach comes free with it: a pass still in
        flight answers with `wake_at`, which is an `UPDATE` conditional on the visibility
        still being the one it took, and a deleted row has none. So the deadline it was
        about to write updates nothing and a deleted workflow is not put back to sleep.
        """
        async with self.pool.connection() as connection:
            await connection.execute(CANCEL, (self.namespace, workflow))

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

    async def deliver(self, workflow: str, value: object) -> Entry:
        """Append the message and make the workflow ready, together or not at all."""
        codec = self.checkpointer.codec
        async with self.checkpointer.pool.connection() as connection, connection.cursor() as cursor:
            await cursor.execute(APPEND, {"workflow": workflow, "value": codec.encode(value)})
            key, encoded = cast(tuple[str, str], await cursor.fetchone())
            await cursor.execute(
                SCHEDULE,
                {"namespace": self.scheduler.namespace, "workflow": workflow, "visible_at": self.scheduler.now()},
            )
            return Entry(key=key, value=codec.decode(encoded))

    async def delete(self, workflow: str) -> int:
        """
        Cancel the workflow's wakeups and forget its records, together or not at all.

        Three statements in one commit, so the ordering `SplitDurable` has to reason about
        does not arise: there is no window in which the records are gone and a wakeup is
        not, and none in which the fence has been raised for a deletion that did not
        happen. Which is the same thing `arrive` gets from this store and for the same
        reason, one datastore.
        """
        async with self.checkpointer.pool.connection() as connection, connection.cursor() as cursor:
            await cursor.execute(CANCEL, (self.scheduler.namespace, workflow))
            await cursor.execute(SUPERSEDE, (workflow,))
            await cursor.execute(DISCARD, (workflow,))
            return cursor.rowcount
