# without-durability-postgres

[`without-durability`](../without-durability/index.md)'s two interfaces over one
Postgres: three tables, and no mechanism of its own.

```python
from psycopg_pool import AsyncConnectionPool
from without_durability_postgres import PostgresCheckpointer, PostgresDurable, PostgresScheduler, migrate

pool = AsyncConnectionPool(dsn, open=False)
await pool.open(wait=True)
await migrate(pool)
durable = PostgresDurable(PostgresCheckpointer(pool=pool), PostgresScheduler(pool=pool))
```

## The same two interfaces over one Postgres

This package is `Checkpointer` and `Scheduler` again, over three tables in one
database. It is the other half of the argument the Redis store makes: the interface
states the guarantees, a store says how it reaches them, and putting two real
stores side by side is what turns that from a design intention into something you
can read.

```text
workflow_checkpoint   one row per (workflow, step), the value as jsonb,
                      a `seq` column for the order they were recorded in
workflow_claim        one row per workflow: whose pass it is, and until when
workflow_queue        one row per (namespace, workflow), scored by when it is visible
workflow_seq          the sequence behind `seq`, which `append` also names its keys from
```

What is worth reading it for is how little of it is mechanism. Every write that
had to be a Lua script is one statement here, or one transaction, and neither is
a thing this app supplies. Redis needs scripts because it has no way to say
"check this, then write that, and let nobody in between"; SQL says it by default.
So the comparison to draw is not which store is better but *where the atomic unit
came from*, and here it came with the database:

| | Redis | Postgres |
|---|---|---|
| Claim the workflow | the `CLAIM` script: read the lease, compare, write, expire | one upsert whose `DO UPDATE` carries a `WHERE` |
| Record under the fence | the `RECORD` script: read the token, refuse or `HSETNX`, read back | one statement: a `FOR UPDATE` CTE feeding an upsert |
| Supply from outside a pass | the `SUPPLY` script: `HSETNX`, read back | the same upsert without the CTE |
| Append under a key the store names | the `APPEND` script: `HLEN`, then `HSET` under it | one insert naming its key from `nextval` |
| Step and checkpoint together | the `TRANSACT` script, a wrapper the effect is spliced into | `BEGIN` ... `COMMIT`, with the effect's own SQL in the middle |
| Take the next ready workflow | the `TAKE` script, serialized against itself | `FOR UPDATE SKIP LOCKED` |

The append is the one row where SQL is not simply the shorter answer. Redis and
SQLite can count what is already there, because a Lua script and a SQLite write
each run alone; Postgres admits concurrent writers, so a maximum read inside the
insert is a race two callers can both win, and the loser's message would vanish
into first-writer-wins with no error to show for it. Hence a sequence: `nextval`
is atomic and never repeats. It is shared across workflows and it skips numbers
on rollback, which is why the interface asks only that keys sort within one
workflow rather than that they be contiguous.

It is the *same* sequence that gives the row its `seq`, and that is the part
worth stating, because the obvious design has two. An inbox key drawn from one
counter and a load position drawn from another are two orders, and two callers
appending at once can take them the opposite way round: the keys sort one way and
`load` renders the other, which is exactly the pair of guarantees `append` owes
together. So the statement draws one number in a CTE and spends it twice, as the
key and as the position, and the two orders cannot disagree because there is only
one. That is also why `seq` carries a `DEFAULT` rather than being an identity
column: an identity is a number no statement may supply, and this one has to.

The claim is the one to look at, because every clause of the script it replaces
survives as a clause of the statement:

```sql
INSERT INTO workflow_claim AS held (workflow, token, held_until)
VALUES (%(workflow)s, 1, now() + %(lease)s)
ON CONFLICT (workflow) DO UPDATE
    SET token = held.token + 1, held_until = now() + %(lease)s
    WHERE held.held_until <= now()
RETURNING token
```

The `WHERE` on `DO UPDATE` is the "is it free" check: a conflicting row whose
lease has not elapsed fails it, the update does not happen, and `RETURNING` yields
no row, which is how a lost race is reported. The insert arm covers a workflow
nobody has ever claimed, and Postgres serializes two of those against each other
on the primary key, so the loser waits and then takes the `DO UPDATE` path rather
than both winning. The clock is the server's, for the reason the Lua script's is:
a lease compared against the claimant's own clock is only as good as the agreement
between the two, which is exactly what fails when a machine is unhealthy enough to
stall mid-pass.

`FOR UPDATE` in `record` is the one piece that is easy to leave out and wrong to.
Without it the fence is read from the statement's snapshot, so a claim committing
a microsecond after the statement began goes unseen and a superseded pass's write
lands. Taking the row lock makes the write queue behind any claim in flight and
then re-read the row it locked, so the token it compares against is the newest one.

### What the move takes away

Four things that were live questions on Redis do not arise here, and one of them
closes a load-bearing gap rather than a detail.

**A workflow id carries no constraints.** `RedisCheckpointer`
[asks an id](../without-durability-redis/index.md#what-redis-holds-and-for-how-long)
not to contain braces and to stay short, purely because it builds key names by
interpolating one. Here the id is a query parameter, so there is nothing to ask:
an id full of braces, quotes, and semicolons addresses exactly itself. That is
the tell the Redis store predicted, that the constraint was a property of building
keys by concatenation rather than of workflow ids.

**Nothing expires, so nothing is lost by expiring.** The Redis store re-arms a TTL
on every write, which sweeps finished workflows for free and costs the sharp edge
in the gaps below: a workflow suspended for longer than the TTL loses its
checkpoint while its wakeup survives, and resumes into an empty one. Rows do not
do that. The same change also lets the fencing token be an ordinary counter rather
than the hybrid logical clock the Redis store needs, because a token can only rewind if
a claim row disappears, and here that happens only if a sweep deletes it.

**Comparing two encodings is semantic.** `record` reports whether the value stored
is this pass's own, and on Redis and SQLite that comparison is between strings.
Here it is between `jsonb` values, which are normalized, so two documents that
differ only in key order or whitespace are equal. Nothing depends on the
difference, since `CheckpointCodec` requires a deterministic `encode` and one
store has one codec, but it is the stronger comparison of the two.

**A commit is a commit.** `RedisCheckpointer` has to hedge on whether `record`
returning means the value survived, because default persistence and asynchronous
replication do not give that. A default Postgres has `synchronous_commit` on, so
it does, which is why the test service is deliberately *not* tuned for speed: an
`fsync=off` in `compose.yaml` would make the suite quick by removing the one
property the store is claiming.

### One database, not two

The queue being a table is what makes "you need no second system" a claim this can
actually make, and `PostgresDurable.arrive` is where it stops being an
architecture diagram and becomes a guarantee. The Redis deployment is one server
holding two structures that are deliberately in different slots; this is one
database holding three tables, so recording an order and queueing the workflow is
two statements between one `BEGIN` and one `COMMIT`. `deliver` is the same move
for a message rather than a named value, and it matters more there: a lost `arrive`
wakeup is repaired by asking again under the same key, where asking again with a
message appends a second one. That is the strongest thing
here that Redis cannot do at all, and it is the same reason `transact` works:
both things live in one datastore.

What `SKIP LOCKED` buys is the other half. `RedisSetScheduler` gets exclusive takes by
running its scan inside a script that is serialized against every other script;
Postgres gets them by having the second poller step over the row the first is
updating rather than queue behind it. Same outcome, and the second one is a lock
manager doing its ordinary job rather than a global lock being borrowed.

## Gaps

Beyond [the ones every store carries](../without-durability/index.md#gaps):

- **The sweep is homework.** Redis's TTL is a control plane you get for free. Here
  a finished workflow's rows stay until something deletes them, and nothing here
  does, so a long-running deployment needs a job that deletes checkpoints and
  claims past some age. That job is also the only way to bring back the reused-id
  hazard the plain counter avoids, so it should delete a workflow's claim and its
  checkpoint together or neither.
- **There is still no blocking read.** `PostgresScheduler` polls, exactly as the
  sorted-set queue does, so the poll interval is a floor under how fast anything
  starts. Postgres can close this where a sorted set cannot (`LISTEN`/`NOTIFY` on a
  dedicated connection, woken by the writer or by a trigger), and this does not,
  which is the honest state of it rather than a claim that a table cannot wait.
- **The column narrows the codec.** Redis and SQLite hold text, so any
  `CheckpointCodec[str]` fits. A `jsonb` column will only take JSON, so a codec here
  MUST render JSON _text_, which rules out (say) a msgpack-in-base64 one. What that
  still leaves free is the library and the value mapping, which is where the
  interesting codecs differ anyway; the alternative was a `text` column, and giving
  up indexing and the `jsonb` operators to hold encodings nobody was going to use
  was the worse trade.
- **`migrate` is not a migration tool.** It runs three `CREATE TABLE IF NOT EXISTS`
  under an advisory lock, which is enough to boot a fleet of workers concurrently
  (concurrent `IF NOT EXISTS` is a duplicate-key error on the system catalog, not a
  no-op) and nothing like enough to change the shape of these tables later. That is
  Alembic's job, or sqitch's, or numbered SQL files'.
