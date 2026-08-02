# without-durability-postgres

[`without-durability`](https://pypi.org/project/without-durability/)'s two seams
over one Postgres: three tables, and no mechanism of its own.

```python
from psycopg_pool import AsyncConnectionPool
from without_durability_postgres import PostgresCheckpointer, PostgresDurable, PostgresScheduler, migrate

pool = AsyncConnectionPool(dsn, open=False)
await pool.open(wait=True)
await migrate(pool)
durable = PostgresDurable(PostgresCheckpointer(pool=pool), PostgresScheduler(pool=pool))
```

What is worth reading this package for is how little of it is mechanism. Every
write the Redis store needs a Lua script for is one statement here, or one
transaction, and neither is something this package supplies. Redis needs scripts
because it has no way to say "check this, then write that, and let nobody in
between"; SQL says it by default. The claim is an upsert whose `DO UPDATE` carries
a `WHERE` on the lease; the fenced record is one statement whose `FOR UPDATE` CTE
serializes it against a claim in flight; the queue takes with
`FOR UPDATE SKIP LOCKED`, so several workers polling one table fan out instead of
queueing on its head.

Three things that are live questions over Redis do not arise. A workflow id is a
query parameter rather than key structure, so it carries no contract at all.
Nothing expires, so the TTL that can lose a suspended workflow is gone, and the
fencing token can be an ordinary counter. And a default Postgres commits
synchronously, so `record` returning means what the durable runner assumes it
means.

`SqlEffect` is a callback handed a cursor inside the open transaction, so a step's
own business write and its checkpoint commit together: exactly-once for that step,
over the application's own tables. `PostgresDurable` extends that to the seam
above, committing a value's arrival and the workflow's wakeup at once, which is
what makes "you need no second system" a claim this can actually make.

It is also where "one datastore" stops meaning "Postgres". A transaction is local
on a single node; on a sharded deployment the checkpoint, claim, and queue tables
must be distributed by the workflow id and co-located, or that one commit becomes
a two-phase commit across nodes. `PostgresDurable` requires its two stores to
share a pool, checked at construction, which is the necessary half of that answer
rather than the sufficient one.

See the
[`without-durability-postgres` guide](https://without.help/without-durability-postgres/)
(with the [API reference](https://without.help/without-durability-postgres/reference/))
for the statements, the schema, and what a deployment still owes (a sweep, and a
real migration tool).
