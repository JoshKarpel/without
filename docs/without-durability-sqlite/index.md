# without-durability-sqlite

[`without-durability`](../without-durability/index.md)'s two interfaces over one SQLite
file. No server, and no third-party dependency: the driver is in the standard
library.

```python
from without_durability_sqlite import SqliteCheckpointer, SqliteDurable, SqliteScheduler, connect, migrate

database = connect("workflows.db")
await migrate(database)
durable = SqliteDurable(SqliteCheckpointer(database), SqliteScheduler(database))
...
await database.aclose()
```

It is the smallest thing that still meets every requirement the interface states,
which is the clearest way to say what the interface is for: a durable workflow does not need a
cluster, a database server, or a dependency.

Close it with `aclose` and never with `connection.close()`. `sqlite3.close()` frees
the connection under any thread still executing a statement on it, which segfaults the
process rather than raising, and a cancelled caller leaves exactly such a thread behind
(see [An effect is synchronous here](#an-effect-is-synchronous-here)). `aclose` waits
that out.

## What settles itself here

Two questions the other stores answer carefully do not arise.

**There is one writer at a time, by construction.** `BEGIN IMMEDIATE` takes the
write lock for the whole transaction, so the fence check and the write it guards
cannot be interleaved with anything. Postgres needs `FOR UPDATE` on the claim row to
get that, because there readers and writers run concurrently and a statement's
snapshot can be stale; Redis needs a Lua script. Here the transaction *is* the
exclusion, and `transact` is a plain sequence of statements inside one.

**There is nothing to co-locate.** The datastore is a file, so `transact` and
`arrive` reach every table an application keeps in it. On Redis that question is a
hash tag and on sharded Postgres it is a distribution column; here it has one answer
and it is yes. What that buys is the guarantee DBOS gets from Postgres (a step's own
business write committing with its checkpoint) for an application that never needed
Postgres.

The clock is a third. Redis and Postgres read their *server's* clock because the
claimant is a different machine, and a lease compared against the caller's own clock
is only as good as the agreement between the two. SQLite is the caller's machine, so
that argument does not apply. `unixepoch('now', 'subsec')` stays in the SQL anyway,
because it costs nothing and keeps the three stores reading the same.

## The statements

The same shapes as the Postgres store, minus what SQLite makes unnecessary:

| | Postgres | SQLite |
|---|---|---|
| Claim | upsert whose `DO UPDATE` carries a `WHERE` | the same |
| Record under the fence | a `FOR UPDATE` CTE feeding an upsert | the upsert alone; the statement is its own transaction and there is one writer |
| Take the next ready workflow | `FOR UPDATE SKIP LOCKED` | a plain `UPDATE ... RETURNING`; there is no concurrent writer to step over |
| Step and checkpoint together | `BEGIN` ... `COMMIT` | `BEGIN IMMEDIATE` ... `COMMIT` |

`IMMEDIATE` rather than the default deferred begin is the one detail worth pointing
at: a deferred transaction takes the write lock at its first *write*, so a fence read
before that would be unprotected and could be overtaken. Taking the lock up front is
what makes the read and the write it guards one step.

## An effect is synchronous here

`SqliteEffect` is `Callable[[sqlite3.Cursor], object]`, not a coroutine, and that is
the difference from `SqlEffect` rather than an oversight. The whole transaction runs
on one worker thread, so an effect is ordinary blocking code there and awaiting
inside it would be both impossible and pointless.

`sqlite3` is a blocking API, so every call hops to a thread via `asyncio.to_thread`,
and that hop is where the concurrency comes from. A single-threaded event loop does
_not_ serialize these calls, because the whole point of the hop is to get them off
that thread: twenty passes are twenty pool workers inside one connection at once. One
`asyncio.Lock` puts them back in a queue.

What the lock is for is narrower than "the connection is not thread-safe", and worth
being exact about, because SQLite's own answer sounds like it covers the case. The
library is normally built serialized (`sqlite3.threadsafety == 3`), so sharing a
connection across threads is already safe from corruption. What serialized mode
promises is that calls behave "as if they had all been made in the same order from a
single thread": linearization, not isolation. A transaction is connection state, so a
caller landing mid-`BEGIN IMMEDIATE` joins that transaction rather than waiting for
it, and its write succeeds, reads back, and then vanishes when the other caller rolls
back. That is what the lock removes, and no threading mode removes it.

The lock is also held until the worker *thread* finishes rather than until the calling
coroutine returns, since a thread cannot be cancelled: a cancelled caller that let go
of the connection would hand it to the next one mid-statement, which is the same
failure by another route.

## Durability is the point, so it is not tuned away

`connect` opens with `journal_mode=WAL` and `synchronous=FULL`. The usual advice
under WAL is `NORMAL`, and it trades away exactly the property this package exists
for: a commit can be lost on power loss or an OS crash. Everything `run_durably`
reasons about assumes the commit held, so this pays the fsync. `busy_timeout` is set
so a second process finding the write lock taken waits rather than failing, which is
the ordinary case when two processes share the file. The pragma carries whole
milliseconds, so `timeout` is a count of
[`Milliseconds`](../without/index.md#durations-that-cross-an-integer-boundary-withoutdurations) rather
than a duration that would be truncated on its way into the statement.

## Gaps

- **One machine.** Every process sharing this store shares a filesystem, so the
  exclusion holds across the processes on one box and not across a fleet. That is
  the deployment this is for rather than a defect: a CLI that resumes, a desktop
  app, an agent on a laptop, a single node that would rather not run Postgres to
  remember what it was doing. Reach for another store when a second machine appears.
- **No blocking read, and no way to add one.** `SqliteScheduler` polls, so the poll
  interval is a floor under how fast anything starts. Unlike the Postgres store there
  is not even a `LISTEN`/`NOTIFY` left on the table: within one process an
  `asyncio.Event` would do it, across processes on one machine it would take a
  filesystem watch, and neither is here.
- **One connection gives up the half of WAL worth having.** WAL exists so one writer
  runs alongside many readers, and funnelling everything through a single connection
  serializes reads too: `load` and `next_ready` queue behind whatever commit is in
  flight, `synchronous=FULL` fsync included. Buying that back means more connections
  (a reader pool, or one per thread, against the same file) rather than a cleverer
  lock, since the lock exists to keep transactions from interleaving on one
  connection and a second connection has no such problem. That is a larger store than
  this one, and the reason it has not been paid for is that a single node running one
  worker rarely notices.
- **Nothing sweeps.** Rows stay until something deletes them, so a long-running
  deployment needs a job that removes finished workflows. Nothing here is that job.
- **`migrate` is not a migration tool.** It runs the schema under SQLite's own
  exclusive transaction, which is enough to boot concurrently and nothing like enough
  to change the shape of these tables later. `user_version` is where SQLite keeps
  that, and a deployment that needs it should use it.
- **It needs SQLite 3.42 or newer, and nothing checks.** Every clock read is
  `unixepoch('now', 'subsec')`, and the `subsec` modifier arrived in 3.42
  (2023-05-16); without it those reads are whole seconds, so a lease and a
  visibility can round together and two workers polling within the same second can
  both find a row visible. `requires-python` cannot express this: Python bundles a
  recent SQLite on Windows and macOS, but on Linux `sqlite3` links whatever
  `libsqlite3` the distribution ships. Check `sqlite3.sqlite_version` on the
  machine that will run it.
