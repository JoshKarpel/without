# without-durability-sqlite

[`without-durability`](https://pypi.org/project/without-durability/)'s two
interfaces over one SQLite file. No server, and no third-party dependency: the driver is in
the standard library.

```python
from without_durability_sqlite import SqliteCheckpointer, SqliteDurable, SqliteScheduler, connect, migrate

database = connect("workflows.db")
await migrate(database)
durable = SqliteDurable(SqliteCheckpointer(database), SqliteScheduler(database))
...
await database.aclose()
```

It is the smallest thing that still meets every requirement the interface states,
which is the clearest way to say what the interface is for: a durable workflow does not need
a cluster, a database server, or a dependency.

Two questions the other stores have to answer carefully settle themselves here.
There is one writer at a time by construction, so `BEGIN IMMEDIATE` takes the
write lock for the whole transaction and the fence check and the write it guards
cannot be interleaved: Postgres needs `FOR UPDATE` on the claim row to get that
and Redis needs a Lua script, while here the transaction *is* the exclusion. And
there is nothing to co-locate, because the datastore is a file, so `transact` and
`arrive` reach every table an application keeps in it. That last one is the same
guarantee DBOS gets from Postgres, for an application that never needed Postgres.

What it costs is the shape of the whole thing: one machine. Every process sharing
this store shares a filesystem, so the exclusion holds across the processes on one
box and not across a fleet. That is the deployment this is for rather than a
defect to apologise for: a CLI that resumes, a desktop app, an agent on a laptop,
a single node that would rather not run Postgres to remember what it was doing.

See the
[`without-durability-sqlite` guide](https://without.help/without-durability-sqlite/)
(with the [API reference](https://without.help/without-durability-sqlite/reference/))
for the statements, why `connect` pays the fsync that the usual WAL advice trades
away, why an effect here is a synchronous callback where the Postgres store's is
`async`, how the blocking driver is kept off the event loop, and why closing goes
through `aclose` rather than the connection.
