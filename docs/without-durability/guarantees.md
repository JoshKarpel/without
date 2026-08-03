# Where the guarantee lives

A protocol of `load` and `record` is not enough to run a workflow safely, whatever
the store underneath it can do, because it has no way to say "only if nobody else is
running this" or "only if I am still the one who may write". That is the same
problem Temporal answers with a server and DBOS answers by requiring Postgres. This
page is where the answer here is written down: what `Checkpointer` demands of a
store, what `Durable` demands of a pair of them, and what the two questions cost.

## Temporal, DBOS, and a third position

Temporal and DBOS sit at two ends of one axis, and the axis is *who enforces that
only one writer touches a workflow at a time*.

Temporal puts it in a server. A workflow execution belongs to a shard, a shard has
one owning host, and that ownership is what orders the writes to its history.
Portable persistence is the *consequence*, not the motive: because the server
supplies the ordering itself, the database underneath only has to do conditional
single-partition updates, which is why Cassandra qualifies. The server exists so
that the storage requirements can be weak.

DBOS is the inverse. There is no server, so the database has to supply the
semantics, and Postgres can. What that buys beyond exclusion is the thing no amount
of care in user code reproduces: a step's business write and its checkpoint commit
in one transaction, which makes that step exactly-once rather than at-least-once.

This puts it in the interface, which is a third position rather than a midpoint on that
line: `Checkpointer` states the guarantees as requirements, and an implementation
says how many of them it can meet. All three stores here meet all of them, which is
the point of having three. What it costs is that they are still not
*interchangeable*, and the bottom row is why.

| Capability | Redis | SQL (Postgres, SQLite) | What it buys |
|---|---|---|---|
| `record` a value durably | yes | yes | resumption at all |
| Record only if absent, returning the winner and which pass it was | `HSETNX` and an encoding comparison, in a script | an upsert whose `DO UPDATE` returns the row already there | two passes that both ran an effect agree on its result instead of diverging, and a graph run knows to stop |
| Exclusive pass with a fencing token | `HINCRBY` plus a lease, in a script | an upsert whose `DO UPDATE` carries a `WHERE` | one pass at a time, holding even when a process stalls past its lease |
| Step and checkpoint in one commit | a Lua script, for effects in *this* Redis | a transaction, for effects in *this* database | exactly-once for that step |

All four are implemented in every store, and the fourth is worth stating carefully
because the obvious phrasing is wrong. It is not that Redis lacks what Postgres has:
a Lua script is an atomic commit over Redis data, so a step whose effect *is* a Redis
write records itself in the same script exactly as DBOS records itself in the same
transaction. The real constraint is that you can only transact within a single
datastore. Postgres wins this row only for effects that live in that Postgres, and
loses it for everything else in precisely the way Redis does. What it wins in
practice is that the effects usually *do* live there, which is a fact about where
applications keep their data and not about the database.

## `transact`, and what an effect has to be

`Run.transact` is where that lands. `step` performs an effect and then writes the
record, so a crash in between leaves the effect done and unrecorded and the next pass
repeats it. `transact` hands the store an effect it can perform itself, and the store
does the work and writes the record in one commit, so there is no in-between for a
crash to occupy:

```python
await run.transact(
    "reserved",
    LuaEffect(
        source="return cjson.encode(redis.call('HINCRBY', KEYS[1], ARGV[1], tonumber(ARGV[2])))",
        keys=(f"{checkpointer.hash_key(workflow)}:ledger",),
        args=("piano", 1),
    ),
    as_count,
)
```

That step is exactly-once on Redis. Run the workflow ten times and the ledger moves
once, without an idempotency key and without the effect being written to tolerate
repetition. The same step against `PostgresCheckpointer` is the same sentence with
the store's own language in it, and the effect is now an ordinary application write
rather than something staged into the checkpoint's datastore:

```python
async def reserve(cursor: AsyncCursor[TupleRow]) -> object:
    await cursor.execute(
        "UPDATE stock SET reserved = reserved + 1 WHERE sku = %s RETURNING reserved",
        ("piano",),
    )
    return (await cursor.fetchone())[0]

await run.transact("reserved", reserve, as_count)
```

What it costs is that the effect has to be something the store can perform, which
means it has to live in the store. An effect that leaves the datastore (a payment
gateway, a carrier) cannot be in the commit, is not a transaction anyone can offer,
and belongs in `step` behind an idempotency key. On a Redis Cluster the same
constraint appears as a slot: an effect's keys must carry the workflow's own `{id}`
tag, because a script spanning two slots is a distributed transaction wearing a local
disguise. Postgres asks the same question once it is sharded rather than being exempt
from it (see [One datastore is a question](#one-datastore-is-a-question-not-a-product)).

This is why `Checkpointer` is generic. `Checkpointer[Effect]` names the type of thing
*this* store can commit alongside a record, and there is no shared answer: Redis takes
a Lua script, Postgres an async callback handed a cursor inside the open transaction,
SQLite the same callback without the `async`, and `MemoryCheckpointer` a function over
its own dict. `Effect` defaults to `Never`, so a store with nothing to offer here says
so in its type and `transact` becomes uncallable rather than absent, while code that
never transacts keeps writing the bare `Checkpointer` and still accepts every store.

So a family of stores is not one good implementation and one compromise. It is the
same offer made to several populations, each able to co-commit for the effects that
live where its checkpoint lives.

## Four notes on the shape that took

None of these is obvious from the protocol alone.

- **A lease alone is not exclusion.** A process that stalls past its lease still
  believes it holds the workflow. Only the store knows better, so every write carries
  the token it was claimed with and the store refuses anything below the highest it
  has issued (`Fenced`). This is why `Pass` carries a number rather than a name, and
  why the number is minted by the store rather than by the claimant.
- **Each script is a script because it is only correct as one step.** Checking
  whether a workflow is free and taking it; checking a token and applying the write it
  guards; testing whether a key is recorded and reading back the winner. Split any of
  them into two round trips and the gap is where the guarantee leaks. SQL says the
  same thing with a statement or a transaction, which is the whole difference between
  the stores.
- **Writes from outside a pass do not take the claim.** `supply` is what the
  `Checkpointer` half offers, and gating it on the claim would mean an approval
  failing because a worker happened to be mid-pass, for a value nothing is racing it
  to write. It keeps first-writer-wins, which is what makes a resubmitted order
  harmless.
- **The store says who won; the caller cannot work it out.** `record` returns a
  `Recorded`, which carries the stored value _and_ whether this pass is the one that
  put it there. Inferring the second from the first looks free and is wrong: a result
  crosses the codec both ways, so a pass that won outright can be handed back
  something unequal (a tuple returns as a list under `JsonCodec`), and `run_durably`
  reading that as a lost race would fail a run in which nothing raced. The store is
  the only party holding both encodings, so it answers.

## Every step names its parser, and the graph names none

A step hands back what the *store* holds, not the object its effect produced, so
`run.step("charged", charge)` returning the effect's own type was a lie the type
checker accepted. Not only after a crash: a step returning a tuple is handed a list on
the very pass that ran it. So `step`, `transact`, and `awaiting` take a
`parse: Callable[[object], T]`, and the return type is proven by a function that ran
rather than asserted by a `cast`.

The effect's own type is deliberately not tied to the parser's. What goes in and what
comes out are related by encode-then-decode, which is not the identity, so one type
for both would assert something false. `Run.sleep` is the proof rather than the
exception: it records an ISO string and reads back a `datetime`.

`run_durably` needs none of this, and the asymmetry is the point rather than an
inconsistency. It holds *both* values at the moment it records: what the node
returned, and what the store now has. So it verifies instead of parsing, and refuses a
node whose result does not survive its own store, naming the node, on the pass that
wrote it. That check matters more for a graph than a parser would, because a graph
feeds a node's result straight to its dependents: without it they would see a `tuple`
on the pass that computed it and a `list` on the pass that restored it, with no crash
needed for the two to disagree.

Verifying beats parsing whenever you still hold what you sent. `Run.awaiting` is
exactly the case that does not: it reads a value some *other* process wrote, so there
is nothing to compare against and only a parser can establish its shape.

## The codec is an interface too

What a step's result *becomes* in the store is a boundary decision, and boundary
decisions belong to the application: what a workflow's steps return, what an operator
needs to read out of the store, and what a service in another language has to parse
are questions this library cannot answer. So `CheckpointCodec` is a protocol every
store takes, defaulting to `JsonCodec` over the stdlib.

It is one object rather than a pair of functions because both requirements on it are
about the pair. `decode(encode(x))` MUST equal `x`, or a resumed pass sees something
the first pass did not, silently, one crash later. And `encode` MUST be deterministic,
because `record` decides who won a race by comparing encodings.

Only the encoded side is a type parameter. `Encoded` genuinely varies (every store
here holds text, and one holding bytes would say so), while the decoded side cannot:
a checkpoint is heterogeneous by construction, since one codec carries a workflow's
string, its mapping, and its deadline alike. Precision belongs inside a codec instead,
where a pydantic `TypeAdapter` can be as exact as it likes while still presenting
`object` at the interface, which is the move `without_dag.Node` already makes.

`MemoryCheckpointer` applies the codec too, which is the part that is easy to skip and
is exactly what makes a double lie. A dict can hold a value directly, so encoding into
it looks like ceremony, but then a step's result comes back by identity in the suite
and through a round trip in production, and every property that depends on the round
trip passes in tests and fails in deployment. So it holds encoded values, and reading
a checkpoint means `load`.

## Losing the workflow is not the workflow failing

`Fenced`, `Contended`, and `Suspended` descend from `BaseException` rather than
`Exception`, for the reason `asyncio.CancelledError` does. Each says something about
whether _this pass_ may continue, not about the work; an `except Exception` written to
handle a declined gateway must not absorb one.

`Suspended` is the one a driver never sees, because `resume` catches it and returns a
`Sleeping` or a `Waiting`. It still descends from `BaseException` for the half of its
life that matters: the part where it is travelling up through the workflow author's own
code, past whatever they wrapped their steps in.

The case that forced it is a saga, which is an `except Exception` around a forward run
that drives a rollback. A `Fenced` forward run is not a failure: it says another pass
holds this workflow and is advancing it, so a loser that compensated would refund a
charge the winner is still building on. Making the exception's own shape enforce that
beats keeping a list of types correct at every `except` site, which matters more here
than it would inside a library, since the `except` in question is one an application
wrote (see [Sagas are not a feature here](index.md#sagas-are-not-a-feature-here)).

## One interface or two

A workflow's durable state is two things, what it has done and whether it may run
now, and they are two protocols on the grounds that they can be two stores. They can:
a Postgres checkpoint beside an SQS queue is an ordinary architecture. But *can be
unbundled* is not the same as *should be handed to the caller unbundled*.

The argument for that is already in the Redis stream scheduler, about `wake_due`:
the protocol names the *transition*, so a caller cannot hold a claimed-but-unqueued id
at all, which is the state that was lossy. Making it unrepresentable beats remembering
to do both halves. Recording the value a workflow is waiting on and making the workflow
runnable are *also* durable only together, so they get the same treatment.

Two more tells that the boundary would be in the wrong place at `Scheduler`. It would
have to state a cross-call ordering rule in prose ("a `wake_at` survives a `done` for a
delivery taken before it, because the worker calls them in that order"), and a protocol
that constrains the order its own methods are called in is carrying coupling it isn't
expressing. And three of its seven methods are no-ops in every implementation but the
Redis stream, which says the protocol is shaped around one implementation's mechanism
(stream, group, pending list, timer) rather than around the question "when may this
workflow run".

So the answer is not one big implementation, which would bundle a mechanism to
repair an interface and forfeit the split deployment. It is to **bundle the
interface and leave the mechanisms unbundled**. `Durable` owns the two stores and names the transitions
across them; `Checkpointer` and `Scheduler` are unchanged underneath and are what
implementations actually are:

```python
await payments.durable.arrive(workflow, "order", order.items)   # one call, no order to get right
```

What varies between implementations is not whether `arrive` exists but what it
guarantees, which is exactly how `Checkpointer` already treats `transact`.
`SplitDurable` composes any two stores and does two writes; `PostgresDurable` and
`SqliteDurable` require that their two stores share one pool or one connection
(checked at construction, not documented) and do one commit.

The ordering inside `SplitDurable` is the whole of what it can offer, and it is not
arbitrary. It records first, so a crash leaves a workflow holding its value and waiting
for a wakeup, which anything asking again supplies. The reverse would queue a pass that
wakes, finds nothing recorded, and answers for the delivery, which drops the value for
good.

What this costs: a third named concept, and a `SplitDurable` whose guarantee is
deliberately weaker than the interface's strongest form. The second is the one to watch,
because a weaker guarantee behind an identical signature is how a system teaches people
to assume the stronger one.

## One datastore is a question, not a product

"Both things live in one datastore" is easy to read as "both things are in Postgres",
and that reading is wrong in a way that matters at exactly the scale where you would
care. The real question is whether the two writes land in one *local* commit, and every
store asks it, just at a different place and with a different answer when you get it
wrong.

**Redis Cluster refuses.** Keys declared to a script must hash to one slot, or the
server rejects the call before running anything (`CROSSSLOT Keys in request don't hash
to the same slot`). And the rule is about locality rather than declaration: a script
that reaches a key it never declared, owned by another node, dies partway with
`ERR Script attempted to access a non local key in a cluster node script`, having
written nothing. On a single node owning every slot the same script succeeds, which is
why a single-node test can't tell you this. There is no escalation path: a cross-node
atomic write is not expensive on Redis Cluster, it is unavailable.

**Sharded Postgres escalates.** Vanilla single-node Postgres does not shard, so a
transaction is one WAL and one fsync and the local-commit claim is unconditional. Under
[Citus](https://www.citusdata.com/blog/2017/11/22/how-citus-executes-distributed-transactions/)
it is not: a transaction touching shards on more than one node becomes a real
distributed transaction, with the coordinator running `PREPARE TRANSACTION` and then
`COMMIT PREPARED`, a distributed deadlock detector, and `max_prepared_transactions` to
size on every worker. It still commits atomically, which is more than Redis offers, but
it is a different guarantee with different failure modes and an operational tax,
arriving silently.

**SQLite has one answer and it is yes.** The datastore is a file, so there is nothing
to co-locate and no sharding to grow into, which is the whole of what buys the smallest
store the strongest form of the guarantee.

The escape is the same shape on both of the first two sides, which is the point worth
taking away. Redis's hash tag has an exact analogue: distribute `workflow_checkpoint`,
`workflow_claim`, and `workflow_queue` by the workflow id and co-locate them, and every
transaction here stays on one node. `LuaEffect.keys` forces an author to confront that
question because a cluster will not let them avoid it; `PostgresDurable` can only state
it, and does, by requiring its two stores to share a pool. Sharing a pool is the
necessary half and not the sufficient one: on a sharded deployment, co-location is the
rest.

So the honest form of the rule is that Redis makes you answer the question at
development time and Postgres lets you answer it at scale, which is a real convenience
and a real trap.
