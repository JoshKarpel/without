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

## Six notes on the shape that took

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
- **The store says what order they came in, for the same reason.** A workflow's
  records have two independent writers: the pass, through `record`, and anything
  outside it, through `supply`. Neither can order itself against the other. A counter
  either one keeps is read from a stale in-process snapshot or observed from the store
  and then raced, so the pass writes response N, the handler loads and takes N+1, and
  the pass's own next write is N+1 too. The store sees every write, so the store is
  the only thing that can say, and `load` returns its records in the order they were
  first recorded. First-writer-wins already decides what a key holds; this says the
  same writer decides where it sits, so a losing write moves neither.

  The order is the guarantee and the number behind it is _not_. `load` returns a
  `dict`, which preserves insertion order, so a caller reads the order by iterating
  and no store owes a sequence anyone outside it can see. That leaves the requirement
  invisible in the signature, which is the cost of keeping it out of the API: nothing
  but the cross-store conformance suite holds an implementation to it, and a
  third-party store can satisfy the type while ignoring the contract.

  What each store reaches for differs, and the differences are instructive. SQLite
  names the `rowid` it already assigns as an explicit `seq INTEGER PRIMARY KEY`,
  which is why its checkpoint table is the one table there that is not
  `WITHOUT ROWID`, and why it declares a column it could have left implicit: SQLite
  reserves the right to renumber the rowids of a table that has no explicit
  `INTEGER PRIMARY KEY` when the database is `VACUUM`ed. Postgres adds a `seq`
  column off a sequence, because a heap scan looks like insertion order right up
  until the no-op conflict update rewrites a tuple and moves it. Redis has the hardest job and the least
  obvious answer: it packs the position into the hash field in front of the encoded
  value, because a hash preserves insertion order only while it is listpack-encoded
  and stops once it converts to a hashtable. Keeping the order _in_ the field is what
  keeps it to one key, so there is no second structure that has to expire in step with
  the first.
- **The store names an inbox key, because only the store can.** `append` is `supply`
  under a key the store picks, and that is the whole difference between them. It owes
  three things a caller cannot arrange: two concurrent appends to one workflow get
  distinct keys and neither value is lost, the keys sort into append order _within_ that
  workflow, and the entry is an ordinary record that `load` returns in place.

  The keys need not be contiguous and need not order across workflows, and saying so is
  what makes the requirement implementable. A shared counter with gaps satisfies it and
  is far easier to make atomic than per-workflow numbering: Postgres takes `nextval` off
  a sequence, since it is the store with genuinely concurrent writers and a maximum read
  inside the insert is a race two callers can both win, with the loser's message
  vanishing into first-writer-wins and no error to show for it. SQLite reads the highest
  `seq` in one statement, which is atomic there because SQLite admits one writer at a
  time. Redis and the in-memory double take the count of the workflow's own records.

  What every one of them arrives at, by a different route, is that the key and the load
  position are _one_ number. Two counters would be two orders, and the second and third
  requirements above are a claim that those orders agree: under concurrent appends the
  keys would sort one way and `load` render the other, which is a store meeting each
  guarantee alone and neither together. Redis and the double get it for free, since the
  count they name a key from is already the position. Postgres has to arrange it, by
  drawing one `nextval` in a CTE and writing it as both the key and the row's `seq`,
  which is why that column takes a `DEFAULT` rather than being an identity: an identity
  is a number no statement may supply, and this one has to.

  Nothing is ever consumed, which is the load-bearing half. A destructive read would
  move a value out of the inbox and into the workflow's own records, leaving two copies
  to keep in step; append-only means the entry _is_ the record and a pass writes a
  reference to it. That reference replays correctly for exactly the reason the entry is
  safe to share: first-writer-wins, so the key still holds what it held. Forking is then
  free, since a consumer copying a prefix of `load` copies entries like anything else
  and needs no rule about the unread ones.

  What a destructive queue would buy is competing consumers, and that is already
  answered a layer down: `claim` guarantees one pass per workflow, so there is nobody to
  distribute the work between.
- **The store says when each record landed, for the third time and the same reason.**
  `history` returns what `load` returns, in the same order, each with the moment it was
  written. The clock is the _store's_, read at the winning write, which is the same clock
  every lease here is measured by and for the same argument: the writer is a different
  machine, so a moment stamped by whichever process happened to record it is only as good
  as the agreement between the two. First-writer-wins already decides what a key holds and
  where it sits; this says it decides when the key was written too, so a store stamping on
  every write would report a replayed step as having run at the moment of the replay.

  It is a second read rather than a richer `load` because the two have different readers.
  A pass calls `load` at its top and has no use for the times; what wants them is outside
  a pass, reading one workflow at a time, so the write path stamps every record and the
  read path splits.
- **Deleting a workflow raises its fence rather than removing it.** `discard` forgets every
  record and takes the fencing token _up_, keeping the claim row. That is the whole
  difference between a delete a caller could write for itself and one that is safe: a pass
  in flight holds a `Pass` and is about to write, and deleting the claim hands the next
  claim token 1 on the stores whose tokens are a counter, so the pass holding 7 outranks
  it and fills the deleted workflow back up one step at a time. Raising it instead refuses
  that pass at its next write and leaves the id claimable immediately, since what is kept
  is the ordering rather than the claim.

  The queue closes the same race from its own end, and it has to: a worker answers for its
  delivery _after_ the pass, so `Scheduler.cancel` sweeping the queue is undone a moment
  later by the deadline that pass chose. So `wake_at` MUST NOT reinstate a workflow whose
  delivery has been cancelled since it was taken, which the visibility-scored queues get
  for free (the receipt is a score, and a removed entry has none) and the Redis stream
  answers by asking whether its entry is still there.

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
trip passes in tests and fails in deployment. So its `Stored` carries the encoding, as a
hash field and a `TEXT` column do, and reading a checkpoint means `load`.

## Losing the workflow is not the workflow failing

`Fenced`, `Contended`, and `Suspended` descend from `BaseException` rather than
`Exception`, for the reason `asyncio.CancelledError` does. Each says something about
whether _this pass_ may continue, not about the work; an `except Exception` written to
handle a declined gateway must not absorb one.

`Suspended` is the one a driver never sees, because `resume` catches it and returns a
`Sleeping` or a `Blocked`. It still descends from `BaseException` for the half of its
life that matters: the part where it is travelling up through the workflow author's own
code, past whatever they wrapped their steps in.

`BaseException` is not enough on its own, though, which is worth stating because it looks
like it should be. It defeats `except Exception`; it does not defeat `asyncio.wait` or
`gather(return_exceptions=True)`, which hand exceptions back as values rather than raising
them, so a suspension can be captured without anyone writing an `except` at all. Each wait
therefore writes itself onto the `Run` before raising, and a body that returns having
reached one is refused (`Swallowed`) rather than reported as a finished workflow. That
also settles a second thing the type could not: `asyncio.gather` propagates only the first
exception, so a report built from what came out would name one key of a fan-out's several.

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

`Scheduler` needed the same move internally, and the way it was found is worth
recording, because the argument above predicted it. It used to state a cross-call
ordering rule in prose ("a `wake_at` survives a `done` for a delivery taken before it,
because the worker calls them in that order"), and a protocol that constrains the order
its own methods are called in is carrying coupling it isn't expressing. The coupling was
real: on a store that holds one entry per workflow, scheduling and acknowledging as two
calls is a read-modify-write over a value somebody else may have just written, so a
confirmation that landed while a pass was ending was overwritten by the deadline that
pass chose, and a workflow that should have run at once waited out a settlement window.
So `wake_at` takes the `Delivery` rather than a workflow id and answers for it too: one
call, no order to get right, and the receipt is what lets the store tell its own
delivery from a wakeup that arrived since.

One more tell that the boundary would be in the wrong place there. Three of its methods
are no-ops in every implementation but the Redis stream, which says the protocol is
shaped around one implementation's mechanism (stream, group, pending list, timer) rather
than around the question "when may this workflow run".

So the answer is not one big implementation, which would bundle a mechanism to
repair an interface and forfeit the split deployment. It is to **bundle the
interface and leave the mechanisms unbundled**. `Durable` owns the two stores and names the transitions
across them; `Checkpointer` and `Scheduler` are unchanged underneath and are what
implementations actually are:

```python
await payments.durable.arrive(workflow, "order", order.items)  # one call, no order to get right
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
