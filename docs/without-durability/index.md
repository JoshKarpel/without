# without-durability

Durable workflows built on `without-dag` and `without`, with a queue worker that
makes them a running service.

It exists to answer one question about the substrate: once workflow state is a
checkpoint any process can read, how much of a workflow engine is left? The
answer this arrives at is a dict lookup, a queue, and whatever atomicity the
store already has - a handful of small Lua scripts on Redis, ordinary
transactions on Postgres or SQLite. That is a claim about *mechanism*, and it
holds. It is not a claim that this is a substitute for Temporal, DBOS, or
LangGraph, and [Gaps](#gaps) is the specific list of why not.

Where that atomicity comes from is the interesting part, and it is the reason
[Where the guarantee lives](#where-the-guarantee-lives) is its own section. A
protocol of `load` and `record` is not enough to run a workflow safely, whatever
the store underneath it can do, because it has no way to say "only if nobody else
is running this" or "only if I am still the one who may write." That is the same
problem Temporal answers with a server and DBOS answers by requiring Postgres.

```text
seams.py     the three contracts: Checkpointer, Scheduler, Durable
graph.py     run_durably / run_saga over a without-dag CompiledGraph
stepwise.py  the same durability for an ordinary async function
worker.py    the queue worker and its timer
memory.py    both seams over dicts, so a test injects a store rather than a container
```

Stores live in their own packages, so this one depends on nothing but `without`
and `without-dag`:

- [`without-durability-redis`](../without-durability-redis/index.md), where each guarantee is a small Lua script
- [`without-durability-postgres`](../without-durability-postgres/index.md), where each is an ordinary transaction
- [`without-durability-sqlite`](../without-durability-sqlite/index.md), the same over one file, with no server and no driver

The worked example that drives all of them (an order-fulfilment graph, a payout
workflow, and an HTTP API in front of the worker) lives in the repository's
`integration` package rather than shipping here, because what a pass actually
*does* is the application's.

## Two mechanisms, one checkpoint

`Checkpointer` (in `seams.py`) is the only seam either mechanism talks through:
`claim` takes the right to run a pass, `load` returns what a workflow has
recorded, `record` adds to it under that claim, `supply` adds to it from outside
one, and `release` hands it back. A Redis hash or a Postgres table in production,
a plain dict in a test. `Scheduler` beside it holds the other half of a workflow's
state, its right to run, and `Durable` is the pair plus the moves that have to
cross both at once (see [One seam or two](#one-seam-or-two)).

`durable.core` is a graph. Fulfilling an order charges the card and reserves the
stock concurrently, ships, and renders a receipt, with every effect injected as a
`Services` callable and every node named in the source, so a result is stored
under a key that means the same thing after a crash. `run_durably` loads the
checkpoint, streams the graph, records each `(key, result)` before pulling the
next, and returns the output, so re-running a finished workflow performs no
effects. `run_saga` adds the compensating half: on failure it parses the
checkpoint into how far the run got (`Reached`) and drives a rollback graph under
its own key, itself checkpointed, so an interrupted rollback resumes instead of
refunding twice. Which step compensates which is domain knowledge written by
hand, as it is in every engine that formalizes sagas.

`durable.stepwise` is the same durability without the graph, and the two sit
together deliberately: one checkpoint, two ways to spend it. A workflow is an
ordinary async function whose effects are named (`await run.step("charged",
charge, as_text)`), resuming means calling it again, and each step it reaches
hands back what is already recorded instead of running. What it asks in return is the rule the
rest of this repo already keeps, since the code *between* the steps re-runs:
effects live in steps, the code around them is pure. Temporal and DBOS state that
same rule as workflow determinism. Nothing here enforces it (see
[Gaps](#gaps)).

Two things fall out that a fixed graph cannot express, both in `durable.payout`.
The fan-out is data-dependent at run time (one capture step per line item a
*step* returned, keyed by sku, so a crash resumes item by item), and a step that
cannot finish now raises `Suspended` instead of blocking. That last one is what
buys a settlement window waited out across crashes (`run.sleep` records the
*deadline*, so a crash on day two does not restart the clock) and a human
approval (`run.awaiting` suspends until another process writes one field into the
workflow's hash, which is a signal without a mailbox: the wait outlives the
process that was waiting). What the graph keeps in exchange is the eager check,
since it knows every key before it runs anything, and a structure you can
diagram.

## The service

`durable.api` and `durable.worker` are the piece that notices a suspension and
comes back. They are an API server and a queue worker, deployed separately,
sharing only one `Durable`. Neither mentions Redis or Postgres, which is what lets
the same two processes run over either.

The API never runs a workflow. Submitting an order and confirming a payout are the
same one-line move, `arrive`, because both are values the workflow is waiting on
and `Run.awaiting` cannot tell which is which. That is what stands in for a client
library talking to a workflow server, and it is why the API holds nothing and can
be restarted or scaled at will. The workflow
id is the request's `Idempotency-Key`, so a resubmitted order addresses the same
workflow rather than starting a second one, and `GET /orders/{id}` renders the
checkpoint as the progress view, since the durable state *is* the state. That
view answers questions about one workflow by id and nothing else; there is no
index to list or search across them.

The worker is a `Sink` over a `Stream` plus a timer, which is `without`'s own
vocabulary doing the work:

```text
deliveries ──▶ pool of N passes ──▶ Suspended with a due?  ──▶ wake_at (a clock)
      ▲                         │  Suspended without one? ──▶ nothing to do
      │                         └─▶ done (this wakeup is answered for)
reclaim one, else read one
timer ──▶ wake_due (one move, in the store)
```

The two arms of that branch are the two ways a workflow waits, and `Suspended`
says which: a deadline the workflow chose gets scheduled, a value the world owes
it does not, because the API's confirmation is what will queue it. Nothing polls
a workflow to ask whether it can proceed. `durable.scheduler` is those two
structures, a Redis stream of ready ids and a sorted set scored by deadline, plus
one small Lua script.

That script is `wake_due`, and it earns its place twice. Taking a workflow off
the sleepers and queueing it are durable only *together*: a timer that did them
as two calls would lose the workflow whenever it died in between, leaving it in
neither structure and asleep forever. Running the move in the server closes that,
and because the script is serialized against itself it also decides which of
several timers owns a wakeup, so every worker can run one and none needs to be
elected.

A *stream* rather than a list for the same reason. `BLPOP` hands an id over and
forgets it, so a worker that dies mid-pass takes the wakeup with it; `XREADGROUP`
moves the entry into that consumer's pending list, where it stays until
acknowledged and where `XAUTOCLAIM` can hand it to another worker. That is why a
delivery is a value with a receipt rather than a bare id, and why the
acknowledgement comes after the pass and its error handling, on every path the
process observed. Cancellation is the one path that skips the ack, deliberately:
a worker shutting down mid-pass has not finished, so leaving the delivery
outstanding is what lets someone else reclaim it.

Concurrency falls out of the same pull-driven shape. A worker runs up to `POOL`
passes at once through `without`'s `limit_concurrency`, which advances a lazy
source only when a slot frees, and every pull takes exactly one delivery: a
reclaimed one if some workflow was abandoned, otherwise a fresh read. So "pull
one at a time" and "run twenty at a time" are the same sentence, and a worker
holds precisely as many scheduler as it is working on. At capacity it simply stops
reading, and the work stays in the stream where another worker can take it, which
is backpressure without a mechanism for it.

Deploying the pair is two entrypoints over the same two stores:

```python
redis = Redis(host=..., decode_responses=True)
durable = SplitDurable(RedisCheckpointer(redis=redis), RedisStreamScheduler(redis=redis))

# the API process
async with serving(payments_app(Payments(durable=durable))):
    await asyncio.Event().wait()

# the worker process, however many of them
await work(durable)
```

Or over one Postgres, where both stores are tables in the same database and the
first three lines are the only difference:

```python
pool = AsyncConnectionPool(dsn, open=False)
await pool.open(wait=True)
await migrate(pool)  # three CREATE TABLE IF NOT EXISTS, under an advisory lock
durable = PostgresDurable(PostgresCheckpointer(pool=pool), PostgresScheduler(pool=pool))
```

## Where the guarantee lives

Temporal and DBOS sit at two ends of one axis, and the axis is *who enforces that
only one writer touches a workflow at a time*.

Temporal puts it in a server. A workflow execution belongs to a shard, a shard
has one owning host, and that ownership is what orders the writes to its history.
Portable persistence is the *consequence*, not the motive: because the server
supplies the ordering itself, the database underneath only has to do conditional
single-partition updates, which is why Cassandra qualifies. The server exists so
that the storage requirements can be weak.

DBOS is the inverse. There is no server, so the database has to supply the
semantics, and Postgres can. What that buys beyond exclusion is the thing no
amount of care in user code reproduces: a step's business write and its
checkpoint commit in one transaction, which makes that step exactly-once rather
than at-least-once.

This puts it in the seam, which is a third position rather than a midpoint on
that line: `Checkpointer` states the guarantees as requirements, and an
implementation says how many of them it can meet. Both implementations here meet
all of them, which is the point of having two. What it costs is that they are
still not *interchangeable*, and the bottom row is why.

| Capability | Redis | Postgres | What it buys |
|---|---|---|---|
| `record` a value durably | yes | yes | resumption at all |
| Record only if absent, returning the winner and which pass it was | `HSETNX` and an encoding comparison, in a script | `INSERT ... ON CONFLICT ... RETURNING`, comparing `jsonb` | two passes that both ran an effect agree on its result instead of diverging, and a graph run knows to stop |
| Exclusive pass with a fencing token | `HINCRBY` plus a lease, in a script | an upsert whose `DO UPDATE` carries a `WHERE` | one pass at a time, holding even when a process stalls past its lease |
| Step and checkpoint in one commit | a Lua script, for effects in *this* Redis | a transaction, for effects in *this* database | exactly-once for that step |

All four are implemented in both stores, and the fourth is worth stating
carefully because the obvious phrasing is wrong. It is not that Redis lacks what
Postgres has: a Lua script is an atomic commit over Redis data, so a step whose
effect *is* a Redis write records itself in the same script exactly as DBOS
records itself in the same transaction. The real constraint is that you can only
transact within a single datastore. Postgres wins this row only for effects that
live in that Postgres, and loses it for everything else in precisely the way
Redis does. What it wins in practice is that the effects usually *do* live there,
which is a fact about where applications keep their data and not about the
database.

`Run.transact` is where that lands. `step` performs an effect and then writes the
record, so a crash in between leaves the effect done and unrecorded and the next
pass repeats it. `transact` hands the store an effect it can perform itself, and
the store does the work and writes the record in one commit, so there is no
in-between for a crash to occupy:

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

That step is exactly-once on Redis. Run the workflow ten times and the ledger
moves once, without an idempotency key and without the effect being written to
tolerate repetition. The same step against `PostgresCheckpointer` is the same
sentence with the store's own language in it, and the effect is now an ordinary
application write rather than something staged into the checkpoint's datastore:

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
gateway, a carrier) cannot be in the commit, is not a transaction anyone can
offer, and belongs in `step` behind an idempotency key. On a Redis Cluster the
same constraint appears as a slot: an effect's keys must carry the workflow's own
`{id}` tag, because a script spanning two slots is a distributed transaction
wearing a local disguise. Postgres asks the same question once it is sharded
rather than being exempt from it (see
[One datastore is a question, not a product](#one-datastore-is-a-question-not-a-product)).

This is why `Checkpointer` is generic. `Checkpointer[Effect]` names the type of
thing *this* store can commit alongside a record, and there is no shared answer:
Redis takes a Lua script, Postgres takes an async callback handed a cursor inside
the open transaction, and an in-memory store takes a function over its own dict
(`tests/durable/doubles.py`). `Effect` defaults to `Never`, so a store with
nothing to offer here says so in its type and `transact` becomes uncallable
rather than absent, while code that never transacts keeps writing the bare
`Checkpointer` and still accepts every store.

So a family of stores is not one good implementation and one compromise. It is
the same offer made to two populations, each able to co-commit for the effects
that live where its checkpoint lives.

Three notes on the shape that took, since none of them is obvious:

- **A lease alone is not exclusion.** A process that stalls past its lease still
  believes it holds the workflow. Only the store knows better, so every write
  carries the token it was claimed with and the store refuses anything below the
  highest it has issued (`Fenced`). This is why `Pass` carries a number rather
  than a name, and why the number is minted by `HINCRBY` in the store rather than
  by the claimant.
- **Each script is a script because it is only correct as one step.** Checking
  whether a workflow is free and taking it; checking a token and applying the
  write it guards; testing whether a key is recorded and reading back the winner.
  Split any of them into two round trips and the gap is where the guarantee
  leaks. This is the same reasoning `wake_due` already carried.
- **Writes from outside a pass do not take the claim.** `supply` is what the
  `Checkpointer` half offers, and gating it on the claim would mean an approval
  failing because a worker happened to be mid-pass, for a value nothing is racing
  it to write. It keeps first-writer-wins, which is what makes a resubmitted order
  harmless.
- **The store says who won; the caller cannot work it out.** `record` returns a
  `Recorded`, which carries the stored value _and_ whether this pass is the one
  that put it there. Inferring the second from the first looks free and is wrong:
  a result crosses the codec both ways, so a pass that won outright can be handed
  back something unequal (a tuple returns as a list under `JsonCodec`), and
  `run_durably` reading that as a lost race would fail a run in which nothing
  raced. The store is the only party holding both encodings, so it answers.

### Every step names its parser, and the graph names none

A step hands back what the *store* holds, not the object its effect produced, so
`run.step("charged", charge)` returning the effect's own type was a lie the type
checker accepted. Not only after a crash: a step returning a tuple is handed a
list on the very pass that ran it. So `step`, `transact`, and `awaiting` take a
`parse: Callable[[object], T]`, and the return type is proven by a function that
ran rather than asserted by a `cast`.

The effect's own type is deliberately not tied to the parser's. What goes in and
what comes out are related by encode-then-decode, which is not the identity, so
one type for both would assert something false. `Run.sleep` is the proof rather
than the exception: it records an ISO string and reads back a `datetime`.

`run_durably` needs none of this, and the asymmetry is the point rather than an
inconsistency. It holds *both* values at the moment it records: what the node
returned, and what the store now has. So it verifies instead of parsing, and
refuses a node whose result does not survive its own store, naming the node, on
the pass that wrote it. That check matters more for a graph than a parser would,
because a graph feeds a node's result straight to its dependents: without it they
would see a `tuple` on the pass that computed it and a `list` on the pass that
restored it, with no crash needed for the two to disagree.

Verifying beats parsing whenever you still hold what you sent. `Run.awaiting` is
exactly the case that does not: it reads a value some *other* process wrote, so
there is nothing to compare against and only a parser can establish its shape.

### The codec is a seam too

What a step's result *becomes* in the store is a boundary decision, and boundary
decisions belong to the application: what a workflow's steps return, what an
operator needs to read out of the store, and what a service in another language
has to parse are questions this library cannot answer. So `CheckpointCodec` is a
protocol every store takes, defaulting to `JsonCodec` over the stdlib.

It is one object rather than a pair of functions because both requirements on it
are about the pair. `decode(encode(x))` MUST equal `x`, or a resumed pass sees
something the first pass did not, silently, one crash later. And `encode` MUST be
deterministic, because `record` decides who won a race by comparing encodings.

The in-memory doubles apply it too, which is the part that is easy to skip and is
exactly what makes a double lie. A dict can hold a value directly, so encoding
into it looks like ceremony, but then a step's result comes back by identity in
the suite and through a round trip in production, and every property that depends
on the round trip passes in tests and fails in deployment. `MemoryCheckpointer`
holds encoded values, so reading a checkpoint means `load`.

### Losing the workflow is not the workflow failing

`Fenced`, `Contended`, and `Suspended` descend from `BaseException` rather than
`Exception`, for the reason `asyncio.CancelledError` does. Each says something
about whether _this pass_ may continue, not about the work; an `except Exception`
written to handle a declined gateway must not absorb one.

The case that forced it is `run_saga`. It compensates on failure, and a `Fenced`
forward run is not a failure: it says another pass holds this workflow and is
advancing it, so a loser that compensated would refund a charge the winner is
still building on. Making the exception's own shape enforce that beats keeping a
list of types correct at every `except` site.

## One seam or two

A workflow's durable state is two things, what it has done and whether it may run
now, and they were two protocols on the grounds that they can be two stores. They
can: a Postgres checkpoint beside an SQS queue is an ordinary architecture. But
*can be unbundled* is not the same as *should be handed to the caller unbundled*,
and this design got that wrong once and then argued itself out of it.

The argument is already in `scheduler.py`, about `wake_due`:

> The naming matters as much as the atomicity: the protocol names the
> *transition*, so a caller cannot hold a claimed-but-unqueued id at all, which is
> the state that was lossy. Making it unrepresentable beats remembering to do both
> halves.

Taking a workflow off the sleepers and queueing it are durable only together, so
the protocol offers the move and not its halves. Recording the value a workflow is
waiting on and making the workflow runnable are *also* durable only together, and
that pair was two calls in the caller's hands, in an order it had to get right,
with a crash window nothing in the types mentioned. The inconsistency was internal
to this design rather than a matter of taste.

Two more tells that the boundary was in the wrong place. `Scheduler` had to state a
cross-call ordering rule in prose ("a `wake_at` survives a `done` for a delivery
taken before it, because the worker calls them in that order"), and a protocol
that constrains the order its own methods are called in is carrying coupling it
isn't expressing. And three of its seven methods are no-ops in two of the three
implementations, which says the protocol is shaped around one implementation's
mechanism (stream, group, pending list, timer) rather than around the question
"when may this workflow run".

So the fix is not one big interface, which would bundle a mechanism to repair a
contract and forfeit the split deployment. It is to **bundle the contract and
leave the mechanisms unbundled**. `Durable` owns the two stores and names the
transitions across them; `Checkpointer` and `Scheduler` are unchanged underneath and
are what implementations actually are:

```python
await payments.durable.arrive(workflow, "order", order.items)   # one call, no order to get right
```

What varies between implementations is not whether `arrive` exists but what it
guarantees, which is exactly how `Checkpointer` already treats `transact`.
`SplitDurable` composes any two stores and does two writes; `PostgresDurable`
requires that its two stores share one pool (checked at construction, not
documented) and does one commit.

The ordering inside `SplitDurable` is the whole of what it can offer, and it is
not arbitrary. It records first, so a crash leaves a workflow holding its value
and waiting for a wakeup, which anything asking again supplies. The reverse would
queue a pass that wakes, finds nothing recorded, and answers for the delivery,
which drops the value for good.

What this costs: a third named concept, and a `SplitDurable` whose guarantee is
deliberately weaker than the seam's strongest form. The second is the one to watch,
because a weaker guarantee behind an identical signature is how a system teaches
people to assume the stronger one.

### One datastore is a question, not a product

"Both things live in one datastore" is easy to read as "both things are in
Postgres", and that reading is wrong in a way that matters at exactly the scale
where you would care. The real question is whether the two writes land in one
*local* commit, and every store asks it, just at a different place and with a
different answer when you get it wrong.

**Redis Cluster refuses.** Keys declared to a script must hash to one slot, or the
server rejects the call before running anything (`CROSSSLOT Keys in request don't
hash to the same slot`). And the rule is about locality rather than declaration: a
script that reaches a key it never declared, owned by another node, dies partway
with `ERR Script attempted to access a non local key in a cluster node script`,
having written nothing. On a single node owning every slot the same script
succeeds, which is why a single-node test can't tell you this. There is no
escalation path: a cross-node atomic write is not expensive on Redis Cluster, it
is unavailable.

**Sharded Postgres escalates.** Vanilla single-node Postgres does not shard, so a
transaction is one WAL and one fsync and the local-commit claim is unconditional.
Under [Citus](https://www.citusdata.com/blog/2017/11/22/how-citus-executes-distributed-transactions/)
it is not: a transaction touching shards on more than one node becomes a real
distributed transaction, with the coordinator running `PREPARE TRANSACTION` and
then `COMMIT PREPARED`, a distributed deadlock detector, and
`max_prepared_transactions` to size on every worker. It still commits atomically,
which is more than Redis offers, but it is a different guarantee with different
failure modes and an operational tax, arriving silently.

The escape is the same shape on both sides, which is the point worth taking away.
Redis's hash tag has an exact analogue: distribute `workflow_checkpoint`,
`workflow_claim`, and `workflow_queue` by the workflow id and co-locate them, and
every transaction here stays on one node. `LuaEffect.keys` forces an author to
confront that question because a cluster will not let them avoid it;
`PostgresDurable` can only state it, and does, by requiring its two stores to share
a pool. Sharing a pool is the necessary half and not the sufficient one: on a
sharded deployment, co-location is the rest.

So the honest form of the rule is that Redis makes you answer the question at
development time and Postgres lets you answer it at scale, which is a real
convenience and a real trap.

## Gaps

These hold whatever store is underneath. Each store carries its own on top, and
those are on its page: [Redis](../without-durability-redis/index.md#gaps),
[Postgres](../without-durability-postgres/index.md#gaps),
[SQLite](../without-durability-sqlite/index.md#gaps). They are ordinary missing
features, expected in something this size:

- **An effect outside the store is still at-least-once.** The claim stops a
  second pass from starting, and the fence stops a superseded one from writing,
  but neither reaches the gap between an effect happening and its record landing.
  A pass that crashes in that window leaves the gateway charged and the
  checkpoint silent, and the next pass charges again. `transact` closes that, but
  only for effects the store can perform itself, which a payment gateway is not.
  For everything that crosses a boundary the answer is the ordinary one: make the
  effect idempotent, which is what the workflow id is for.
- **A split store's `arrive` still has a window.** `SplitDurable` records and then
  queues, so a process that dies between them leaves a workflow holding its value
  with nothing scheduled to run it. It is the recoverable half by design, and for
  the API an ordinary client retry under the same idempotency key supplies the
  missing wakeup, but nothing here retries on the caller's behalf and no sweep
  looks for workflows in that state. `PostgresDurable` does not have this gap.
- **The lease is a guess, not a bound.** `LEASE` has to exceed the longest a pass
  can honestly take. Set it too short and a slow pass is fenced mid-flight and
  has to be re-run; too long and a crashed worker's workflow waits that long.
  Nothing renews it while a pass runs, deliberately: the fence makes an overrun
  safe rather than corrupting, so renewal would buy throughput, not correctness.
- **No retries, backoff, or timeouts.** A step that raises is logged and
  acknowledged, and the workflow stops until something else wakes it. There is
  no dead-letter and no heartbeat.
- **No history.** The checkpoint is latest-state only: no timestamps, no attempt
  counts, and no record of a failure. From the store, a failed workflow and a
  suspended one are indistinguishable.
- **No enumeration, cancellation, or search.** Nothing can terminate or reset a
  workflow, and nothing exposes a list of them. How hard that would be to add
  differs in kind between the stores: Redis checkpoints are keyed with no index
  over them, so listing means `SCAN`, where a table answers "which workflows are
  suspended" with an ordinary query. That is the clearest thing a SQL store offers
  that this does not yet spend.
- **Determinism is documented, not enforced.** Nothing stops a workflow calling
  out to the network between two steps. `Run` does take its clock as an argument,
  which covers the most common case. The failure mode is milder than Temporal's,
  since keying by name means a workflow that takes a different branch on replay
  runs different steps and leaves the old records unread rather than erroring,
  but that also means it goes undetected.
- **The default codec restricts what a step can return,** and nothing bounds a
  payload's size. `JsonCodec` is the stdlib's `json`, so a step result must be
  JSON-*native* rather than merely JSON-serializable: a tuple encodes and comes
  back a list, which breaks the round trip `CheckpointCodec` requires. Swapping in
  a codec that knows the application's types is a constructor argument
  (`RedisCheckpointer(redis=..., codec=...)`) rather than a change to any store,
  which is what the seam buys; what it does not buy is a shipped alternative, and
  there is none here. `PostgresCheckpointer` also narrows the choice, since a
  `jsonb` column will only take JSON text.

## Against the alternatives

The interesting comparison is not the feature list, which is one-sided, but where
each system puts the same three concerns: what the durable state *is*, what
identifies a step across a replay, and who guarantees one writer.

| | Closest piece here | What it has that this does not |
|---|---|---|
| **Temporal** | `stepwise` | An event history replayed in order, a versioning API for changing in-flight code, retries and timeouts and heartbeats, visibility and search, a determinism sandbox |
| **DBOS** | `stepwise` over `PostgresCheckpointer`, almost exactly: a library plus a database, no server of its own | Recovery of pending workflows at startup, queues with concurrency and rate limits, workflow and step status tables, decorators that make all of it invisible, a real migration story |
| **LangGraph** | `run_durably` over `without-dag` | The same graph-plus-checkpointer shape, with per-superstep state snapshots, time travel, and a platform for scheduling |
| **Restate** | the `api`/`worker` pair | Also a single binary rather than a cluster, with exclusion structural in keyed virtual objects rather than leased |

Restate is the one that most tests the premise, because it accepts the same
starting position (a durable workflow should not need a cluster) and still
concludes it needs a log and a leader per key. What it gets for that is exclusion
that does not expire: a lease has to be guessed at, and a partition leader does
not.

### Credit, and how this sits next to DBOS

[DBOS Transact](https://github.com/dbos-inc/dbos-transact-py) is the direct
inspiration for the Postgres half, and two of its findings are load-bearing here.
The first is that a library plus a database is enough, so the exclusion a durable
workflow needs does not require a server to be built for it. The second is
sharper and is the whole reason `transact` exists: a step's own business write and
its checkpoint, committed together, is what moves that step from at-least-once to
exactly-once, and no amount of care in user code reproduces it. Both are theirs,
and this repo would not have looked for either. What is borrowed is those ideas
and nothing else: the three tables here were designed for this toy's own two
protocols and look nothing like theirs.

Where this differs is worth stating plainly, because it is a different position
rather than a smaller version of the same one:

- **The guarantee lives in the seam, not in the database.** DBOS requires
  Postgres, and that requirement is what lets it supply the semantics. Here
  `Checkpointer` states the requirements and Postgres is one implementation that
  meets them, alongside Redis, which meets them too. What DBOS gets for its choice
  is that it can assume the semantics everywhere; what this gets is that a
  deployment brings whatever it already runs.
- **Steps are named at the call site, not declared by a decorator.**
  `await run.step("charged", ..., as_text)` puts what is durable, under what key, and
  how it comes back, all in
  the line that does it. A decorator makes the ordinary case shorter and moves the
  key off the call. That is the usual locality trade, taken deliberately in the
  other direction, and DBOS's version is far nicer to use.
- **This is a validation artifact and that is not a modest disclaimer.** No
  recovery of pending workflows at startup, no status tables, no queues with
  concurrency or rate limits, no retries, no observability. The [Gaps](#gaps)
  above are the specific list. DBOS is a product built to run production
  workflows; this exists to find out what the substrate makes cheap.

Two choices here differ from all of them in a way worth naming, with what each
costs:

- **Steps are keyed by name, not by position.** Temporal replays a workflow
  against an ordered history, which is why changing workflow code with executions
  in flight needs an explicit versioning API. Keying by name makes inserting and
  reordering steps free, and makes nondeterminism degrade rather than raise. It
  costs the detection: nothing notices that a workflow's code changed, or that a
  recorded value's shape did. `Run.claim` catches two steps sharing a name within
  one pass, which is the sharpest version of the failure, but only within a pass.
- **A durable timer and an external signal are the same thing.**
  `Suspended(key, due=...)` and `Suspended(key)` differ only in whether anyone
  schedules the wakeup, and both are satisfied by an entry in the same mapping,
  which is also why starting a workflow and signalling one are the same two lines
  in `api.py`. Temporal and DBOS have separate machinery for each. What that
  buys is a smaller vocabulary; what it costs is that the store cannot tell an
  awaited value from a recorded result, so an approval written for a workflow
  that never asked for one simply sits there unread.

## Tests

Two layers, split along the line the packages are split along.

Each store's own package tests what that store *is*: its scripts or its
statements, its exclusion under real concurrency, its failure modes. Those drive
a real server (Redis, Postgres) or a real file (SQLite) rather than a fake.

The repository's `integration` package tests what a workflow gets, once, against
*every* store at the same time. One fixture builds a `Durable` per backend and one
suite runs the same saga, the same suspension, and the same API-plus-worker flow
across all of them, so "a workflow cannot tell which store it got" is a claim the
suite makes rather than one this page asserts.

Everything here runs against dicts by default, because `Checkpointer` and
`Scheduler` are injected and `memory.py` ships an implementation of both. That is
also why the worker can be driven without a server at all.
