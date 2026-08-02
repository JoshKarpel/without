# durable

A durable workflow mechanism built on `without-dag` and `without`, with an API
server and a queue worker that make it a running service.

It exists to answer one question about the substrate: once workflow state is a
checkpoint any process can read, how much of a workflow engine is left? The
answer this arrives at is a dict lookup, a queue, and whatever atomicity the
store already has - a handful of small Lua scripts on Redis, ordinary
transactions on Postgres. That is a claim about *mechanism*, and it holds. It is
not a claim that this is a substitute for Temporal, DBOS, or LangGraph, and
[Gaps](#gaps) is the specific list of why not.

Where that atomicity comes from is the interesting part, and it is the reason
[Where the guarantee lives](#where-the-guarantee-lives) is its own section. A
protocol of `load` and `record` is not enough to run a workflow safely, whatever
the store underneath it can do, because it has no way to say "only if nobody else
is running this" or "only if I am still the one who may write." That is the same
problem Temporal answers with a server and DBOS answers by requiring Postgres.

```text
core.py      a fulfilment graph and its compensations (pure)
shell.py     run_durably / run_saga over a Checkpoints seam
stepwise.py  the same durability for an ordinary async function
payout.py    a workflow the graph cannot express (pure)
store.py     Checkpoints as a Redis hash, JSON-encoded: the claim, and LuaEffect
wakeups.py   a Redis stream of ready ids and a deadline-scored sorted set
schedule.py  the same Wakeups as one sorted set, as a drop-in alternative
postgres.py  both seams over one Postgres: three tables, and SqlEffect
worker.py    the queue worker and its timer
api.py       three endpoints, none of which runs a workflow
```

## Two mechanisms, one checkpoint

`Checkpoints` (in `shell.py`) is the only seam either mechanism talks through:
`claim` takes the right to run a pass, `load` returns what a workflow has
recorded, `record` adds to it under that claim, `supply` adds to it from outside
one, and `release` hands it back. A Redis hash in production, a plain dict in a
test.

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
...)`), resuming means calling it again, and each step it reaches hands back what
is already recorded instead of running. What it asks in return is the rule the
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
sharing only the two stores. Neither mentions Redis or Postgres: both are written
against `Checkpoints` and `Wakeups`, which is what lets the same two processes run
over either.

The API never runs a workflow. Submitting an order and confirming a payout are
the same two-line move, `record` one value then `make_ready`, because both are
values the workflow is waiting on and `Run.awaiting` cannot tell which is which.
That is what stands in for a client library talking to a workflow server, and it
is why the API holds nothing and can be restarted or scaled at will. The workflow
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
a workflow to ask whether it can proceed. `durable.wakeups` is those two
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
holds precisely as many wakeups as it is working on. At capacity it simply stops
reading, and the work stays in the stream where another worker can take it, which
is backpressure without a mechanism for it.

Deploying the pair is two entrypoints over the same two stores:

```python
redis = Redis(host=..., decode_responses=True)
checkpoints, wakeups = RedisCheckpoints(redis=redis), RedisWakeups(redis=redis)

# the API process
async with serving(payments_app(Payments(checkpoints=checkpoints, wakeups=wakeups))):
    await asyncio.Event().wait()

# the worker process, however many of them
await work(checkpoints, wakeups)
```

Or over one Postgres, where both stores are tables in the same database and the
two lines above it are the only difference:

```python
pool = AsyncConnectionPool(dsn, open=False)
await pool.open(wait=True)
await migrate(pool)  # three CREATE TABLE IF NOT EXISTS, under an advisory lock
checkpoints, wakeups = PostgresCheckpoints(pool=pool), PostgresSchedule(pool=pool)
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
that line: `Checkpoints` states the guarantees as requirements, and an
implementation says how many of them it can meet. Both implementations here meet
all of them, which is the point of having two. What it costs is that they are
still not *interchangeable*, and the bottom row is why.

| Capability | Redis | Postgres | What it buys |
|---|---|---|---|
| `record` a value durably | yes | yes | resumption at all |
| Record only if absent, returning the winner | `HSETNX` in a script | `INSERT ... ON CONFLICT ... RETURNING` | two passes that both ran an effect agree on its result instead of diverging |
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
        keys=(f"{checkpoints.hash_key(workflow)}:ledger",),
        args=("piano", 1),
    ),
)
```

That step is exactly-once on Redis. Run the workflow ten times and the ledger
moves once, without an idempotency key and without the effect being written to
tolerate repetition. The same step against `PostgresCheckpoints` is the same
sentence with the store's own language in it, and the effect is now an ordinary
application write rather than something staged into the checkpoint's datastore:

```python
async def reserve(cursor: AsyncCursor[TupleRow]) -> object:
    await cursor.execute(
        "UPDATE stock SET reserved = reserved + 1 WHERE sku = %s RETURNING reserved",
        ("piano",),
    )
    return (await cursor.fetchone())[0]

await run.transact("reserved", reserve)
```

What it costs is that the effect has to be something the store can perform, which
means it has to live in the store. An effect that leaves the datastore (a payment
gateway, a carrier) cannot be in the commit, is not a transaction anyone can
offer, and belongs in `step` behind an idempotency key. On a Redis Cluster the
same constraint appears as a slot: an effect's keys must carry the workflow's own
`{id}` tag, because a script spanning two slots is a distributed transaction
wearing a local disguise.

This is why `Checkpoints` is generic. `Checkpoints[Effect]` names the type of
thing *this* store can commit alongside a record, and there is no shared answer:
Redis takes a Lua script, Postgres takes an async callback handed a cursor inside
the open transaction, and an in-memory store takes a function over its own dict
(`tests/durable/doubles.py`). `Effect` defaults to `Never`, so a store with
nothing to offer here says so in its type and `transact` becomes uncallable
rather than absent, while code that never transacts keeps writing the bare
`Checkpoints` and still accepts every store.

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
- **Writes from outside a pass do not take the claim.** `supply` is what the API
  calls, and gating it on the claim would mean an approval failing because a
  worker happened to be mid-pass, for a value nothing is racing it to write. It
  keeps first-writer-wins, which is what makes a resubmitted order harmless.

## What Redis holds, and for how long

Four keys, and they have four genuinely different lifetimes. This matters more
than it looks: three of the gaps below are lifetime mismatches between them
rather than anything wrong with a single structure.

| Key | Type | Written by | Grows | Ends |
|---|---|---|---|---|
| `workflow:{id}` | HASH | `supply` from the API, `record` from a pass | one field per completed step | TTL, re-armed on every write |
| `workflow:{id}:pass` | HASH, 2 fields | `claim` and `release` | never (fixed shape) | TTL, re-armed with the checkpoint |
| `{ns}:ready` | STREAM + group | `make_ready`, and the timer | one entry per wakeup, forever | nothing trims it |
| `{ns}:sleeping` | ZSET | `wake_at`, drained by `wake_due` | one member per sleeping workflow | removed when it comes due |

The braces are Redis Cluster's hash tag, so the first two land on one slot and a
script may touch both. The last two are shared by every workflow, which is why
they carry the queue's namespace rather than a workflow's id.

That split decides where a workflow id is *structure* and where it is merely
data. In the two per-workflow keys it is interpolated into the key name, so it
carries a contract: no braces (they would delimit the hash tag instead of it),
bounded length, and not ending in `:unwind`, which is how `run_saga` names a
rollback. In the queue it is a stream field or a sorted-set member, so none of
that applies. `RedisCheckpoints` documents the contract and does not enforce it,
on the grounds that a UUID satisfies it without trying and validating every call
to catch someone who went out of their way is the wrong trade. A store that bound
the id as a query parameter rather than concatenating it would have no contract
at all, which is the tell that this is a property of building keys by
interpolation rather than of workflow ids.

### One order, end to end

Following a payout that captures two items, sleeps out a settlement window, and
waits for a human. Each column is one key; `▪` is a value arriving.

```text
                       workflow:{id}      workflow:{id}:pass   {ns}:ready      {ns}:sleeping
                       HASH: checkpoint   HASH: claim + fence  STREAM: wakeups ZSET: deadlines
POST /orders
  supply("order")   ─▶ ▪ order
  make_ready        ─────────────────────────────────────────▶ ▪ 1-0
worker pulls
  XREADGROUP        ───────────────────────────────────────── ▫ 1-0 pending
  claim             ────────────────────▶ ▪ token=1 until=T₁
  step "items"      ─▶ ▪ items
  step "captured:*" ─▶ ▪ captured:piano
                    ─▶ ▪ captured:stool
  sleep "settling"  ─▶ ▪ settling=D            (the deadline, not the duration)
  Suspended(due=D)  ──────────────────────────────────────────────────────────▶ ▪ score=D
  release           ────────────────────▶ ▪ token=1 until=0
  XACK              ───────────────────────────────────────── ▫ 1-0 acked, still in the stream
timer, once D passes
  wake_due (Lua)    ─────────────────────────────────────────▶ ▪ 2-0        ◀── ▪ removed
worker pulls again
  claim             ────────────────────▶ ▪ token=2 until=T₂  (the fence advances)
  awaiting          ─  suspends: "approved-by" is not there
  Suspended(due=None) ── nothing scheduled: only the world can answer this
  release, XACK     ────────────────────▶ until=0             ▫ 2-0 acked
POST /confirmation
  supply("approved-by") ▪ approved-by
  make_ready        ─────────────────────────────────────────▶ ▪ 3-0
worker pulls again
  claim             ────────────────────▶ ▪ token=3 until=T₃
  step "paid"       ─▶ ▪ paid
  release, XACK     ────────────────────▶ until=0             ▫ 3-0 acked
                       ▲                  ▲                    ▲               ▲
                    5 fields, TTL       reset to token=0     3 entries,      empty
                    re-armed each write   only when the       none removed,   again
                                          workflow expires    ever
```

Two things the diagram is meant to make obvious. The checkpoint only ever grows,
because `HSETNX` never replaces a field, so what an operator sees with
`redis-cli` is the complete history of what happened even though it is not an
event log. And the stream only ever grows too, but for a worse reason: `XACK`
moves an entry out of the pending list and leaves it in the stream.

### Each structure's life

**The checkpoint** is born on the first write, which is usually the API's
`supply("order")` rather than anything the workflow did. It gains one field per
completed step and loses none. Its TTL is re-armed by every write, so a workflow
making progress keeps itself alive and one that stops does not. This is the key
whose contents are entirely the user's vocabulary: no engine field lives in it,
which is what lets `GET /orders/{id}` be `HGETALL` with no filtering.

**The claim** is born on the first `claim` and has a fixed two-field shape.
`token` only rises, `until` moves forward on a claim and to zero on a release. It
shares the checkpoint's TTL and is re-armed alongside it.

Sharing that lifetime is what makes the token's arithmetic worth a second look.
Both keys expire together, so a workflow quiet for longer than the TTL is
forgotten entirely, fence included. Were the token a plain counter, a reused id
would restart it at 1 while a pass stalled since before the expiry still held
token 3, and the corpse would outrank the living. So the token is
`max(now_ms, previous + 1)`: a hybrid logical clock rather than a counter, seeded
from the server's wall clock so that any later claim is stamped with a later
millisecond, and falling back to `previous + 1` to stay strictly monotonic within
one incarnation even if the clock steps backwards. That closes the hazard without
having to couple the two keys' lifetimes at all.

**The queue** is the one with no lifetime at all. Every `make_ready` and every
`wake_due` appends, and nothing removes. Trimming has to be by `MINID` behind
what every consumer group has acknowledged, since trimming by `MAXLEN` would drop
the oldest entries, which are the ones nobody has run yet.

**The sleepers** is an index rather than a record: the deadline itself lives in
the checkpoint, put there by `run.sleep`. Losing this set leaves workflows asleep
forever rather than corrupt, and rebuilding it is a scan over checkpoints. It
carries no TTL of its own, which is the other half of the sharp edge above: a
workflow can be woken by a deadline that outlived the checkpoint the deadline was
recorded in.

### The same thing as one sorted set

`schedule.py` is an alternative `Wakeups` that replaces the stream *and* the
sleeping set with a single ZSET scored by when each workflow becomes visible.
Queued now is a score in the past, sleeping is a score in the future, and being
worked on is a score one lease ahead. It is a drop-in: the end-to-end test runs
the same API and worker over both.

```text
                       {ns}:schedule (ZSET, score = visible at)
  make_ready        ─▶ ▪ score = now
  next_ready        ─▶ ▪ score = now + lease      ◀── the score is also the receipt
  wake_at(D)        ─▶ ▪ score = D
  done(receipt)     ─▶ remove, but only if the score is still the receipt
```

What that buys: no timer (being due and being ready are the same score), no
consumer group, no pending list, no `XAUTOCLAIM`, no acknowledgement that can be
lost, and no unbounded growth, because a ZSET holds each workflow once however
many wakeups arrive. `wake_due`, `reclaim`, and `prepare` all become no-ops,
which is the finding rather than an omission.

What it costs is one subtlety and one real regression. The subtlety is that
because a workflow appears once, a wakeup landing mid-pass has nowhere to go
except on top of the entry that pass is holding, so removing the entry afterwards
would throw the wakeup away. That is the classic lost wakeup, and the fix is that
the score a pass took *is* its receipt: anything that wants another pass writes a
different score, so finishing is conditional on the score being unchanged. A
compare-and-set where the version number is a value the design already had to
store.

The regression is latency. `XREADGROUP BLOCK` parks a worker inside Redis, so an
idle one costs nothing and a submitted order is picked up the instant it lands. A
sorted set has no blocking read, so `RedisSchedule` polls, and the poll interval
is a floor under how fast anything starts. In one line: a stream is cheaper to
wait on, a sorted set is cheaper to reason about.

## The same two seams over one Postgres

`postgres.py` is `Checkpoints` and `Wakeups` again, over three tables in one
database. It is the other half of the argument the Redis store makes: the seam
states the guarantees, a store says how it reaches them, and putting two real
stores side by side is what turns that from a design intention into something you
can read.

```text
workflow_checkpoint   one row per (workflow, step), the value as jsonb
workflow_claim        one row per workflow: whose pass it is, and until when
workflow_queue        one row per (namespace, workflow), scored by when it is visible
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
| Step and checkpoint together | the `TRANSACT` script, a wrapper the effect is spliced into | `BEGIN` ... `COMMIT`, with the effect's own SQL in the middle |
| Take the next ready workflow | the `TAKE` script, serialized against itself | `FOR UPDATE SKIP LOCKED` |

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

Three things that were live questions on Redis do not arise here, and one of them
is a load-bearing gap rather than a detail.

**A workflow id carries no contract.** `RedisCheckpoints` asks an id not to
contain braces and to stay short, purely because it builds key names by
interpolating one. Here the id is a query parameter, so there is nothing to ask:
an id full of braces, quotes, and semicolons addresses exactly itself. That is
the tell the Redis store's own docstring predicted, that the contract was a
property of building keys by concatenation rather than of workflow ids.

**Nothing expires, so nothing is lost by expiring.** The Redis store re-arms a TTL
on every write, which sweeps finished workflows for free and costs the sharp edge
in the gaps below: a workflow suspended for longer than the TTL loses its
checkpoint while its wakeup survives, and resumes into an empty one. Rows do not
do that. The same change also lets the fencing token be an ordinary counter rather
than the hybrid logical clock `store.py` needs, because a token can only rewind if
a claim row disappears, and here that happens only if a sweep deletes it.

**A commit is a commit.** `RedisCheckpoints` has to hedge on whether `record`
returning means the value survived, because default persistence and asynchronous
replication do not give that. A default Postgres has `synchronous_commit` on, so
it does, which is why the test service is deliberately *not* tuned for speed: an
`fsync=off` in `compose.yaml` would make the suite quick by removing the one
property the store is claiming.

### What it costs

**The sweep is now homework.** Redis's TTL is a control plane you get for free.
Here a finished workflow's rows stay until something deletes them, and nothing
here does, so a long-running deployment needs a job that deletes checkpoints and
claims past some age. That job is also the only way to bring back the reused-id
hazard the counter avoids, so it should delete a workflow's claim and its
checkpoint together or neither.

**There is still no blocking read.** `PostgresSchedule` polls, exactly as
`RedisSchedule` does, so the poll interval is a floor under how fast anything
starts. Postgres can close this where a sorted set cannot (`LISTEN`/`NOTIFY` on a
dedicated connection, woken by the writer or by a trigger), and this does not,
which is the honest state of it rather than a claim that a table cannot wait.

**`migrate` is not a migration tool.** It runs three `CREATE TABLE IF NOT EXISTS`
under an advisory lock, which is enough to boot a fleet of workers concurrently
(concurrent `IF NOT EXISTS` is a duplicate-key error on the system catalog, not a
no-op) and nothing like enough to change the shape of these tables later. That is
Alembic's job, or sqitch's, or numbered SQL files'.

### One database, not two

The queue being a table is what makes "you need no second system" a claim this can
actually make. The Redis deployment is one server holding two unrelated
structures; this is one database holding three tables, which also means a queue
write and a checkpoint write *could* be one commit. Nothing here does that,
because the worker and the API are written against the two protocols and neither
knows they share a datastore. It is worth naming anyway, because it is the shape
of the next thing: making a workflow ready in the transaction that records the
step which decided to is a guarantee neither seam can currently express.

What `SKIP LOCKED` buys is the other half. `RedisSchedule` gets exclusive takes by
running its scan inside a script that is serialized against every other script;
Postgres gets them by having the second poller step over the row the first is
updating rather than queue behind it. Same outcome, and the second one is a lock
manager doing its ordinary job rather than a global lock being borrowed.

## Gaps

Two of these are load-bearing, in the sense that a deployment would be wrong
rather than merely limited. Both are about the Redis store specifically, and the
Postgres one is the answer to each, which is most of why it exists. They are
listed first.

**Redis is not obviously durable enough for the claim `run_durably` makes.** That
docstring reasons carefully about the crash window between an effect and its
record, which only matters if `record` returning means the value survived. Redis
with default persistence and asynchronous replication does not give that: a
failover can roll back acknowledged writes, and nothing here uses `WAIT`. The
test `compose.yaml` disables persistence outright, which is right for tests and
means the suite never exercises the question. Temporal on Cassandra or Postgres
and DBOS on Postgres commit for real, and so does `PostgresCheckpoints`.

**The checkpoint TTL can lose a workflow that is behaving correctly.**
`RedisCheckpoints.ttl` defaults to one day and is re-armed only on `record`. A
workflow suspended for longer than that writes nothing while it waits, so its
hash expires, while its entry in the sleeping sorted set (which has no TTL)
survives. The timer then fires, the worker resumes, `load` returns `{}`, and the
workflow suspends on its first `awaiting` forever, with whatever effects it
already performed left standing. The `ttl` knob is the right idea and its default
is shorter than the sleep semantics this same package advertises. Rows do not
expire, so `PostgresCheckpoints` trades this failure for a sweep somebody has to
write.

The rest are ordinary missing features, expected in something this size, and hold
for either store unless they name one:

- **An effect outside the store is still at-least-once.** The claim stops a
  second pass from starting, and the fence stops a superseded one from writing,
  but neither reaches the gap between an effect happening and its record landing.
  A pass that crashes in that window leaves the gateway charged and the
  checkpoint silent, and the next pass charges again. `transact` closes that, but
  only for effects the store can perform itself, which a payment gateway is not.
  For everything that crosses a boundary the answer is the ordinary one: make the
  effect idempotent, which is what the workflow id is for.
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
  suspended" with an ordinary query. That is the clearest thing Postgres offers
  that this does not yet spend.
- **Determinism is documented, not enforced.** Nothing stops a workflow calling
  out to the network between two steps. `Run` does take its clock as an argument,
  which covers the most common case. The failure mode is milder than Temporal's,
  since keying by name means a workflow that takes a different branch on replay
  runs different steps and leaves the old records unread rather than erroring,
  but that also means it goes undetected.
- **The stream is never trimmed.** `XACK` clears the pending list but leaves the
  entry, so `RedisWakeups` needs a job trimming by `MINID` behind what every
  group has acknowledged. Capping the stream's *length* would be the wrong bound,
  since that drops the oldest entries, which are the ones nobody has run yet.
  `RedisSchedule` does not have this gap at all, which is the strongest single
  argument for it.
- **JSON restricts what a step can return,** and nothing bounds a payload's size.
  The codec is the app's boundary decision, so `store.py` and `postgres.py` are
  the only files a richer one changes: one spends a `json.dumps`, the other
  declares a `jsonb` column and lets the driver do it.

## Against the alternatives

The interesting comparison is not the feature list, which is one-sided, but where
each system puts the same three concerns: what the durable state *is*, what
identifies a step across a replay, and who guarantees one writer.

| | Closest piece here | What it has that this does not |
|---|---|---|
| **Temporal** | `stepwise` | An event history replayed in order, a versioning API for changing in-flight code, retries and timeouts and heartbeats, visibility and search, a determinism sandbox |
| **DBOS** | `stepwise` over `PostgresCheckpoints`, almost exactly: a library plus a database, no server of its own | Recovery of pending workflows at startup, queues with concurrency and rate limits, workflow and step status tables, decorators that make all of it invisible, a real migration story |
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
  `Checkpoints` states the requirements and Postgres is one implementation that
  meets them, alongside Redis, which meets them too. What DBOS gets for its choice
  is that it can assume the semantics everywhere; what this gets is that a
  deployment brings whatever it already runs.
- **Steps are named at the call site, not declared by a decorator.**
  `await run.step("charged", ...)` puts what is durable, and under what key, in
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

The Redis and Postgres tests drive real servers rather than fakes, under the
`compose` mark that the [integration README](../../../README.md) describes. Each
store gets the same end-to-end test, submitting over HTTP, waiting out a real
one-second window, confirming, and reading the payout back, which is what keeps
"the same API and worker over either store" a claim rather than an aspiration.
They share their gateway doubles (`tests/durable/gateways.py`) for the same
reason: where the two suites differ should be the store and nothing else.

Everything else runs against dicts, because `Checkpoints` and `Wakeups` are
injected: `tests/durable/doubles.py` is the in-memory pair, and it is what lets
the worker be driven without a server at all.
