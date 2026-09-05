# without-durability

Durable workflows built on `without-dag` and `without`, with a queue worker that
makes them a running service.

It exists to answer one question about the substrate: once workflow state is a
checkpoint any process can read, how much of a workflow engine is left? The answer
this arrives at is a dict lookup, a queue, and whatever atomicity the store already
has - a handful of small Lua scripts on Redis, ordinary transactions on Postgres or
SQLite. That is a claim about *mechanism*, and it holds. It is not a claim that this
is a substitute for Temporal, DBOS, or LangGraph, and [Gaps](#gaps) is the specific
list of why not.

Where that atomicity comes from is the interesting part, and it has its own page:
[Where the guarantee lives](guarantees.md).

```text
interfaces.py  the three interfaces: Checkpointer, Scheduler, Durable
graph.py     run_durably over a without-dag CompiledGraph
stepwise.py  the same durability for an ordinary async function
codec.py     what a step's result becomes in a store, and how it comes back
worker.py    the queue worker and its timer
memory.py      both interfaces over dicts, so a test injects a store rather than a container
```

Stores live in their own packages, so this one depends on nothing but `without` and
`without-dag`:

- [`without-durability-redis`](../without-durability-redis/index.md), where each guarantee is a small Lua script
- [`without-durability-postgres`](../without-durability-postgres/index.md), where each is an ordinary transaction
- [`without-durability-sqlite`](../without-durability-sqlite/index.md), the same over one file, with no server and no driver

The worked example that drives all of them (an order-fulfilment graph, a payout
workflow, and an HTTP API in front of the worker) lives in the repository's
`integration` package rather than shipping here, because what a pass actually *does*
is the application's.

## Two mechanisms, one checkpoint

`Checkpointer` (in `interfaces.py`) is the only interface either mechanism talks through:
`claim` takes the right to run a pass, `load` returns what a workflow has recorded in
the order it was first recorded, `record` adds to it under that claim, `supply` adds to
it from outside one, and `release` hands it back. A Redis hash or a Postgres table in production, a plain dict
in a test. `Scheduler` beside it holds the other half of a workflow's state, its right
to run, and `Durable` is the pair plus the moves that have to cross both at once.

`run_durably` is the graph mechanism, and the whole of it. It loads the checkpoint,
streams a `CompiledGraph`, records each `(key, result)` before pulling the next, and
returns the output, so re-running a finished workflow performs no effects.
`integration.durable.core` is the worked example: fulfilling an order charges the card
and reserves the stock concurrently, ships, and renders a receipt, with every effect
injected and every node named in the source, so a result is stored under a key that
means the same thing after a crash.

### Sagas are not a feature here

A compensating transaction needs no mechanism, because a rollback is another workflow:
a second graph, run through `run_durably` under a second id. That makes it checkpointed
on exactly the same terms as the forward run, so a crash partway through a rollback
resumes it rather than refunding twice. Written out, it is this:

```python
try:
    return await run_durably(forward, checkpointer, holder, order)
except Exception:
    undoing = await claimed(checkpointer, f"{holder.workflow}:rollback")
    try:
        reached = Reached.of(await checkpointer.load(holder.workflow))
        await run_durably(unwind, checkpointer, undoing, reached)
    finally:
        await checkpointer.release(undoing)
    raise
```

Two of its properties fall out of shapes that are already there rather than being
arranged. `except Exception` is the right net without a list of types to keep correct,
because cancellation and every `Interruption` descend from `BaseException`, and each of
those stops for a reason that makes unwinding actively wrong: a `Fenced` loser that
compensated would refund a charge the winner is still building on. And deciding what to
give back is a pure function of a value, since the checkpoint _is_ a mapping of results,
so `Reached.of` parses it into the question the rollback asks rather than interrogating
a replay log or a running engine.

What is left is which step compensates which, which is domain knowledge written by hand,
as it is in every engine that formalizes sagas. So there is nothing for a library
function to hold except the rollback's id, and that is a name in the _application's_
namespace: reserving a suffix for it would oblige every place that mints an id to know
the reservation and refuse it, on pain of one workflow addressing another's
compensation.

So the suffix above is `:rollback` for no reason at all, and `integration`'s suite
(which runs this same shape against every store) uses `:unwind` for no reason either.
Neither is a convention, and nothing below this line knows which one you picked.

`stepwise` is the same durability without the graph, and the two sit together
deliberately: one checkpoint, two ways to spend it. A workflow is an ordinary async
function whose effects are named (`await run.step("charged", charge, as_text)`),
resuming means calling it again, and each step it reaches hands back what is already
recorded instead of running. What it asks in return is the rule the rest of this repo
already keeps, since the code *between* the steps re-runs: effects live in steps, the
code around them is pure. Temporal and DBOS state that same rule as workflow
determinism. Nothing here enforces it (see [Gaps](#gaps)).

Two things fall out that a fixed graph cannot express, both in
`integration.durable.payout`. The fan-out is data-dependent at run time (one capture
step per line item a *step* returned, keyed by sku, so a crash resumes item by item),
and a step that cannot finish now stops the pass instead of blocking. That last one is
what buys a settlement window waited out across crashes (`run.sleep` comes back as a
`Sleeping` carrying the *deadline* it recorded, so a crash on day two does not restart
the clock) and a human approval (`run.awaiting` comes back as a `Waiting` until
another process writes one field into the workflow's checkpoint, which is a signal
without a mailbox: the wait outlives the process that was waiting). What the graph
keeps in exchange is the eager check, since it knows every key before it runs
anything, and a structure you can diagram.

## The service

`integration.durable.api` and `without_durability.worker` are the piece that notices a
suspension and comes back. They are an API server and a queue worker, deployed
separately, sharing only one `Durable`. Neither mentions Redis or Postgres, which is
what lets the same two processes run over either.

The API never runs a workflow. Submitting an order and confirming a payout are the
same one-line move, `arrive`, because both are values the workflow is waiting on and
`Run.awaiting` cannot tell which is which. That is what stands in for a client library
talking to a workflow server, and it is why the API holds nothing and can be restarted
or scaled at will. The workflow id is the request's `Idempotency-Key`, so a
resubmitted order addresses the same workflow rather than starting a second one, and
`GET /orders/{id}` renders the checkpoint as the progress view, since the durable
state *is* the state. That view answers questions about one workflow by id and nothing
else; there is no index to list or search across them.

The worker is a `Sink` over a `Stream` plus a timer, which is `without`'s own
vocabulary doing the work:

```text
deliveries ──▶ pool of N passes ──▶ Sleeping  ──▶ wake_at (a clock)
      ▲                         │  Waiting   ──▶ nothing to do
      │                         │  Completed ──▶ nothing to do
      │                         └─▶ done (this wakeup is answered for)
reclaim one, else read one
timer ──▶ wake_due (one move, in the store)
```

`resume` returns what the pass came to rather than raising two of the three, so the
worker is a `match` over a sealed union closed with `assert_never`: a fourth outcome
would be a type error there rather than a workflow that quietly stops being woken. A
`Sleeping` carries the deadline the workflow chose, so the worker schedules it; a
`Waiting` carries nothing to schedule, because only the API's confirmation can queue
it. Nothing polls a workflow to ask whether it can proceed.

The two waits are separate types rather than one with a nullable `due` for the ordinary
reason: a driver has to branch on it either way, and two shapes that differ in what they
carry should not be one shape that sometimes carries it. Inside a pass a suspension is
still an exception (`ScheduledWakeup`, `InputNeeded`), because unwinding straight-line
code needs one; `resume` is the boundary where it becomes a value.

Concurrency falls out of the same pull-driven shape. A worker runs up to `POOL` passes
at once through `without`'s `limit_concurrency`, which advances a lazy source only when
a slot frees, and every pull takes exactly one delivery: a reclaimed one if some
workflow was abandoned, otherwise a fresh read. So "pull one at a time" and "run twenty
at a time" are the same sentence, and a worker holds precisely as many deliveries as it
is working on. At capacity it simply stops reading, and the work stays in the queue
where another worker can take it, which is backpressure without a mechanism for it.

Deploying the pair is two entrypoints over the same two stores:

```python
redis = Redis(host=..., decode_responses=True)
durable = SplitDurable(RedisCheckpointer(redis=redis), RedisStreamScheduler(redis=redis))

# the API process
async with serving(payments_app(Payments(durable=durable))):
    await asyncio.Event().wait()

# the worker process, however many of them
await work(durable, submitted)
```

Or over one Postgres, where both stores are tables in the same database and the first
three lines are the only difference:

```python
pool = AsyncConnectionPool(dsn, open=False)
await pool.open(wait=True)
await migrate(pool)  # three CREATE TABLE IF NOT EXISTS, under an advisory lock
durable = PostgresDurable(PostgresCheckpointer(pool=pool), PostgresScheduler(pool=pool))
```

### How the timings relate

How long a pass may honestly take is the one timing a deployment usually has to
change, and it is a single number on the scheduler:
`PostgresScheduler(pool=pool, lease=timedelta(minutes=5))` keeps a taken workflow
invisible for five minutes, and `work` reads that back and claims the workflow for five
minutes to match. It sits on the store rather than being an argument to `work` because
the queue is where half of it is already written (a visibility-scored store puts it in
the row it takes), and because the two halves disagreeing is a quiet failure rather
than a loud one: a delivery that becomes reclaimable before its holder's claim lapses
goes to a worker that cannot write to it yet, which spends a whole pass discovering
that.

There are six durations across the stores and the worker, and the natural worry is that
they form a hierarchy nobody has written down. They mostly do not, and where one
relation would have had teeth it was designed out rather than documented:

| Duration | Where it is set | What it depends on |
|---|---|---|
| `lease` | the scheduler | how long a pass can honestly take, which is a property of the *workflow*, not of any other setting |
| `poll` | the scheduler | nothing; above `within` it merely stops having an effect |
| `within` | `work` | nothing; it is a shutdown bound, not a rate |
| `tick` | `work` | nothing; it is the granularity a slept-out workflow wakes at |
| `contended` | `work` | nothing; a preference about how eagerly to re-look |
| `ttl` | `RedisCheckpointer` | the longest a workflow may *wait*, which is again the workflow's property |

Two of those are worth spelling out. `poll` and `within` interact but cannot conflict,
because `next_ready` sleeps for `min(poll, remaining)`: a poll interval longer than the
read budget costs one extra attempt and nothing else, so the pair needs no rule. And
`ttl` looks like it must exceed `lease`, since a checkpoint that expires under a live
claim would take the fencing token with it, but it does not: the Redis `CLAIM` script
stamps each token `max(now_ms, previous + 1)`, a hybrid logical clock, precisely so
that a claim key recreated after an expiry still outranks a pass stalled since before
it. The dependency was removed rather than left for an operator to respect, which is
the shape to prefer whenever it is available.

What that leaves is one relation with real teeth, and it is the one no check can reach:
a `lease` has to exceed the longest a pass takes, and a Redis `ttl` has to exceed the
longest sleep or approval a workflow can sit in. Both right-hand sides are facts about
the workflow body rather than about any configured value, so they are stated as
requirements and guessed at by whoever deploys. What *is* enforced is the floor: every
one of these is refused at construction unless it is a positive duration, since none of
them has a meaningful zero and a duration read from an unset setting is how one
arrives.

## Gaps

These hold whatever store is underneath. Each store carries its own on top, and those
are on its page: [Redis](../without-durability-redis/index.md#gaps),
[Postgres](../without-durability-postgres/index.md#gaps),
[SQLite](../without-durability-sqlite/index.md#gaps). They are ordinary missing
features, expected in something this size:

- **An effect outside the store is still at-least-once.** The claim stops a second pass
  from starting, and the fence stops a superseded one from writing, but neither reaches
  the gap between an effect happening and its record landing. A pass that crashes in
  that window leaves the gateway charged and the checkpoint silent, and the next pass
  charges again. `transact` closes that, but only for effects the store can perform
  itself, which a payment gateway is not. For everything that crosses a boundary the
  answer is the ordinary one: make the effect idempotent, which is what the workflow id
  is for.
- **A split store's `arrive` still has a window.** `SplitDurable` records and then
  queues, so a process that dies between them leaves a workflow holding its value with
  nothing scheduled to run it. It is the recoverable half by design, and for the API an
  ordinary client retry under the same idempotency key supplies the missing wakeup, but
  nothing here retries on the caller's behalf and no sweep looks for workflows in that
  state. `PostgresDurable` and `SqliteDurable` do not have this gap.
- **The lease is a guess, not a bound.** `Scheduler.lease` has to exceed the longest a
  pass can honestly take, and nothing can work that out for you. Set it too short and a
  slow pass is fenced mid-flight and has to be re-run; too long and a crashed worker's
  workflow waits that long. Nothing renews it while a pass runs, deliberately: the fence
  makes an overrun safe rather than corrupting, so renewal would buy throughput, not
  correctness. What is *not* left to a guess is the two windows agreeing, since the
  worker takes both from this one number.
- **No retries, backoff, or timeouts.** A step that raises is logged and acknowledged,
  and the workflow stops until something else wakes it. There is no dead-letter and no
  heartbeat.
- **No history.** The checkpoint is latest-state only: no timestamps, no attempt counts,
  and no record of a failure. From the store, a failed workflow and a suspended one are
  indistinguishable.
- **No enumeration, cancellation, or search.** Nothing can terminate or reset a
  workflow, and nothing exposes a list of them. How hard that would be to add differs in
  kind between the stores: Redis checkpoints are keyed with no index over them, so
  listing means `SCAN`, where a table answers "which workflows are suspended" with an
  ordinary query. That is the clearest thing a SQL store offers that this does not yet
  spend.
- **Determinism is documented, not enforced.** Nothing stops a workflow calling out to
  the network between two steps. `Run` does take its clock as an argument, which covers
  the most common case. The failure mode is milder than Temporal's, since keying by name
  means a workflow that takes a different branch on replay runs different steps and
  leaves the old records unread rather than erroring, but that also means it goes
  undetected.
- **The default codec restricts what a step can return,** and nothing bounds a payload's
  size. `JsonCodec` is the stdlib's `json`, so a step result must be JSON-*native*
  rather than merely JSON-serializable: a tuple encodes and comes back a list, which
  breaks the round trip `CheckpointCodec` requires. Swapping in a codec that knows the
  application's types is a constructor argument (`RedisCheckpointer(redis=..., codec=...)`)
  rather than a change to any store, which is what the interface buys; what it does not buy
  is a shipped alternative, and there is none here. `PostgresCheckpointer` also narrows
  the choice, since a `jsonb` column will only take JSON text.

Where this sits next to Temporal, DBOS, LangGraph, and Restate is
[its own page](alternatives.md).

## Tests

Two layers, split along the line the packages are split along.

Each store's own package tests what that store *is*: its scripts or its statements, its
exclusion under real concurrency, its failure modes. Those drive a real server (Redis,
Postgres) or a real file (SQLite) rather than a fake.

The repository's `integration` package tests what a workflow gets, once, against
*every* store at the same time. One fixture builds a `Durable` per backend and one
suite runs the same saga, the same suspension, and the same API-plus-worker flow across
all of them, so "a workflow cannot tell which store it got" is a claim the suite makes
rather than one this page asserts.

Everything here runs against dicts by default, because `Checkpointer` and `Scheduler`
are injected and `memory.py` ships an implementation of both. That is also why the
worker can be driven without a server at all.
