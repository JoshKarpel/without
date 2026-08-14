# without-durability-redis

[`without-durability`](../without-durability/index.md)'s two interfaces over Redis: a
workflow's completed steps as one hash, its claim as another, and a queue of
workflows that can run now.

```python
from redis.asyncio import Redis
from without_durability import SplitDurable
from without_durability_redis import RedisCheckpointer, RedisStreamScheduler

redis = Redis(host=..., decode_responses=True)  # this client owns both ends of every key
durable = SplitDurable(RedisCheckpointer(redis=redis), RedisStreamScheduler(redis=redis))
```

Every guarantee here is a small Lua script, and each is a script for the same
reason: it is only correct as *one* step. Checking whether a workflow is free and
taking it; checking a fencing token and applying the write it guards; testing
whether a key is recorded and reading back the winner. Split any of them into two
round trips and the gap between them is where the guarantee leaks.

`LuaEffect` is what this store can commit alongside a record, so a step whose
effect *is* a Redis write happens exactly once. That is worth stating plainly,
because the usual framing (that exactly-once needs Postgres) is wrong about why: a
Lua script is an atomic commit over Redis data, and the real constraint is that
you can only transact within one datastore.

On a cluster that constraint becomes a slot. A workflow's two keys are hash-tagged
(`workflow:{id}`) so a script may touch both, and an effect's keys must carry the
same tag. Redis enforces this rather than trusting it: declared keys spanning
slots are refused outright, and a script reaching a key another node owns dies
partway having written nothing.

Two queues ship, and the difference between them is the finding rather than a
choice you have to make carefully. `RedisStreamScheduler` is a stream read as a consumer
group beside a deadline-scored sorted set, which buys a blocking read so an idle
worker costs nothing. `RedisSetScheduler` is one sorted set scored by when each
workflow becomes visible, which makes the timer, the consumer group, the pending
list, and the trimmer all disappear, and costs the blocking read.

## What Redis holds, and for how long

Four keys, and they have four genuinely different lifetimes. This matters more
than it looks: three of the gaps below are lifetime mismatches between them
rather than anything wrong with a single structure.

| Key | Type | Written by | Grows | Ends |
|---|---|---|---|---|
| `workflow:{id}` | HASH | `supply` from the API, `record` from a pass | one field per completed step | TTL, re-armed on every write |
| `workflow:{id}:pass` | HASH, 2 fields | `claim` and `release` | never (fixed shape) | TTL, re-armed with the checkpoint |
| `{ns}:ready` | STREAM + group | `make_ready`, and the timer | one entry per wakeup | `trimming`, behind what every group acknowledged |
| `{ns}:sleeping` | ZSET | `wake_at`, drained by `wake_due` | one member per sleeping workflow | removed when it comes due |

The braces are Redis Cluster's hash tag, so the first two land on one slot and a
script may touch both. The last two are shared by every workflow, which is why
they carry the queue's namespace rather than a workflow's id.

That split decides where a workflow id is *structure* and where it is merely
data. In the two per-workflow keys it is interpolated into the key name, so it
carries two constraints: no braces (they would delimit the hash tag instead of
it), and bounded length. In the queue it is a stream field or a sorted-set
member, so neither applies. `RedisCheckpointer` documents them and does not enforce them,
on the grounds that a UUID satisfies them without trying and validating every call
to catch someone who went out of their way is the wrong trade. A store that bound
the id as a query parameter rather than concatenating it would have no constraints
at all, which is the tell that this is a property of building keys by
interpolation rather than of workflow ids.

### One order, end to end

Following a payout that captures two items, sleeps out a settlement window, and
waits for a human. Each column is one key; `▪` is a value arriving.

```text
                       workflow:{id}      workflow:{id}:pass   {ns}:ready      {ns}:sleeping
                       HASH: checkpoint   HASH: claim + fence  STREAM: wakeups   ZSET: deadlines
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
  Sleeping(D)       ─────────────────────────────────────────────────────────▶ ▪ score=D
  release           ────────────────────▶ ▪ token=1 until=0
  XACK              ───────────────────────────────────────── ▫ 1-0 acked, still in the stream
timer, once D passes
  wake_due (Lua)    ─────────────────────────────────────────▶ ▪ 2-0        ◀── ▪ removed
worker pulls again
  claim             ────────────────────▶ ▪ token=2 until=T₂  (the fence advances)
  awaiting          ─  suspends: "approved-by" is not there
  Waiting           ── nothing scheduled: only the world can answer this
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
                    re-armed each write   only when the       acked and       again
                                          workflow expires    trimmable
```

Two things the diagram is meant to make obvious. The checkpoint only ever grows,
because `HSETNX` never replaces a field, so what an operator sees with
`redis-cli` is the complete history of what happened even though it is not an
event log. And the stream grows for a different reason: `XACK` moves an entry out
of the pending list and leaves it in the stream, so the three entries above are
still there when the workflow is done, waiting for `trimming` to take them.

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

**The queue** is the one whose lifetime is a job rather than a property. Every
`make_ready` and every `wake_due` appends, and `XACK` clears the pending list
while leaving the entry, so the only thing that removes anything is `trimming`
running beside the worker. It trims what every consumer group has acknowledged
(`XTRIM ... ACKED`) rather than by length, since capping the *length* would drop
the oldest entries, which are the ones nobody has run yet.

**The sleepers** is an index rather than a record: the deadline itself lives in
the checkpoint, put there by `run.sleep`. Losing this set leaves workflows asleep
forever rather than corrupt, and rebuilding it is a scan over checkpoints. It
carries no TTL of its own, which is the other half of the sharp edge above: a
workflow can be woken by a deadline that outlived the checkpoint the deadline was
recorded in.

### The same thing as one sorted set

`RedisSetScheduler` replaces the stream *and* the
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
lost, and nothing to trim, because a ZSET holds each workflow once however many
wakeups arrive. `wake_due`, `reclaim`, and `prepare` all become no-ops, which is
the finding rather than an omission.

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
sorted set has no blocking read, so `RedisSetScheduler` polls, and the poll interval
is a floor under how fast anything starts.

### Choosing between them

Both ship, because the trade is real rather than one being an early draft of the
other:

| | `RedisStreamScheduler` | `RedisSetScheduler` |
|---|---|---|
| Waiting for work | `XREADGROUP BLOCK` parks inside Redis, so an idle worker costs nothing and a submission is picked up the instant it lands | polls, so the interval (50ms by default) is a floor under how fast anything starts |
| Growth | one entry per wakeup, bounded by running `trimming` beside the worker | one entry per workflow however many wakeups arrive, so there is nothing to bound |
| Redis version | `trim` needs 8.2, for `XTRIM ... ACKED` | any |
| Moving parts | consumer group, pending list, `XAUTOCLAIM`, a deadline-scored sorted set, a timer, a Lua script to move between them, and a trimmer | one sorted set and two small scripts |
| Losing a wakeup | impossible by construction: every `make_ready` appends a new entry | prevented by the receipt, since the score a pass took is what `done` compares |
| Taking over a dead worker | `reclaim`, off the pending list | nothing to do; the lease elapses and the row becomes visible |

Two rows are worth reading together. The stream's whole advantage is the first
one, and its whole cost is the fourth, so the question is whether the latency
floor matters to what you are building. For a workflow whose steps are network
calls, one poll interval is noise; for a submit-to-first-pass path a user is
watching, it is the whole budget.

The `Scheduler` protocol carries `prepare`, `wake_due`, and `reclaim` only because
the stream needs them, and they are no-ops in every other implementation this
project ships. That is a fair criticism of the interface, and the reason it is not
acted on is this table: a queue with an acknowledgement and a pending list is a
real shape, so the protocol keeps room for one.

In one line: a stream is cheaper to wait on, a sorted set is cheaper to reason
about.

## Gaps

Beyond [the ones every store carries](../without-durability/index.md#gaps), two
here are load-bearing, in the sense that a deployment would be wrong rather than
merely limited.

**Redis is not obviously durable enough for the claim `run_durably` makes.** That
docstring reasons carefully about the crash window between an effect and its
record, which only matters if `record` returning means the value survived. Redis
with default persistence and asynchronous replication does not give that: a
failover can roll back acknowledged writes, and nothing here uses `WAIT`. The
test `compose.yaml` disables persistence outright, which is right for tests and
means the suite never exercises the question. Temporal on Cassandra or Postgres,
DBOS on Postgres, and both of this project's SQL stores commit for real.

**The checkpoint TTL can lose a workflow that is behaving correctly.**
`RedisCheckpointer.ttl` defaults to one day and is re-armed only on `record`. A
workflow suspended for longer than that writes nothing while it waits, so its
hash expires, while its entry in the sleeping sorted set (which has no TTL)
survives. The wakeup then fires, the worker resumes, `load` returns `{}`, and the
workflow suspends on its first `awaiting` forever, with whatever effects it
already performed left standing. The `ttl` knob is the right idea and its default
is shorter than the sleep semantics this same package advertises. Rows do not
expire, so the SQL stores trade this failure for a sweep somebody has to write.

And one ordinary one:

- **`trim` needs Redis 8.2.** `ACKED` arrives there, and it is what lets the
  server decide which entries every group has finished with. On an older server
  `RedisStreamScheduler` still works and still grows; `RedisSetScheduler` has
  nothing to trim on any version.
