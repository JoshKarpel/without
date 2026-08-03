# Against the alternatives

The interesting comparison is not the feature list, which is one-sided, but where each
system puts the same three concerns: what the durable state *is*, what identifies a
step across a replay, and who guarantees one writer.

| | Closest piece here | What it has that this does not |
|---|---|---|
| **Temporal** | `stepwise` | An event history replayed in order, a versioning API for changing in-flight code, retries and timeouts and heartbeats, visibility and search, a determinism sandbox |
| **DBOS** | `stepwise` over `PostgresCheckpointer`, almost exactly: a library plus a database, no server of its own | Recovery of pending workflows at startup, queues with concurrency and rate limits, workflow and step status tables, decorators that make all of it invisible, a real migration story |
| **LangGraph** | `stepwise` | Per-superstep state snapshots over a statically declared graph that may be traversed cyclically, plus time travel and a platform for scheduling |
| **Restate** | the API-plus-worker pair | Also a single binary rather than a cluster, with exclusion structural in keyed virtual objects rather than leased |

LangGraph's row is the one that is easy to get wrong. "Graph plus checkpointer" reads
like `run_durably` over `without-dag`, and the structures are not the same kind of
thing. A LangGraph graph is declared up front, as a `CompiledGraph` is, but its edges
may be conditional and its paths may loop: a node can be reached again on the next turn
of a cycle. `without-dag` is acyclic by construction, and deliberately so, since that
is what lets a plan be topologically sorted once and each node run exactly once. So the
piece here that expresses a LangGraph-shaped workflow is `stepwise`, whose control flow
is ordinary Python and may therefore loop; what it gives up in exchange is the eager
check over a structure known before the run.

Restate is the one that most tests the premise, because it accepts the same starting
position (a durable workflow should not need a cluster) and still concludes it needs a
log and a leader per key. What it gets for that is exclusion that does not expire: a
lease has to be guessed at, and a partition leader does not.

## Credit, and how this sits next to DBOS

[DBOS Transact](https://github.com/dbos-inc/dbos-transact-py) is the direct inspiration
for the SQL stores, and two of its findings are load-bearing here. The first is that a
library plus a database is enough, so the exclusion a durable workflow needs does not
require a server to be built for it. The second is sharper and is the whole reason
`transact` exists: a step's own business write and its checkpoint, committed together,
is what moves that step from at-least-once to exactly-once, and no amount of care in
user code reproduces it. Both are theirs, and this repo would not have looked for
either. What is borrowed is those ideas and nothing else: the three tables here were
designed for this toy's own two protocols and look nothing like theirs.

Where this differs is worth stating plainly, because it is a different position rather
than a smaller version of the same one:

- **The guarantee lives in the interface, not in the database.** DBOS requires Postgres, and
  that requirement is what lets it supply the semantics. Here `Checkpointer` states the
  requirements and Postgres is one implementation that meets them, alongside Redis and
  SQLite, which meet them too. What DBOS gets for its choice is that it can assume the
  semantics everywhere; what this gets is that a deployment brings whatever it already
  runs.
- **Steps are named at the call site, not declared by a decorator.**
  `await run.step("charged", ..., as_text)` puts what is durable, under what key, and
  how it comes back, all in the line that does it. A decorator makes the ordinary case
  shorter and moves the key off the call. That is the usual locality trade, taken
  deliberately in the other direction, and DBOS's version is far nicer to use.
- **This is a validation artifact and that is not a modest disclaimer.** No recovery of
  pending workflows at startup, no status tables, no queues with concurrency or rate
  limits, no retries, no observability. The [gaps](index.md#gaps) are the specific
  list. DBOS is a product built to run production workflows; this exists to find out
  what the substrate makes cheap.

## Two choices that differ from all of them

Each is worth naming with what it costs.

- **Steps are keyed by name, not by position.** Temporal replays a workflow against an
  ordered history, which is why changing workflow code with executions in flight needs
  an explicit versioning API. Keying by name makes inserting and reordering steps free,
  and makes nondeterminism degrade rather than raise. It costs the detection: nothing
  notices that a workflow's code changed, or that a recorded value's shape did.
  `Run.claim` catches two steps sharing a name within one pass, which is the sharpest
  version of the failure, but only within a pass.
- **A durable timer and an external signal are the same thing.** `Sleeping` and
  `Waiting` differ only in whether anyone schedules the wakeup, and both are
  satisfied by an entry in the same mapping, which is also why starting a workflow and
  signalling one are the same call in the API. Temporal and DBOS have separate machinery
  for each. What that buys is a smaller vocabulary; what it costs is that the store
  cannot tell an awaited value from a recorded result, so an approval written for a
  workflow that never asked for one simply sits there unread.
