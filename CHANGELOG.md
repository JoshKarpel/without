# Changelog

## Unreleased

### Added

- **`without-dag`**: resuming a graph from a checkpoint. `run(...)` and `run.stream(...)` take a
  `checkpoint` of `{node key: result}`, the same mapping `stream` emits, and a node named in it is
  not run: its result is taken as given and fed to its dependents, so a run picks up where an
  interrupted one stopped and a checkpoint covering the whole graph performs no effects at all. The
  execution seam already treated a pre-supplied key as done; what was missing was a key worth
  storing, so `node` now takes one as its first argument (`graph.node("charged", charge, order)`)
  and `NodeKey` is a `str`. A name chosen in the source means the same thing on the other side of a
  crash, where an `object()` minted at build time does not, and it must be distinct from every other
  key in the graph (entries are keyed by position, `input:0`). A checkpoint key that names no node
  is rejected rather than ignored, since that is the shape of one written by a different version of
  the graph. `stream` being pull-driven makes the store write a barrier: nothing downstream of a
  completed step starts until the consumer asks for the next result.
- **`integration`**: `durable`, a durable workflow and its saga, sinking to Redis. An order
  fulfilment graph (charge and reserve concurrently, ship, render) whose completions are recorded to
  a Redis hash under the workflow's idempotency key and read back to resume, plus `run_saga`, which
  on failure parses the checkpoint into how far the run got and drives a compensation graph that is
  itself checkpointed, so an interrupted rollback resumes rather than refunding twice. Its Redis
  tests run against a real server: the `test` recipe starts the new `compose.yaml` with podman,
  hands pytest each published address, and takes the stack down from an exit trap. They carry a
  `compose` mark and skip where podman is not installed.
- **`integration`**: `durable.stepwise`, the same durability without the graph, sharing the one
  `Checkpointer` seam. A workflow is an ordinary async function whose effects are named
  (`await run.step("charged", ...)`); resuming calls it again and each step hands back what is
  recorded. It asks one thing in return, because the code *between* steps re-runs: effects live in
  steps, the code around them is pure (Temporal and DBOS state the same rule as workflow
  determinism). Keying by name rather than by position is what keeps it that mild, since reordering
  or inserting a step changes nothing. Two things follow that a fixed graph cannot express, both in
  `durable.payout`: a fan-out whose width comes from a step's *result*, one key per item so a crash
  resumes item by item; and `Suspended`, raised by a step that cannot finish now, which is what
  makes a settlement window (`run.sleep` records the deadline, so a crash mid-wait does not restart
  the clock) and a human approval (`run.awaiting`, satisfied by another process writing one field
  into the workflow's hash) ordinary lines of code.
- **`integration`**: `durable.api` and `durable.worker`, an API server and a queue worker that make
  the durable mechanism a running service, sharing only Redis. The API runs no workflow: submitting
  an order and confirming a payout are the same two lines (record the value the workflow is waiting
  on, then make it ready), because `Run.awaiting` cannot tell an initial payload from a human's
  answer, and the workflow id is the request's `Idempotency-Key`, so resubmitting addresses the same
  workflow rather than starting a second. The worker is a `Sink` over the queue plus a timer:
  `Suspended` carrying a deadline is scheduled into a sorted set, `Suspended` without one is left
  for the API's confirmation to enqueue, and nothing polls a workflow to ask whether it can proceed.
  `durable.wakeups` is the two structures that take (a Redis stream of ready ids read as a consumer
  group, and a deadline-scored sorted set) plus the one Lua script that moves a workflow between
  them, since taking it off the sleepers and queueing it are durable only together: as two calls, a
  timer that died in between would leave the workflow in neither structure and asleep forever.
  Running the move in the server closes that gap and settles which of several timers owns a wakeup,
  so every worker can run one. A stream rather than a list because a list loses work: a delivery
  stays pending until acknowledged, so a worker that dies mid-pass leaves it to be reclaimed rather
  than taking it to the grave, and the ack lands after the pass on every path but cancellation. A
  worker runs up to `POOL` passes at once through `without`'s `limit_concurrency`, and every pull
  takes exactly one delivery (a reclaimed one if any workflow was abandoned, otherwise a fresh read),
  so it holds precisely as many wakeups as it is working on and stops reading at capacity. The
  end-to-end test submits over HTTP, waits out a real one-second window, confirms, and reads the
  payout back.
- **`integration`**: `durable`'s `Checkpointer` seam now states the guarantees a store has to
  provide, and the Redis one provides them. A protocol of `load` and `record` was too weak to run
  a workflow safely at any scale: it had no way to say "only if nobody else is running this" or
  "only if I am still the one who may write", so two wakeups for one workflow (which the
  submit-then-confirm flow produces every time) ran two passes that both found a step unrecorded
  and both performed its effect. `claim` now takes the right to run a pass and `record` carries the
  `Pass` it was granted, which makes "you cannot write without holding the workflow" structural
  rather than remembered. The token is a *fencing* number minted by the store, because a lease
  alone is not exclusion: a process that stalls past its lease still believes it holds the
  workflow, and only the store knows better, so a write from a superseded pass is refused
  (`Fenced`) rather than applied. `record` is also conditional, never overwriting a recorded step
  and returning whatever is stored after the call, so two passes that both ran an effect at least
  agree on its result; `Run.step` hands that value back rather than its own. `supply` is the
  unclaimed half for values that come from outside a pass (the API's order and approval), which
  keeps first-writer-wins without making an approval fail because a worker is mid-pass, and makes
  a resubmitted order genuinely idempotent rather than an overwrite. In `RedisCheckpointer` each of
  these is one Lua script, for the reason `wake_due` already was: checking whether a workflow is
  free and taking it, or checking a token and applying the write it guards, are only correct as a
  single step, and the keys are hash-tagged so a workflow's two land on one slot. The worker claims
  before each pass and releases after, and a wakeup for a workflow someone else holds is scheduled
  to look again rather than run beside them. What this does not reach is the gap between an effect
  and its record, which stays at-least-once: closing that needs the step and the checkpoint in one
  transaction, which is what DBOS gets from Postgres and no Redis store can offer.
- **`integration`**: `Run.transact`, which performs an effect and records it in one commit, making
  that step exactly-once rather than at-least-once. `step` runs an effect and then writes the
  record, so a crash between them leaves the effect done and unrecorded and the next pass repeats
  it; `transact` hands the store an effect it can perform itself, so there is no in-between. The
  reason this works on Redis is worth stating, because the usual framing (that exactly-once needs
  Postgres) is wrong about why: a Lua script is an atomic commit over Redis data, and the real
  constraint is that you can only transact within one datastore. Postgres wins only for effects
  that live in that Postgres, and loses for everything else exactly as Redis does. So `Checkpointer`
  is now generic over the type of effect a store can commit: `LuaEffect` for Redis (a script
  spliced into a wrapper supplying the fence check and the record, with `KEYS` and `ARGV` rebound
  so it is written as if it ran alone), a function over its own dict for the in-memory double, a
  session callback for a Postgres store that does not exist yet. The parameter defaults to `Never`,
  so a store with nothing to offer says so in its type and `transact` becomes uncallable rather
  than absent, and code that never transacts keeps the bare `Checkpointer` annotation. On a cluster
  the effect's keys must carry the workflow's own hash tag, since a script spanning two slots is a
  distributed transaction wearing a local disguise.
- **`integration`**: a workflow's fencing token is now `max(now_ms, previous + 1)`, a hybrid
  logical clock rather than a counter. The checkpoint and the claim expire together, so a workflow
  quiet for longer than the TTL is forgotten entirely; a counter would then hand a reused id token
  1 while a pass stalled since before the expiry still held token 3, and the corpse would outrank
  the living. Seeding from the server clock closes that without coupling the two keys' lifetimes,
  and falling back to `previous + 1` keeps it strictly monotonic within one incarnation even if the
  clock steps backwards.
- **`integration`**: `durable.schedule`, a second `Scheduler` that replaces the stream and the
  sleeping sorted set with one sorted set scored by when each workflow becomes visible. Queued now
  is a score in the past, sleeping is a score in the future, and being worked on is a score one
  lease ahead, so there is nothing to move between structures: `wake_due`, `reclaim`, and `prepare`
  all become no-ops, the timer has nothing to do, and the unbounded stream stops being a gap
  because a sorted set holds each workflow once however many wakeups arrive. Holding it once is
  also the catch, since a wakeup landing mid-pass has nowhere to go but on top of the entry that
  pass is holding, and removing that entry afterwards would throw it away. The score a pass took
  *is* its receipt, so finishing removes the workflow only if the score is unchanged, and anything
  that wanted another pass (a confirmation, the pass's own `wake_at`, another worker taking over an
  overrun) wrote a different one and survives. What it costs is the blocking read: `XREADGROUP
  BLOCK` parks a worker inside Redis, a sorted set has none, so this polls and the interval is a
  floor under how fast anything starts. It is a drop-in, and the end-to-end test now runs the same
  API and worker over both queues to keep it one.
- **`integration`**: `durable.postgres`, both seams over one Postgres: `PostgresCheckpointer` and
  `PostgresScheduler` across three tables (`workflow_checkpoint`, `workflow_claim`,
  `workflow_queue`), with `SqlEffect` as the effect type `transact` takes there, an async callback
  handed a cursor inside the open transaction. It is the other half of the argument the Redis store
  makes, and what it shows is where the atomic unit came from: every write that had to be a Lua
  script is one statement or one transaction here, because SQL says "check this, then write that,
  and let nobody in between" by default. The claim is an upsert whose `DO UPDATE` carries a `WHERE`
  on the lease, so a conflicting row that is still held fails the predicate and `RETURNING` yields
  nothing, which is how a lost race is reported; `record` is a `FOR UPDATE` CTE over the claim row
  feeding an upsert whose `DO UPDATE SET value = the value already there` returns the winner's, and
  the row lock is what makes the fence serialize against a claim in flight rather than read a stale
  snapshot; the queue takes with `FOR UPDATE SKIP LOCKED`, so several workers polling one table fan
  out instead of queueing on its head. Three things that were live questions on Redis do not arise:
  a workflow id is a query parameter rather than key structure, so it carries no contract at all;
  nothing expires, so the TTL that can lose a suspended workflow is gone and the fencing token can
  be a plain counter rather than a hybrid logical clock; and a default Postgres commits
  synchronously, so `record` returning means what `run_durably` assumes it means. What it costs is
  that sweeping finished workflows becomes a job somebody writes, that `next_ready` still polls
  (`LISTEN`/`NOTIFY` would close that and does not yet), and that `migrate` is three
  `CREATE TABLE IF NOT EXISTS` under an advisory lock rather than a migration tool. The queue being
  a table in the same database is what makes "no second system" a claim this can make, and the
  end-to-end test now runs the same API and worker over Postgres as well.
- **`integration`**: `durable.Durable`, one seam over `Checkpointer` and `Scheduler` that names the
  transitions crossing both. Making a workflow runnable was two writes to two stores in the
  caller's hands, in an order it had to get right, with a crash window nothing in the types
  mentioned; `arrive(workflow, key, value)` is now one call and the API's submit and confirm are
  one line each. The argument was already in this package, about `wake_due`: a protocol that names
  the transition rather than its halves makes the lossy intermediate state unrepresentable, which
  beats remembering to do both halves. It had just not been carried across the two seams. Two other
  tells that the boundary was misplaced: `Scheduler` had to state a cross-call ordering rule in prose,
  and three of its seven methods are no-ops in two of three implementations, which is a protocol
  shaped around one implementation's mechanism rather than around its question. The fix is not one
  big interface, which would bundle a mechanism to repair a contract and forfeit the split
  deployment (a Postgres checkpoint beside an SQS queue is ordinary), so the contract is bundled and
  the mechanisms are not: `Checkpointer` and `Scheduler` are unchanged underneath, and what varies is
  what `arrive` *guarantees*. `SplitDurable` composes any two stores and does two writes, recording
  before it queues, because recorded-and-unqueued is recoverable by anything that asks again where
  queued-with-nothing-recorded drops the value; `PostgresDurable` requires its two stores to share
  one pool, checked at construction rather than documented, and does one commit. `Scheduler` and
  `Delivery` moved into the contracts module beside `Checkpointer`, so all three contracts are in one file and
  everything naming a product implements them.
- **`integration`**: corrected what "one datastore" means for `transact` and `arrive`, which had
  been written as though a Postgres transaction were unconditionally local. Measured rather than
  recalled: Redis Cluster rejects a script whose declared keys span slots (`CROSSSLOT`) and kills
  one that reaches an undeclared non-local key partway through, having written nothing, so a
  cross-node atomic write is unavailable rather than expensive, and a single node owning every slot
  cannot show you either rule. Sharded Postgres instead escalates: under Citus a transaction
  touching shards on two nodes becomes a real distributed transaction with `PREPARE TRANSACTION` /
  `COMMIT PREPARED`, a deadlock detector, and `max_prepared_transactions` to size, which still
  commits atomically but is a different guarantee arriving silently. The escape is the same shape on
  both sides: Redis's hash tag and Citus co-location by workflow id are one idea, and sharing a pool
  is the necessary half of it rather than the sufficient one.
- **`without-durability`** (new package): the durable-workflow mechanism, extracted from the
  `integration` toy now that it has earned its own name. It holds the three contracts (`seams.py`),
  the graph runners (`graph.py`), the stepwise mechanism, the queue worker, and `memory.py`, whose
  in-memory stores are promoted from test doubles to a shipped artifact because "a store is
  injected" is the design and this is the store a test should inject. It depends on `without` and
  `without-dag` and nothing else; every store is its own package, so nobody installing the core
  pulls a driver they will not use. What stays in `integration` is what a *deployment* supplies:
  the fulfilment graph, the payout workflow, the body the worker runs, and the HTTP API.
- **`without-durability-redis`, `without-durability-postgres`, `without-durability-sqlite`** (new
  packages): one per store, matching how `without-env` and `without-configmap` already split two
  config sources. The SQLite one is the smallest thing that meets every requirement the seam
  states, with no server and no third-party driver: `BEGIN IMMEDIATE` *is* the exclusion, so it
  needs neither Postgres's `FOR UPDATE` nor Redis's Lua, and because the datastore is a file there
  is nothing to co-locate, which is DBOS's guarantee for an application that never needed Postgres.
  Its effect type is a *synchronous* callback where the Postgres one is `async`, because the whole
  transaction runs on one worker thread. Its scope is one machine, which is the deployment it is
  for rather than a defect.
- **`without-durability-redis`**: `trim`, which bounds a stream that otherwise only grows, closing
  the one gap that made the sorted-set queue strictly better. It is `XTRIM ... MAXLEN 0 ACKED`
  (Redis 8.2+), so the *server* decides what every consumer group has finished with rather than
  this computing a floor client-side and racing every ack that lands meanwhile. One hazard is
  guarded rather than inherited: with no consumer groups at all `ACKED` has no effect and the trim
  degrades to a plain `MAXLEN 0`, which would delete orders queued before the first worker booted,
  so `trim` refuses a stream that has no groups.
- **`without`**: `ticks(every)`, a `Stream` of moments, one now and one every interval after. It is
  the clock as a source, so periodic work stops being a `while True` with a `sleep` buried in it
  and becomes a `Sink` that says only what happens per event, composed with a stream that says when.
  `waking` and `trimming` are both sinks over it now, which means the same code runs off a timer,
  off a queue an operator pokes, or off a fixed list of instants in a test. Each tick carries its
  own moment, so a consumer needs no clock of its own and a test controls time by choosing values.
- **`without-web`**: reverse routing. `url_for(route, values)` renders a route back to a concrete
  path from the values for its path parameters, the inverse of the trie walk. It is a plain
  function of the route *value* (routes are identified by value, no registry), each value fed back
  through its converter to prove it round-trips (parse, don't validate, in reverse). Because
  `mount` bakes any prefix into the route, a route is a self-contained value whose segments are its
  full path, so reversing needs no router and holds no hidden prefix: a handler links by referencing
  a route value (immutable), and a websocket handler reverses an HTTP route to link to its resource
  with the same call.
- **`without-http`**: granular client request timeouts. A `Timeout` value bounds each phase
  independently (`connect`, `read`, `write`, `pool`), each a `timedelta` and an *inactivity* bound
  that re-arms on progress, disabled by default (a deadline is the caller's policy, not the
  transport's). Each axis applies through its own bound (`connecting()`, `reading()`, `writing()`,
  `pooling()`), so the axis-to-error mapping lives on `Timeout` rather than at every call site. A
  timeout raises a typed `ConnectTimeout` / `ReadTimeout` / `WriteTimeout` / `PoolTimeout` under
  `HTTPTimeout` (itself a `TimeoutError`), so a caller can tell how far the request got and retry
  the right ones. Also: per-host connection bounds and gating of HTTP/2 stream issuance against the
  server's `SETTINGS_MAX_CONCURRENT_STREAMS`. `max_connections_per_host` bounds concurrent HTTP/1.1
  connections to one origin (the acquire-wait the `pool` axis guards); `max_keepalive_per_host`
  bounds how many *idle* connections are retained per origin once a burst subsides, so the pool ramps
  up under load but settles back down when quiet. Both unbounded by default, and must be `>= 1` when
  set.
- **`without-http`**: socket options on the client pool and on `serving`, as `(level, option, value)`
  triples built by pure producers and combined by concatenation, the way headers are: `tcp_keepalive`,
  `send_buffer_size`, and `receive_buffer_size` each describe one concern and know nothing of each
  other, so `ConnectionPool(socket_options=tcp_keepalive() + send_buffer_size(1 << 16))` needs no
  merge step that understands what any of them mean. `serving(socket_options=...)` applies them to the
  *listening* socket, whose buffer sizes every accepted connection inherits.
  TCP keepalive is the default (`socket_options=tcp_keepalive()`), so the kernel probes an
  otherwise-idle pooled connection and drops it when a peer has vanished *silently* (a crash, a
  partition, a NAT dropping the flow), which a clean server-side close does not: that sends a `FIN` the
  pool already detects before reuse. This matters most because request timeouts are disabled by
  default, so nothing else would notice a dead idle socket until a request hung on it. Pass `()` for
  the kernel's own defaults.
- **`without-asgi`**: `file_response(path)` streams a file as the `ResponseStart` + `ResponseBody` event
  stream a handler yields, with `Content-Type` guessed from the suffix (`mimetypes.guess_file_type`,
  overridable) and `Content-Length` from `stat`, the body read in `chunk_size` pieces off the event loop
  (`asyncio.to_thread`) so a large file is never buffered whole. It is a coroutine, not an async
  generator: awaiting it runs the `stat` up front, so a missing file raises `FileNotFoundError` before
  any `ResponseStart` is emitted and a handler can still answer a clean `404`. Reads and writes are
  lockstep by default; wrap the result in `spool` for read-ahead.
- **`without-asgi`**: `headers`, a module of pure functions over the raw ASGI header pairs
  (`RawHeaders`) rather than a wrapper type. `get_all` returns every value under a name as an
  immutable tuple and `first` the first (for singleton fields, where a duplicate is a protocol
  violation); `add`, `replace`,
  `remove`, `subset`, and `merge` are `RawHeaders -> RawHeaders` transforms. All match field names
  case-insensitively (RFC 9110) and preserve duplicates, so a multi-valued `Set-Cookie` survives
  intact. `RawHeaders` is the one representation the ASGI spec fixes on both edges, so operating on
  it directly keeps reads a scan and writes a straight pass-through, no value to wrap or unwrap.
- **`without-web`**: `once` and `optional`, parse adapters for singleton request fields. Each
  lifts a one-value `parse` into the tuple-taking form `query_param`/`header_param` feed: `once`
  requires the value exactly once (returning `V`), `optional` allows zero or one (returning
  `V | None`, `None` when absent). A duplicated value raises `ValueError` in both (a duplicated
  singleton violates RFC 9110 §5.3). Reading a single value stays a policy the call site chooses
  rather than a second extractor.
- **`without-web`**: `ExtractionError`, a `ValueError` subtype marking a request rejected *while one
  of its typed values was being extracted*. The `query_param`/`header_param`/`body` extractors raise
  it directly when their `parse` rejects (a `once`/`optional` cardinality check, a converter, a
  pydantic `ValidationError`), gathering at the raise site what a `recover` policy needs: `field`
  names the request part that failed (the parameter name, or `None` for the body) and `cause` carries
  the underlying error as a first-class value, so a policy matches
  `case ExtractionError(cause=ValidationError())` for a 422 versus `case ExtractionError()` for a 400
  naming the `field`, without reaching into `__cause__`. The router wraps any stray, unattributed
  `ValueError` (from a custom extractor or an `into` factory) as a backstop. Making the boundary a
  single matchable type is what lets a plain `ValueError` raised deeper in a handler surface as a 500
  rather than masquerading as a client 400.

### Changed

- **`without-web`**: the extractor context type `Request` is renamed `RequestHead` and no longer
  carries the request body. `RequestHead` is exactly the parsed head an extractor reads (scope,
  path params, query params), mirroring `without-http`'s `ResponseHead`. It is now the top of a
  small context lattice each route builds concretely: `HttpRequestHead` (scope narrowed to
  `HttpScope`) for HTTP routes, `WebsocketRequestHead` (`WebsocketScope`) for websocket routes, and
  `BufferedRequest` (an `HttpRequestHead` plus the buffered `body`) for the buffered-HTTP path.
  Custom extractors typed on `Request` become `RequestHead` (or a narrower context if they read the
  concrete scope or body).
- **`without-web`**: `Extractor` gains a request-context type parameter, `Extractor[C, V]` (was
  `Extractor[V]`), contravariant in `C`. This makes the wrong extractor on the wrong route a *static*
  type error rather than a runtime guard: a `body` token (`Extractor[BufferedRequest, V]`) on a
  streaming or websocket route, or an `http_scope`/`websocket_scope` on the wrong protocol, no longer
  type-checks, so the former runtime `TypeError`/`ValueError` guards in `body`/`http_scope`/
  `websocket_scope`/`handle_stream`/`ws` are removed. Permissive tokens
  (`path_param`/`query_param`/`header_param`/`catch_all`) are `Extractor[RequestHead, V]` and still
  serve any route. A custom extractor annotated `Extractor[V]` must add its context:
  `Extractor[RequestHead, V]` for a scope/path/query read.
- **`without-web`**: query and header extractor `parse` callbacks now receive an immutable `tuple`
  of values rather than a `list` (`query_param`, `header_param`, and the `once`/`optional`
  adapters), and `RequestHead.query_params` values are tuples. The parsed head is a value no
  consumer can mutate out from under another (values over places); a `parse` typed on `list` must
  widen to `tuple`.
- **`without-core`** (imported as `without`): the `buffer` wiring connector is renamed `spool`, and its
  `maxsize` argument renamed `ahead`, so `spool(source, ahead=n)` reads as the read-ahead it is (drive a
  source ahead of its consumer through a bounded queue on a background task). Behavior is unchanged.

- **`without-web`**: routing and mounting reworked around self-contained route values. `mount(prefix,
  *middleware)` and `ws_mount(...)` are transforms that bake the prefix (and per-route middleware)
  into routes, reusable and usable as decorators; `delegate(prefix, app)` and `ws_delegate(...)`
  mount an opaque BYO app as a black box with the prefix-trimmed scope. This replaces the former
  `Mount`/`WebsocketMount` wrapper (a transparent sub-router is now just its baked routes), so a
  route carries its own full path — matching, OpenAPI, and reverse routing all read it directly, and
  a nested opaque app is trimmed by its full accumulated prefix by construction. Reverse routing is
  now the free `url_for` function rather than a `Router.url_for` method plus a `url_for()` extractor
  injected through `Match`.
- **`without-http`**: the client sends the request body concurrently with reading the response
  (consumer-driven duplex) instead of sending it whole first. A server can now answer early (a `413`,
  a redirect) without deadlocking a large upload, and a caller can drive genuine bidirectional
  streaming over HTTP/2: the request head is sent before the first body chunk is produced, so both a
  client-speaks-first duplex (feed a queue-backed body in reaction to the response) and a
  server-speaks-first one (let the server respond before any body chunk is ready) work. Connection
  teardown is a single release-exactly-once path shared by the background sender and the response body.
  Closing an early-answered HTTP/1.1 connection is now a bounded *lingering close* (a half-close `FIN`
  plus a short, fixed drain window, never draining to end-of-input) rather than a reset that could
  race ahead of and discard the response the server already sent, and the client stops streaming its
  body the moment the peer half-closes rather than writing on into a closing connection. See the new
  [Security](https://without.help/without-http/security/) page.

### Fixed

- **`without-http`**: an HTTP/1.1 connection is no longer dropped after every request whose app never
  read the body. `h11` advances the client's state only as events are *pulled*, and an ASGI app may
  ignore `receive` entirely, so a body-less `GET` left its `EndOfMessage` unread and the request was
  indistinguishable from a peer still owing a body: it failed the keep-alive check and the connection
  closed. That hit any app that skips the body (FastAPI, on a request with no body parameter) on every
  request, and under load surfaced as a small fraction of requests never answered, the pooled-connection
  race of a client writing into a connection the server was concurrently closing. The events the app
  left unread are now consumed from `h11`'s buffer once it responds. Only *buffered* bytes count: a
  `NEED_DATA` means the body genuinely has not arrived, so an early response to an in-flight body still
  correctly declines reuse and takes the lingering close.

- **`without-asgi`**: `make_asgi_app` now closes the inbound stream when a connection handler exits,
  so a handler that abandons the request body early (reads part of it, then returns) has the inbound
  generator's `finally` run deterministically instead of leaving it suspended for garbage collection.
  This is the server-side mirror of the client folding connection release into its response-body
  generator; the handler's inbound stream is wrapped in `aclosing`, covering both the HTTP and
  WebSocket paths.

## 0.0.1

### Added

- **`without-core`** (imported as `without`): the narrow-waist core. The `Stream` / `Processor` / `Context`
  contracts, the builders (`from_map`, `from_scan`, `from_sink`, `from_fold`, and
  the polarity-dual predicate filters `from_selector` / `from_filter`), the wiring
  connectors (`compose`, which also composes a processor onto a terminal `Sink`;
  `tee`, its terminal fan-out counterpart, splitting a stream across several `Sink`
  branches so a shared prefix runs once; `sample`, `stream_from_iterable`,
  `stream_from_queue`, `collect`, `buffer`, `stack`), and the `with`-scoped task
  helpers
  (`background_task`, `limit_concurrency`, `sleep_forever`, `cancel_futures`,
  `as_async_iterator`).
- **`without-env`**: a static `Context` loaded once from environment variables
  with `pydantic-settings`.
- **`without-configmap`**: a behavior source backed by a Kubernetes ConfigMap
  mount, reloaded with `watchfiles` (watches the mount directory to catch the
  atomic `..data` symlink swap).
- **`without-asgi`**: adapters between an ASGI app's `receive`/`send` and typed
  event streams, complete in both the app and server directions, plus
  `make_asgi_app` and the unopinionated routing/middleware vocabulary.
- **`without-web`**: an opinionated HTTP/WebSocket router with trie matching,
  typed path parameters, converters, extractors, 405-vs-404, mounting, scoped
  middleware, exception handlers, and structure-recovered OpenAPI.
- **`without-http`**: an `asyncio` ASGI server and connection-pooling HTTP client
  built on the sans-IO `h11`/`h2`/`wsproto` state machines, serving HTTP/1.1,
  HTTP/2, and WebSockets (over the HTTP/1.1 upgrade), with TLS, keep-alive,
  streaming and buffered bodies, trailers, and client middleware.
- **`without-dag`**: bounded-concurrency execution of DAG-shaped async workflows,
  a typed `Graph` builder, and a single-input `CompiledGraph` that lifts straight
  into a `Processor` via `from_map`.
- **`without-logging`**: a logging pipeline. Stdlib log records parsed into
  immutable `Record` values at a `capture` boundary (stdlib as a one-way source),
  the message resolved and any exception captured as a structured
  `TracebackException` at that edge (no live traceback carried downstream, and its
  formatting left to the app), filtered with the core `from_selector` (plus the
  `at_least` level predicate) and enriched with `add_fields`, drained to a sink the
  app owns (or several at once, each with its own tail, through the core `tee`).
  Per-call-site context binds at the edge with the scoped `bind(**fields)` context
  manager and the `merge_context` `Record -> Record` enrichment composed into the
  default parser (the structlog-style `bind_contextvars` equivalent), since the
  pipeline runs off the caller's task and cannot recover it. Optional opt-in
  renderers `render_json` (fields flat) and `render_console` (human line) cover the
  common encodings without the core forcing one, with
  the timestamp and exception encodings injected: `exception_to_dict` (structured
  frames) or `exception_to_text` (flat traceback), and `iso_timestamp` by default.
  `offload` bridges a
  blocking worker onto a dedicated thread (delivering items in bursts, so the
  worker flushes when it catches up, no per-write thread hop) so file I/O stays off
  the event loop. Destination-shaped writers take strings (render a `Record` to text
  with a `from_map(Record -> str)` in front) and own the newline framing:
  `to_rotating_file` owns the byte count and clock, rotating on any combination of
  `max_bytes` (size), `max_age` (relative interval), and `schedule` (absolute
  wall-clock boundaries, built from times of day with `at_times`); `to_stream` writes
  to a caller-owned text stream (`sys.stderr`, a socket) without closing it.
- Documentation site (mkdocs-material + mkdocstrings): narrative guides, an API
  reference recovered from the source docstrings, and a package dependency graph
  derived from the workspace `pyproject.toml` files.
