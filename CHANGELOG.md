# Changelog

## 0.0.4

### Added

- **`without-http`**: response decompression as opt-in middleware. `decompress()` offers
  `accept-encoding` outbound and wraps the response body in an incremental decoder inbound, so a
  streamed body decodes chunk by chunk and trailers pass through untouched. It is middleware rather
  than pool behavior because the transport must never silently rewrite bytes: a caller that wants
  the wire encoding reads the undecorated client. The coding table is the argument
  (`DEFAULT_DECOMPRESSORS`, gzip and zstd from the stdlib and brotli from the bundled bindings), and
  the `accept-encoding` offer is *derived from its keys*, so what is advertised and what can be
  decoded cannot disagree; registering a coding this package does not ship is one entry
  (`decompress({**DEFAULT_DECOMPRESSORS, b"lzma": make_lzma})`) rather than a fork. The decoded
  response is self-consistent: `content-encoding` and `content-length` described the *encoded* body,
  so both leave the head instead of contradicting the bytes the stream now yields, an unknown or
  stacked coding passes through whole, and a truncated compressed stream raises `ConnectionError`
  rather than passing a prefix off as the whole body. This is also how the no-unbidden-headers
  position holds rather than bends: composing the middleware is how a client opts into offering
  `accept-encoding` at all.
- **`without-http`**: request compression, the same mechanism pointed the other way. `compressing`
  is the middleware over any coding and a `Compressor` factory, with `gzip_compress`,
  `zstd_compress`, and `brotli_compress` as the three that ship. Bodies compress as they stream, so
  a large upload is never buffered whole, and per-call composition means one client can send
  compressed to a peer that wants it and plain to one that does not.
- **`without-http`**: `basic_auth(username, password)` and `bearer_auth(token)`. Both are
  `add_headers` one-liners, which is the point: the challenge-free schemes need no new mechanism,
  and naming them saves every caller from re-deriving the base64 and the scheme token. Digest is
  deliberately still absent, because answering a challenge is a looping middleware rather than a
  header.
- **`without-http`**: `user_agent(*segments)`, and `USER_AGENT` as the library's own
  `without-http/<version>` identity, which is what it sends when given no segments. Requests still
  say exactly what the caller said; this is how a caller opts into an identity for the peers that
  vary on one (and the ones, like the GitHub API, that refuse a request without it).
- **`without-http`**: Happy Eyeballs on by default, and resolution as an injectable step.
  `tcp_connect(resolve=..., happy_eyeballs_delay=...)` builds the pool's default `Connect`: it
  races address families per [RFC 8305](https://datatracker.ietf.org/doc/html/rfc8305) through
  [aiohappyeyeballs](https://github.com/aio-libs/aiohappyeyeballs), so a dual-stack host with one
  black-holed family costs a 250 ms delay rather than a full connect timeout. Splitting `Resolve`
  out is what makes DNS policy the caller's: a cache, DNS-over-HTTPS, or a test's canned addresses
  swap in without touching how the winning address is connected. The race drives plain
  `loop.sock_connect`, so it behaves the same on any event loop, where asyncio's own racing is fused
  to its own resolution.
- **`without-http`**: the server supplies the ASGI
  [`tls` extension](https://asgi.readthedocs.io/en/latest/specs/tls.html) on every TLS scope, HTTP,
  HTTP/2, and WebSocket alike, so an mTLS deployment's client certificate reaches the handler as a
  PEM chain with its subject as an RFC 4514 distinguished name, and `parse_tls` finally has a
  producer inside this stack rather than only a parser. The facts are read once per connection off
  the finished handshake rather than per request, since a completed handshake does not change under
  the connection. `server_cert` and `cipher_suite` are `None`, which the spec permits and which is a
  CPython limit rather than a shortcut: an `ssl.SSLContext` never exposes the certificate it loaded,
  and `SSLObject.cipher()` reports a suite by name with no IANA identifier. `client_cert_error` is
  `None` because a certificate that fails verification fails the handshake, so no scope is ever
  built for it.
- **`without-http`**: two bounds on the request *head*, which was previously whatever h11 and h2
  chose. They are separate knobs because the protocols measure different things:
  `max_incomplete_event_bytes` is how much of an unfinished HTTP/1.1 event (a request line and its
  headers, a chunk header) may accumulate before the parse is abandoned with a `431`, and
  `max_header_list_bytes` is advertised over HTTP/2 as `MAX_HEADER_LIST_SIZE`, bounding an
  *uncompressed* header list, which is what makes it a defense against an hpack bomb. Each defaults
  to its protocol library's own default (16 KiB and 64 KiB), so the numbers differ; collapsing them
  into one knob would have silently retightened or loosened one protocol. Both are on `serving`,
  `served_pipe`, and `loopback_client`, like every other per-connection bound.
- **`without-http`**: a served scope advertises the extensions its wire layer implements, where it
  previously carried none at all: `http.response.early_hint` on HTTP scopes,
  `websocket.http.response` on WebSocket scopes, and `tls` on both over TLS. A third-party ASGI
  framework that checks the scope before using an extension, as the spec tells it to, now finds
  them, where before it correctly concluded there were none; a `without-asgi` app speaks the typed
  vocabulary directly and never had to check. The in-memory `asgi_client` already advertised
  `http.response.trailers`, so the wire scopes are what changed.
- **`without-asgi`**: `form_content` and `multipart_content`, joining `json_content` as producers of
  the same `Content` value, plus `FilePart` and `StreamingContent`. A multipart body streams its
  file parts rather than buffering them, which is why it is a `StreamingContent`: the shape follows
  the size of what it carries rather than being uniform for its own sake. Both work as a request
  body through `without-http`'s `request` and as a response body, since `Content` is the shared
  vocabulary of the package both sides depend on.
- Documentation: [Alternatives](https://without.help/without-http/alternatives/), a
  feature-by-feature register of `without-http` against httpx, aiohttp, and niquests on the client
  side, and against uvicorn, hypercorn, and granian on the server side. Every cell cites its
  source, gaps are marked by *how they close* (a composition against an interface that already
  ships, genuinely new mechanism, or a stated position with its cost named), and open gaps link
  the issue tracking them.
  It is a roadmap as much as a comparison, and it is what drove most of the additions above.

## 0.0.3

### Added

- **`without-dag`**: resuming a graph from a checkpoint. `run(...)` and `run.stream(...)` take a
  `checkpoint` of `{node key: result}`, the same mapping `stream` emits, and a node named in it is
  not run: its result is taken as given and fed to its dependents, so a run picks up where an
  interrupted one stopped and a checkpoint covering the whole graph performs no effects at all. The
  execution interface already treated a pre-supplied key as done; what was missing was a key worth
  storing, so `node` now takes one as its first argument (`graph.node("charged", charge, order)`)
  and `NodeKey` is a `str`. A name chosen in the source means the same thing on the other side of a
  crash, where an `object()` minted at build time does not, and it must be distinct from every other
  key in the graph (entries are keyed by position, `input:0`). A checkpoint key that names no node
  is rejected rather than ignored, since that is the shape of one written by a different version of
  the graph. `stream` being pull-driven makes the store write a barrier: nothing downstream of a
  completed step starts until the consumer asks for the next result.
- **`without-durability`** (new package): durable workflows over a checkpoint any process can read.
  Two mechanisms spend the one checkpoint. `run_durably` drives a `without-dag`
  `CompiledGraph`, recording each `(node key, result)` before pulling the next, so a resumed run
  re-enters only what had not finished. A saga is not a third mechanism: a rollback is another
  graph, so compensating is an `except Exception` around that call and a second call to it under
  an id the application chose, which leaves the library reserving no name in anyone else's
  namespace (the guide writes the eight lines out). `stepwise` needs no graph: a workflow is an ordinary async function whose
  effects are named (`await run.step("charged", charge, as_text)`), resuming calls it again, and
  each step hands back what is recorded. It asks one thing in return, because the code *between*
  steps re-runs: effects live in steps, the code around them is pure, which Temporal and DBOS state
  as workflow determinism. Keying by name rather than by position keeps that mild, since reordering
  or inserting a step changes nothing, and it buys two shapes a fixed graph cannot express: a
  fan-out whose width comes from a step's *result*, one key per item so a crash resumes item by
  item, and a step that cannot finish now stopping the pass rather than blocking, which is how a
  settlement window (`run.sleep`) and a human approval (`run.awaiting`) become ordinary lines.
  `resume` reports that as an `Outcome` (`Completed`, `Sleeping`, or `Waiting`) rather than
  raising, so a driver matches over three values and closes with `assert_never` instead of writing
  an `except` no type checker can call incomplete; the worker does exactly that. Inside a workflow
  a suspension is still an exception (`Suspended`, and its `ScheduledWakeup`/`InputNeeded` cases),
  because that is the only way to stop in the middle of straight-line code, and it descends from
  `BaseException` so an `except Exception` around a step cannot swallow it.
- **`without-durability`**: the `Checkpointer`, `Scheduler`, and `Durable` interfaces, which are where
  the guarantee lives. A protocol of `load` and `record` is too weak to run a workflow safely at any
  scale: it cannot say "only if nobody else is running this" or "only if I am still the one who may
  write", so two wakeups for one workflow (which the submit-then-confirm flow produces every time)
  run two passes that both find a step unrecorded and both perform its effect. `claim` takes the
  right to run a pass and every write carries the `Pass` it was granted, so "you cannot write
  without holding the workflow" is structural rather than remembered. The token is a *fencing*
  number minted by the store, because a lease alone is not exclusion: a process that stalls past its
  lease still believes it holds the workflow, and only the store knows better, so a superseded
  write is refused (`Fenced`). `record` never overwrites a recorded step and returns a `Recorded`,
  the value stored after the call *and* whether it is this pass's own, which only the store can say
  since a result crosses the codec both ways. `supply` is the unclaimed half, for values arriving
  from outside a pass, which keeps first-writer-wins without making an approval fail because a
  worker is mid-pass. `Durable` bundles the two stores and names the transitions crossing them, so
  `arrive(workflow, key, value)` is one call rather than two writes in an order the caller has to
  get right: `SplitDurable` composes any two stores and records before it queues, where a store over
  one datastore commits both at once.
- **`without-durability`**: `Run.transact`, which performs an effect and records it in one commit,
  making that step exactly-once rather than at-least-once. `step` runs an effect and then writes the
  record, so a crash between them repeats it; `transact` hands the store an effect it can perform
  itself, so there is no in-between. That it works on Redis is worth stating, because the usual
  framing (that exactly-once needs Postgres) is wrong about why: a Lua script is an atomic commit
  over Redis data, and the real constraint is that you can only transact within one datastore, so
  Postgres wins only for effects that live in that Postgres. `Checkpointer` is therefore generic
  over the effect type a store can commit, defaulting to `Never` so a store with nothing to offer
  makes `transact` uncallable rather than absent. What "one datastore" means was measured rather
  than recalled: Redis Cluster rejects a script whose declared keys span slots (`CROSSSLOT`) and
  kills one reaching an undeclared non-local key partway through, so a cross-node atomic write is
  unavailable rather than expensive; sharded Postgres instead escalates silently to a two-phase
  commit under Citus. The escape is one idea on both sides, Redis's hash tag and co-location by
  workflow id, and sharing a pool is its necessary half rather than its sufficient one.
- **`without-durability`**: `work(durable, body)`, a queue worker over the same interfaces, and `passes`,
  `ready`, and `waking` as the `Sink`-over-`Stream` pieces it composes. A worker runs up to `POOL`
  passes at once through `without`'s `limit_concurrency`, and every pull takes exactly one delivery
  (a reclaimed one if any workflow was abandoned, otherwise a fresh read), so it holds precisely as
  many wakeups as it is working on and stops reading at capacity. It matches on the pass's
  `Outcome`, closed with `assert_never`: a `Sleeping` is scheduled, a `Waiting` is left for whoever
  owes the value to queue, a `Completed` needs nothing, and nothing polls a workflow to ask
  whether it can proceed. The acknowledgement lands after the pass on every path but cancellation,
  so a worker that dies mid-pass leaves its delivery to be reclaimed. How long a pass may honestly
  take is one number rather than two, and it lives on the scheduler
  (`PostgresScheduler(pool=pool, lease=...)`): `work` reads it and claims the workflow for exactly
  as long, because the two windows disagreeing fails quietly. The rest of the loop's timings are
  arguments to `work` (`tick`, `within`, `contended`, `limit`), and every duration across the stores
  and the worker is refused at construction unless it is positive.
- **`without-durability-redis`** (new package): both interfaces over Redis, where each guarantee is a
  small Lua script, for the reason `wake_due` already was: checking whether a workflow is free and
  taking it, or checking a token and applying the write it guards, are only correct as a single
  step. A workflow's two keys are hash-tagged so they land on one slot, and `LuaEffect` is what this
  store can commit alongside a record. The fencing token is `max(now_ms, previous + 1)`, a hybrid
  logical clock rather than a counter: the checkpoint and the claim expire together, so a counter
  would hand a reused id token 1 while a pass stalled since before the expiry still held token 3.
  Two queues ship. `RedisStreamScheduler` is a stream read as a consumer group beside a
  deadline-scored sorted set, which buys a blocking read; a stream rather than a list because a list
  loses work, since a delivery stays pending until acknowledged. `RedisSetScheduler` is one sorted
  set scored by when each workflow becomes visible, which makes the timer, the consumer group, the
  pending list, and the trimmer all disappear, and costs the blocking read. Holding each workflow
  once is its catch, since a wakeup landing mid-pass has nowhere to go but on top of the entry that
  pass is holding, so the score a pass took *is* its receipt and finishing is conditional on it
  being unchanged. `trim` bounds the stream with `XTRIM ... MAXLEN 0 ACKED` (Redis 8.2+), so the
  *server* decides what every group has finished with; it refuses a stream with no groups, where
  `ACKED` has no effect and the trim would degrade to deleting a queue nobody has read yet.
- **`without-durability-postgres`** (new package): both interfaces over three tables in one database,
  with `SqlEffect` as the effect type `transact` takes there. It is the other half of the Redis
  store's argument, and what it shows is where the atomic unit came from: every write that had to be
  a Lua script is one statement or one transaction here, because SQL says "check this, then write
  that, and let nobody in between" by default. The claim is an upsert whose `DO UPDATE` carries a
  `WHERE` on the lease; `record` is a `FOR UPDATE` CTE over the claim row feeding an upsert, where
  the row lock is what makes the fence serialize against a claim in flight rather than read a stale
  snapshot; the queue takes with `FOR UPDATE SKIP LOCKED`, so several workers polling one table fan
  out instead of queueing on its head. Three live Redis questions do not arise: a workflow id is a
  query parameter rather than key structure, nothing expires so the fencing token can be a plain
  counter, and a default Postgres commits synchronously. `PostgresDurable` makes `arrive` one
  commit, which is what makes "no second system" a claim this can make. What it costs is that
  sweeping finished workflows becomes a job somebody writes, that `next_ready` still polls
  (`LISTEN`/`NOTIFY` would close that and does not yet), and that `migrate` is three
  `CREATE TABLE IF NOT EXISTS` under an advisory lock rather than a migration tool.
- **`without-durability-sqlite`** (new package): the same three tables over one file, and the
  smallest thing that meets every requirement the interface states, with no server and no third-party
  driver. `BEGIN IMMEDIATE` *is* the exclusion, so it needs neither Postgres's `FOR UPDATE` nor
  Redis's Lua, and because the datastore is a file there is nothing to co-locate, which is DBOS's
  guarantee for an application that never needed Postgres. Its effect type is a *synchronous*
  callback where the Postgres one is `async`, because the whole transaction runs on one worker
  thread. `connect` opens with `synchronous=FULL` rather than the usual `NORMAL`, since that trades
  away exactly the property the package exists for. Its scope is one machine, which is the
  deployment it is for rather than a defect, and it needs SQLite 3.42 or newer, which
  `requires-python` cannot express: on Linux `sqlite3` links whatever `libsqlite3` the distribution
  ships.
- **`without-durability`**: `CheckpointCodec`, the interface deciding what a step's result becomes in a
  store, with `JsonCodec` over the stdlib as every store's default. What a checkpoint is encoded as
  is a boundary decision, so it belongs to the application rather than to four stores answering it
  identically and wrongly for anyone whose steps return a domain value `json.dumps` has never heard
  of; swapping one in is now a constructor argument. It is one object rather than a pair of
  functions because both requirements are about the pair: `decode(encode(x))` MUST equal `x`, or a
  resumed pass reads something the first pass never wrote, and `encode` MUST be deterministic,
  because `record` decides who won a race by comparing encodings. `PostgresCheckpointer` narrows the
  choice to codecs producing JSON *text*, since that is what a `jsonb` column takes; keeping the
  column buys the indexing and the operators, and the codec still owns the value mapping.
  `MemoryCheckpointer` applies it too, which is the part that is easy to skip and is exactly what
  makes a double lie: a dict can hold a value directly, so encoding into it looks like ceremony, but
  then every property that depends on the round trip passes in the suite and fails in a deployment.
  It holds encoded values, so reading a checkpoint means `load`.
- **`without-durability`**: every durable read names its parser. `Run.step`, `Run.transact`, and
  `Run.awaiting` take a `parse: Callable[[object], T]` and return a `T` a function actually
  produced, where they previously cast. The cast was unsound on every path rather than only after
  a crash: a step hands back what the *store* holds, read through a codec, so one returning a tuple
  was handed a list on the pass that ran it while its signature still promised a tuple. The parsers
  were already there, wrapped around the call sites (`parse_items`, `parse_approver`); moving them
  inside means a step whose result is used unparsed is no longer expressible. The effect's own
  return type is deliberately *not* tied to the parser's, because what goes in and what comes out
  are related by encode-then-decode rather than by identity: `Run.sleep` records an ISO string and
  reads back a `datetime`, which is the ordinary case and not the exception.
- **`without-durability`**: `run_durably` refuses a node whose result does not survive its own
  store, on the pass that wrote it. It needs no per-node parser because it holds both values at
  once, what the node returned and what the store now has, so it verifies where `stepwise` has to
  parse. The check earns more here than a parser would: a graph feeds a node's result straight to
  its dependents, so without it they see a tuple on the pass that computed it and a list on the one
  that restored it, with no crash needed for the two to disagree. `without-dag` is untouched, and
  the split is the general rule rather than a convenience: verifying beats parsing whenever the
  caller still holds what it sent, and `Run.awaiting` is exactly the case that does not, since it
  reads a value another process wrote.
- **`without-durability`**: `Interruption`, a `BaseException` base for `Fenced`, `Contended`, and
  `Suspended`, for the reason `asyncio.CancelledError` has one. Each says something about whether
  *this pass* may continue rather than about the work, so an `except Exception` written to handle a
  declined gateway must not absorb one. The case that forced it is a saga, whose `except Exception`
  compensates on failure: a `Fenced` forward run is not a failure but a lost race, and a loser that
  unwound would refund a charge the winner is still building on. That the rule is carried by the
  exceptions' own shape matters more once the saga is application code rather than a shipped
  runner, since the `except` it has to survive is one somebody else wrote. The worker has a
  matching arm, treating a claim
  lost mid-pass as the deferral it already applies to a claim refused up front, rather than as a
  workflow that failed.
- **`without-durability`**: the two ways of waiting are separate types rather than one carrying a
  nullable deadline, on both sides of `resume`. Inside a pass, `Suspended` is the base of a
  `ScheduledWakeup` whose `due` is always present and an `InputNeeded` that carries none; coming
  back out, they are a `Sleeping` and a `Waiting`. It is the difference a driver has to branch on
  either way, so neither side makes it a field that is sometimes there.
- **`integration`**: `durable`, the deployment half of the durable-workflow work, which is what
  `without-durability` deliberately does not ship. An order fulfilment graph (charge and reserve
  concurrently, ship, render) and its compensating rollback; a payout workflow written as ordinary
  code, with a data-dependent fan-out, a settlement window, and a human approval; the body the
  worker runs; and an HTTP API in front of it whose three endpoints run no workflow, since
  submitting an order and confirming a payout are the same `arrive` call and the workflow id is the
  request's `Idempotency-Key`. `tests/durable/stores.py` builds one `Durable` per store and one
  suite runs the same saga, the same suspension, and the same API-plus-worker flow against all four,
  so "a workflow cannot tell which store it got" is a claim the suite makes rather than a page
  asserts. Those tests drive real servers: the `test` recipe starts the new `compose.yaml` with
  docker or podman, whichever it finds, hands pytest each published address, and takes the stack
  down from an exit trap. They carry a `compose` mark and skip where neither is installed.
- **`without`**: `ticks(every)`, a `Stream` of moments, one now and one every interval after. It is
  the clock as a source, so periodic work stops being a `while True` with a `sleep` buried in it
  and becomes a `Sink` that says only what happens per event, composed with a stream that says when.
  `waking` and `trimming` are both sinks over it now, which means the same code runs off a timer,
  off a queue an operator pokes, or off a fixed list of instants in a test. Each tick carries its
  own moment, so a consumer needs no clock of its own and a test controls time by choosing values.
  An interval that is not positive is refused, as `drive` refuses a `limit` below one: taken
  literally it is a loop that yields as fast as its sink can consume, which pins a core to do
  housekeeping, and a duration read from a setting that was never set is how one arrives.
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
- **`without-asgi`**: `Content`, a body paired with the headers that describe it, plus `json_content`
  and `Response.from_content`. Encoding a value produces two things that must travel together, the
  bytes and the `content-type` naming them, and every caller that separated them re-derived the same
  three lines: the app layer, the router's own tests, and every test that sent a JSON body each
  carried a private `json_response`. `Content` carries no policy, so `json_content` is one producer
  of it and a form or msgpack encoder is another, and the serializer stays an argument
  (`json_content(order, dumps=...)`) with the stdlib as the default, because a default should add no
  dependency. It is strict where JSON is (`allow_nan=False`, so a `NaN` fails at the sender) and
  leaves key order alone, since sorting is a policy some callers want and a cost every response
  would pay. `Response.from_content(status, content, headers=...)` layers the caller's headers over
  the content's, and `without-http`'s `request` takes the same value as a request body, which is why
  it lives in the package both sides already depend on. This walks back `without-web`'s "ships no
  `json_response`-style helper" stance on the narrow point of the *shape*: what a handler must not
  have imposed on it is the serializer, and that is still injected.
- **`without-http`**: `without_http.testing`, three more `Client`s that reach an app (or nothing)
  without binding a socket. `mock_client(handler)` answers from a function, which is the whole of
  mocking once a client is one, with `respond(...)` building the canned response.
  `asgi_client(app)` builds an `HttpScope` from each request and drives `app(scope, receive, send)`
  directly, streaming: the head returns the moment the app sends `http.response.start` and body
  chunks cross a one-slot queue, so duplex handlers are testable, and the app's lifespan runs for
  the block through the same `run_lifespan` a server uses (which `httpx.ASGITransport` leaves to
  the caller). Its scope advertises `http.response.trailers`, the one extension in-memory delivery
  can honestly offer, since a `ClientResponse` carries trailing blocks through to
  `read_with_trailers`, so an app that negotiates trailers takes that path here. `loopback_client(app)` is `serving` minus `asyncio.start_server`: the real
  `ConnectionPool` and the real server, wired to each other over `pipe()`, two cross-wired
  `StreamReader`s with genuine backpressure, so framing, keep-alive, HTTP/2 by prior knowledge, and
  the server's crash-to-`500` isolation all run with no port and no file descriptor. All three
  speak plain ASGI and plain request values, so they drive a FastAPI or Starlette app as readily as
  a `without` one, and `base_url(...)` composes on when a test would rather write `"/items"`.
  Below the clients, `served_pipe(app, ...)` hands over the client end of a `pipe()` with the
  server on the other, for a conformance test that writes frames rather than requests (a malformed
  request line, an h2 preface followed by an illegal frame, a reset flood); it runs the lifespan and
  cancels the connection on exit as `serving` does, and the server presents as `SERVER_ADDRESS`
  (with `AUTHORITY` spelling the `host:port` bytes such a test writes into `:authority` or `Host`).
  `without-http`'s own HTTP/1.1 and HTTP/2 server suites run on it, leaving a bound socket to the
  tests that need what only a kernel provides: TLS, socket options, and a third-party client.

### Changed

- **`without-http`**: a client *is* a function from a request to a response. The type formerly
  called `ClientExchange` is now `Client`, `ConnectionPool` satisfies it by being callable
  (`await pool(request)`), and the caller-facing surface is a free `request(client, method, url,
  ...)` context manager rather than a method on the pool. Everything the pool held that was not
  about connections has left it: `middleware` is gone, because a decorated client is just
  `stack(add_headers(...), cookies(jar))(pool)`, and `timeout` is gone, because a deadline belongs
  to the caller rather than to the connection and now rides on `ClientRequest.timeout` (set it per
  call with `request(..., timeout=...)`, or across a client with the new `deadline(...)`
  middleware, which fills in only a request that states no budget of its own). What is left on the
  pool is connections: TLS, HTTP/2, the per-host bounds, socket options, and the new injectable
  `connect`, which is the one step that touches the network. Migration is mechanical:
  `pool.request(m, u, ...)` becomes `request(pool, m, u, ...)`, `ConnectionPool(middleware=mw)`
  becomes composing `mw(pool)` where the client is built, and `ConnectionPool(timeout=t)` becomes
  `deadline(t)(pool)`.
- **`without-asgi`**: a scope whose `asgi` key (or `asgi["version"]`) is missing parses as version
  `"2.0"` rather than raising `KeyError`, which is what the spec tells applications to assume.
  Real producers omit it: starlette's `TestClient` sends a lifespan scope with no `asgi` key at
  all, and a `without` app driven through it previously crashed on the first request.
- **`without`**: the module holding the substrate is `without.interfaces` rather than
  `without.contracts`. Every name is re-exported from the package's top-level `__init__`, so
  `from without import Processor` is unaffected and only a direct submodule import has to change.
  The core called this idea a *contract* while the prose about it called it an *interface*, and
  one word is worth more than the shade of meaning each carried.
- **`without-dag`**: `Graph.node` takes the node's key as its first argument
  (`graph.node("charged", charge, order)`), and `NodeKey` is a `str` rather than any `Hashable`.
  A key was previously an `object()` the builder minted, which is unique but means nothing on the
  other side of a crash; a name chosen in the source is what lets a run's `(key, result)` pairs be
  stored and handed back as a `checkpoint`, so the key had to become something a store can hold and
  a human can recognise in one. It must be distinct from every other key in the graph, and entries
  are keyed by position (`input:0`), which a node may not take. Existing graphs add a name per
  `node` call; nothing else about the builder changes.

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

- **`without-durability-sqlite`**: `Database.aclose()`, and closing the connection any other way is
  now a documented mistake. `sqlite3.close()` frees the connection and finalizes its statements
  under any thread still executing one, which segfaults the process rather than raising, and
  `Database.run` makes that reachable by design: a cancelled caller unwinds immediately while its
  thread runs on, precisely so the connection is not handed to the next caller mid-transaction. A
  shutdown that follows a cancellation therefore closed on top of a statement in flight. It
  surfaced as an intermittently dying test worker, roughly one run in twenty-five, whenever a
  workflow's worker task was cancelled just before its store was torn down. `aclose` takes the same
  guard `run` releases from the thread, so the close waits the statement out; the guard is released
  afterwards, so a `run` arriving later fails loudly on a closed connection. The close itself runs
  on a thread like every other driver call, since under WAL it performs the final checkpoint (and,
  with `synchronous=FULL`, an fsync), which is blocking disk I/O the event loop should not carry.

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
