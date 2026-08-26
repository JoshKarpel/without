# without-async

The asyncio primitives the rest of the family is built from: scoped background
tasks, bounded concurrency, an optional timeout, and the duration counts that
cross an integer boundary. See the
[`without_async` API reference](reference.md) for the full surface.

## What is in here, and how you can tell

Every signature in this package mentions standard library types and nothing
else. That is the membership rule, and it is the whole reason the package
exists: a consumer that wants a scoped background task or an exact millisecond
count takes these primitives without taking the stream vocabulary along with
them. `without-durability-sqlite` reaches for `Milliseconds` to set a
`busy_timeout` and never touches a `Stream`; under one package it would have
declared a dependency on the substrate to say so.

It sits at the bottom of the
[dependency graph](../architecture/package-graph.md) and imports no other
`without` package, which is what makes it a layer the rest can be written against.
It has no third-party dependencies either, though that is where things stand
rather than a promise: async helpers have not needed one yet, and if one turned
out to be fundamental it would be taken on the same terms as anywhere else (see
the [Philosophy](../philosophy.md) on a dependency being a choice, so take only
the ones that aren't).

The signature rule is also what keeps the package from becoming the place things
land because it is already there. "Utilities" names no contents and admits
anything, so nothing can be argued out of it; "only the standard library appears
in the signature" is decidable by reading one line, which gives a reviewer
something to point at.

Where the name over-reaches: `Seconds` and `Milliseconds` are not asyncio at all,
and `async` does not cover them. They stay here rather than in a package of their
own because every duration either type is asked to carry is one something waits
out (a reconnect delay, a keepalive probe, a lock acquisition), which is the same
subject `timeout` and `sleep_forever` have. That is a reason, not an exact fit,
and the fit is what a third name would have bought for a ninety-line
distribution.

## Tasks (`without_async.tasks`)

The async task helpers: `sleep_forever`; the `with`-scoped `background_task`
(starts a task on entry, cancels-then-awaits it on exit, so nothing leaks);
`timeout`, a `timedelta | None`-typed wrapper over `asyncio.timeout` that models
"no limit" as `None` (an always-open context) rather than a sentinel float;
`limit_concurrency`, a bounded-concurrency driver that pulls work from a source
only while below the limit (so a lazy source is never advanced past it); and its
building blocks `cancel_futures` (cancel a set, then await them all) and
`as_async_iterator` (normalize a sync or async iterable into one async iterator).

`background_task` is what the substrate's own behavior edge runs on:
[`sample`](../without-streams/index.md#wiring-without_streamswiring) keeps its
held value current inside one, which is the sense in which this package sits
*below* streams rather than beside them.

## Durations that cross an integer boundary (`without_async.durations`)

A `timedelta` is the right type for a duration everywhere in this project,
because it names its unit and nothing downstream has to guess whether a bare
number meant seconds or milliseconds. What it cannot say is that a duration
*survives* the boundary it is about to cross. A TCP keepalive knob carries whole
seconds, an SSE `retry:` line carries whole milliseconds, SQLite's `busy_timeout`
carries whole milliseconds: each one truncates whatever it is handed, and
truncation is at its worst where it is least visible. Half a millisecond of
`retry:` becomes `retry: 0`, which does not mean "almost no wait", it means
"reconnect immediately".

`Seconds` and `Milliseconds` are what such a parameter declares instead, and what
makes them worth having is what they *cannot* hold:

```python
tcp_keepalive(idle=Seconds(60), interval=Seconds(10))
yield Retry(Milliseconds(30_000))
```

Each is a count, not a duration, so there is no argument to either constructor
that names a finer unit. `duration` is the `timedelta` back out, for arithmetic
and for anything taking a plain duration, and `of` is the one way in from one:

```python
Seconds.of(settings.keepalive_idle)  # raises on a duration finer than a second
```

That is the only place the question "does this divide?" is ever asked, which is
the point: past construction there is nothing left for a boundary to check, and
no truncation left for it to do.

## Testing (`without_async.testing`)

Two deterministic single-turn helpers, for the case where a test waits on code
that offers no signal of its own. `yield_once` gives a just-scheduled task
exactly one event-loop turn, which is what makes "it is still pending" a
meaningful assertion rather than a race. `resolved_next_turn` resolves a future
on the next turn, so a step under test genuinely suspends and resumes the way it
would around real contained I/O.

Both are for the narrow case where the code being waited on is not yours to
instrument. Where a real signal exists (an `asyncio.Event` a worker sets,
`Context.updated()`, a stream driven to completion), wait on that instead:
yielding once and hoping is a race however many times it is repeated.

## What is deliberately absent

A task group, a nursery, a supervisor, a retry helper, a rate limiter. Each is a
policy the layer above should own, and each would put a decision in the one
package every other package inherits. What is here is the set the workspace's
own code needed, and it grows on the same evidence.
