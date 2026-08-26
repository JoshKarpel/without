# without-async

The asyncio primitives the rest of `without` is built from. It sits below the
substrate and speaks only the standard library: every signature here mentions
`asyncio`, `collections.abc`, and `datetime` types and nothing else, which is
what lets a package take it without taking the stream vocabulary along.

- `background_task` starts a task on entry and cancels-then-awaits it on exit,
  so nothing leaks past the `with` block.
- `timeout` models "no limit" as `None` rather than as a sentinel float.
- `limit_concurrency` drives work from a source only while below the limit, so a
  lazy source is never advanced past it, over `cancel_futures` and
  `as_async_iterator`.
- `Seconds` and `Milliseconds` are counts, not durations: a boundary that
  carries whole units declares one instead of a `timedelta` it would silently
  truncate. These are the two symbols the package name does not cover; they sit
  here because every duration they carry is one something waits out.
- `without_async.testing` holds `yield_once` and `resolved_next_turn`, the two
  deterministic single-turn helpers a test reaches for when the code it waits on
  offers no signal of its own.

See the [`without-async` guide](https://without.help/without-async/)
(with the [API reference](https://without.help/without-async/reference/))
for the full surface.
