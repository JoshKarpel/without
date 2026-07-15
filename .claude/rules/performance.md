---
paths:
  - "**/*.py"
---

# Performance on the Hot Path

`without` is a narrow waist: every request, every socket read, and every response
crosses the same handful of functions. Cost paid *per crossing* multiplies by
traffic, so these are design-time habits for code on those paths, not
micro-optimizations to sprinkle on afterward. When a change is meant to be faster,
measure it (the `python-profiling` skill has the tools); don't assert a speedup you
haven't observed.

## Fast-path the disabled default

When a wrapper exists only to support an *optional* feature, skip it entirely when
the feature is off, rather than paying its setup on every call. An
`@asynccontextmanager` timeout, a running byte tally, a size check: each is dead
weight when its bound is `None`. Branch on the disabling value first and take the
bare path.

```python
if idle_timeout is None:
    return await reader.read(_BUFFER)
async with timeout(idle_timeout):
    return await reader.read(_BUFFER)
```

The default configuration is what most deployments run, so the cheap path should be
the default path.

## Hand immutable values through, don't rebuild them

If you already hold a value in the shape a boundary accepts, pass it; don't
reconstruct it element by element. A `tuple[tuple[bytes, bytes], ...]` of headers is
already a valid ASGI headers iterable, so `"headers": headers` beats
`[[k, v] for k, v in headers]` and allocates nothing. This is values-over-places on
a hot path: an immutable value can be shared with any consumer without a defensive
copy, because none of them can mutate it. Rebuild only when the target type
genuinely differs, and prefer the shape that lets you skip the rebuild.

## Do shared work once, not once per consumer

When several consumers on one request need the same derived value, compute it once
where the request is assembled and let each consumer read the result, rather than
each recomputing it from the raw input. Parsing the query string once when a
`Request` is built (so every `query_param` reads the parsed mapping) turns N parses
into one, where N is the number of extractors a handler declares. The multiplier is
per-consumer, so the win grows with how many read the value.

## Build bytes and strings by join, not repeated growth

To assemble a buffer from chunks, collect the pieces and join once: the join sizes
the result exactly and copies each byte a single time. `bytearray.extend` in a loop
followed by `bytes(...)` reallocates as the buffer grows and then copies the whole
thing again, moving every byte roughly twice.

```python
chunks: list[bytes] = []
async for event in events:
    chunks.append(event.body)
return b"".join(chunks)
```

## Don't signal across threads when you're already on the right one

Cross-thread handoffs cost a wakeup. `loop.call_soon_threadsafe` writes the event
loop's self-pipe to wake it; `loop.call_soon` does not. If the caller is already on
the loop thread, take the cheap one. Capture the loop's thread id where you know you
are on it (at construction) and branch on `threading.get_ident()`, falling back to
the threadsafe call only for genuinely off-thread callers.

## Watch for accidental quadratic work in schedulers

Re-registering interest in every in-flight item on each step is O(N²) over the
lifetime of an N-wide fan-out. `asyncio.wait(pending, return_when=FIRST_COMPLETED)`
in a loop adds and removes a done-callback on *all* pending futures every time one
finishes. Attach the callback once at spawn and drain completions off an
`asyncio.Queue` instead, so each completion is handled in amortized O(1).

## Keep blocking syscalls off the event loop

A blocking call on the loop thread stalls *every* coroutine, because the loop is
single-threaded. Releasing the GIL does not save you: it lets other OS threads run,
but the loop's own thread is still parked in the syscall, so no coroutine advances.
Regular-file operations (`open`, `stat`, `read`) can block on a slow or networked
filesystem, so offload them with `asyncio.to_thread`. Offload one operation per hop
so a pool thread is held only briefly, rather than pinning a thread for a whole
transfer and capping concurrency at the pool size.
