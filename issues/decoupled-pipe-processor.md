---
title: Add a decoupled M:N processor (a `pipe` combinator)
labels: [without]
---

## Summary

There is no first-class way to write a processor whose input consumption and
output production advance at *different rates*, joined by a buffer. A `pipe`
combinator would fill the M:N "independent cadence" tier the current builders and
connectors do not cover.

## Package(s)

`without` (core).

## Notes

Sketch (from the original design note): `pipe(ingest, buffer=...)` runs `ingest`
as a background task that drains the input stream and calls an `emit` to push
outputs, while the processor yields from the buffer independently, so input and
output cadences decouple. It belongs next to the other wiring connectors and
follows the same discipline they do: a `finally: queue.shutdown()` ends the
downstream stream, and `buffer` is the same backpressure knob (`1` = O(1) memory
with the slowest side gating, larger = more slack, `0` = unbounded).

Open questions to settle: the name, the exact shape of `ingest` (one coroutine vs.
two over shared local state, keeping mutation from escaping per values-over-places),
and whether it lands in core or as a recipe. Relates to intra-request concurrency,
which the toy examples do not yet exercise.
