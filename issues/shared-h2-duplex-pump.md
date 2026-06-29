---
title: Extract a shared h2 duplex pump abstraction (server + client)
labels: [without-http]
---

## Summary

The h2 connection pump (read loop, per-stream queues, flow control) is mirror-
symmetric between the server (`_serve_h2_connection`) and the client's
`_Http2Connection`. Factor a shared duplex-pump primitive both can take,
parameterized by initiator side, stream-id allocation, and the two flow-control
directions. The highest-value shell-sharing candidate.

## Package(s)

`without-http`.

## Notes

Both are "read bytes, feed a shared `h2.Connection` under a lock, dispatch events
to per-stream queues, drain per-stream send under flow control." Caveat: the
flow-control wakeup invariants differ subtly enough to merge carefully, not assume
it is free. First step is to sketch the one abstraction's signature and decide
whether they actually collapse or the invariant differences make it not worth it.
