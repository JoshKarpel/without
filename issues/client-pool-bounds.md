---
title: Bound the connection pool per host
labels: [without-http]
---

## Summary

`ConnectionPool` is unbounded for HTTP/1.1 on two axes: peak connections per
origin (a fresh socket opens whenever no idle one is free, so N concurrent
requests open N sockets) and idle retention per origin (the idle list grows
without limit). Add two httpx-shaped knobs.

## Package(s)

`without-http` (client).

## Notes

Two knobs, differing sharply in cost:

- **Idle cap** (`max_keepalive_per_host`): cheap and pure. On return, `aclose()`
  instead of pooling once the idle list is full. Bounds fd/memory retention with
  no new concurrency machinery.
- **Peak cap** (`max_connections_per_host`): the real feature. A per-origin
  `asyncio.Semaphore` acquired before checkout/open and released when the
  connection is returned or closed, so a request *waits* when the origin is
  saturated. The delicate part is bracketing the whole borrow so the permit is
  released exactly once even on partial-read abort or cancellation; this dovetails
  with the existing single release point (the body stream's `finally`). Optional
  acquire timeout and fairness are follow-ons.

h2 is fine on connection count (one multiplexed connection per origin) but does
not gate against the server's `SETTINGS_MAX_CONCURRENT_STREAMS`; that h2 stream
limit is tied to the consumer-driven-duplex item. Build as one focused change;
lean toward both knobs, since a client hitting an origin usually wants a peak cap.
