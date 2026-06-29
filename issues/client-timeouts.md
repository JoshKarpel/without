---
title: Add granular client timeouts (connect, read, write, pool)
labels: [without-http]
---

## Summary

The HTTP client has no request timeouts: a hung connect, a stalled server, or a
saturated pool blocks forever. Add granular, httpx-shaped timeouts so a caller can
bound each phase of a request independently.

## Package(s)

`without-http` (client). The TLS-only `ssl_handshake_timeout` /
`ssl_shutdown_timeout` on the server are unrelated; this is client request timing.

## Notes

Follow httpx's four-axis model rather than one blunt deadline, since the phases
fail for different reasons and want different limits:

- **connect** — establishing the TCP (and TLS) connection to the origin.
- **read** — waiting for the next chunk of the response (the most common one to
  tune; a slow or hung server).
- **write** — sending the next chunk of the request body.
- **pool** — waiting to acquire a connection from the pool, which only bites once
  the pool is bounded, so this dovetails with the per-host pool bounds
  (`client-pool-bounds.md`): the pool-acquire wait and its timeout are the same
  bracket.

Design choices to settle:

- A `Timeout` value (per-axis, with a shared default and `None` to disable an
  axis) passed at `ConnectionPool(...)` and overridable per `pool.request(...)`,
  mirroring how `middleware` already layers pool-wide plus per-request.
- Implement with `asyncio.timeout(...)` around each phase. The delicate part is the
  same as the pool work: a timeout firing mid-borrow must run the single release
  path exactly once (return-to-pool vs close vs reset) so a timed-out request never
  strands or double-frees a connection.
- A timeout raises a typed error (e.g. `ConnectTimeout` / `ReadTimeout` /
  `WriteTimeout` / `PoolTimeout`) so a caller can distinguish and retry the right
  ones; keep it parse-don't-validate at the boundary rather than a bare
  `TimeoutError`.
