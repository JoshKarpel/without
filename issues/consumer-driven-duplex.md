---
title: Support consumer-driven request/response duplex (and the h2 stream limit)
labels: [without-http]
---

## Summary

Today a client request sends its whole body before reading the response (fine for
request/response HTTP, wrong for full-duplex). A consumer-driven duplex would let
the caller send the request body and read the response concurrently.

## Package(s)

`without-http` (client).

## Notes

Tied to the h2 stream limit: a large burst of concurrent requests can over-issue
streams on the one multiplexed connection because the pool does not gate against
the server's `SETTINGS_MAX_CONCURRENT_STREAMS`. The duplex rework and the stream
gating are the same area (consumer-driven flow over one connection) and pair with
the per-host pool bounds. This is the largest of the client-side items.
