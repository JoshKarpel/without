---
title: Implement server-side response trailer sending (h11 + h2)
labels: [without-http]
---

## Summary

The HTTP client now *receives* trailers (h11 chunked and h2), but the
without-http server still rejects them on the response path. Sending trailing
headers after the response body (gRPC's `grpc-status` is the canonical case) is
unimplemented.

## Package(s)

`without-http` (server), `without-asgi` (the `ResponseTrailers` outbound type and
the `trailers` flag on `ResponseStart` already exist).

## Notes

Both wire mappings need the tail: h11 emits a chunked trailer section, h2 carries
the trailing HEADERS frame with `END_STREAM`. The ASGI contract already has the
`http.response.trailers` extension and `transform.app`'s `request_digest`
middleware negotiates it, so the app-facing shape is settled; this is the server
transport half. Landing it lets a without-http-to-without-http round-trip exercise
trailers end to end instead of only against raw test servers.
