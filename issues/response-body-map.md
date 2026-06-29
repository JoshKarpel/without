---
title: Add ResponseBody.map() ergonomic sugar
labels: [without-http]
---

## Summary

Output-affecting client middleware already *works*, but reads awkwardly: you
rebuild a `ResponseBody` around `events()`, e.g.
`wrap(response=lambda r: ClientResponse(r.head, ResponseBody(transform(r.body.events()))))`.
Add `ResponseBody.map(transform)` so byte-counting / decompression read as one
call.

## Package(s)

`without-http` (client).

## Notes

Pure ergonomics, not a capability gap (a test already exercises the verbose form,
uppercasing a body). `map` must propagate both natural exhaustion and
`aclose()`/`GeneratorExit` to the inner stream so the connection release in the
body's `finally` still fires.
