---
title: Add transport-level 503 overload shedding for arbitrary hosted ASGI apps
labels: [without-http]
---

## Summary

without-native apps get request-level overload shedding from the
`limit_concurrent_requests` middleware (it wraps the handler, so it applies under
any transport). An arbitrary third-party ASGI app hosted via plain ASGI
(FastAPI, Starlette) has no without-middleware to inject, so it would need a 503
(with `Retry-After`) emitted at the transport instead.

## Package(s)

`without-http` (server), `without-asgi`.

## Notes

This is exactly uvicorn's reason for putting `--limit-concurrency` in the
transport. Deferred until overload-protecting third-party hosted apps matters;
without-native apps are fully covered by the middleware. Keep the shed response a
caller-supplied `Response` value (default `503` + `Retry-After`) so a JSON API or
a `429` is a different value, not a new code path.
