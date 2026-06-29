---
title: Close the inbound stream when make_asgi_app's handler exits
labels: [without-asgi]
---

## Summary

When a handler completes or is cancelled, `make_asgi_app` never explicitly closes
the inbound stream. This is the server-side symmetric case to the client's
partial-read abort (which the client handles), and it can leave request-body
resources dangling.

## Package(s)

`without-asgi`.

## Notes

The client folds connection release into the body generator's `finally` and the
`async with` always calls `aclose()`. The server has no equivalent close of the
inbound stream when a handler abandons the request body early. Where the client
*can* punt (the user holds the continuation), the server cannot, so the fix is to
ensure the inbound generator's `finally` runs. Small but real resource-leak
surface.
