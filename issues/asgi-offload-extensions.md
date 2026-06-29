---
title: Implement the ASGI offload extensions (zero-copy send, path send, response debug)
labels: [without-http]
---

## Summary

The optional ASGI offload extensions `ZeroCopySend`, `PathSend`, and
`ResponseDebug` are modeled as outbound types but raise `NotImplementedError` in
the without-http server (both HTTP/1.1 and HTTP/2).

## Package(s)

`without-http` (server). The outbound types already exist in `without-asgi`, so no
change is needed there; the implementation is entirely in without-http's server
wire paths (`h11`/`h2`), where the sends currently raise.

## Notes

These are genuinely optional and depend on kernel/transport support (`sendfile`,
zero-copy paths). Low priority relative to push/trailers. An app should be able to
discover support via the scope's `extensions` mapping rather than having a send
blow up, so part of this is advertising the extension only when the server can
honor it (parse, don't validate at the boundary).
