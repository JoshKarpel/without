---
title: Support WebSockets over HTTP/2 (RFC 8441 extended CONNECT)
labels: [without-http]
---

## Summary

WebSockets currently ride only the HTTP/1.1 `Upgrade`. Over HTTP/2 the handshake
is replaced by h2 headers (`:method = CONNECT`, `:protocol = websocket`); frames
then flow as that stream's DATA frames. Support extended CONNECT so WebSockets
work over h2.

## Package(s)

`without-http` (server and client).

## Notes

- `h2` already supports extended CONNECT (advertise
  `SETTINGS_ENABLE_CONNECT_PROTOCOL = 1`), and the long-lived stream slots into
  the existing multiplexed per-stream processor model with `WINDOW_UPDATE` flow
  control, so recognizing the request is small.
- **Real cost:** `wsproto`'s high-level `WSConnection` is bound to the h11
  handshake and can't be reused over h2. The h2 path must drive the lower-level
  `wsproto.frame_protocol.FrameProtocol`. To avoid two divergent WebSocket paths,
  factor the existing v1 handler around the frame layer so H1-vs-H2 differ only in
  handshake + framing transport.
- **Real risk:** few tools speak WS-over-h2, so end-to-end verification is thin.
  Validate the frame layer with the transport-independent Autobahn TestSuite and a
  known-good peer rather than relying on loopback alone.
