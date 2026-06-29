---
title: Add HTTP/3 over QUIC (aioquic)
labels: [without-http]
---

## Summary

Add HTTP/3 as a separate transport path within `without-http`, over QUIC (UDP)
via `aioquic`, producing and consuming the same vocabulary as the TCP h11/h2
path. The per-connection-processor model carries over unchanged; only the
transport shell is new. The largest deferred transport item.

## Package(s)

`without-http` (server), eventually the client's extended-CONNECT path.

## Notes

Design constraints already settled:

- **Explicit opt-in, default off** (`http3=True`), reached through the existing
  `serving` entrypoint, never a parallel `serve_quic`. With h3 enabled, `serving`
  brings up the TCP (h1/h2) and QUIC/UDP (h3) listeners concurrently on the same
  port and injects `Alt-Svc` into h1/h2 responses so clients discover h3.
- **`aioquic` is a heavy optional dependency** behind an extra
  (`without-http[http3]`); never auto-enable on a stray importable dependency.
- **QUIC is TLS-only** (no cleartext h3), so h3 comes up only with a cert.
  `serving` must reject illegal combinations at startup (parse, don't validate):
  `http3=True` without TLS or without `aioquic` fails loudly.
- **Edge-tier only.** h3 cannot reach an app behind upstream TLS termination
  (QUIC welds in TLS 1.3, so there is no plaintext h3 to forward). `Alt-Svc`
  auto-injection MUST be gated on this process actually terminating h3, or a
  backend would advertise an endpoint nothing listens on.
