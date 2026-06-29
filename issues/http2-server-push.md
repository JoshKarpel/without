---
title: Implement HTTP/2 server push
labels: [without-http]
---

## Summary

The HTTP/2 server does not support server push: the `ServerPush` outbound event
is rejected (`NotImplementedError`) in the wire mapping, mirroring the HTTP/1.1
path that has no equivalent. The client already *receives* pushed responses' data
model; the server side of pushing is unimplemented.

## Package(s)

`without-http` (server), `without-asgi` (the `ServerPush` outbound type already
exists).

## Notes

The h2 wire mapping (`h2_wire`) currently supports response start/body and 103
early hints and rejects the rest. Server push needs the `PUSH_PROMISE` machinery
on `h2.connection.H2Connection`, a new pushed-stream lifecycle, and a decision on
how an ASGI app opts in (ASGI models push as an extension event). Note that server
push is widely deprecated/disabled in browsers; weigh whether it is worth building
beyond protocol completeness. Pairs naturally with server-side trailer sending,
which touches the same "h2 rejects the rest" code path.
