# CLAUDE.md — without-asgi

## Reference the ASGI spec

This package is an ASGI boundary adapter, so its scope and message types must
track the ASGI specification:

- Index: https://asgi.readthedocs.io/en/latest/index.html
- HTTP & WebSocket scopes: https://asgi.readthedocs.io/en/latest/specs/www.html
- Lifespan scope: https://asgi.readthedocs.io/en/latest/specs/lifespan.html

The published docs are built from rST in the `django/asgiref` repo under
`specs/` (e.g.
https://raw.githubusercontent.com/django/asgiref/main/specs/www.rst), which is a
useful fallback when readthedocs rate-limits. Before changing a scope or event
type, read the relevant spec page and work from the field list there.

## Coverage

The scope dataclasses in `scope.py` (`HttpScope`, `WebsocketScope`,
`LifespanScope`, and the shared `Asgi`) model every field the spec defines, with
per-field docstrings quoting the spec. The `parse_*` functions enforce the
required-versus-optional split and the spec defaults. `type` is consumed by the
`parse_scope` discriminator rather than stored on a dataclass.

The HTTP and WebSocket event types are modeled in both directions: inbound
(`parse_inbound`, `parse_websocket_inbound`) and outbound (`encode_outbound`,
`encode_websocket_outbound`), with lifespan events on their own pair. App-built
outbound events carry the spec's optional-field defaults on the dataclass;
parser-built scopes and inbound events do not (the parser always supplies every
field, so a default there would mask a bug).

Every extension from the reference docs is modeled: HTTP/2 server push, zero-copy
send, path send, early hints, response trailers (with the `trailers` flag on
`ResponseStart`), debug, the WebSocket denial response, and TLS (`parse_tls`
reads the `tls` extension out of a scope's generic `extensions` mapping).

Sourced from the spec on 2026-06-23 (HTTP & WebSocket spec version 2.5, lifespan
spec version 2.0, TLS extension version 0.2). One easy-to-miss detail when
revising: the `asgi.spec_version` default differs by scope (HTTP/WebSocket
default `"2.0"`, lifespan `"1.0"`).

## Note: `scope["state"]` is intentionally not surfaced

The spec's `state` namespace is the standardized mechanism for passing data from
the lifespan cycle into each request (the server passes a shallow copy into every
connection scope). `without` deliberately does not model it: `make_asgi_app`
threads lifespan-derived state to handlers explicitly through its internal
`_Cell`, so surfacing `scope["state"]` would be a redundant, parallel state path.
