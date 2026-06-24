# integration

Not a real package, and never published: the name deliberately sits outside the
`without*` family that the publish workflow globs, and a `Private :: Do Not
Upload` classifier makes PyPI reject it as a backstop. It is the aggregator where
`without` and every plugin are imported together and exercised as a whole. Each
new plugin is added to this package's dependencies so its interaction with the
rest gets a home for tests.

It also hosts validation artifacts that are not meant to be distributed. `kv` is
a toy line-protocol key-value server (Redis-ish) built on `without`, proving the
contract supports long-lived processor state and request/response. It splits into
`kv.core` (the pure keyspace: parse a line, fold it into an immutable `Store`,
render a reply) and `kv.shell` (a generic line-server transport plus the wiring
that runs the core over it), a small demonstration that `without` is a principled
way to write an imperative shell.

`transform` is a text-transform service built on the `without-asgi` adapters.
`POST /transform` reads the request body, uppercases/lowercases/title-cases it
per a `?mode=` query override, and caps the size, with the default mode and the
limit both from a `without-configmap` `Context`. Each HTTP request and WebSocket
connection snapshots the config the moment it arrives, so a ConfigMap reload takes
effect on the next one rather than changing an open connection mid-flight (`GET
/modes` reports the current values, and a WebSocket `/stream` transforms each text
frame with its connect-time snapshot). It shows the
framework-shaped concerns (routing, middleware, lifespan) as plain `without`
wiring, and splits along the functional-core/imperative-shell line.
`transform.core` is pure and HTTP-unaware: the `Settings` model, the `Mode`
transforms over decoded text, and the `UnknownMode` error it raises rather than
encoding a status. `transform.router` is a small protocol-generic `Router`
assembled from `without-asgi`'s routing tools, since the adapters ship those but
no router of their own. `transform.app` is the shell: the HTTP and WebSocket
handlers own the bytes (the size limit, the decode, the query parse), call the
core, and render its result or raised error as JSON; it also holds the middleware
stack applied across every route and the config-watch kept for the server's
lifetime via `without-asgi`'s `make_asgi_app`.
