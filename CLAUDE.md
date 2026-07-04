# CLAUDE.md

## Start here

Before doing anything else, read [`PHILOSOPHY.md`](PHILOSOPHY.md) to get the
project's mindset: the narrow-waist bet, the stream/processor/context substrate,
functional-core/imperative-shell, and values-over-places. It is the durable
rationale for why the code is shaped the way it is, and the right frame for any
new work.

Then skim the per-package `README.md` files for the area you are touching: the
core (`packages/without`), the ASGI boundary (`packages/without-asgi`), the HTTP
server and client (`packages/without-http`), the opinionated router
(`packages/without-web`), the config sources (`packages/without-env`,
`packages/without-configmap`), the DAG executor (`packages/without-dag`), and the
`integration` toys that exercise the whole stack.
