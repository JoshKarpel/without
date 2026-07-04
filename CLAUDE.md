# CLAUDE.md

## Start here

Before doing anything else, read [`PHILOSOPHY.md`](PHILOSOPHY.md) to get the
project's mindset: the narrow-waist bet, the stream/processor/context substrate,
functional-core/imperative-shell, and values-over-places. It is the durable
rationale for why the code is shaped the way it is, and the right frame for any
new work.

Then read the per-package guide in [`docs/guides/`](docs/guides) for the area you
are touching: the core (`without.md`), the ASGI boundary (`without-asgi.md`), the
HTTP server and client (`without-http.md`), the opinionated router
(`without-web.md`), the config sources (`without-env.md`, `without-configmap.md`),
and the DAG executor (`without-dag.md`). Those guides carry the design narrative;
each package's `README.md` is now a short orientation that links to them, and the
`integration` toys that exercise the whole stack live under `packages/integration`.

The guides, an API reference recovered from the source docstrings, and the
derived package dependency graph are published as a documentation site (see
[`mkdocs.yml`](mkdocs.yml) and the hand-written [`docs/hooks.py`](docs/hooks.py));
build it locally with `just docs` (serve) or `just docs-build` (strict build).
