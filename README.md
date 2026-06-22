# without

A decoupled-IO substrate for connecting streams of events to stateful processors
backed by contexts, aiming for maximum concurrency from testable,
dependency-injected code. I/O is not banned, it is separated into the right
abstractions (sources at the edge, behaviors via `sample`, effects contained in
a processor's step) so the parts stay reusable.

The bet: Python has many frameworks with similar-but-subtly-different shapes
(ASGI apps, Kafka consumers, asyncio protocols, config reloaders) that do not
interoperate because none of them names the shared lower layer. `without` names
that layer as a narrow contract, so the pieces compose. It is meant to feel like
a library (your control flow stays visible) rather than a framework.

See `plans/BIG_IDEA.md` for the original pitch and `plans/REVIEW_BIG_IDEA.md` for the
critical review and open questions.

## Layout

This is a `uv` workspace of flat, version-locked packages (no namespace
packages). Each package is its own top-level import.

- `packages/without` — the core: the contracts every plugin speaks
  (`without.contracts`), the stream edge connectors (`without.wiring`), and a
  `with`-scoped background task helper (`without.tasks`). Imported as `without`.
- `packages/without-env` — first plugin: a static `Context` parsed from
  environment variables (`pydantic-settings`). Imported as `without_env`.
- `packages/without-integration` — not a real package: depends on `without` and
  every plugin so they can be exercised together in cross-package tests.

Planned plugins, in the order they should be attempted:

1. `without-env` — config from env vars; a static context. **(done)**
2. `without-configmap` — config from a k8s mount (`watchfiles` + `pydantic`);
   proves the context-updated-by-a-stream loop.
3. a toy line-protocol server (Redis-ish); proves long-lived processor state.
4. HTTP (sans-IO deps); the real test of the contract.

## Development

```bash
uv sync
just test    # mypy + pytest
```
