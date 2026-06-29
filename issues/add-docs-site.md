---
title: Add a documentation site (mkdocs-material + mkdocstrings)
labels: [docs]
---

## Summary

Stand up a proper documentation site with
[mkdocs-material](https://squidfunk.github.io/mkdocs-material/) and the
[mkdocstrings](https://mkdocstrings.github.io/) plugin (Python handler), so the
docstrings already written across the packages render as API reference. Much of
what currently lives in the package READMEs should move into the site.

## Package(s)

All (repo-wide docs), a new top-level `docs/` + `mkdocs.yml`.

## Notes

The codebase is already docstring-rich and the contracts carry RFC 2119 normative
language, so mkdocstrings can recover an API reference from the source rather than
duplicating it by hand (the declarative "describe the shape once, recover the
rest" stance). Structure to aim for:

- A narrative section seeded from `PHILOSOPHY.md` and the per-package README prose
  (substrate, functional-core/imperative-shell, the HTTP server/client, the
  router). The READMEs then shrink to a short orientation + a link to the site,
  rather than carrying the full design narrative.
- An auto-generated API reference per package via `mkdocstrings` (Griffe), so
  signatures and docstrings stay in one place.
- Move the package dependency graph (currently a hand-drawn Mermaid diagram in the
  root `README.md`) into the site and **derive it by reading the declared
  dependencies** rather than maintaining it by hand. The workspace `pyproject.toml`
  files already declare the edges, so the graph can be generated from them (e.g. a
  small `gen-files`/macro step that emits Mermaid from the parsed deps) and cannot
  drift out of sync. Same declarative move as the API reference: state the shape
  once (the deps), recover the diagram.
- Wire it into CI (build on PR, publish on merge, e.g. GitHub Pages).

Decide the README-vs-site split deliberately: a README should orient a reader
landing on the repo; the site is the durable home for the deep material currently
duplicated in long READMEs.
