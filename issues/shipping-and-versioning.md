---
title: Decide how to ship and version the project
labels: [packaging]
---

## Summary

The project is an unpublished `uv` workspace of several packages with no release
story yet: how versions are assigned, how the packages are published, and how they
depend on each other across a release are all open. Decide the shipping and
versioning model.

## Package(s)

`packaging` (workspace-wide).

## Notes

Open questions:

- **Versioning.** Release the packages version-locked together (one version for
  the whole workspace) or version them independently? Lockstep is simpler for a
  tightly coupled set; independent versioning lets a plugin move without a core
  bump.
- **Intra-workspace deps.** The packages currently depend on each other with
  unpinned bounds, which is unsafe to publish: a released package must pin the
  exact sibling versions it was built against. One option is to pin each
  `pyproject.toml`; another is to have the publish step rewrite intra-workspace
  deps to exact versions at build time. Which one falls out of the versioning model
  chosen above.
- **Publish mechanics.** The workflow that builds and uploads to PyPI (the
  `without*` glob already excludes `integration`), plus tagging, changelog, and a
  cooldown policy for consumers.

Nothing has shipped yet, so this whole path is untested end to end; settle the
model before the first release rather than retrofitting it.
