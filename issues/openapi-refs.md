---
title: Emit OpenAPI shared components / $ref instead of inlining schemas
labels: [without-web]
---

## Summary

`openapi(...)` currently inlines every schema into the output. There is no shared
components section and no `$ref` reuse, so a schema used by many routes is
duplicated and the document is larger than it needs to be.

## Package(s)

`without-web`.

## Notes

Needs a component registry and a ref strategy that stays compatible with the
injected `schema_for` (the app may supply pydantic's `model_json_schema`, a
dataclass walker, or a raw mapping), so the dedup key has to work without
without-web knowing the schema library. Reverse routing (`url_for`) is a related
structure-from-the-route-table feature, tracked separately in
`reverse-routing-url-for.md`.
