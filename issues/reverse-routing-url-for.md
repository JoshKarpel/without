---
title: Add reverse routing (url_for)
labels: [without-web]
---

## Summary

There is no way to build a URL from a route and its parameters. Add reverse
routing (`url_for`-style): given a route identity and the values for its path
params, render the concrete path, so handlers and templates can link to routes
without hand-assembling strings that drift when a pattern changes.

## Package(s)

`without-web`.

## Notes

The route table already holds everything needed: each route carries its pattern
(literal segments plus typed `path_param` slots), so reversing is the inverse of
the trie walk, filling each param slot from supplied values and rejecting a value
its converter would not have parsed (parse, don't validate, in reverse). Decide
how a route is named/identified for lookup (the `Route` value itself, the handler,
or an explicit name) and how mounts compose the prefix. Pairs with the OpenAPI
work (see `openapi-refs.md`) since both recover structure from the route table,
but it is a distinct feature.
