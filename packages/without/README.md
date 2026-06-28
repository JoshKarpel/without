# without

The narrow waist of the project: the contracts every plugin speaks, plus the
stream connectors and a `with`-scoped background task helper.

See `without.contracts` for the typed sketch of the core types and the builders
that lift a step into a processor or a leaf, `without.wiring` for the edge
connectors that wire processors together (plus the source adapters that feed
them), and `without.tasks` for the async task helpers: `sleep_forever`, the
`with`-scoped `background_task`, `limit_concurrency` (a bounded-concurrency driver
that pulls work from a source only while below the limit, so a lazy source is
never advanced past it), and its building blocks `cancel_futures` (cancel a set,
then await them all) and `as_async_iterator` (normalize a sync or async iterable
into one async iterator).

Graph/DAG recovery from declared inputs (the `@node` decorator and mermaid
visualizer) was prototyped and then set aside to focus on the streams core; it
will return later, likely built on stdlib `graphlib`.
