# without

The narrow waist of the project: the contracts every plugin speaks, plus the
stream connectors and a `with`-scoped background task helper.

See `without.contracts` for the typed sketch of the core types and the builders
that lift a step into a processor or a leaf, `without.wiring` for the edge
connectors that wire processors together (plus the source adapters that feed
them), and `without.tasks` for the `with`-scoped background task helper.

Graph/DAG recovery from declared inputs (the `@node` decorator and mermaid
visualizer) was prototyped and then set aside to focus on the streams core; it
will return later, likely built on stdlib `graphlib`.
