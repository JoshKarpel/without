# without

The narrow waist of the project: the contracts every plugin speaks, plus the
stream connectors and a `with`-scoped background task helper.

See `without.contracts` for the typed sketch of `Processor`, `Context`,
`Stream`, and `Transition` (and `from_reducer`, the pure way to build a
processor), `without.wiring` for the edge connectors `pipe` (event) and `sample`
(behavior), and `without.tasks` for `background_task`.

Graph/DAG recovery from declared inputs (the `@node` decorator and mermaid
visualizer) was prototyped and then set aside to focus on the streams core; it
will return later, likely built on stdlib `graphlib`.
