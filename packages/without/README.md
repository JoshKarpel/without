# without

The narrow waist of the project: the contracts every plugin speaks, plus the
stream connectors and a `with`-scoped background task helper.

See `without.contracts` for the typed sketch of `Processor`, `Context`,
`Stream`, and `Transition` (and `from_reducer`, which builds a processor from an
async reducer whose step may `await` contained I/O, plus `from_mapper`, its
stateless counterpart for a step that maps each event straight to a single
output), `without.wiring` for the
edge connectors (`pipe`, `distribute`, `tee`, `broadcast`, `route`, and `merge`
on the event edge; `sample` on the behavior edge), and `without.tasks` for
`background_task`.

Graph/DAG recovery from declared inputs (the `@node` decorator and mermaid
visualizer) was prototyped and then set aside to focus on the streams core; it
will return later, likely built on stdlib `graphlib`.
