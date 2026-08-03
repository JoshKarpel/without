# without

The narrow waist of the project: the interfaces every plugin speaks, plus the
stream connectors and a `with`-scoped background task helper.

Three types carry the whole model (`without.interfaces`):

- A `Stream[T]` is an asynchronous sequence of values: the one shape every
  connection takes, whoever does the I/O.
- A `Processor[In, Out]` transforms a stream of inputs into a stream of outputs:
  the only node type, and the only thing a user writes.
- A `Context[T]` is a stream viewed as its latest sampled value: `current()`
  reads the latest and never blocks.

Processors are built, not subclassed, from a 2×2 of builders (`from_map`,
`from_scan`, `from_sink`, `from_fold`) and connected with a small wiring
vocabulary (`compose`, `sample`, and the source/terminal adapters).

See [`PHILOSOPHY.md`](../../PHILOSOPHY.md) for why the model is shaped this way,
and the [`without` guide](https://without.help/without/)
(with the [API reference](https://without.help/reference/without/))
for the full surface.
