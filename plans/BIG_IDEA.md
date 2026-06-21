I have a maybe-strange idea for a python "framework" for writing testable, dependency-injected code that
achieves maximum concurrency. the idea is not fully formed I think its fundamentally about connecting
streams of events to "processors", which are backed by "contexts". so you feed an event and the current
context into a procesosr and it processes the event, maybe statefully. the lifespan of the processor depends
on the stream you want to ingest; for example, when serving a single http request you might decide the
processor lifespan is a single reuest (backed by long-lived contexts that supply config or db connection
pools or whatever), while if you wanted to clone redis you'd probably have a long-lived processor that holds
the state (or maybe that should be a context? we could have mutable and immutable contexts, as determined
by how the caller defines them, e.g. mutable class vs frozen pydantic model representing config). contexts
can be updated by streams of events too, e.g. file watcher reloading config from a k8s mount. I'm imagining
a monorepo-y approach with a core package for the "executor" that connects the parts and defines the
contracts, and then multiple packages (all version-locked together) that provide tooling for different kinds
of context and streams, e.g. config from a k8s configmap (watchfiles + pydantic), config from env vars
(pydantic-settings), handling http requests (very complex! we'll need sans-io deps I think?)

one thing I'm not sure about is how much we should care about how the handlers work - I really like the idea
with a sans-io approach and granular events (e.g., individual events inside an http or websocket request or whatever)
we should be able to achieve DAG-like execution of user's code, but one idea I like is that the control flow should be totally
user-visible - this package should act more like a library than a framework.

https://sans-io.readthedocs.io/index.html
https://sans-io.readthedocs.io/how-to-sans-io.html

The key ideas I'd like to explore are:
- Anything can be modeled as a stateful stream processor (with different lifespans of that state, maybe ingesting multiple streams, etc.)
- Any workflow can be executed as a DAG if you get granular enough (declare what inputs you need, not the order that things happen in)
- Decoupling I/O from logic (sans-IO) enables easier testing and encourages explicit dependency injection
