import pytest
from without.graph import CycleError, Registry


def test_inputs_are_inferred_from_parameter_names() -> None:
    registry = Registry()

    @registry.node
    def parse(raw: object) -> object: ...

    @registry.node
    def render(parse: object, theme: object) -> object: ...

    graph = registry.graph()

    assert graph.nodes["parse"].inputs == ("raw",)
    assert graph.nodes["render"].inputs == ("parse", "theme")


def test_explicit_inputs_override_the_signature() -> None:
    registry = Registry()

    @registry.node(inputs=["upstream"])
    def widget(whatever: object) -> object: ...

    assert registry.graph().nodes["widget"].inputs == ("upstream",)


def test_decorator_returns_the_function_unchanged() -> None:
    registry = Registry()
    sentinel = object()

    @registry.node
    def compute() -> object:
        return sentinel

    assert compute() is sentinel


def test_duplicate_node_name_is_rejected() -> None:
    registry = Registry()

    @registry.node(name="shared")
    def first() -> object: ...

    with pytest.raises(ValueError, match="already registered"):

        @registry.node(name="shared")
        def second() -> object: ...


def test_topological_order_respects_declared_dependencies() -> None:
    registry = Registry()

    @registry.node
    def extract(source: object) -> object: ...

    @registry.node
    def transform(extract: object) -> object: ...

    @registry.node
    def load(transform: object) -> object: ...

    order = registry.graph().topological_order()

    assert order.index("extract") < order.index("transform") < order.index("load")


def test_cycle_in_declared_inputs_raises() -> None:
    registry = Registry()

    @registry.node(inputs=["pong"])
    def ping() -> object: ...

    @registry.node(inputs=["ping"])
    def pong() -> object: ...

    with pytest.raises(CycleError, match="ping, pong"):
        registry.graph().topological_order()


def test_mermaid_renders_edges_and_marks_external_sources() -> None:
    registry = Registry()

    @registry.node
    def parse(payload: object) -> object: ...

    @registry.node
    def index(parse: object) -> object: ...

    mermaid = registry.graph().to_mermaid()

    assert mermaid.splitlines()[0] == "flowchart TD"
    assert "    payload([payload])" in mermaid
    assert "    payload --> parse" in mermaid
    assert "    parse --> index" in mermaid
