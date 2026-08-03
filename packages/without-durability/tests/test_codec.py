from __future__ import annotations

from math import inf
from math import nan

import pytest
from hypothesis import given
from hypothesis import strategies as st
from without_durability import JSON
from without_durability import MemoryCheckpointer
from without_durability import Recorded
from without_durability import claimed

ORDER = "ord-88"

# What `JsonCodec` claims to carry: the JSON-native values a workflow's steps may return.
# Deliberately not `st.floats()` unrestricted, since the two values it excludes are the
# subject of their own tests below.
json_values = st.recursive(
    st.none() | st.booleans() | st.integers() | st.floats(allow_nan=False, allow_infinity=False) | st.text(),
    lambda children: st.lists(children) | st.dictionaries(st.text(), children),
    max_leaves=12,
)


@given(json_values)
def test_a_json_native_value_survives_the_round_trip(value: object) -> None:
    assert JSON.decode(JSON.encode(value)) == value


@given(st.dictionaries(st.text(), st.integers(), min_size=2))
def test_equal_mappings_encode_identically_however_they_were_built(mapping: dict[str, int]) -> None:
    # The determinism the protocol requires, and the one a mapping is most likely to
    # break: `record` decides who won a race by comparing encodings, so an encoder that
    # follows insertion order reports a conflict between two passes that computed the
    # same thing by different routes.
    rebuilt = dict(reversed(list(mapping.items())))

    assert JSON.encode(rebuilt) == JSON.encode(mapping)


async def test_two_passes_that_built_one_mapping_differently_agree_on_who_won() -> None:
    # The consequence, at the interface where it does harm: a `first=False` here is
    # `run_durably` stopping a run over a difference that does not exist.
    checkpointer = MemoryCheckpointer()
    holder = await claimed(checkpointer, ORDER)

    await checkpointer.record(holder, "items", {"widget": 1200, "gizmo": 800})
    again = await checkpointer.record(holder, "items", {"gizmo": 800, "widget": 1200})

    assert again == Recorded(value={"widget": 1200, "gizmo": 800}, first=True)


@pytest.mark.parametrize("value", [nan, inf, -inf], ids=["nan", "inf", "-inf"])
def test_a_number_json_cannot_hold_is_refused_where_it_is_produced(value: float) -> None:
    # `json.dumps` renders these as the bare tokens `NaN` and `Infinity`, which are not
    # JSON: `NaN` decodes back unequal to itself, and a store whose column is `jsonb`
    # rejects the write at the far end of a workflow the in-memory double accepted. The
    # round trip is a requirement on the codec, so the codec is where it fails.
    with pytest.raises(ValueError, match="not JSON compliant"):
        JSON.encode(value)


def test_a_mapping_whose_keys_cannot_be_ordered_is_refused_rather_than_encoded() -> None:
    # The price of sorting, stated where it is paid. Such a mapping already broke the
    # round trip (JSON keys are text, so the integer comes back as a string), so this
    # fails where the value is produced rather than one pass later.
    with pytest.raises(TypeError):
        JSON.encode({1: "a", "b": 2})
