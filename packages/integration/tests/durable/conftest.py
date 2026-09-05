from __future__ import annotations

from uuid import uuid4

import pytest

# The one fixture every store-backed test here shares, and the one the `durable` fixture in
# `stores.py` namespaces its scheduler by. It lives in a conftest rather than being imported
# the way `durable` is, because importing a fixture makes every parameter that takes it a
# redefinition of the imported name: `durable` pays one `noqa` per test for the privilege of
# naming its parametrization at the import site, and a plain fixture buys nothing with it.


@pytest.fixture
def workflow() -> str:
    # Every test gets its own id rather than clearing the store, because the servers are
    # shared by every worker in the session: clearing would pull another test's checkpoint
    # out from under it.
    return f"test-{uuid4().hex}"
