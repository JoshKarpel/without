from __future__ import annotations

import pytest
from without_html.nodes import CHECKED_ATTRIBUTE_NAMES
from without_html.render import tag_markup


@pytest.fixture(autouse=True)
def cold_caches() -> None:
    """
    Start every test in this package with the package's process-global memos empty.

    Both are filled on first use and outlive the test that filled them, so a test that
    checks a cold path only sees it if nothing earlier warmed the entry. Clearing here
    rather than in those tests makes that independent of ordering (the suite runs
    shuffled) and of how many times the suite runs in one process (a mutation run runs
    it many times over).
    """
    CHECKED_ATTRIBUTE_NAMES.clear()
    tag_markup.cache_clear()
