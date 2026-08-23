#!/usr/bin/env bash
# Regenerate the cog element constructors, then hand all formatting to ruff. Running
# both in one hook means the net result is the formatter's fixed point, so the hook is
# idempotent (cog alone is not: it does not mirror ruff's blank lines).
#
# The outputs are named here rather than taken from the caller, because the hook fires on
# a change to the *generators* as well, and pre-commit hands a hook only the files that
# changed. Passed those, a change to `tags.py` would regenerate nothing and the checked-in
# constructors would keep the old shape with the run still green.
set -euo pipefail

readonly generated=(
    packages/without_html/src/without_html/elements.py
    packages/without_html/src/without_html/render.py
    packages/without_html/src/without_html/__init__.py
)

uv run cog -r -I packages/without_html/tools "${generated[@]}"
uv run ruff format "${generated[@]}"
