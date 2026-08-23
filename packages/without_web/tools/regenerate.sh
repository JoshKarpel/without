#!/usr/bin/env bash
# Regenerate the cog overload ladders, then hand all formatting to ruff. Running
# both in one hook means the net result is the formatter's fixed point, so the
# hook is idempotent (cog alone is not: it does not mirror ruff's blank lines).
#
# The outputs are named here rather than taken from the caller, because the hook fires on
# a change to the *generator* as well, and pre-commit hands a hook only the files that
# changed. Passed those, a change to `ladders.py` would regenerate nothing and the
# checked-in ladders would keep the old shape with the run still green.
set -euo pipefail

readonly generated=(
    packages/without_web/src/without_web/handlers.py
    packages/without_web/src/without_web/extractors.py
)

uv run cog -r -I packages/without_web/tools "${generated[@]}"
uv run ruff format "${generated[@]}"
