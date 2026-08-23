#!/usr/bin/env bash
# Regenerate the cog overload ladder, then hand all formatting to ruff. Running
# both in one hook means the net result is the formatter's fixed point, so the
# hook is idempotent (cog alone is not: it does not mirror ruff's blank lines).
#
# The output is named here rather than taken from the caller, because the hook fires on a
# change to the *generator* as well, and pre-commit hands a hook only the files that
# changed. Passed those, a change to `dag_ladders.py` would regenerate nothing and the
# checked-in ladder would keep the old shape with the run still green.
set -euo pipefail

readonly generated=(packages/without_dag/src/without_dag/graph.py)

uv run cog -r -I packages/without_dag/tools "${generated[@]}"
uv run ruff format "${generated[@]}"
