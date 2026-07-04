#!/usr/bin/env bash
# Regenerate the cog overload ladder, then hand all formatting to ruff. Running
# both in one hook means the net result is the formatter's fixed point, so the
# hook is idempotent (cog alone is not: it does not mirror ruff's blank lines).
set -euo pipefail

uv run cog -r -I packages/without-dag/tools "$@"
uv run ruff format "$@"
