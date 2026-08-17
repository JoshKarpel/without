#!/usr/bin/env bash
# Create the py.typed marker for any workspace package missing one and git add it,
# so the pre-commit run fails naturally on the new files and a re-run passes.
# Without the marker, type checkers treat an installed package as untyped and
# ignore its annotations entirely (PEP 561).
set -euo pipefail

cd "$(dirname "$0")/.."

for module in packages/*/src/*/; do
  marker="${module}py.typed"
  if [[ -f "${module}__init__.py" && ! -f "${marker}" ]]; then
    touch "${marker}"
    git add --intent-to-add -- "${marker}"
    echo "created missing typing marker: ${marker}"
  fi
done
