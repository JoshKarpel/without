#!/usr/bin/env just --justfile

set ignore-comments

[default]
[doc('List available recipes')]
list:
    just --list

alias l := list

[doc('Run a recipe whenever source files change')]
watch *CMD:
    uv run watchfiles --verbosity warning '{{ CMD }}' packages/

alias w := watch

[doc('Run type checking and tests')]
test *args:
    uv run mypy
    uv run pytest --failed-first --cov {{ args }}

alias t := test

[doc('Profile the suite: show the slowest fixtures, setup, calls, and teardown')]
durations *args:
    uv run pytest --pytest-durations=30 --pytest-durations-group-by=function {{ args }}

alias td := durations

# mutmut reads its config only from the working directory and must run inside a package
# for the src/ layout to resolve, so it can't live in the root pyproject; the recipe
# writes it per run instead, identically for every package.
[doc('Mutation-test one package, e.g. `just mutate without-dag`; pass `--help` to see mutmut subcommands (results, browse, ...)')]
mutate pkg *args='run':
    #!/usr/bin/env bash
    set -euo pipefail
    cd packages/{{ pkg }}
    trap 'rm -f setup.cfg' EXIT
    # -n0 overrides the workspace's `-n auto`: pytest-xdist would re-exec its workers in
    # subprocesses that never see mutmut's in-process sys.path insert, so they would import
    # the original, unmutated code instead of the mutated copy mutmut builds under ./mutants.
    cat > setup.cfg <<'CFG'
    [mutmut]
    source_paths=src
    pytest_add_cli_args_test_selection=tests/
    pytest_add_cli_args=-n0
    CFG
    uv run mutmut {{ args }}

alias m := mutate

[doc('Serve the documentation site with live reload')]
docs *args:
    uv run --group docs mkdocs serve {{ args }}

alias d := docs

[doc('Build the documentation site into ./site')]
docs-build *args:
    uv run --group docs mkdocs build --strict {{ args }}

[confirm('This uploads a real 0.0.0 placeholder and reserves the given name(s) on PyPI. Continue?')]
[doc('Reserve PyPI project name(s) with an empty 0.0.0 placeholder release (needs UV_PUBLISH_TOKEN)')]
bootstrap-pypi +names:
    uv run --script scripts/bootstrap_pypi.py {{ names }}

[doc('Benchmark one framework (without|fastapi) with vegeta+austin on PATH; add --server to pick the server; extra args pass to the harness')]
bench framework *args:
    mise exec -- uv run python -m benchmarks.harness {{ framework }} {{ args }}

alias b := bench

[doc('Plot latency + throughput vs rate from the vegeta results in results/')]
plot *args:
    mise exec -- uv run python -m benchmarks.plot {{ args }}

[doc('Summarize per-package + per-frame self-time from the austin profiles in results/')]
hotspots *args:
    uv run python -m benchmarks.hotspots {{ args }}

[doc('Upgrade all dependencies')]
upgrade:
    uv lock --upgrade

alias update := upgrade
alias u := upgrade
