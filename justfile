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
