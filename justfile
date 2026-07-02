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

[doc('Benchmark one stack (without|fastapi) with vegeta+austin on PATH; extra args pass to the harness')]
bench framework *args:
    mise exec -- uv run python -m benchmarks.harness {{ framework }} {{ args }}

alias b := bench

[doc('Plot latency + throughput vs rate from the vegeta results in results/')]
plot *args:
    mise exec -- uv run python -m benchmarks.plot {{ args }}

[doc('Upgrade all dependencies')]
upgrade:
    uv lock --upgrade

alias update := upgrade
alias u := upgrade
