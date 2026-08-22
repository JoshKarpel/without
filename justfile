#!/usr/bin/env just --justfile

set ignore-comments

# How long the compose stack gets to pull its images and report both services healthy.
# Generous against a cold cache, since overshooting only costs a hung run the time it
# was going to lose anyway, while undershooting fails a slow but working pull.
compose-timeout := "5m"

[default]
[doc('List available recipes')]
list:
    just --list

alias l := list

[doc('Run a recipe whenever source files change')]
watch *CMD:
    uv run watchfiles --verbosity warning '{{ CMD }}' packages/

alias w := watch

# The services in compose.yaml back the tests that drive a real dependency instead of
# a fake, and starting them is the harness's job rather than a fixture's: up once here,
# and down again from an EXIT trap, so a failed run, a Ctrl-C, or a crashed pytest
# cannot leave a container behind. That also keeps pytest's own machinery out of it:
# no session fixture to coordinate across xdist workers, and the image pull happens
# before the first test rather than inside its timeout.
#
# Either engine serves, and the recipe takes the first it finds: docker compose, then
# podman-compose. That order is not a judgement between them. podman does not run a
# healthcheck itself, it asks systemd to, one transient timer per container, and
# GitHub's Ubuntu runner installs podman as a static bundle built without systemd; the
# checks in compose.yaml therefore never run there, health never leaves `starting`, and
# the `--wait` below never returns. Docker's daemon runs them in-process, so it is the
# engine that works in both places. podman is otherwise the nicer neighbour, rootless
# and daemonless with nothing running between test runs, and `podman compose` only
# wraps an external implementation, so podman-compose is called directly and pinned in
# the dev group like every other tool. A machine with neither still runs everything
# else: the tests that need a service skip themselves when the address is unset, which
# is also how the macOS and Windows CI legs run (GitHub's runners ship no container
# runtime that can run a Linux image).
[doc('Run type checking and tests')]
test *args:
    #!/usr/bin/env bash
    set -euo pipefail
    engine=""
    # `OSType`, not merely a reachable daemon: the Windows runner has docker too, in
    # Windows-container mode, where every image in compose.yaml is unrunnable. That leg
    # is meant to reach the `else` and skip, the way it did when it had no engine at all.
    if [[ "$(docker info --format '{{ '{{' }}.OSType{{ '}}' }}' 2> /dev/null)" == "linux" ]]; then
      engine="docker compose"
      compose=(docker compose --file compose.yaml --project-name without-tests)
    elif command -v podman > /dev/null; then
      engine="podman-compose"
      compose=(uv run podman-compose --file compose.yaml --project-name without-tests)
    fi
    if [[ -n "$engine" ]]; then
      trap '"${compose[@]}" down --volumes > /dev/null 2>&1' EXIT
      # --wait holds until each service is *healthy* (see the healthchecks in
      # compose.yaml), so the first test cannot race a server that is still starting.
      # The output (image pulls, container ids, the wait's own bookkeeping) is worth
      # seeing only when it fails, so it is held back until it does.
      #
      # Bounded, because a wait that never returns is the one failure this held-back
      # output cannot report: it prints nothing while it hangs, and whatever it had
      # buffered dies with the shell that gets killed. That is worth a few lines here
      # rather than a CI job spending its whole timeout in silence. `timeout` is GNU
      # coreutils, which a Linux box has and a mac does not, so a mac running an engine
      # of its own waits unbounded rather than not at all.
      status=0
      if command -v timeout > /dev/null; then
        output="$(timeout {{ compose-timeout }} "${compose[@]}" up --detach --wait 2>&1)" || status=$?
      else
        output="$("${compose[@]}" up --detach --wait 2>&1)" || status=$?
      fi
      if [[ "$status" -ne 0 ]]; then
        echo "$output" >&2
        if [[ "$status" -eq 124 ]]; then
          echo "the services in compose.yaml were not healthy within {{ compose-timeout }}" >&2
        fi
        exit 1
      fi
      export WITHOUT_TESTS_REDIS="$("${compose[@]}" port redis 6379 2> /dev/null)"
      export WITHOUT_TESTS_POSTGRES="$("${compose[@]}" port postgres 5432 2> /dev/null)"
      # The services are up, so the code only their tests reach is measurable. Coverage
      # substitutes this into its omit globs, where any non-empty value makes them match
      # nothing; the value says so out loud (see the note in pyproject.toml).
      export WITHOUT_COMPOSE_AVAILABLE=prefix-that-will-not-match
    else
      echo "no container engine found, so the tests that need the services in compose.yaml will skip"
    fi
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
    # Take the distribution name (`without-html`) or the directory name (`without_html`):
    # the directories are underscored so that each package's `tests` is a distinct module
    # to pytest and mypy, but the hyphenated name is what everything else calls a package.
    pkg="{{ pkg }}"
    cd "packages/${pkg//-/_}"
    trap 'rm -f setup.cfg' EXIT
    # -n0 overrides the workspace's `-n auto`: pytest-xdist would re-exec its workers in
    # subprocesses that never see mutmut's in-process sys.path insert, so they would import
    # the original, unmutated code instead of the mutated copy mutmut builds under ./mutants.
    # -m "not no_mutation" excludes tests marked @pytest.mark.no_mutation: ones that assert an async
    # generator's aclose()-triggered `finally`, which mutmut's function trampoline does not run, so they
    # fail the mutmut baseline though they pass the real suite (see docs/contributing/mutation-testing.md).
    # do_not_mutate_patterns skips exhaustiveness guards: an `assert_never(unreachable)` arm (and its
    # `case _ as unreachable:` header) is unreachable by construction, so no test can ever kill a mutation
    # of it. One alternation regex, since mutmut only splits this list on newlines (indented continuations
    # would bake leading whitespace into each pattern).
    cat > setup.cfg <<'CFG'
    [mutmut]
    source_paths=src
    pytest_add_cli_args_test_selection=tests/
    pytest_add_cli_args=-n0
        -m
        not no_mutation
    do_not_mutate_patterns=assert_never|as unreachable
    CFG
    # A package whose source is pure pass-through (without-env) yields no mutants, and mutmut
    # hardcodes exit(1) in that case with no config knob to allow it. Only `run` hits this, so
    # only `run` is wrapped (browse's TUI must not be piped). A zero-mutant run is success; a run
    # that DID build mutants but left them uncovered still fails (files mutated > 0).
    case "{{ args }}" in
      run | run\ *)
        out="$(mktemp)"
        trap 'rm -f setup.cfg "$out"' EXIT
        if uv run mutmut {{ args }} 2>&1 | tee "$out"; then exit 0; fi
        if grep -q '(0 files mutated' "$out" && grep -q 'could not find any test case' "$out"; then
          echo "no mutants generated for {{ pkg }}; nothing to mutation-test"
          exit 0
        fi
        exit 1
        ;;
      *)
        exec uv run mutmut {{ args }}
        ;;
    esac

alias m := mutate

# Mirrors `mutate`'s `*args`: `mutate-all` runs every mutant in each package by default,
# or `mutate-all results` lists each package's survivors without re-running. The `without*`
# glob is exactly the mutation targets, so the `benchmarks` and `integration` toys are skipped.
# One package failing (survivors, or a broken run) does not abort the sweep; a final table
# reports each package's status so nothing is silently skipped.
[doc('Mutation-test every package and report each one, e.g. `just mutate-all` or `just mutate-all results`')]
mutate-all *args='run':
    #!/usr/bin/env bash
    set -uo pipefail
    declare -A status
    for dir in packages/without*/; do
      pkg="$(basename "$dir")"
      echo
      echo "===== $pkg ====="
      if just mutate "$pkg" {{ args }}; then
        status["$pkg"]="ok"
      else
        status["$pkg"]="FAILED"
      fi
    done
    echo
    echo "===== summary ====="
    rc=0
    for pkg in "${!status[@]}"; do
      printf '%-20s %s\n' "$pkg" "${status[$pkg]}"
      [[ "${status[$pkg]}" == "ok" ]] || rc=1
    done
    exit "$rc"

alias ma := mutate-all

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

[doc('Benchmark HTML rendering against htpy, Jinja, and hand-written f-strings')]
bench-render *args:
    uv run python -m benchmarks.render.bench {{ args }}

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
