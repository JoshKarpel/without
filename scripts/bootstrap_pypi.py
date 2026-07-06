# /// script
# requires-python = ">=3.14"
# ///
"""
Reserve PyPI projects with empty `0.0.0` placeholder releases.

A brand-new project cannot use this repo's trusted-publishing workflow directly:
pending trusted publishers must be unique on `(owner, repo, workflow,
environment)`, so the `without*` projects that share one workflow cannot all be
pre-registered (https://github.com/pypi/warehouse/issues/16920). The way around
it is to make each project *exist* first, then attach a normal trusted publisher
(those freely share the tuple).

This script creates each named project by uploading a minimal, empty `0.0.0`
distribution. The first real release (a `v0.1.0`+ tag) supersedes it via the
normal publish workflow. It is idempotent (names already on the index are
skipped), and you name exactly the projects to reserve: PyPI caps new-project
creation at a few per day, so the initial bootstrap is done in batches, and a
single new package added later is the same one-name command.

Auth uses an account-scoped token, since the projects do not exist yet: set
`UV_PUBLISH_TOKEN` (or let `uv publish` prompt). Pass `--test` to target TestPyPI.
"""

from __future__ import annotations

import argparse
import subprocess
import tempfile
import urllib.error
import urllib.request
from pathlib import Path

PYPI_INDEX = "https://pypi.org"
PYPI_UPLOAD = "https://upload.pypi.org/legacy/"
TEST_PYPI_INDEX = "https://test.pypi.org"
TEST_PYPI_UPLOAD = "https://test.pypi.org/legacy/"


def placeholder_pyproject(name: str) -> str:
    return (
        "[build-system]\n"
        'requires = ["uv_build>=0.11,<0.12"]\n'
        'build-backend = "uv_build"\n'
        "\n"
        "[project]\n"
        f'name = "{name}"\n'
        'version = "0.0.0"\n'
        'description = "Placeholder reserving the PyPI project name; the first real release supersedes it."\n'
        'requires-python = ">=3.9"\n'
    )


def write_placeholder(name: str, root: Path) -> Path:
    """Write a minimal empty package for `name` under `root`, returning its directory."""
    package_dir = root / name
    module_dir = package_dir / "src" / name.replace("-", "_")
    module_dir.mkdir(parents=True)
    (module_dir / "__init__.py").write_text("")
    (package_dir / "pyproject.toml").write_text(placeholder_pyproject(name))
    return package_dir


def already_registered(name: str, index_url: str) -> bool:
    """Whether `name` already exists on the index (so its placeholder must be skipped)."""
    try:
        with urllib.request.urlopen(f"{index_url}/pypi/{name}/json", timeout=30):
            return True
    except urllib.error.HTTPError as error:
        if error.code == 404:
            return False
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description="Reserve named PyPI projects with empty 0.0.0 placeholder releases.")
    parser.add_argument("names", nargs="+", help="distribution names to reserve")
    parser.add_argument("--test", action="store_true", help="target TestPyPI instead of PyPI")
    parser.add_argument("--dry-run", action="store_true", help="build the placeholders but do not upload")
    args = parser.parse_args()

    index_url = TEST_PYPI_INDEX if args.test else PYPI_INDEX
    upload_url = TEST_PYPI_UPLOAD if args.test else PYPI_UPLOAD
    requested: list[str] = args.names

    to_reserve = []
    for name in requested:
        if already_registered(name, index_url):
            print(f"skip {name}: already registered on {index_url}")
        else:
            to_reserve.append(name)

    if not to_reserve:
        print("nothing to reserve")
        return

    with tempfile.TemporaryDirectory() as scratch:
        sources = Path(scratch) / "sources"
        dists = Path(scratch) / "dists"
        sources.mkdir()
        dists.mkdir()
        for name in to_reserve:
            subprocess.run(["uv", "build", str(write_placeholder(name, sources)), "--out-dir", str(dists)], check=True)
            print(f"built {name} 0.0.0 placeholder")

        if args.dry_run:
            print(f"dry run: built {len(to_reserve)} placeholder(s), not uploading")
            return

        subprocess.run(
            ["uv", "publish", "--publish-url", upload_url, *(str(p) for p in sorted(dists.iterdir()))], check=True
        )
        print(f"reserved {len(to_reserve)} project(s) on {index_url}")


if __name__ == "__main__":
    main()
