# /// script
# requires-python = ">=3.14"
# ///
"""
Stamp the shared release version and pin intra-workspace dependencies.

The workspace publishes every `without*` member at one lockstep version (issue
#18). A built distribution strips `[tool.uv.sources]`, so a sibling dependency
declared as a bare name would ship with no version bound and could resolve
against an incompatible release. Run this against a release checkout before
building: it rewrites each publishable member's own version and pins its
sibling dependencies to `== <version>`, so every wheel pins exactly the
siblings it was built against.

The edits are deliberately not committed; they exist only in the publish
workflow's ephemeral checkout.
"""

from __future__ import annotations

import re
import sys
import tomllib
from pathlib import Path

PACKAGES_DIR = Path(__file__).resolve().parent.parent / "packages"


def requirement_name(requirement: str) -> str:
    """The distribution name from a PEP 508 requirement (`without-core>=1` -> `without-core`)."""
    return re.split(r"[<>=!~;\[ ]", requirement, maxsplit=1)[0].strip()


def stamp_and_pin(pyproject_text: str, version: str, siblings: frozenset[str]) -> str:
    """
    Set the member's own version to `version` and pin each sibling dependency to it.

    Only dependencies naming a `sibling` are rewritten; third-party requirements
    are left untouched. Editing the raw text (rather than re-serializing the
    parsed document) keeps formatting and comments stable.
    """
    document = tomllib.loads(pyproject_text)
    project = document["project"]
    pinned = pyproject_text.replace(f'version = "{project["version"]}"', f'version = "{version}"', 1)
    for requirement in project.get("dependencies", []):
        name = requirement_name(requirement)
        if name in siblings:
            pinned = pinned.replace(f'"{requirement}"', f'"{name}=={version}"')
    return pinned


def publishable_pyprojects(packages_dir: Path) -> list[Path]:
    """Each publishable member's `pyproject.toml`: the `without*` family the publish glob selects."""
    return sorted(packages_dir.glob("without*/pyproject.toml"))


def main(version: str, packages_dir: Path) -> None:
    names = {
        path: str(tomllib.loads(path.read_text(encoding="utf-8"))["project"]["name"])
        for path in publishable_pyprojects(packages_dir)
    }
    siblings = frozenset(names.values())
    for path, name in names.items():
        path.write_text(stamp_and_pin(path.read_text(encoding="utf-8"), version, siblings), encoding="utf-8")
        print(f"pinned {name} -> {version}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit(f"usage: {Path(sys.argv[0]).name} <version>")
    main(sys.argv[1], PACKAGES_DIR)
