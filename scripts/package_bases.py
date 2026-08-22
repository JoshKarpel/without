"""
Emit the workspace's mypy package bases, for the `cog` block in `pyproject.toml`.

A build-time tool, kept out of every shipped package and imported only via
`cog -I scripts`.
"""

from __future__ import annotations

from pathlib import Path

REPOSITORY = Path(__file__).parent.parent


def source_roots() -> list[str]:
    """Every package's `src` directory, relative to the repository root, in sorted order."""
    return sorted(f"{path.parent.name}/src" for path in REPOSITORY.glob("packages/*/src"))


def emit() -> str:
    """The `mypy_path` entries as TOML list items, for a `cog.outl(emit())` block."""
    return "\n".join(f'    "packages/{root}",' for root in source_roots())
