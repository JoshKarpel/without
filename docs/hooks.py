# A mkdocs hook (https://www.mkdocs.org/user-guide/configuration/#hooks) that
# recovers pages from the workspace's own declarations rather than maintaining
# them by hand, so none can drift from the source it describes: the package
# dependency graph (from each member's pyproject.toml deps, as Mermaid), one
# mkdocstrings API-reference page per publishable package, and the root prose
# (PHILOSOPHY.md, CHANGELOG.md) copied into the site tree. The nav lists these
# pages by hand in mkdocs.yml; this hook supplies only their content.

from __future__ import annotations

import tomllib
from pathlib import Path
from typing import TYPE_CHECKING

from mkdocs.structure.files import File

if TYPE_CHECKING:
    from mkdocs.config.defaults import MkDocsConfig
    from mkdocs.structure.files import Files

REPO_ROOT = Path(__file__).parent.parent
PACKAGES_DIR = REPO_ROOT / "packages"


def load_members() -> dict[str, dict[str, object]]:
    """Map each workspace member's distribution name to its parsed `[project]`."""
    members = {}
    for pyproject in sorted(PACKAGES_DIR.glob("*/pyproject.toml")):
        project = tomllib.loads(pyproject.read_text(encoding="utf-8"))["project"]
        members[project["name"]] = project
    return members


def import_name(distribution_name: str) -> str:
    return distribution_name.replace("-", "_")


def workspace_edges(members: dict[str, dict[str, object]]) -> list[tuple[str, str]]:
    """
    Every "member depends on member" edge, from the declared dependencies.

    A dependency string is a PEP 508 specifier (`without-asgi>=1`); only the
    name before any version marker or extras can name a workspace member.
    """
    member_names = set(members)
    edges = []
    for name, project in members.items():
        dependencies = project.get("dependencies", [])
        if not isinstance(dependencies, list):
            continue
        for dependency in dependencies:
            depended = str(dependency).split(";")[0].strip()
            for separator in ("<", ">", "=", "~", "!", "[", " "):
                depended = depended.split(separator)[0]
            if depended.strip() in member_names:
                edges.append((name, depended.strip()))
    return sorted(edges)


def render_graph_page(members: dict[str, dict[str, object]]) -> str:
    lines = ["graph TD"]
    lines.extend(f"    {import_name(name)}[{name}]" for name in members)
    lines.extend(f"    {import_name(source)} --> {import_name(target)}" for source, target in workspace_edges(members))
    mermaid = "\n".join(lines)
    return (
        "# Package dependency graph\n\n"
        "Each arrow reads *depends on*. This diagram is generated from the\n"
        "`dependencies` declared in each package's `pyproject.toml`, so it cannot\n"
        "drift out of sync with the actual workspace edges.\n\n"
        f"```mermaid\n{mermaid}\n```\n"
    )


def publishable(members: dict[str, dict[str, object]]) -> list[str]:
    """The `without*` family, ordered with the core first: exactly what publishes."""
    others = sorted(name for name in members if name.startswith("without") and name != "without")
    return ["without", *others]


def render_reference_page(distribution_name: str, description: object) -> str:
    module = import_name(distribution_name)
    summary = f"{description}\n\n" if description else ""
    return f"# `{module}`\n\n{summary}::: {module}\n"


def on_files(files: Files, config: MkDocsConfig) -> Files:
    members = load_members()

    files.append(File.generated(config, "architecture/package-graph.md", content=render_graph_page(members)))

    for name in publishable(members):
        content = render_reference_page(name, members[name].get("description"))
        files.append(File.generated(config, f"reference/{import_name(name)}.md", content=content))

    for source_name, dest_uri in (("PHILOSOPHY.md", "philosophy.md"), ("CHANGELOG.md", "changelog.md")):
        source = REPO_ROOT / source_name
        files.append(File.generated(config, dest_uri, content=source.read_text(encoding="utf-8")))

    return files
