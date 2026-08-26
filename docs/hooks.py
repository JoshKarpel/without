# A mkdocs hook (https://www.mkdocs.org/user-guide/configuration/#hooks) that
# recovers pages from the workspace's own declarations rather than maintaining
# them by hand, so none can drift from the source it describes: the package
# dependency graph (from the published `without*` packages' pyproject.toml deps,
# as Mermaid), one mkdocstrings API-reference page per publishable package
# (emitted as `<package>/reference.md`, alongside that package's hand-written
# guide), and the root prose (PHILOSOPHY.md, CHANGELOG.md) copied into the site
# tree. The nav lists these pages by hand in mkdocs.yml; this hook supplies only
# their content.

from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from mkdocs.structure.files import File

if TYPE_CHECKING:
    from mkdocs.config.defaults import MkDocsConfig
    from mkdocs.structure.files import Files

REPO_ROOT = Path(__file__).parent.parent
PACKAGES_DIR = REPO_ROOT / "packages"


@dataclass(frozen=True, slots=True)
class Member:
    """
    A workspace member, parsed from its `pyproject.toml`.

    `name` is the published distribution name and `module` is the importable
    package; every member's module is its distribution name with `-` normalized
    to `_`.
    """

    name: str
    module: str
    description: str
    dependencies: tuple[str, ...]


def import_name(distribution_name: str) -> str:
    return distribution_name.replace("-", "_")


def parse_member(document: dict[str, object]) -> Member:
    project = document["project"]
    if not isinstance(project, dict):
        raise TypeError(f"expected a [project] table, got {type(project)}")
    name = str(project["name"])
    raw_dependencies = project.get("dependencies", [])
    dependencies = tuple(str(dep) for dep in raw_dependencies) if isinstance(raw_dependencies, list) else ()
    return Member(
        name=name,
        module=import_name(name),
        description=str(project.get("description", "")),
        dependencies=dependencies,
    )


def load_members() -> dict[str, Member]:
    """Map each workspace member's distribution name to its parsed `pyproject.toml`."""
    members = {}
    for pyproject in sorted(PACKAGES_DIR.glob("*/pyproject.toml")):
        member = parse_member(tomllib.loads(pyproject.read_text(encoding="utf-8")))
        members[member.name] = member
    return members


def workspace_edges(members: dict[str, Member]) -> list[tuple[str, str]]:
    """
    Every "member depends on member" edge, from the declared dependencies.

    A dependency string is a PEP 508 specifier (`without-asgi>=1`); only the
    name before any version marker or extras can name a workspace member.
    """
    member_names = set(members)
    edges = []
    for name, member in members.items():
        for dependency in member.dependencies:
            depended = dependency.split(";")[0].strip()
            for separator in ("<", ">", "=", "~", "!", "[", " "):
                depended = depended.split(separator)[0]
            if depended.strip() in member_names:
                edges.append((name, depended.strip()))
    return sorted(edges)


def render_graph_page(members: dict[str, Member]) -> str:
    published = {name: members[name] for name in publishable(members)}
    # Bottom-to-top so the most depended-on packages (arrow heads) rank at the top
    # and dependents build upward from them, while each arrow still reads "depends on".
    lines = ["graph BT"]
    lines.extend(f"    {import_name(name)}[{name}]" for name in published)
    lines.extend(
        f"    {import_name(source)} --> {import_name(target)}" for source, target in workspace_edges(published)
    )
    mermaid = "\n".join(lines)
    return (
        "# Package dependency graph\n\n"
        "Each arrow reads *depends on*. This diagram is generated from the\n"
        "`dependencies` declared in each published `without*` package's\n"
        "`pyproject.toml`, so it cannot drift out of sync with the actual\n"
        "workspace edges.\n\n"
        f"```mermaid\n{mermaid}\n```\n"
    )


def publishable(members: dict[str, Member]) -> list[str]:
    """The `without*` family: exactly what publishes."""
    return sorted(name for name in members if name.startswith("without"))


def render_reference_page(member: Member) -> str:
    summary = f"{member.description}\n\n" if member.description else ""
    return f"# `{member.module}`\n\n{summary}::: {member.module}\n"


def on_files(files: Files, config: MkDocsConfig) -> Files:
    members = load_members()

    files.append(File.generated(config, "architecture/package-graph.md", content=render_graph_page(members)))

    for name in publishable(members):
        member = members[name]
        files.append(File.generated(config, f"{member.name}/reference.md", content=render_reference_page(member)))

    root_pages = (
        ("PHILOSOPHY.md", "philosophy.md"),
        ("CHANGELOG.md", "changelog.md"),
    )
    for source_name, dest_uri in root_pages:
        source = REPO_ROOT / source_name
        files.append(File.generated(config, dest_uri, content=source.read_text(encoding="utf-8")))

    return files
