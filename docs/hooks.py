# A mkdocs hook (https://www.mkdocs.org/user-guide/configuration/#hooks) that
# recovers pages from the workspace's own declarations rather than maintaining
# them by hand, so none can drift from the source it describes: the package
# dependency graph (from each member's pyproject.toml deps, as Mermaid), one
# mkdocstrings API-reference page per publishable package, and the root prose
# (PHILOSOPHY.md, CHANGELOG.md) copied into the site tree. The nav lists these
# pages by hand in mkdocs.yml; this hook supplies only their content.

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

    `name` is the published distribution name; `module` is the importable
    package, which can differ: the core distributes as `without-core` but
    imports as `without` via a `[tool.uv.build-backend] module-name` override.
    """

    name: str
    module: str
    description: str
    dependencies: tuple[str, ...]


def import_name(distribution_name: str) -> str:
    return distribution_name.replace("-", "_")


def module_override(document: dict[str, object]) -> str | None:
    """The `[tool.uv.build-backend] module-name`, when a member renames its import away from its distribution."""
    tool = document.get("tool")
    uv = tool.get("uv") if isinstance(tool, dict) else None
    backend = uv.get("build-backend") if isinstance(uv, dict) else None
    override = backend.get("module-name") if isinstance(backend, dict) else None
    return override if isinstance(override, str) else None


def parse_member(document: dict[str, object]) -> Member:
    project = document["project"]
    if not isinstance(project, dict):
        raise TypeError(f"expected a [project] table, got {type(project)}")
    name = str(project["name"])
    raw_dependencies = project.get("dependencies", [])
    dependencies = tuple(str(dep) for dep in raw_dependencies) if isinstance(raw_dependencies, list) else ()
    return Member(
        name=name,
        module=module_override(document) or import_name(name),
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


def publishable(members: dict[str, Member]) -> list[str]:
    """The `without*` family, ordered with the core first: exactly what publishes."""
    others = sorted(name for name in members if name.startswith("without") and name != "without-core")
    return ["without-core", *others]


def render_reference_page(member: Member) -> str:
    summary = f"{member.description}\n\n" if member.description else ""
    return f"# `{member.module}`\n\n{summary}::: {member.module}\n"


def on_files(files: Files, config: MkDocsConfig) -> Files:
    members = load_members()

    files.append(File.generated(config, "architecture/package-graph.md", content=render_graph_page(members)))

    for name in publishable(members):
        files.append(
            File.generated(config, f"reference/{members[name].module}.md", content=render_reference_page(members[name]))
        )

    for source_name, dest_uri in (("PHILOSOPHY.md", "philosophy.md"), ("CHANGELOG.md", "changelog.md")):
        source = REPO_ROOT / source_name
        files.append(File.generated(config, dest_uri, content=source.read_text(encoding="utf-8")))

    return files
