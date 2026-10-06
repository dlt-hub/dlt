"""What a workbench toolkit consists of for a given agent, as install actions.

Shared by install and update. Reads the toolkit and the agent's current MCP config,
never writes to the project.
"""
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

from dlt._workspace.cli.dlthub.ai.agents import InstallAction, TComponentType, _AIAgent
from dlt._workspace.cli.dlthub.ai.utils import read_workbench_toolkit_mcp_servers
from dlt._workspace.cli.formatters import parse_frontmatter


def validate_md_frontmatter(md_file: Path) -> Optional[str]:
    """Validate YAML frontmatter in a markdown file. Returns error message or None."""
    try:
        parse_frontmatter(md_file.read_text(encoding="utf-8"))
    except yaml.YAMLError as ex:
        return "%s: invalid YAML frontmatter: %s" % (md_file.name, ex)
    return None


def validate_skill_dir(skill_path: Path) -> List[str]:
    """Validate all markdown files in a skill directory. Returns list of errors."""
    errors: List[str] = []
    for md_file in sorted(skill_path.rglob("*.md")):
        err = validate_md_frontmatter(md_file)
        if err:
            errors.append(err)
    return errors


def plan_toolkit_components(
    toolkit_dir: Path,
    agent: _AIAgent,
    project_root: Path,
    toolkit_name: str,
    overwrite: bool = False,
) -> Tuple[List[InstallAction], List[str]]:
    """Build install actions for skills, ignore file, commands and rules, without MCP
    config and shared targets. Returns (actions, validation_warnings)."""
    actions: List[InstallAction] = []
    warnings: List[str] = []

    # skills (directory-based, each with SKILL.md + optional files)
    skills_dir = toolkit_dir / "skills"
    if skills_dir.is_dir():
        for skill_path in sorted(skills_dir.iterdir()):
            if not skill_path.is_dir() or not (skill_path / "SKILL.md").exists():
                continue
            errors = validate_skill_dir(skill_path)
            if errors:
                for err in errors:
                    warnings.append("Skipping skill %s: %s" % (skill_path.name, err))
                continue
            actions.extend(
                agent.install_actions(
                    "skill", skill_path, skill_path.name, toolkit_name, project_root, overwrite
                )
            )

    # ignore file (.claudeignore → agent-specific name)
    ignore_file = toolkit_dir / ".claudeignore"
    if ignore_file.is_file():
        raw_content = ignore_file.read_text(encoding="utf-8")
        actions.extend(
            agent.install_actions(
                "ignore", raw_content, ".claudeignore", toolkit_name, project_root, overwrite
            )
        )

    # commands and rules (markdown files with frontmatter)
    component_types: List[Tuple[TComponentType, str]] = [("command", "commands"), ("rule", "rules")]
    for component_type, dir_name in component_types:
        src_dir = toolkit_dir / dir_name
        if not src_dir.is_dir():
            continue
        for md_file in sorted(src_dir.glob("*.md")):
            err = validate_md_frontmatter(md_file)
            if err:
                warnings.append("Skipping %s %s: %s" % (component_type, md_file.stem, err))
                continue
            source_name = md_file.stem
            raw_content = md_file.read_text(encoding="utf-8")
            actions.extend(
                agent.install_actions(
                    component_type, raw_content, source_name, toolkit_name, project_root, overwrite
                )
            )

    return actions, warnings


def plan_mcp_actions(
    toolkit_dir: Path,
    agent: _AIAgent,
    project_root: Path,
    overwrite: bool = False,
) -> List[InstallAction]:
    """Build the action merging the toolkit's MCP servers into the agent config. Servers
    already in the config are left alone unless `overwrite`."""
    mcp_servers = read_workbench_toolkit_mcp_servers(toolkit_dir)
    if not mcp_servers:
        return []
    config_path = agent.mcp_config_path(project_root)
    existing_content = ""
    existing_servers: Dict[str, Any] = {}
    if config_path.is_file():
        existing_content = config_path.read_text(encoding="utf-8")
        existing_servers = agent.parse_mcp_servers(existing_content)

    new_servers = (
        dict(mcp_servers)
        if overwrite
        else {name: config for name, config in mcp_servers.items() if name not in existing_servers}
    )
    if not new_servers:
        return []
    merged = agent.merge_mcp_servers(existing_content, new_servers)
    return [
        InstallAction(
            kind="mcp",
            source_name=", ".join(sorted(new_servers)),
            dest_path=config_path,
            op="save",
            content_or_path=merged,
            conflict=False,
        )
    ]
