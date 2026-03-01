import os
import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import tomlkit
import tomlkit.exceptions
import yaml

from dlt.common.runtime import run_context
from dlt.version import __version__ as dlt_version

from dlt._workspace.cli import echo as fmt, utils
from dlt._workspace.cli.utils import make_dlt_settings_path
from dlt._workspace.cli.formatters import parse_frontmatter
from dlt._workspace.cli._scaffold_api_client import get_scaffold_files_storage
from dlt._workspace.cli.exceptions import (
    CliCommandException,
    ScaffoldApiError,
    ScaffoldSourceNotFound,
)
from dlt._workspace.cli.ai.agents import AI_AGENTS, TComponentType, _AIAgent, InstallAction
from dlt._workspace.typing import TToolkitInfo
from dlt._workspace.cli.ai.utils import (
    build_toolkits_dependency_map,
    compute_file_hash,
    copy_repo_files,
    extract_toolkit_info,
    fetch_secrets_list,
    fetch_secrets_update_fragment,
    fetch_secrets_view_redacted,
    fetch_workbench_toolkit_info,
    fetch_workbench_base,
    is_toolkit_installed,
    load_toolkits_index,
    read_workbench_toolkit_mcp_servers,
    read_workbench_toolkit_combined_info,
    resolve_toolkit_dependencies,
    save_toolkit_entry,
    scan_workbench_toolkits,
)

_INIT_TOOLKIT = "init"


def ai_context_source_setup(
    source: str,
) -> None:
    """Copies files from context API into the current working folder"""

    fmt.echo("Looking up in dltHub for rules, docs and snippets for %s..." % fmt.bold(source))
    try:
        src_storage = get_scaffold_files_storage(source)
    except ScaffoldSourceNotFound:
        fmt.warning("We have nothing for %s at dltHub yet." % fmt.bold(source))
        return
    except ScaffoldApiError as e:
        fmt.warning("There was an error connecting to the scaffold-api: %s" % str(e))
        return
    src_dir = Path(src_storage.storage_path)

    dest_dir = Path(run_context.active().run_dir)
    copied_files, count_files = copy_repo_files(src_dir, dest_dir)
    if count_files == 0:
        fmt.warning("We have nothing for %s at dltHub yet." % fmt.bold(source))
    else:
        fmt.echo(
            "%s file(s) supporting %s were copied:" % (fmt.bold(str(count_files)), fmt.bold(source))
        )
        for file in copied_files:
            fmt.echo(fmt.bold(file))


@utils.track_command("ai", False, operation="secrets_list")
def ai_secrets_list_command() -> None:
    """Lists project-scoped secret file locations from TOML providers."""
    locations = fetch_secrets_list()
    fmt.echo("Secret file locations:")
    for loc in locations:
        if profile_name := loc.get("profile_name"):
            fmt.echo("  %s (profile: %s)" % (loc["path"], profile_name))
        else:
            fmt.echo("  %s" % loc["path"])


@utils.track_command("ai", False, operation="secrets_view_redacted")
def ai_secrets_view_redacted_command(path: Optional[str] = None) -> None:
    """Prints a redacted secrets TOML.

    Without --path, shows the unified view from the project SecretsTomlProvider
    (merged from all project and global secret files). With --path, shows that exact file.
    """
    result = fetch_secrets_view_redacted(path)
    if result is None:
        if path:
            fmt.warning("Secrets file not found: %s" % path)
        else:
            fmt.warning("No secrets found in project providers.")
        return
    fmt.echo(result)


@utils.track_command("ai", False, operation="secrets_update_fragment")
def ai_secrets_update_fragment_command(fragment: str, path: str) -> None:
    """Merges a TOML fragment into secrets file and prints the redacted result."""
    try:
        result = fetch_secrets_update_fragment(fragment, path)
    except tomlkit.exceptions.TOMLKitError as ex:
        fmt.error("Invalid TOML fragment: %s" % str(ex))
        raise CliCommandException()
    fmt.echo(result)


@utils.track_command("ai", track_before=True, operation="mcp")
def ai_mcp_run_command(
    port: int = 8000,
    stdio: bool = False,
    sse: bool = False,
    features: Optional[List[str]] = None,
) -> None:
    """Start the dlt MCP server for pipeline data inspection."""
    from dlt._workspace.mcp import WorkspaceMCP

    if stdio:
        transport = "stdio"
    elif sse:
        transport = "sse"
    else:
        transport = "streamable-http"
    if transport != "stdio":
        fmt.echo("Starting dlt MCP server", err=True)
    extra_features = set(features) if features else set()
    mcp_server = WorkspaceMCP("dlt", port=port, extra_features=extra_features)
    mcp_server.run(transport=transport)


@utils.track_command("ai", track_before=True, operation="mcp-install")
def ai_mcp_install_command(
    agent: Optional[str] = None,
    features: Optional[List[str]] = None,
    name: str = "dlt-workspace",
    overwrite: bool = False,
) -> None:
    """Install dlt MCP server config into the agent's config file."""
    from dlt.common.runtime.run_context import active as active_run_context

    project_root = Path(active_run_context().run_dir)
    variant = _resolve_agent(agent, project_root)

    args = ["uv", "run", "dlt", "ai", "mcp", "run", "--stdio"]
    if features:
        args.extend(["--features"] + features)

    server_config: Dict[str, Any] = {"command": args[0], "args": args[1:], "type": "stdio"}
    new_servers = {name: server_config}

    config_path = variant.mcp_config_path(project_root)
    existing_content = ""
    if config_path.is_file():
        existing_content = config_path.read_text(encoding="utf-8")

    if not overwrite:
        existing_servers = variant.parse_mcp_servers(existing_content)
        if name in existing_servers:
            fmt.echo("MCP server %s already configured in %s" % (fmt.bold(name), config_path))
            return

    merged = variant.merge_mcp_servers(existing_content, new_servers)
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(merged, encoding="utf-8")
    fmt.echo("Installed MCP server %s in %s" % (fmt.bold(name), config_path))


def _fetch_workbench_base_cli(location: str, branch: Optional[str]) -> Optional[Path]:
    """Fetch AI workbench repo, warn on failure. CLI wrapper around fetch_workbench_base."""
    try:
        return fetch_workbench_base(location, branch)
    except FileNotFoundError as ex:
        fmt.warning(str(ex))
        return None


def _validate_md_frontmatter(md_file: Path) -> Optional[str]:
    """Validate YAML frontmatter in a markdown file. Returns error message or None."""
    try:
        parse_frontmatter(md_file.read_text(encoding="utf-8"))
    except yaml.YAMLError as ex:
        return "%s: invalid YAML frontmatter: %s" % (md_file.name, ex)
    return None


def _validate_skill_dir(skill_path: Path) -> List[str]:
    """Validate all markdown files in a skill directory. Returns list of errors."""
    errors: List[str] = []
    for md_file in sorted(skill_path.rglob("*.md")):
        err = _validate_md_frontmatter(md_file)
        if err:
            errors.append(err)
    return errors


def _plan_toolkit_install(
    toolkit_dir: Path,
    agent: "_AIAgent",
    project_root: Path,
    toolkit_name: str,
    overwrite: bool = False,
) -> Tuple[List["InstallAction"], List[str]]:
    """Scan toolkit directory and build install actions. Reads source files but does not
    write to project_root. Returns (actions, validation_warnings)."""
    actions: List[InstallAction] = []
    warnings: List[str] = []

    # skills (directory-based, each with SKILL.md + optional files)
    skills_dir = toolkit_dir / "skills"
    if skills_dir.is_dir():
        for skill_path in sorted(skills_dir.iterdir()):
            if not skill_path.is_dir() or not (skill_path / "SKILL.md").exists():
                continue
            errors = _validate_skill_dir(skill_path)
            if errors:
                for err in errors:
                    warnings.append("Skipping skill %s: %s" % (skill_path.name, err))
                continue
            _, _, out_name = agent.transform("skill", "", skill_path.name, toolkit_name)
            dest = agent.component_dir("skill", project_root) / out_name
            actions.append(
                InstallAction(
                    kind="skill",
                    source_name=skill_path.name,
                    dest_path=dest,
                    op="copytree",
                    content_or_path=skill_path,
                    conflict=not overwrite and dest.exists(),
                )
            )

    # ignore file (.claudeignore → agent-specific name)
    ignore_file = toolkit_dir / ".claudeignore"
    if ignore_file.is_file():
        raw_content = ignore_file.read_text(encoding="utf-8")
        _, out_content, out_filename = agent.transform(
            "ignore", raw_content, ".claudeignore", toolkit_name
        )
        dest = agent.component_dir("ignore", project_root) / out_filename
        actions.append(
            InstallAction(
                kind="ignore",
                source_name=".claudeignore",
                dest_path=dest,
                op="save",
                content_or_path=out_content,
                conflict=not overwrite and dest.exists(),
            )
        )

    # commands and rules (markdown files with frontmatter)
    component_types: List[Tuple[TComponentType, str]] = [("command", "commands"), ("rule", "rules")]
    for component_type, dir_name in component_types:
        src_dir = toolkit_dir / dir_name
        if not src_dir.is_dir():
            continue
        for md_file in sorted(src_dir.glob("*.md")):
            err = _validate_md_frontmatter(md_file)
            if err:
                warnings.append("Skipping %s %s: %s" % (component_type, md_file.stem, err))
                continue
            source_name = md_file.stem
            raw_content = md_file.read_text(encoding="utf-8")
            out_type, out_content, out_filename = agent.transform(
                component_type, raw_content, source_name, toolkit_name
            )
            dest = agent.component_dir(out_type, project_root) / out_filename
            if out_type == "skill":
                dest = dest / "SKILL.md"
            actions.append(
                InstallAction(
                    kind=out_type,
                    source_name=source_name,
                    dest_path=dest,
                    op="save",
                    content_or_path=out_content,
                    conflict=not overwrite and dest.exists(),
                )
            )

    # mcp server definitions (merge into agent config)
    mcp_servers = read_workbench_toolkit_mcp_servers(toolkit_dir)
    if mcp_servers:
        config_path = agent.mcp_config_path(project_root)
        existing_content = ""
        existing_servers: Dict[str, Any] = {}
        if config_path.is_file():
            existing_content = config_path.read_text(encoding="utf-8")
            existing_servers = agent.parse_mcp_servers(existing_content)

        new_servers = (
            dict(mcp_servers)
            if overwrite
            else {
                name: config for name, config in mcp_servers.items() if name not in existing_servers
            }
        )
        if new_servers:
            merged = agent.merge_mcp_servers(existing_content, new_servers)
            actions.append(
                InstallAction(
                    kind="mcp",
                    source_name=", ".join(sorted(new_servers)),
                    dest_path=config_path,
                    op="save",
                    content_or_path=merged,
                    conflict=False,
                )
            )

    return actions, warnings


def _execute_install(
    actions: List["InstallAction"],
    overwrite: bool = False,
    toolkit_meta: Optional[TToolkitInfo] = None,
    agent_name: Optional[str] = None,
    project_root: Optional[Path] = None,
) -> int:
    """Write non-conflicting actions to disk. Returns count of items installed."""
    installed = 0
    written_paths: List[Tuple[Path, str]] = []  # (dest_path, kind)
    mcp_server_names: List[str] = []
    for action in actions:
        if action.conflict:
            continue
        action.dest_path.parent.mkdir(parents=True, exist_ok=True)
        if action.op == "copytree":
            shutil.copytree(
                action.content_or_path,
                action.dest_path,
                dirs_exist_ok=overwrite,
            )
        else:
            action.dest_path.write_text(
                action.content_or_path, encoding="utf-8"  # type: ignore[arg-type]
            )
        if action.kind == "mcp":
            # source_name is ", ".join(sorted(new_servers))
            mcp_server_names.extend(s.strip() for s in action.source_name.split(",") if s.strip())
        else:
            written_paths.append((action.dest_path, action.op))
        installed += 1

    if installed > 0 and toolkit_meta is not None:
        tracked_files: Optional[Dict[str, Any]] = None
        if project_root is not None:
            tracked_files = {}
            for dest_path, op in written_paths:
                if op == "copytree":
                    for f in sorted(dest_path.rglob("*")):
                        if f.is_file():
                            rel = str(f.relative_to(project_root))
                            tracked_files[rel] = {"sha3_256": compute_file_hash(f)}
                else:
                    rel = str(dest_path.relative_to(project_root))
                    tracked_files[rel] = {"sha3_256": compute_file_hash(dest_path)}
        save_toolkit_entry(
            toolkit_meta,
            agent=agent_name,
            files=tracked_files,
            mcp_servers=sorted(mcp_server_names) if mcp_server_names else None,
        )
    return installed


def _resolve_agent(agent: Optional[str], project_root: Path) -> "_AIAgent":
    """Resolve an explicit agent name or auto-detect. Raises on failure."""
    if agent is not None:
        if agent not in AI_AGENTS:
            fmt.error("Unknown agent: %s" % agent)
            raise CliCommandException()
        return AI_AGENTS[agent]()

    # check init toolkit in .toolkits index for a previously recorded agent
    index = load_toolkits_index()
    init_entry = index.get(_INIT_TOOLKIT)
    if isinstance(init_entry, dict):
        recorded = init_entry.get("agent")
        if recorded and recorded in AI_AGENTS:
            return AI_AGENTS[recorded]()

    detected = _AIAgent.detect_all(project_root)
    if not detected:
        available = ", ".join(sorted(AI_AGENTS))
        fmt.error("Could not detect AI coding agent. Use --agent to specify one of: %s" % available)
        raise CliCommandException()
    best_agent, best_level = detected[0]
    at_best = [a for a, lvl in detected if lvl == best_level]
    if len(at_best) > 1:
        names = ", ".join(a.name for a in at_best)
        fmt.error("Multiple AI coding agents detected: %s. Use --agent to specify one." % names)
        raise CliCommandException()
    fmt.echo("Detected AI coding agent: %s" % fmt.bold(best_agent.name))
    return best_agent


def _report_and_execute(
    actions: List["InstallAction"],
    validation_warnings: List[str],
    overwrite: bool = False,
    strict: bool = False,
    toolkit_meta: Optional[TToolkitInfo] = None,
    agent_name: Optional[str] = None,
    project_root: Optional[Path] = None,
) -> int:
    """Print planned actions, skip conflicts, execute, return count installed.

    In strict mode, validation warnings are treated as errors and raise CliCommandException.
    """
    for w in validation_warnings:
        fmt.warning(w)
    if strict and validation_warnings:
        fmt.error(
            "%d validation error(s). Fix the issues above or install without --strict."
            % len(validation_warnings)
        )
        raise CliCommandException()
    for a in actions:
        if a.conflict:
            fmt.warning(
                "  Skipping %s %s (already exists at %s)" % (a.kind, a.source_name, a.dest_path)
            )
        else:
            fmt.echo("  + %s %s -> %s" % (a.kind, fmt.bold(a.source_name), a.dest_path))
    installed = _execute_install(
        actions,
        overwrite=overwrite,
        toolkit_meta=toolkit_meta,
        agent_name=agent_name,
        project_root=project_root,
    )
    fmt.echo("%s item(s) installed." % fmt.bold(str(installed)))
    workflow_entry_skill = toolkit_meta.get("workflow_entry_skill") if toolkit_meta else None
    if installed > 0 and workflow_entry_skill:
        fmt.echo("Use %s skill to start!" % fmt.bold(workflow_entry_skill))
    return installed


def _install_toolkit(
    name: str,
    base: Path,
    agent: "_AIAgent",
    project_root: Path,
    overwrite: bool = False,
    strict: bool = False,
) -> None:
    """Core install logic: read plugin.json, version-check, plan, execute."""
    toolkit_dir = base / name
    meta = read_workbench_toolkit_combined_info(toolkit_dir)
    if meta is None:
        fmt.warning(
            "Toolkit %s not found (missing %s)" % (fmt.bold(name), ".claude-plugin/plugin.json")
        )
        return

    try:
        toolkit_meta = extract_toolkit_info(meta, name)
    except ValueError as ex:
        fmt.error(str(ex))
        raise CliCommandException()
    toolkit_name = toolkit_meta["name"]
    toolkit_version = toolkit_meta["version"]

    installed_index = load_toolkits_index()
    local = installed_index.get(toolkit_name)
    if local and not overwrite:
        local_version = local.get("version", "?")
        if local_version == toolkit_version:
            fmt.echo("Toolkit %s %s is already installed." % (toolkit_name, local_version))
        else:
            fmt.echo(
                "Toolkit %s %s is installed, version %s available. Use --overwrite to update."
                % (toolkit_name, local_version, toolkit_version)
            )
        if workflow_entry_skill := toolkit_meta.get("workflow_entry_skill"):
            fmt.echo("Use %s skill to start!" % fmt.bold(workflow_entry_skill))
        return

    actions, warnings = _plan_toolkit_install(
        toolkit_dir, agent, project_root, toolkit_name, overwrite=overwrite
    )
    if not actions and not warnings:
        fmt.echo("No components found in toolkit %s." % fmt.bold(name))
        return

    _report_and_execute(
        actions,
        warnings,
        overwrite=overwrite,
        strict=strict,
        toolkit_meta=toolkit_meta,
        agent_name=agent.name,
        project_root=project_root,
    )


def _install_dependencies(
    name: str,
    toolkits: Dict[str, TToolkitInfo],
    base: Path,
    agent: "_AIAgent",
    project_root: Path,
) -> None:
    """Install upstream dependencies for `name` that are not yet installed."""
    dep_map = build_toolkits_dependency_map(toolkits)
    try:
        deps = resolve_toolkit_dependencies(name, dep_map)
    except ValueError as ex:
        fmt.warning(str(ex))
        return
    for dep in deps:
        if is_toolkit_installed(dep):
            continue
        _install_toolkit(dep, base, agent, project_root)


@utils.track_command("ai", False, operation="status")
def ai_status_command() -> None:
    """Show current AI setup status: dlt version, agent, toolkits, and readiness checks."""
    fmt.echo("dlt %s" % fmt.bold(dlt_version))

    # detect agent
    project_root = Path(run_context.active().run_dir)
    index = load_toolkits_index()
    init_entry = index.get(_INIT_TOOLKIT)
    agent_name: Optional[str] = None
    if isinstance(init_entry, dict):
        agent_name = init_entry.get("agent")
    if not agent_name:
        detected = _AIAgent.detect_all(project_root)
        if detected:
            agent_name = detected[0][0].name
    if agent_name:
        fmt.echo("Agent: %s" % fmt.bold(agent_name))

    # initialized?
    if not os.path.isfile(make_dlt_settings_path("config.toml")):
        fmt.warning("Workspace not yet initialized (dlt init not yet run)")

    # init toolkit installed?
    has_init = _INIT_TOOLKIT in index
    if not has_init:
        fmt.warning("MCP server and workflow rules not available (dlt ai init not yet run)")

    # installed toolkits (excluding init)
    toolkits = {k: v for k, v in index.items() if k != _INIT_TOOLKIT}
    if toolkits:
        fmt.echo("\nInstalled toolkits:")
        for name, entry in sorted(toolkits.items()):
            skill = entry.get("workflow_entry_skill", "")
            if skill:
                fmt.echo("  %s — start with %s skill" % (fmt.bold(name), fmt.bold(skill)))
            else:
                fmt.echo("  %s" % fmt.bold(name))
    else:
        fmt.warning("No toolkit with workflow is installed!")

    # check if MCP dependencies are available
    try:
        from dlt._workspace.mcp import WorkspaceMCP  # noqa: F401

        WorkspaceMCP("dlt")
    except Exception as ex:
        fmt.warning("MCP server cannot be started due to:")
        fmt.echo("  %s" % str(ex))


@utils.track_command("ai", False, operation="init")
def ai_init_command(
    agent: Optional[str],
    location: str,
    branch: Optional[str] = None,
) -> None:
    """Install the init toolkit into the current project."""
    project_root = Path(run_context.active().run_dir)
    var = _resolve_agent(agent, project_root)

    fmt.echo("Initializing AI rules for %s from %s..." % (fmt.bold(var.name), fmt.bold(location)))

    base = _fetch_workbench_base_cli(location, branch)
    if base is None:
        return

    _install_toolkit(_INIT_TOOLKIT, base, var, project_root)


@utils.track_command("ai", False, operation="toolkit_install")
def ai_toolkit_install_command(
    name: str,
    agent: Optional[str],
    location: str,
    branch: Optional[str] = None,
    overwrite: bool = False,
    strict: bool = False,
) -> None:
    """Install toolkit components into the current project."""
    project_root = Path(run_context.active().run_dir)
    var = _resolve_agent(agent, project_root)

    fmt.echo(
        "Installing toolkit %s for %s from %s..."
        % (fmt.bold(name), fmt.bold(var.name), fmt.bold(location))
    )

    base = _fetch_workbench_base_cli(location, branch)
    if base is None:
        return

    toolkits = scan_workbench_toolkits(base)
    _install_dependencies(name, toolkits, base, var, project_root)
    _install_toolkit(name, base, var, project_root, overwrite=overwrite, strict=strict)


@utils.track_command("ai", False, operation="toolkit_list")
def ai_toolkit_list_command(
    location: str,
    branch: Optional[str] = None,
) -> None:
    """List available toolkits with name and description."""
    base = _fetch_workbench_base_cli(location, branch)
    if base is None:
        return
    toolkits = scan_workbench_toolkits(base, listed_only=True)
    if not toolkits:
        fmt.echo("No toolkits found.")
        return

    installed = load_toolkits_index()
    installed_tks: List[TToolkitInfo] = []
    available_tks: List[TToolkitInfo] = []
    for tk in toolkits.values():
        if tk["name"] in installed:
            installed_tks.append(tk)
        else:
            available_tks.append(tk)

    if installed_tks:
        fmt.echo("Installed toolkits:")
        for tk in installed_tks:
            tk_name = tk["name"]
            description = tk["description"]
            remote_version = tk["version"]
            local_version = installed[tk_name].get("version", "?")
            if remote_version != local_version:
                ver = "%s, %s available" % (
                    fmt.bold(local_version),
                    fmt.style(remote_version, fg="yellow"),
                )
            else:
                ver = fmt.bold(local_version)
            fmt.echo("  %-20s %s (%s)" % (fmt.bold(tk_name), description, ver))

    if available_tks:
        if installed_tks:
            fmt.echo("")
        fmt.echo("Available toolkits:")
        for tk in available_tks:
            tk_name = tk["name"]
            description = tk["description"]
            ver = " (%s)" % fmt.bold(tk["version"])
            fmt.echo("  %-20s %s%s" % (fmt.bold(tk_name), description, ver))


@utils.track_command("ai", False, operation="toolkit_info")
def ai_toolkit_info_command(
    name: str,
    location: str,
    branch: Optional[str] = None,
) -> None:
    """Show what's inside a toolkit."""
    try:
        info = fetch_workbench_toolkit_info(name, location, branch)
    except (ValueError, FileNotFoundError) as ex:
        fmt.error(str(ex))
        raise CliCommandException()
    if info is None:
        fmt.warning(
            "Toolkit %s not found (missing %s)" % (fmt.bold(name), ".claude-plugin/plugin.json")
        )
        return

    fmt.echo("Toolkit: %s" % fmt.bold(info["name"]))
    if info["description"]:
        fmt.echo("  %s" % info["description"])
    if workflow_entry_skill := info.get("workflow_entry_skill"):
        fmt.echo("Use %s skill to start!" % fmt.bold(workflow_entry_skill))

    for label, items in [
        ("Skills", info["skills"]),
        ("Commands", info["commands"]),
        ("Rules", info["rules"]),
    ]:
        if items:
            fmt.echo("\n%s:" % label)
            for item in items:
                fmt.echo("  %-20s %s" % (fmt.bold(item["name"]), item["description"]))

    if info.get("mcp_servers"):
        fmt.echo("\nMCP servers:")
        for srv_name, srv_config in info["mcp_servers"].items():
            cmd = srv_config.get("command", "")
            srv_args = " ".join(srv_config.get("args", []))
            fmt.echo("  %-20s %s %s" % (fmt.bold(srv_name), cmd, srv_args))

    if info["has_ignore"]:
        fmt.echo("\nIgnore: .claudeignore")
