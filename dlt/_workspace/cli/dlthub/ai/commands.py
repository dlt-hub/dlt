import shutil
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple
import tomlkit
import tomlkit.exceptions

from dlt.common.runtime import run_context
from dlt._workspace.cli import echo as fmt, utils
from dlt._workspace.cli.utils import DEFAULT_MCP_FEATURES
from dlt._workspace.cli.exceptions import (
    CliCommandException,
)
from dlt._workspace.cli.dlthub.ai.agents import AI_AGENTS, _AIAgent, InstallAction
from dlt._workspace.cli.dlthub.ai.typing import TAiStatusInfo, TToolkitIndexEntry, TToolkitInfo
from dlt._workspace.cli.dlthub.ai.planning import plan_mcp_actions, plan_toolkit_components
from dlt._workspace.cli.dlthub.ai.update import (
    TFileState,
    ToolkitUpdatePlan,
    apply_toolkit_update,
    failed_dependencies,
    plan_toolkit_update,
    to_rel_path,
    toolkit_update_order,
    update_toolkit_entry,
)
from dlt._workspace.cli.dlthub.ai.utils import (
    build_toolkits_dependency_map,
    compute_file_hash,
    extract_toolkit_info,
    fetch_ai_status,
    fetch_secrets_list,
    fetch_secrets_update_fragment,
    fetch_secrets_view_redacted,
    fetch_workbench_toolkit_info,
    fetch_workbench_base,
    is_toolkit_installed,
    load_toolkits_index,
    read_workbench_toolkit_combined_info,
    resolve_toolkit_dependencies,
    safe_write_text,
    make_toolkit_entry,
    save_toolkit_entry,
    fetch_workbench_toolkits,
    _INIT_TOOLKIT,
)


@utils.track_command("ai", False, operation="secrets.list")
def ai_secrets_list_command() -> None:
    """Lists project-scoped secret file locations from TOML providers."""
    locations = fetch_secrets_list()
    fmt.echo("Secret file locations:")
    for loc in locations:
        if profile_name := loc.get("profile_name"):
            fmt.echo("  %s (profile: %s)" % (loc["path"], profile_name))
        else:
            fmt.echo("  %s" % loc["path"])


@utils.track_command("ai", False, operation="secrets.view_redacted")
def ai_secrets_view_redacted_command(path: Optional[str] = None) -> None:
    """Prints a redacted secrets TOML."""
    result = fetch_secrets_view_redacted(path)
    if result is None:
        if path:
            fmt.warning("Secrets file not found: %s" % path)
        else:
            fmt.warning("No secrets found in project providers.")
        return
    fmt.echo(result)


@utils.track_command("ai", False, operation="secrets.update_fragment")
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
    from dlt._workspace.mcp.server import resolve_features

    resolved = resolve_features(features)
    fmt.echo(
        "Starting dlt MCP server with features: %s" % ", ".join(sorted(resolved)),
        err=True,
    )
    mcp_server = WorkspaceMCP("dlt", port=port, features=resolved)
    mcp_server.run(transport=transport)


@utils.track_command("ai", track_before=True, operation="mcp.install")
def ai_mcp_install_command(
    agent: Optional[str] = None,
    features: Optional[List[str]] = None,
    name: str = "dlt-workspace",
    overwrite: bool = False,
) -> None:
    """Install dlt MCP server config into the agent's config file."""
    from dlt._workspace.mcp.server import resolve_features

    project_root = Path(run_context.active().run_dir)
    variant = _resolve_agent(agent, project_root)

    resolved = resolve_features(features)
    args = ["uv", "run", fmt.get_cli_host_name(), "ai", "mcp", "run", "--stdio"]
    if resolved != DEFAULT_MCP_FEATURES:
        args.extend(["--features"] + sorted(resolved))

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


def _plan_toolkit_install(
    toolkit_dir: Path,
    agent: "_AIAgent",
    project_root: Path,
    toolkit_name: str,
    overwrite: bool = False,
) -> Tuple[List["InstallAction"], List[str]]:
    """Scan toolkit directory and build install actions. Reads source files but does not
    write to project_root. Returns (actions, validation_warnings)."""
    components, warnings = plan_toolkit_components(
        toolkit_dir, agent, project_root, toolkit_name, overwrite
    )
    mcp_actions = plan_mcp_actions(toolkit_dir, agent, project_root, overwrite)
    shared = agent.shared_actions(components, project_root, workbench_base=toolkit_dir.parent)
    return components + mcp_actions + shared, warnings


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
            safe_write_text(action.dest_path, action.content_or_path)  # type: ignore[arg-type]
        if action.kind == "mcp":
            # source_name is ", ".join(sorted(new_servers))
            mcp_server_names.extend(s.strip() for s in action.source_name.split(",") if s.strip())
        elif action.skip_index:
            # shared merge targets (e.g. AGENTS.md) — don't track per-toolkit
            pass
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
                            rel = to_rel_path(f, project_root)
                            tracked_files[rel] = {"sha3_256": compute_file_hash(f)}
                else:
                    rel = to_rel_path(dest_path, project_root)
                    tracked_files[rel] = {"sha3_256": compute_file_hash(dest_path)}
        save_toolkit_entry(
            make_toolkit_entry(
                toolkit_meta,
                agent=agent_name,
                files=tracked_files,
                mcp_servers=sorted(mcp_server_names) if mcp_server_names else None,
            )
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
    fmt.echo(
        "%s item(s) installed. Please restart your %s session for changes to take effect"
        % (fmt.bold(str(installed)), fmt.bold(str(agent_name)))
    )
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

    toolkit_meta = extract_toolkit_info(meta, name)
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
                "Toolkit %s %s is installed, version %s available. Use `%s` to update."
                % (
                    toolkit_name,
                    local_version,
                    toolkit_version,
                    fmt.cli_cmd("ai toolkit update " + toolkit_name),
                )
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


@contextmanager
def _value_error_as_cli_error() -> Iterator[None]:
    """Reports a `ValueError` (invalid metadata, circular dependencies) as a CLI error."""
    try:
        yield
    except ValueError as ex:
        fmt.error(str(ex))
        raise CliCommandException()


def _resolve_installed_agent(name: str, entry: TToolkitIndexEntry) -> _AIAgent:
    """The agent the toolkit was installed for."""
    recorded = entry.get("agent")
    if recorded is None or recorded not in AI_AGENTS:
        fmt.error(
            "Toolkit %s was installed for unknown agent %s. Reinstall it with `%s`."
            % (
                fmt.bold(name),
                fmt.bold(str(recorded)),
                fmt.cli_cmd("ai toolkit install %s --agent <agent> --overwrite" % name),
            )
        )
        raise CliCommandException()
    return AI_AGENTS[recorded]()


def _ensure_dependencies(
    name: str,
    toolkits: Dict[str, TToolkitInfo],
    base: Path,
    agent: _AIAgent,
    project_root: Path,
) -> None:
    """Install missing dependencies of `name`, fail when any is still missing."""
    with _value_error_as_cli_error():
        closure = resolve_toolkit_dependencies(name, build_toolkits_dependency_map(toolkits))
    _install_dependencies(name, toolkits, base, agent, project_root)
    missing = [dep for dep in closure if not is_toolkit_installed(dep)]
    if missing:
        fmt.error(
            "Toolkit %s depends on %s which could not be installed."
            % (fmt.bold(name), ", ".join(fmt.bold(dep) for dep in missing))
        )
        raise CliCommandException()


def _read_workbench_toolkit_meta(name: str, toolkit_dir: Path) -> TToolkitInfo:
    """Metadata of the installed toolkit `name` as currently published in the workbench.
    Raises when the toolkit is gone or its metadata is invalid."""
    meta = read_workbench_toolkit_combined_info(toolkit_dir)
    if meta is None:
        # a renamed toolkit looks the same: its directory is gone
        fmt.error(
            "Toolkit %s is no longer in the workbench, it may have been removed or renamed."
            " Use `%s` to see available toolkits."
            % (fmt.bold(name), fmt.cli_cmd("ai toolkit list"))
        )
        raise CliCommandException()
    with _value_error_as_cli_error():
        toolkit_meta = extract_toolkit_info(meta, name)
    # the toolkit was found by its installed name, but the index entry is saved under the
    # name declared in plugin.json: a different name would add a second index entry
    if toolkit_meta["name"] != name:
        fmt.error(
            "Workbench directory %s declares toolkit name %s, expected %s."
            % (fmt.bold(name), fmt.bold(toolkit_meta["name"]), fmt.bold(name))
        )
        raise CliCommandException()
    return toolkit_meta


_SKIP_REASONS: Dict[TFileState, str] = {
    "modified": "modified locally",
    "deleted": "deleted locally",
    "untracked": "not installed by this toolkit",
}


def _report_update_plan(plan: ToolkitUpdatePlan, project_root: Path) -> None:
    """Print what an update will write and skip."""
    for w in plan.warnings:
        fmt.warning(w)
    for candidate, decision in plan.updates:
        if decision.write:
            marker = "+" if decision.state == "new" else "~"
            note = "" if decision.state in ("new", "updated") else " (forced)"
            fmt.echo("  %s %s%s" % (marker, candidate.rel_path, note))
        elif decision.state != "unchanged":
            fmt.warning("Skipping %s: %s" % (candidate.rel_path, _SKIP_REASONS[decision.state]))
    for action in plan.shared:
        rel_path = to_rel_path(action.dest_path, project_root)
        if action.kind == "mcp":
            fmt.echo("  + mcp %s -> %s" % (fmt.bold(action.source_name), rel_path))
        else:
            fmt.echo("  ~ %s" % rel_path)
    for path in plan.orphans:
        fmt.warning("%s is no longer part of the toolkit and was left in place" % path)


def _print_update_summary(name: str, plan: ToolkitUpdatePlan, agent: _AIAgent) -> None:
    written = sum(1 for _, d in plan.updates if d.write) + len(plan.shared)
    skipped = sum(1 for _, d in plan.updates if not d.write and d.state != "unchanged")
    if not written and not skipped:
        fmt.echo("Toolkit %s is up to date." % fmt.bold(name))
        return
    unchanged = sum(1 for _, d in plan.updates if d.state == "unchanged")
    fmt.echo(
        "%s updated, %s unchanged, %s skipped."
        % (fmt.bold(str(written)), unchanged, fmt.bold(str(skipped)))
    )
    if skipped:
        fmt.echo("Use --force to overwrite skipped files.")
    if written:
        fmt.echo("Please restart your %s session for changes to take effect" % fmt.bold(agent.name))


def _update_installed_toolkit(
    name: str,
    toolkits: Dict[str, TToolkitInfo],
    base: Path,
    project_root: Path,
    force: bool,
) -> None:
    """Update the installed toolkit `name` for the agent it was installed for, installing
    missing dependencies first. Raises `CliCommandException` after reporting a failure."""
    # re-read: dependency installs and earlier updates change the index
    entry = load_toolkits_index()[name]
    agent = _resolve_installed_agent(name, entry)
    _ensure_dependencies(name, toolkits, base, agent, project_root)
    _update_toolkit(name, entry, base / name, agent, project_root, force)


def _update_toolkit(
    name: str,
    entry: TToolkitIndexEntry,
    toolkit_dir: Path,
    agent: _AIAgent,
    project_root: Path,
    force: bool,
) -> None:
    """Bring one installed toolkit to the workbench content, report and record the result."""
    toolkit_meta = _read_workbench_toolkit_meta(name, toolkit_dir)
    installed_version = entry.get("version", "?")
    available_version = toolkit_meta["version"]
    if installed_version == available_version:
        version_label = installed_version
    else:
        version_label = "%s -> %s" % (installed_version, available_version)
    fmt.echo(
        "Updating toolkit %s %s for %s..." % (fmt.bold(name), version_label, fmt.bold(agent.name))
    )

    plan = plan_toolkit_update(name, entry, toolkit_dir, agent, project_root, force)
    _report_update_plan(plan, project_root)
    apply_toolkit_update(plan)
    update_toolkit_entry(plan, entry, toolkit_meta, agent.name)
    _print_update_summary(name, plan, agent)


def _warning_message(code: str) -> str:
    # built lazily so cli_cmd() reads the active CLI host name (e.g. dlt vs dlthub)
    if code == "not_initialized":
        return f"Workspace not yet initialized ({fmt.cli_cmd('init')} not yet run)"
    if code == "no_init_toolkit":
        return f"Workflow rules not available ({fmt.cli_cmd('ai init')} not yet run)"
    if code == "no_toolkits":
        return "No toolkit with workflow is installed!"
    if code == "mcp_unavailable":
        return "MCP server cannot be started due to:"
    return code


def _print_ai_status(status: TAiStatusInfo) -> None:
    """Render AI status info to the CLI."""
    fmt.echo("dlt %s" % fmt.bold(status["dlt_version"]))
    if status["agent_name"]:
        fmt.echo("Agent: %s" % fmt.bold(status["agent_name"]))

    for code in status["warnings"]:
        fmt.warning(_warning_message(code))
        if code == "mcp_unavailable" and "mcp_error" in status:
            fmt.echo("  %s" % status["mcp_error"])

    toolkits = status["toolkits"]
    if toolkits:
        fmt.echo("\nInstalled toolkits:")
        for name, entry in sorted(toolkits.items()):
            skill = entry.get("workflow_entry_skill", "")
            if skill:
                fmt.echo("  %s — start with %s skill" % (fmt.bold(name), fmt.bold(skill)))
            else:
                fmt.echo("  %s" % fmt.bold(name))


@utils.track_command("ai", False, operation="status")
def ai_status_command() -> None:
    """Show current AI setup status: dlt version, agent, toolkits, and readiness checks."""
    project_root = Path(run_context.active().run_dir)
    _print_ai_status(fetch_ai_status(project_root))


@utils.track_command("ai", False, operation="init")
def ai_init_command(
    agent: Optional[str],
    location: str,
    branch: Optional[str] = None,
    overwrite: bool = False,
) -> None:
    """Install the init toolkit into the current project."""
    project_root = Path(run_context.active().run_dir)
    var = _resolve_agent(agent, project_root)

    fmt.echo("Initializing AI rules for %s from %s..." % (fmt.bold(var.name), fmt.bold(location)))

    base = _fetch_workbench_base_cli(location, branch)
    if base is None:
        return

    _install_toolkit(_INIT_TOOLKIT, base, var, project_root, overwrite=overwrite)

    status = fetch_ai_status(project_root)
    if "mcp_unavailable" in status["warnings"]:
        fmt.warning(f"MCP server cannot be started. Run `{fmt.cli_cmd('ai status')}` for details.")
    if var.name == "cursor":
        fmt.warning(
            "Cursor requires you to manually enable MCP servers per-project."
            " Open Cursor Settings > MCP and enable the servers installed by dlt."
        )

    if len(load_toolkits_index()) == 1:
        fmt.echo()
        fmt.echo(
            f"Now you can install your first toolkit. Use `{fmt.cli_cmd('ai toolkit list')}` for"
            " more information."
        )


@utils.track_command("ai", False, "name", operation="toolkit.install")
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

    toolkits, warnings = fetch_workbench_toolkits(base, strict=strict)
    for w in warnings:
        fmt.warning(w)
    _install_dependencies(name, toolkits, base, var, project_root)
    _install_toolkit(name, base, var, project_root, overwrite=overwrite, strict=strict)


@utils.track_command("ai", False, "name", operation="toolkit.update")
def ai_toolkit_update_command(
    name: Optional[str],
    location: str,
    branch: Optional[str] = None,
    force: bool = False,
) -> None:
    """Update installed toolkits to the workbench content.

    Updates `name` or, when `None`, every installed toolkit. Files edited or deleted
    locally, or not installed by the toolkit, are skipped unless `force`.
    """
    project_root = Path(run_context.active().run_dir)
    installed = load_toolkits_index()
    if name is not None and name not in installed:
        fmt.error(
            "Toolkit %s is not installed. Use `%s` to install it."
            % (fmt.bold(name), fmt.cli_cmd("ai toolkit install " + name))
        )
        raise CliCommandException()
    if not installed:
        fmt.echo(
            "No toolkits installed. Use `%s` to see available toolkits."
            % fmt.cli_cmd("ai toolkit list")
        )
        return

    base = _fetch_workbench_base_cli(location, branch)
    if base is None:
        raise CliCommandException()
    toolkits, warnings = fetch_workbench_toolkits(base)
    for w in warnings:
        fmt.warning(w)

    dep_map = build_toolkits_dependency_map(toolkits)
    targets = [name] if name is not None else toolkit_update_order(installed, dep_map)
    # one broken toolkit must not block the others: updates already applied are safe to keep
    failed: List[str] = []
    for tk_name in targets:
        if blocked := failed_dependencies(tk_name, dep_map, failed):
            # its new version may rely on content the failed dependency did not receive
            fmt.warning(
                "Skipping toolkit %s: its dependency %s failed to update."
                % (fmt.bold(tk_name), ", ".join(fmt.bold(dep) for dep in blocked))
            )
            failed.append(tk_name)
            continue
        try:
            _update_installed_toolkit(tk_name, toolkits, base, project_root, force)
        except CliCommandException:
            # the reason was reported where the exception was raised
            failed.append(tk_name)
    if failed:
        fmt.error(
            "Could not update %s. See the messages above."
            % ", ".join(fmt.bold(tk_name) for tk_name in failed)
        )
        raise CliCommandException()


@utils.track_command("ai", False, operation="toolkit.list")
def ai_toolkit_list_command(
    location: str,
    branch: Optional[str] = None,
) -> None:
    """List available toolkits with name and description."""
    base = _fetch_workbench_base_cli(location, branch)
    if base is None:
        return
    toolkits, warnings = fetch_workbench_toolkits(base, listed_only=True)
    for w in warnings:
        fmt.warning(w)
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


@utils.track_command("ai", False, "name", operation="toolkit.info")
def ai_toolkit_info_command(
    name: str,
    location: str,
    branch: Optional[str] = None,
) -> None:
    """Show what's inside a toolkit."""
    info = fetch_workbench_toolkit_info(name, location, branch)
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
