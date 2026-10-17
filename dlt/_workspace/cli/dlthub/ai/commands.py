import os
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Literal, NamedTuple, Optional, Set, Tuple
import tomlkit
import tomlkit.exceptions
import yaml

from dlt.common.runtime import run_context
from dlt.common.pendulum import pendulum
from dlt._workspace.access import FULL_ACCESS, format_access, parse_access
from dlt._workspace.cli import echo as fmt, utils
from dlt._workspace.cli.utils import DEFAULT_MCP_FEATURES, mcp_stdio_args, resolve_features
from dlt._workspace.cli.formatters import parse_frontmatter
from dlt._workspace.cli.exceptions import (
    CliCommandException,
)
from dlt._workspace.cli.dlthub.ai.agents import (
    AI_AGENTS,
    COMPONENT_MARKERS,
    TComponentType,
    _AIAgent,
    InstallAction,
)
from dlt._workspace.cli.dlthub.ai.typing import TAiStatusInfo, TToolkitIndexEntry, TToolkitInfo
from dlt._workspace.cli.dlthub.ai.utils import (
    DLTHUB_AGENTS_DIR,
    build_toolkits_dependency_map,
    compute_file_hash,
    extract_toolkit_info,
    fetch_ai_status,
    fetch_secrets_list,
    fetch_secrets_update_fragment,
    fetch_secrets_view_redacted,
    fetch_workbench_toolkit_info,
    fetch_workbench_base,
    load_toolkits_index,
    read_workbench_toolkit_mcp_servers,
    read_workbench_toolkit_combined_info,
    resolve_toolkit_dependencies,
    safe_write_bytes,
    save_toolkits_index,
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
    no_default_features: bool = False,
    access: Optional[str] = None,
) -> None:
    """Start the dlt MCP server for pipeline data inspection."""
    from dlt._workspace.mcp import WorkspaceMCP

    if stdio:
        transport = "stdio"
    elif sse:
        transport = "sse"
    else:
        transport = "streamable-http"
    resolved = resolve_features(features, set() if no_default_features else None)
    fmt.echo(
        "Starting dlt MCP server with features: %s" % ", ".join(sorted(resolved)),
        err=True,
    )
    # a person at a terminal passes no --access and gets every tool
    granted = FULL_ACCESS if access is None else parse_access(access)
    fmt.echo("Access granted to the caller: %s" % format_access(granted), err=True)
    mcp_server = WorkspaceMCP("dlt", port=port, features=resolved, access=granted)
    # a stdio server has no console of its own: its banner would land in the client's terminal
    mcp_server.run(transport=transport, show_banner=not stdio)


@utils.track_command("ai", track_before=True, operation="mcp.install")
def ai_mcp_install_command(
    agent: Optional[str] = None,
    features: Optional[List[str]] = None,
    name: str = "dlt-workspace",
    overwrite: bool = False,
) -> None:
    """Install dlt MCP server config into the agent's config file."""
    project_root = Path(run_context.active().run_dir)
    variant = _resolve_agent(agent, project_root, load_toolkits_index() if agent is None else {})

    args = ["uv", "run", fmt.get_cli_host_name(), *mcp_stdio_args(features)]

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
    """Fetch dltHub AI Harness repo, warn on failure. CLI wrapper around fetch_workbench_base."""
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


class ToolkitChanges(NamedTuple):
    ordinary: List[InstallAction]
    shared: List[InstallAction]
    warnings: List[str]
    retained: Dict[Path, str]
    mcp_servers: List[str]
    """Declared server names, including servers that need no configuration change."""


class ExecutionResult(NamedTuple):
    written: List[Tuple[InstallAction, str]]
    removed: List[Path]
    error: Optional[OSError]
    unchanged: List[Tuple[InstallAction, str]]

    @property
    def installed(self) -> int:
        return len({(a.kind, a.source_name, a.source_kind) for a, _ in self.written})


def _plan_toolkit_install(
    toolkit_dir: Path,
    agent: "_AIAgent",
    project_root: Path,
    toolkit_name: str,
    overwrite: bool = False,
    previous_files: Optional[Dict[str, Any]] = None,
    other_claims: Optional[Set[str]] = None,
) -> ToolkitChanges:
    """Plan which toolkit files to write, keep, or remove, then merge shared configuration.

    Invalid components prevent cleanup because their files may still belong to the toolkit.
    """
    actions: List[InstallAction] = []
    warnings: List[str] = []

    # skills (directory-based, each with SKILL.md + optional files)
    skills_dir = toolkit_dir / "skills"
    if skills_dir.is_dir():
        for skill_path in sorted(skills_dir.iterdir()):
            if not skill_path.is_dir():
                continue
            if not (skill_path / COMPONENT_MARKERS["skill"]).is_file():
                continue
            errors = _validate_skill_dir(skill_path)
            if errors:
                for err in errors:
                    warnings.append("Skipping skill %s: %s" % (skill_path.name, err))
                continue
            actions.extend(
                agent.install_actions(
                    "skill",
                    skill_path,
                    skill_path.name,
                    toolkit_name,
                    project_root,
                    # Existing files may still be safe to update, so bypass the adapter's
                    # existence-only conflicts. This plans content without writing it;
                    # _reconcile_toolkit_files below enforces the user's overwrite flag.
                    overwrite=True,
                )
            )

    # agents (directory-based, each with AGENT.md + optional files), copied verbatim
    agents_dir = toolkit_dir / DLTHUB_AGENTS_DIR
    if agents_dir.is_dir():
        for agent_path in sorted(agents_dir.iterdir()):
            if not agent_path.is_dir():
                continue
            if not (agent_path / COMPONENT_MARKERS["agent"]).is_file():
                continue
            errors = _validate_skill_dir(agent_path)
            if errors:
                for err in errors:
                    warnings.append("Skipping agent %s: %s" % (agent_path.name, err))
                continue
            actions.extend(
                agent.install_actions(
                    "agent", agent_path, agent_path.name, toolkit_name, project_root, overwrite=True
                )
            )

    # ignore file (.claudeignore → agent-specific name)
    ignore_file = toolkit_dir / ".claudeignore"
    if ignore_file.is_file():
        raw_content = ignore_file.read_text(encoding="utf-8")
        actions.extend(
            agent.install_actions(
                "ignore", raw_content, ".claudeignore", toolkit_name, project_root, overwrite=True
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
            actions.extend(
                agent.install_actions(
                    component_type,
                    raw_content,
                    source_name,
                    toolkit_name,
                    project_root,
                    overwrite=True,
                )
            )

    # Compare shipped files with the saved hashes and disk contents before choosing
    # writes and removals, so local edits stay protected unless overwrite is requested.
    ordinary, retained = _reconcile_toolkit_files(
        actions,
        previous_files or {},
        other_claims or set(),
        agent,
        project_root,
        overwrite=overwrite,
        complete=not warnings,
    )
    # Don't add AGENTS.md registrations for skipped or removed rules.
    finalized = agent.finalize_actions(
        [a for a in ordinary if a.op != "remove"],
        project_root,
        workbench_base=toolkit_dir.parent,
    )
    shared = [a for a in finalized if a.skip_index or a.kind == "mcp"]

    # mcp server definitions (merge into agent config)
    mcp_servers = read_workbench_toolkit_mcp_servers(toolkit_dir)
    if mcp_servers:
        config_path = agent.mcp_config_path(project_root)
        existing_content = ""
        existing_servers: Dict[str, Any] = {}
        if config_path.is_file():
            existing_content = config_path.read_bytes().decode("utf-8")
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
            if merged.encode("utf-8") != existing_content.encode("utf-8"):
                shared.append(
                    InstallAction(
                        kind="mcp",
                        source_name=", ".join(sorted(new_servers)),
                        dest_path=config_path,
                        op="save",
                        content_or_path=merged,
                        conflict=False,
                    )
                )
    return ToolkitChanges(ordinary, shared, warnings, retained, sorted(mcp_servers))


def _reconcile_toolkit_files(
    shipped: List[InstallAction],
    previous_files: Dict[str, Any],
    other_claims: Set[str],
    agent: "_AIAgent",
    project_root: Path,
    *,
    overwrite: bool,
    complete: bool,
) -> Tuple[List[InstallAction], Dict[Path, str]]:
    """Reconcile shipped paths, this toolkit's record and disk contents.

    Claims by other toolkits protect deletion only. Shared documents never enter
    this calculation. Paths are checked lexically; symlinked ancestor folders work.
    """
    actions = list(shipped)
    retained: Dict[Path, str] = {}
    counts = Counter(a.dest_path for a in shipped)
    tracked = {
        project_root / p
        for p in previous_files
        if not Path(p).is_absolute() and ".." not in Path(p).parts
    }
    shared_paths = {project_root / "AGENTS.md"} | {
        host().mcp_config_path(project_root) for host in AI_AGENTS.values()
    }
    component_dirs = sorted(
        ((kind, agent.component_dir(kind, project_root)) for kind in agent._DIRS),
        key=lambda item: len(item[1].parts),
        reverse=True,
    )
    for rel in sorted(previous_files):
        path = project_root / rel
        if path in counts or path in shared_paths:
            continue
        if not complete:
            retained[path] = "source inventory is incomplete; keeping tracked orphan"
            continue
        if rel in other_claims:
            retained[path] = "also tracked by another toolkit"
            continue
        kind, base = next(
            ((kind, base) for kind, base in component_dirs if base in path.parents),
            ("ignore", project_root),
        )
        actions.append(
            InstallAction(
                kind=kind,
                source_name=rel,
                dest_path=path,
                op="remove",
                content_or_path="",
                conflict=False,
                component_dir=base,
            )
        )

    components: Dict[Optional[Path], List[InstallAction]] = {}
    for action in actions:
        components.setdefault(action.component_dir, []).append(action)

    for folder, component in components.items():
        untracked_folder = (
            folder is not None
            and folder.exists()
            and not overwrite
            and not any(folder in path.parents for path in tracked)
        )
        for action in component:
            path = action.dest_path
            reason = None
            try:
                rel = str(path.relative_to(project_root))
            except ValueError:
                rel = str(path)
                reason = "path is outside the project"
            if ".." in Path(rel).parts or Path(rel).is_absolute():
                reason = "path is outside the project"
            # An absolute index key must not be accepted even if it points inside the project.
            if str(path) in previous_files and Path(str(path)).is_absolute():
                reason = "absolute path in toolkit record"
            if path in shared_paths:
                reason = "shared document is not an ordinary toolkit file"
            elif counts[path] > 1:
                reason = "multiple components ship this destination"
            if reason is None:
                if path.is_symlink() or (path.exists() and not path.is_file()):
                    reason = "destination is not a regular file"
                elif not overwrite:
                    if not path.exists():
                        if untracked_folder:
                            reason = "component has a missing file"
                    else:
                        # Failure to read a user-owned file means we cannot adopt it.
                        # Source-read failures still propagate before any changes.
                        try:
                            disk_bytes = path.read_bytes()
                        except OSError:
                            if not untracked_folder:
                                raise
                            reason = "component has an unreadable file"
                        else:
                            if action.op == "remove" or disk_bytes != _action_bytes(action):
                                baseline = previous_files.get(rel, {}).get("sha3_256")
                                if not baseline or compute_file_hash(path) != baseline:
                                    reason = "file is modified or has no recorded hash"
            if reason:
                retained[path] = reason

        # Adopt a user-owned component only if every shipped file already matches.
        # A matching marker alone must not allow helpers to be added on the next run.
        if untracked_folder and any(a.dest_path in retained for a in component):
            for action in component:
                retained[action.dest_path] = (
                    "component folder exists and is not tracked by this toolkit"
                )

    return [a._replace(conflict=a.dest_path in retained) for a in actions], retained


def _remove_installed_file(path: Path, component_dir: Path) -> None:
    """Removes `path` and the folders it leaves empty below `component_dir`."""
    path.unlink(missing_ok=True)
    parent = path.parent
    while component_dir in parent.parents and parent.is_dir() and not any(parent.iterdir()):
        parent.rmdir()
        parent = parent.parent


def _action_bytes(action: "InstallAction") -> bytes:
    if action.op == "copy":
        return Path(action.content_or_path).read_bytes()
    return str(action.content_or_path).encode("utf-8")


def _execute_install(ordinary: List[InstallAction], shared: List[InstallAction]) -> ExecutionResult:
    """Prepare all sources, then apply writes/removals and retain completed disk hashes.

    Preparation errors propagate before mutation. Disk errors stop execution and
    travel with completed outcomes so the caller can persist them before raising.
    """
    pending = [a for a in ordinary + shared if not a.conflict]
    writes = [(a, _action_bytes(a)) for a in pending if a.op != "remove"]
    removals = [a for a in pending if a.op == "remove"]
    written: List[Tuple[InstallAction, str]] = []
    removed: List[Path] = []
    unchanged: List[Tuple[InstallAction, str]] = []
    try:
        for action, data in writes:
            try:
                matches = action.dest_path.is_file() and action.dest_path.read_bytes() == data
            except OSError:
                # Replacement was already allowed. An unreadable old file need not
                # prevent replacing it; the new file must still be readable afterward.
                matches = False
            if matches:
                unchanged.append((action, compute_file_hash(action.dest_path)))
                continue
            action.dest_path.parent.mkdir(parents=True, exist_ok=True)
            safe_write_bytes(action.dest_path, data)
            try:
                disk_hash = compute_file_hash(action.dest_path)
            except OSError as ex:
                raise OSError(
                    "Wrote %s but could not record its disk hash; rerun update to recover: %s"
                    % (action.dest_path, ex)
                ) from ex
            written.append((action, disk_hash))
        for action in removals:
            assert action.component_dir is not None
            try:
                _remove_installed_file(action.dest_path, action.component_dir)
            except OSError:
                # Unlink may have succeeded before pruning failed. Confirm absence.
                try:
                    action.dest_path.lstat()
                except FileNotFoundError:
                    removed.append(action.dest_path)
                except OSError:
                    pass
                raise
            removed.append(action.dest_path)
    except OSError as ex:
        return ExecutionResult(written, removed, ex, unchanged)
    return ExecutionResult(written, removed, None, unchanged)


def _resolve_agent(
    agent: Optional[str], project_root: Path, records: Dict[str, TToolkitIndexEntry]
) -> "_AIAgent":
    """Resolve an explicit agent name or auto-detect. Raises on failure."""
    if agent is not None:
        if agent not in AI_AGENTS:
            fmt.error("Unknown agent: %s" % agent)
            raise CliCommandException()
        return AI_AGENTS[agent]()

    # check init toolkit in .toolkits index for a previously recorded agent
    init_entry = records.get(_INIT_TOOLKIT)
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


def _report_changes(changes: ToolkitChanges, strict: bool = False) -> None:
    """Report validation and protected files before any writes, enforcing strict mode."""
    for warning in changes.warnings:
        fmt.warning(warning)
    if strict and changes.warnings:
        fmt.error(
            "%d validation error(s). Fix the issues above or run without --strict."
            % len(changes.warnings)
        )
        raise CliCommandException()
    folder_conflicts = {
        a.dest_path: a.component_dir
        for a in changes.ordinary
        if a.conflict and a.component_dir is not None
    }
    reported = set()
    for path, reason in changes.retained.items():
        display_path = (
            folder_conflicts.get(path, path) if reason.startswith("component folder") else path
        )
        if (display_path, reason) not in reported:
            fmt.warning("Keeping %s: %s" % (display_path, reason))
            reported.add((display_path, reason))


def _report_result(changes: ToolkitChanges, result: ExecutionResult) -> None:
    """Show completed changes, excluding files that already matched."""
    protected_folders = {
        a.component_dir for a in changes.ordinary if a.conflict and a.component_dir is not None
    }
    changed_paths = [a.dest_path for a, _ in result.written] + result.removed
    for folder in sorted(protected_folders):
        if not any(folder in path.parents for path in changed_paths):
            continue
        fmt.warning("Partially updated %s; locally protected files were kept." % folder)
    components: Set[Tuple[str, str, Optional[str]]] = set()
    for action, _ in result.written:
        key = (action.kind, action.source_name, action.source_kind)
        if key not in components:
            fmt.echo(
                "  + %s %s -> %s"
                % (
                    action.kind,
                    fmt.bold(action.source_name),
                    action.component_dir or action.dest_path,
                )
            )
            components.add(key)
    for path in result.removed:
        fmt.echo("  - %s" % path)


def _apply_toolkit(
    name: str,
    base: Path,
    agent: "_AIAgent",
    project_root: Path,
    records: Dict[str, TToolkitIndexEntry],
    *,
    mode: Literal["install", "update"],
    overwrite: bool = False,
    strict: bool = False,
) -> Dict[str, TToolkitIndexEntry]:
    """Apply one toolkit and save completed outcomes, including after a disk error."""
    toolkit_dir = base / name
    meta = read_workbench_toolkit_combined_info(toolkit_dir)
    if meta is None:
        fmt.warning("Toolkit %s not found (missing .claude-plugin/plugin.json)" % fmt.bold(name))
        return records
    try:
        toolkit_meta = extract_toolkit_info(meta, name)
    except ValueError as ex:
        if mode != "update":
            raise
        fmt.warning("Skipping toolkit %s: %s" % (name, ex))
        return records
    toolkit_name = toolkit_meta["name"]
    if mode == "update" and toolkit_name != name:
        fmt.warning(
            "Skipping toolkit %s: metadata names a different toolkit (%s)" % (name, toolkit_name)
        )
        return records
    local = records.get(toolkit_name)
    if mode == "install" and local and not overwrite:
        local_version = local.get("version", "?")
        if local_version == toolkit_meta["version"]:
            fmt.echo("Toolkit %s %s is already installed." % (toolkit_name, local_version))
        else:
            fmt.echo(
                "Toolkit %s %s is installed, version %s available. Use `ai toolkit update` to"
                " update." % (toolkit_name, local_version, toolkit_meta["version"])
            )
        if workflow_entry_skill := toolkit_meta.get("workflow_entry_skill"):
            fmt.echo("Use %s skill to start!" % fmt.bold(workflow_entry_skill))
        return records

    if mode == "update":
        missing = [dep for dep in toolkit_meta.get("dependencies", []) if dep not in records]
        if missing:
            fmt.warning(
                "Toolkit %s has missing dependencies: %s. Install them explicitly."
                % (toolkit_name, ", ".join(missing))
            )
    return _apply_contents(
        toolkit_dir,
        toolkit_meta,
        agent,
        project_root,
        records,
        mode=mode,
        overwrite=overwrite,
        strict=strict,
    )


def _apply_contents(
    toolkit_dir: Path,
    toolkit_meta: TToolkitInfo,
    agent: "_AIAgent",
    project_root: Path,
    records: Dict[str, TToolkitIndexEntry],
    *,
    mode: Literal["install", "update"],
    overwrite: bool = False,
    strict: bool = False,
) -> Dict[str, TToolkitIndexEntry]:
    """Apply contents and persist completed disk observations, including on failure."""
    toolkit_name = toolkit_meta["name"]
    local = records.get(toolkit_name)
    other_claims = {
        path
        for other_name, entry in records.items()
        if other_name != toolkit_name
        for path in entry.get("files", {})
    }
    changes = _plan_toolkit_install(
        toolkit_dir,
        agent,
        project_root,
        toolkit_name,
        overwrite=overwrite,
        previous_files=local.get("files", {}) if local else {},
        other_claims=other_claims,
    )
    if mode == "install" and not (
        changes.ordinary
        or changes.mcp_servers
        or changes.shared
        or changes.warnings
        or changes.retained
    ):
        fmt.echo("No components found in toolkit %s." % fmt.bold(toolkit_name))
        return records
    _report_changes(changes, strict)
    result = _execute_install(changes.ordinary, changes.shared)

    if (
        mode == "update"
        or result.written
        or result.removed
        or result.unchanged
        or (changes.mcp_servers and result.error is None)
    ):
        # Protection is an intentional outcome, not a failed update. Keep prior
        # metadata only when validation or execution could not finish.
        partial = changes.warnings or result.error
        entry: TToolkitIndexEntry = dict(local if local and partial else toolkit_meta)  # type: ignore[assignment]
        entry["agent"] = agent.name
        files = dict(local.get("files", {}) if local else {})
        shared_files = dict(local.get("shared_files", {}) if local else {})
        servers = set(local.get("mcp_servers", []) if local else [])
        if result.error is None:
            # Already configured servers satisfy the toolkit even without a merge.
            servers.update(changes.mcp_servers)
        for action, disk_hash in result.written + result.unchanged:
            rel = str(action.dest_path.relative_to(project_root))
            file_map = shared_files if action.kind == "mcp" or action.skip_index else files
            file_map[rel] = {"sha3_256": disk_hash}
            if action.kind == "mcp":
                servers.update(n.strip() for n in action.source_name.split(",") if n.strip())
        for path in result.removed:
            files.pop(str(path.relative_to(project_root)), None)
        entry["files"] = files
        if shared_files:
            entry["shared_files"] = shared_files
        if servers:
            entry["mcp_servers"] = sorted(servers)
        previous = {
            k: v for k, v in (local.items() if local is not None else []) if k != "installed_at"
        }
        entry.pop("installed_at", None)
        if entry != previous:
            entry["installed_at"] = pendulum.now("UTC").isoformat()
            records = {**records, toolkit_name: entry}
            try:
                save_toolkits_index(records)
            except (OSError, yaml.YAMLError) as save_error:
                if result.error is not None:
                    fmt.error(
                        "Toolkit %s failed: %s. Could not save completed changes: %s"
                        % (toolkit_name, result.error, save_error)
                    )
                    raise result.error from save_error
                raise
    _report_result(changes, result)
    if result.error is not None:
        fmt.warning("Could not finish toolkit %s: %s" % (toolkit_name, result.error))
        raise result.error
    if mode == "update" and changes.retained:
        fmt.echo(
            "Toolkit %s checked with %d protected path(s)." % (toolkit_name, len(changes.retained))
        )
    fmt.echo("%s item(s) installed." % fmt.bold(str(result.installed)))
    if result.installed or result.removed:
        fmt.echo("Please restart your %s session for changes to take effect" % fmt.bold(agent.name))
    if result.installed and (workflow_entry_skill := toolkit_meta.get("workflow_entry_skill")):
        fmt.echo("Use %s skill to start!" % fmt.bold(workflow_entry_skill))
    return records


def _select_install_requests(
    name: str,
    toolkits: Dict[str, TToolkitInfo],
    overwrite: bool,
    strict: bool,
) -> List[Tuple[str, bool, bool, bool]]:
    """Ordered (name, is_dependency, overwrite, strict) requests; no execution or index IO."""
    try:
        deps = resolve_toolkit_dependencies(name, build_toolkits_dependency_map(toolkits))
    except ValueError as ex:
        fmt.warning(str(ex))
        deps = []
    return [(dep, True, False, False) for dep in deps] + [(name, False, overwrite, strict)]


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
    records = load_toolkits_index()
    var = _resolve_agent(agent, project_root, records)

    fmt.echo("Initializing AI rules for %s from %s..." % (fmt.bold(var.name), fmt.bold(location)))

    base = _fetch_workbench_base_cli(location, branch)
    if base is None:
        return

    records = _apply_toolkit(
        _INIT_TOOLKIT, base, var, project_root, records, mode="install", overwrite=overwrite
    )

    status = fetch_ai_status(project_root, records)
    if "mcp_unavailable" in status["warnings"]:
        fmt.warning(f"MCP server cannot be started. Run `{fmt.cli_cmd('ai status')}` for details.")
    if var.name == "cursor":
        fmt.warning(
            "Cursor requires you to manually enable MCP servers per-project."
            " Open Cursor Settings > MCP and enable the servers installed by dlt."
        )

    if len(records) == 1:
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
    records = load_toolkits_index()
    var = _resolve_agent(agent, project_root, records)

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
    for request_name, dependency, forced, strict_request in _select_install_requests(
        name, toolkits, overwrite, strict
    ):
        if dependency and request_name in records:
            continue
        records = _apply_toolkit(
            request_name,
            base,
            var,
            project_root,
            records,
            mode="install",
            overwrite=forced,
            strict=strict_request,
        )


@utils.track_command("ai", False, "name", operation="toolkit.update")
def ai_toolkit_update_command(
    name: Optional[str],
    location: str,
    branch: Optional[str] = None,
    overwrite: bool = False,
    strict: bool = False,
) -> None:
    """Update one installed toolkit, or all, preserving locally modified ordinary files."""
    project_root = Path(run_context.active().run_dir)
    records = load_toolkits_index()
    if name is not None and name not in records:
        fmt.error("Toolkit %s is not installed." % name)
        raise CliCommandException()
    names = [name] if name is not None else list(records)
    if not names:
        fmt.echo("No installed toolkits to update.")
        return
    base = _fetch_workbench_base_cli(location, branch)
    if base is None:
        return
    for toolkit_name in names:
        recorded_agent = records[toolkit_name].get("agent")
        agent = _resolve_agent(
            recorded_agent if recorded_agent in AI_AGENTS else None, project_root, records
        )
        fmt.echo("Updating toolkit %s for %s..." % (fmt.bold(toolkit_name), fmt.bold(agent.name)))
        records = _apply_toolkit(
            toolkit_name,
            base,
            agent,
            project_root,
            records,
            mode="update",
            overwrite=overwrite,
            strict=strict,
        )


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
        ("Agents", info["agents"]),
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
