import os
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path, PureWindowsPath
from typing import Any, Dict, List, NamedTuple, Optional, Set, Tuple
import tomlkit
import tomlkit.exceptions
import yaml

from dlt.common.runtime import run_context
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

    new_servers = _select_mcp_servers(
        new_servers, variant.parse_mcp_servers(existing_content), overwrite
    )
    if not new_servers:
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


class ToolkitCandidates(NamedTuple):
    ordinary: List[InstallAction]
    mcp_servers: Dict[str, Any]
    warnings: List[str]


class ToolkitChanges(NamedTuple):
    ordinary: List[InstallAction]
    shared: List[InstallAction]
    warnings: List[str]
    kept: Dict[Path, str]
    blocked: Dict[Path, str]
    released: List[Path]
    mcp_servers: List[str]
    """Declared server names, including servers that need no configuration change."""
    mcp_path: Path

    @property
    def is_empty(self) -> bool:
        return not (
            self.ordinary
            or self.mcp_servers
            or self.shared
            or self.warnings
            or self.kept
            or self.blocked
            or self.released
        )


class Refusal(NamedTuple):
    """Why an action must not run; `fatal` blocks the toolkit, otherwise the path is kept."""

    reason: str
    fatal: bool


class ExecutionResult(NamedTuple):
    written: List[Tuple[InstallAction, Optional[str]]]
    """Completed writes; shared-document outcomes have no ownership hash."""
    removed: List[Path]
    error: Optional[OSError]
    unchanged: List[Tuple[InstallAction, Optional[str]]]

    @property
    def installed(self) -> int:
        return len({(a.kind, a.source_name, a.source_kind) for a, _ in self.written})


def _collect_toolkit_candidates(
    workbench_dir: Path,
    agent: "_AIAgent",
    project_root: Path,
    toolkit_name: str,
) -> ToolkitCandidates:
    """Collect transformed source files and validation warnings, without replacement policy."""
    actions: List[InstallAction] = []
    warnings: List[str] = []

    # skills and agents are directories with a marker file, copied verbatim
    folder_types: List[Tuple[TComponentType, str]] = [
        ("skill", "skills"),
        ("agent", DLTHUB_AGENTS_DIR),
    ]
    for component_type, dir_name in folder_types:
        src_dir = workbench_dir / dir_name
        if not src_dir.is_dir():
            continue
        for component_path in sorted(src_dir.iterdir()):
            if not component_path.is_dir():
                continue
            if not (component_path / COMPONENT_MARKERS[component_type]).is_file():
                continue
            errors = _validate_skill_dir(component_path)
            if errors:
                for err in errors:
                    warnings.append(
                        "Skipping %s %s: %s" % (component_type, component_path.name, err)
                    )
                continue
            actions.extend(
                agent.install_actions(
                    component_type,
                    component_path,
                    component_path.name,
                    toolkit_name,
                    project_root,
                )
            )

    # ignore file (.claudeignore → agent-specific name)
    ignore_file = workbench_dir / ".claudeignore"
    if ignore_file.is_file():
        raw_content = ignore_file.read_text(encoding="utf-8")
        actions.extend(
            agent.install_actions(
                "ignore", raw_content, ".claudeignore", toolkit_name, project_root
            )
        )

    # commands and rules (markdown files with frontmatter)
    component_types: List[Tuple[TComponentType, str]] = [("command", "commands"), ("rule", "rules")]
    for component_type, dir_name in component_types:
        src_dir = workbench_dir / dir_name
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
                )
            )

    return ToolkitCandidates(actions, read_workbench_toolkit_mcp_servers(workbench_dir), warnings)


def _select_mcp_servers(
    desired: Dict[str, Any], existing: Dict[str, Any], overwrite: bool
) -> Dict[str, Any]:
    """Select shared keys to merge; unrelated keys always remain with their owner."""
    return {name: config for name, config in desired.items() if overwrite or name not in existing}


def _materialize_mcp(
    desired: Dict[str, Any], agent: "_AIAgent", project_root: Path, overwrite: bool
) -> List[InstallAction]:
    config_path = agent.mcp_config_path(project_root)
    existing = config_path.read_bytes().decode("utf-8") if config_path.is_file() else ""
    selected = _select_mcp_servers(desired, agent.parse_mcp_servers(existing), overwrite)
    if not selected:
        return []
    merged = agent.merge_mcp_servers(existing, selected)
    if agent.parse_mcp_servers(merged) == agent.parse_mcp_servers(existing):
        return []
    return [InstallAction("mcp", ", ".join(sorted(selected)), config_path, "save", merged)]


def _plan_toolkit_install(
    workbench_dir: Path,
    agent: "_AIAgent",
    project_root: Path,
    toolkit_name: str,
    overwrite: bool = False,
    previous_files: Optional[Dict[str, Any]] = None,
    other_claims: Optional[Dict[str, Set[str]]] = None,
    *,
    update: bool = False,
) -> ToolkitChanges:
    """Resolve candidates before materializing shared documents; no disk mutations."""
    candidates = _collect_toolkit_candidates(workbench_dir, agent, project_root, toolkit_name)
    ordinary, kept, blocked, released = _reconcile_toolkit_files(
        candidates.ordinary,
        previous_files or {},
        other_claims or {},
        agent,
        project_root,
        overwrite=overwrite,
        complete=not candidates.warnings,
        update=update,
    )
    finalized = agent.finalize_actions(
        [a for a in ordinary if a.op != "remove"],
        project_root,
        workbench_base=workbench_dir.parent,
    )
    shared = [a for a in finalized if a.skip_index or a.kind == "mcp"]
    if candidates.mcp_servers:
        shared.extend(_materialize_mcp(candidates.mcp_servers, agent, project_root, overwrite))
    return ToolkitChanges(
        ordinary,
        shared,
        candidates.warnings,
        kept,
        blocked,
        released,
        sorted(candidates.mcp_servers),
        agent.mcp_config_path(project_root),
    )


def _reconcile_toolkit_files(
    shipped: List[InstallAction],
    previous_files: Dict[str, Any],
    other_claims: Dict[str, Set[str]],
    agent: "_AIAgent",
    project_root: Path,
    *,
    overwrite: bool,
    complete: bool,
    update: bool = False,
) -> Tuple[List[InstallAction], Dict[Path, str], Dict[Path, str], List[Path]]:
    """Own ordinary-file replacement, deletion and ownership policy.

    Updates preflight the whole toolkit. Foreign-owned components are skipped
    in full, even with overwrite. Ambiguous ownership blocks the operation.
    Fresh installs may also skip untracked collisions.
    A forced replacement never removes a local edit without a replacement or
    changes a file claimed by another toolkit.
    """
    accepted: List[InstallAction] = []
    kept: Dict[Path, str] = {}
    blocked: Dict[Path, str] = {}
    released: List[Path] = []
    recorded: Dict[str, Any] = {}
    for rel, record in ((_index_key(k), v) for k, v in previous_files.items()):
        if Path(rel).is_absolute() or ".." in Path(rel).parts:
            blocked[project_root / rel] = "toolkit record points outside the project"
        else:
            recorded[rel] = record

    counts = Counter(a.dest_path for a in shipped)
    actions = list(shipped)
    for rel in sorted(recorded):
        path = project_root / rel
        if path in counts:
            continue
        if not complete:
            kept[path] = "source inventory is incomplete; keeping tracked orphan"
        elif rel in other_claims:
            released.append(path)
        else:
            actions.append(_removal_action(agent, project_root, rel))

    tracked = {project_root / rel for rel in recorded}
    shared_paths = {project_root / "AGENTS.md"} | {
        host().mcp_config_path(project_root) for host in AI_AGENTS.values()
    }
    fatal = update or bool(previous_files)
    for action in actions:
        path = action.dest_path
        if action.op != "remove":
            # ownership is decided for the whole component, before any per-file check
            component = action.component_dir or path
            refusal = _ownership_refusal(component, other_claims, tracked, project_root)
            if refusal:
                (blocked if refusal.fatal else kept)[component] = refusal.reason
                continue
        rel = _project_relpath(path, project_root)
        if path in shared_paths:
            refusal = Refusal("shared document is not an ordinary toolkit file", fatal)
        elif counts[path] > 1:
            refusal = Refusal("multiple components ship this destination", fatal)
        elif rel is None:
            refusal = Refusal("path is outside the project", fatal)
        else:
            refusal = _disk_refusal(action, rel, recorded, tracked, overwrite, fatal)
        if refusal:
            (blocked if refusal.fatal else kept)[path] = refusal.reason
        else:
            accepted.append(action)
    return accepted, kept, blocked, released


def _ownership_refusal(
    component: Path, other_claims: Dict[str, Set[str]], tracked: Set[Path], project_root: Path
) -> Optional[Refusal]:
    """Refuses a component other toolkits claim: skipped for one owner, fatal when ambiguous."""
    owners = {
        owner
        for rel, names in other_claims.items()
        if project_root / rel == component or component in (project_root / rel).parents
        for owner in names
    }
    if not owners:
        return None
    if len(owners) > 1 or any(p == component or component in p.parents for p in tracked):
        return Refusal("ambiguous ownership with %s" % ", ".join(sorted(owners)), True)
    return Refusal("owned by %s; skipping this component" % next(iter(owners)), False)


def _removal_action(agent: "_AIAgent", project_root: Path, rel: str) -> InstallAction:
    """Remove action for a recorded file, pruning up to its innermost component folder."""
    path = project_root / rel
    component_dirs = sorted(
        ((kind, agent.component_dir(kind, project_root)) for kind in agent._DIRS),
        key=lambda item: len(item[1].parts),
        reverse=True,
    )
    kind, base = next(
        ((kind, base) for kind, base in component_dirs if base in path.parents),
        ("ignore", project_root),
    )
    return InstallAction(kind, rel, path, "remove", "", component_dir=base)


def _project_relpath(path: Path, project_root: Path) -> Optional[str]:
    try:
        rel = path.relative_to(project_root)
    except ValueError:
        return None
    return None if ".." in rel.parts else rel.as_posix()


def _index_key(rel: str) -> str:
    """Index keys use `/` so records stay valid across platforms."""
    return PureWindowsPath(rel).as_posix()


def _disk_refusal(
    action: InstallAction,
    rel: str,
    recorded: Dict[str, Any],
    tracked: Set[Path],
    overwrite: bool,
    fatal: bool,
) -> Optional[Refusal]:
    """Refuses replacing or removing the file on disk, or returns None when the action may run."""
    path = action.dest_path
    writes = action.op != "remove"
    if path.is_symlink() or (path.exists() and not path.is_file()):
        return Refusal("destination is not a regular file", fatal)
    if writes and not path.exists() and rel in recorded and not overwrite:
        return Refusal("tracked file was deleted locally", True)
    # check the recorded baseline even when disk already matches upstream
    if path.is_file() and (not overwrite or not writes):
        baseline = recorded.get(rel, {}).get("sha3_256")
        if not baseline or compute_file_hash(path) != baseline:
            # an overwrite has no upstream replacement for an edited orphan
            return Refusal(
                "file is modified or has no recorded hash", fatal and (writes or not overwrite)
            )
    if overwrite or not writes:
        return None
    folder = action.component_dir
    if folder is not None and folder.exists() and not any(folder in p.parents for p in tracked):
        return Refusal("component folder exists and is not tracked by this toolkit", fatal)
    return None


def _remove_installed_file(path: Path, component_dir: Path) -> None:
    """Removes `path` and the folders it leaves empty below `component_dir`."""
    path.unlink(missing_ok=True)
    parent = path.parent
    while component_dir in parent.parents and parent.is_dir() and not any(parent.iterdir()):
        parent.rmdir()
        parent = parent.parent


def _remove_confirmed(action: InstallAction, removed: List[Path]) -> None:
    """Removes the action's file, appending it to `removed` once it is gone, even on error."""
    assert action.component_dir is not None
    try:
        _remove_installed_file(action.dest_path, action.component_dir)
    except OSError:
        # unlink may have succeeded before pruning failed
        try:
            action.dest_path.lstat()
        except FileNotFoundError:
            removed.append(action.dest_path)
        except OSError:
            pass
        raise
    removed.append(action.dest_path)


def _action_bytes(action: "InstallAction") -> bytes:
    if action.op == "copy":
        return Path(action.content_or_path).read_bytes()
    return str(action.content_or_path).encode("utf-8")


def _execute_install(ordinary: List[InstallAction], shared: List[InstallAction]) -> ExecutionResult:
    """Prepare all sources, then apply writes/removals and retain completed disk hashes.

    Preparation errors propagate before mutation. Disk errors stop execution and
    travel with completed outcomes so the caller can persist them before raising.
    """
    pending = ordinary + shared
    tracked_paths = {a.dest_path for a in ordinary}
    writes = [(a, _action_bytes(a)) for a in pending if a.op != "remove"]
    removals = [a for a in pending if a.op == "remove"]
    written: List[Tuple[InstallAction, Optional[str]]] = []
    removed: List[Path] = []
    unchanged: List[Tuple[InstallAction, Optional[str]]] = []

    def tracked_hash(path: Path) -> Optional[str]:
        return compute_file_hash(path) if path in tracked_paths else None

    try:
        for action, data in writes:
            try:
                matches = action.dest_path.is_file() and action.dest_path.read_bytes() == data
            except OSError:
                # Replacement was already allowed. An unreadable old file need not
                # prevent replacing it; the new file must still be readable afterward.
                matches = False
            if matches:
                unchanged.append((action, tracked_hash(action.dest_path)))
                continue
            action.dest_path.parent.mkdir(parents=True, exist_ok=True)
            safe_write_bytes(action.dest_path, data)
            try:
                disk_hash = tracked_hash(action.dest_path)
            except OSError as ex:
                raise OSError(
                    "Wrote %s but could not record its disk hash; rerun update --overwrite to"
                    " recover: %s" % (action.dest_path, ex)
                ) from ex
            written.append((action, disk_hash))
        for action in removals:
            _remove_confirmed(action, removed)
    except OSError as ex:
        return ExecutionResult(written, removed, ex, unchanged)
    return ExecutionResult(written, removed, None, unchanged)


def _resolve_agent(
    agent: Optional[str], project_root: Path, installed_toolkits: Dict[str, TToolkitIndexEntry]
) -> "_AIAgent":
    """Resolve an explicit agent name or auto-detect. Raises on failure."""
    if agent is not None:
        if agent not in AI_AGENTS:
            fmt.error("Unknown agent: %s" % agent)
            raise CliCommandException()
        return AI_AGENTS[agent]()

    # check init toolkit in .toolkits index for a previously recorded agent
    init_entry = installed_toolkits.get(_INIT_TOOLKIT)
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


def _report_changes(changes: ToolkitChanges) -> None:
    """Describe preflight outcomes without deciding whether execution is allowed."""
    for warning in changes.warnings:
        fmt.warning(warning)
    for path, reason in changes.kept.items():
        fmt.warning("Keeping %s: %s" % (path, reason))
    for path, reason in changes.blocked.items():
        fmt.warning("Cannot update %s: %s" % (path, reason))


def _report_result(result: ExecutionResult) -> None:
    """Show completed changes, excluding files that already matched."""
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


def _read_workbench_info(workbench_dir: Path, name: str) -> Optional[Dict[str, Any]]:
    """Read a toolkit's metadata from the workbench, warning when it has none."""
    meta = read_workbench_toolkit_combined_info(workbench_dir)
    if meta is None:
        fmt.warning(
            "Toolkit %s not found in the workbench (missing .claude-plugin/plugin.json)"
            % fmt.bold(name)
        )
    return meta


def _skip_or_fail(name: str, reason: str, strict: bool) -> None:
    if strict:
        fmt.error("Cannot update toolkit %s: %s" % (name, reason))
        raise CliCommandException()
    fmt.warning("Skipping toolkit %s: %s" % (name, reason))


def _toolkit_needs_update(installed_version: str, available_version: str) -> bool:
    return installed_version != available_version


def _install_toolkit(
    name: str,
    base: Path,
    agent: "_AIAgent",
    project_root: Path,
    installed_toolkits: Dict[str, TToolkitIndexEntry],
    *,
    overwrite: bool = False,
    strict: bool = False,
) -> Dict[str, TToolkitIndexEntry]:
    """Install one toolkit unless it is already recorded.

    Raises:
        ValueError: If the toolkit metadata is invalid, before any write.
        CliCommandException: If preflight blocks the operation, before any write.
        OSError: If a write or removal fails, after completed changes are recorded.
    """
    workbench_dir = base / name
    meta = _read_workbench_info(workbench_dir, name)
    if meta is None:
        return installed_toolkits
    workbench_info = extract_toolkit_info(meta, name)
    toolkit_name = workbench_info["name"]
    installed_toolkit_info = installed_toolkits.get(toolkit_name)
    if installed_toolkit_info and not overwrite:
        installed_version = installed_toolkit_info.get("version", "?")
        if not _toolkit_needs_update(installed_version, workbench_info["version"]):
            fmt.echo("Toolkit %s %s is already installed." % (toolkit_name, installed_version))
        else:
            fmt.echo(
                "Toolkit %s %s is installed, version %s available. Use `ai toolkit update` to"
                " update." % (toolkit_name, installed_version, workbench_info["version"])
            )
        if workflow_entry_skill := workbench_info.get("workflow_entry_skill"):
            fmt.echo("Use %s skill to start!" % fmt.bold(workflow_entry_skill))
        return installed_toolkits
    return _apply_contents(
        workbench_dir,
        workbench_info,
        agent,
        project_root,
        installed_toolkits,
        overwrite=overwrite,
        strict=strict,
    )


def _update_toolkit(
    name: str,
    base: Path,
    agent: "_AIAgent",
    project_root: Path,
    installed_toolkits: Dict[str, TToolkitIndexEntry],
    *,
    overwrite: bool = False,
    strict: bool = False,
) -> Dict[str, TToolkitIndexEntry]:
    """Update one recorded toolkit. Missing, invalid or renamed sources are skipped.

    Raises:
        CliCommandException: If preflight blocks the operation, before any write, or
            with `strict` when the source is skipped or dependencies are missing.
        OSError: If a write or removal fails, after completed changes are recorded.
    """
    workbench_dir = base / name
    meta = read_workbench_toolkit_combined_info(workbench_dir)
    if meta is None:
        _skip_or_fail(
            name, "not found in the workbench (missing .claude-plugin/plugin.json)", strict
        )
        return installed_toolkits
    try:
        workbench_info = extract_toolkit_info(meta, name)
    except ValueError as ex:
        _skip_or_fail(name, str(ex), strict)
        return installed_toolkits
    if workbench_info["name"] != name:
        _skip_or_fail(
            name, "metadata names a different toolkit (%s)" % workbench_info["name"], strict
        )
        return installed_toolkits
    missing = [
        dep for dep in workbench_info.get("dependencies", []) if dep not in installed_toolkits
    ]
    if missing:
        message = "Toolkit %s has missing dependencies: %s. Install them explicitly." % (
            name,
            ", ".join(missing),
        )
        if strict:
            fmt.error(message)
            raise CliCommandException()
        fmt.warning(message)
    return _apply_contents(
        workbench_dir,
        workbench_info,
        agent,
        project_root,
        installed_toolkits,
        overwrite=overwrite,
        strict=strict,
        update=True,
    )


def _apply_contents(
    workbench_dir: Path,
    workbench_info: TToolkitInfo,
    agent: "_AIAgent",
    project_root: Path,
    installed_toolkits: Dict[str, TToolkitIndexEntry],
    *,
    overwrite: bool = False,
    strict: bool = False,
    update: bool = False,
) -> Dict[str, TToolkitIndexEntry]:
    """Plan, write and record one toolkit's files.

    Raises:
        CliCommandException: If preflight blocks the operation, before any write.
        OSError: If a write or removal fails, after completed changes are recorded.
    """
    toolkit_name = workbench_info["name"]
    installed_toolkit_info = installed_toolkits.get(toolkit_name)
    changes = _plan_toolkit_install(
        workbench_dir,
        agent,
        project_root,
        toolkit_name,
        overwrite=overwrite,
        previous_files=installed_toolkit_info.get("files", {}) if installed_toolkit_info else {},
        other_claims=_paths_claimed_by_others(installed_toolkits, toolkit_name),
        update=update,
    )
    if installed_toolkit_info is None and changes.is_empty:
        fmt.echo("No components found in toolkit %s." % fmt.bold(toolkit_name))
        return installed_toolkits
    _report_changes(changes)
    if changes.blocked or (changes.warnings and (strict or update)):
        fmt.error(
            "Toolkit %s was not changed. Resolve the warnings above and retry." % toolkit_name
        )
        raise CliCommandException()
    result = _execute_install(changes.ordinary, changes.shared)
    entry = _next_index_entry(
        installed_toolkit_info, workbench_info, agent.name, project_root, changes, result
    )
    if entry is not None:
        installed_toolkits = {**installed_toolkits, toolkit_name: entry}
        _save_completed_changes(installed_toolkits, toolkit_name, result.error)
    _report_result(result)
    if result.error is not None:
        fmt.warning("Could not finish toolkit %s: %s" % (toolkit_name, result.error))
        raise result.error
    if installed_toolkit_info is not None and changes.kept:
        fmt.echo("Toolkit %s checked with %d kept path(s)." % (toolkit_name, len(changes.kept)))
    fmt.echo("%s item(s) installed." % fmt.bold(str(result.installed)))
    if result.installed or result.removed:
        fmt.echo("Please restart your %s session for changes to take effect" % fmt.bold(agent.name))
    if result.installed and (workflow_entry_skill := workbench_info.get("workflow_entry_skill")):
        fmt.echo("Use %s skill to start!" % fmt.bold(workflow_entry_skill))
    return installed_toolkits


def _paths_claimed_by_others(
    installed_toolkits: Dict[str, TToolkitIndexEntry], toolkit_name: str
) -> Dict[str, Set[str]]:
    """Maps each path recorded by another toolkit to the toolkits recording it."""
    claims: Dict[str, Set[str]] = {}
    for other_name, entry in installed_toolkits.items():
        if other_name != toolkit_name:
            for path in entry.get("files", {}):
                claims.setdefault(_index_key(path), set()).add(other_name)
    return claims


def _save_completed_changes(
    installed_toolkits: Dict[str, TToolkitIndexEntry],
    toolkit_name: str,
    execution_error: Optional[OSError],
) -> None:
    """Saves the index; when execution also failed, its error is raised instead of the save's."""
    try:
        save_toolkits_index(installed_toolkits)
    except (OSError, yaml.YAMLError) as save_error:
        if execution_error is None:
            raise
        fmt.error(
            "Toolkit %s failed: %s. Could not save completed changes: %s"
            % (toolkit_name, execution_error, save_error)
        )
        raise execution_error from save_error


def _next_index_entry(
    installed_toolkit_info: Optional[TToolkitIndexEntry],
    workbench_info: TToolkitInfo,
    agent_name: str,
    project_root: Path,
    changes: ToolkitChanges,
    result: ExecutionResult,
) -> Optional[TToolkitIndexEntry]:
    """Index entry for the files `result` left on disk, or None when the record is unchanged.

    A toolkit that is not installed gets an entry only when something was written, removed,
    matched or configured.
    """
    if installed_toolkit_info is None and not (
        result.written
        or result.removed
        or result.unchanged
        or (changes.mcp_servers and result.error is None)
    ):
        return None
    # protection is an intentional outcome, not a failed update. keep prior
    # metadata only when validation or execution could not finish
    partial = changes.warnings or result.error
    entry: TToolkitIndexEntry = dict(installed_toolkit_info if installed_toolkit_info and partial else workbench_info)  # type: ignore[assignment]
    entry["agent"] = agent_name
    files = {
        _index_key(rel): record
        for rel, record in (
            installed_toolkit_info.get("files", {}) if installed_toolkit_info else {}
        ).items()
    }
    servers = set(installed_toolkit_info.get("mcp_servers", []) if installed_toolkit_info else [])
    if result.error is None:
        # already configured servers satisfy the toolkit even without a merge
        servers.update(changes.mcp_servers)
    for action, disk_hash in result.written + result.unchanged:
        rel = action.dest_path.relative_to(project_root).as_posix()
        if disk_hash is not None:
            files[rel] = {"sha3_256": disk_hash}
        if action.dest_path == changes.mcp_path:
            servers.update(changes.mcp_servers)
    for path in result.removed + changes.released:
        files.pop(path.relative_to(project_root).as_posix(), None)
    entry["files"] = files
    if servers:
        entry["mcp_servers"] = sorted(servers)
    entry.pop("installed_at", None)
    previous: Dict[str, Any] = dict(installed_toolkit_info) if installed_toolkit_info else {}
    previous.pop("installed_at", None)
    if entry == previous:
        return None
    entry["installed_at"] = datetime.now(timezone.utc).isoformat()
    return entry


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
    installed_toolkits = load_toolkits_index()
    var = _resolve_agent(agent, project_root, installed_toolkits)

    fmt.echo("Initializing AI rules for %s from %s..." % (fmt.bold(var.name), fmt.bold(location)))

    base = _fetch_workbench_base_cli(location, branch)
    if base is None:
        return

    installed_toolkits = _install_toolkit(
        _INIT_TOOLKIT, base, var, project_root, installed_toolkits, overwrite=overwrite
    )

    status = fetch_ai_status(project_root, installed_toolkits)
    if "mcp_unavailable" in status["warnings"]:
        fmt.warning(f"MCP server cannot be started. Run `{fmt.cli_cmd('ai status')}` for details.")
    if var.name == "cursor":
        fmt.warning(
            "Cursor requires you to manually enable MCP servers per-project."
            " Open Cursor Settings > MCP and enable the servers installed by dlt."
        )

    if len(installed_toolkits) == 1:
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
    installed_toolkits = load_toolkits_index()
    var = _resolve_agent(agent, project_root, installed_toolkits)

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
        if dependency and request_name in installed_toolkits:
            continue
        installed_toolkits = _install_toolkit(
            request_name,
            base,
            var,
            project_root,
            installed_toolkits,
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
    """Update installed toolkits after preflighting local edits and deletions."""
    project_root = Path(run_context.active().run_dir)
    installed_toolkits = load_toolkits_index()
    if name is not None and name not in installed_toolkits:
        fmt.error("Toolkit %s is not installed." % name)
        raise CliCommandException()
    names = [name] if name is not None else list(installed_toolkits)
    if not names:
        fmt.echo("No installed toolkits to update.")
        return
    base = _fetch_workbench_base_cli(location, branch)
    if base is None:
        return
    for toolkit_name in names:
        recorded_agent = installed_toolkits[toolkit_name].get("agent")
        agent = _resolve_agent(
            recorded_agent if recorded_agent in AI_AGENTS else None,
            project_root,
            installed_toolkits,
        )
        fmt.echo("Updating toolkit %s for %s..." % (fmt.bold(toolkit_name), fmt.bold(agent.name)))
        installed_toolkits = _update_toolkit(
            toolkit_name,
            base,
            agent,
            project_root,
            installed_toolkits,
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

    installed_toolkits = load_toolkits_index()
    listed_installed: List[TToolkitInfo] = []
    listed_available: List[TToolkitInfo] = []
    for tk in toolkits.values():
        if tk["name"] in installed_toolkits:
            listed_installed.append(tk)
        else:
            listed_available.append(tk)

    if listed_installed:
        fmt.echo("Installed toolkits:")
        for tk in listed_installed:
            tk_name = tk["name"]
            description = tk["description"]
            remote_version = tk["version"]
            installed_version = installed_toolkits[tk_name].get("version", "?")
            if _toolkit_needs_update(installed_version, remote_version):
                ver = "%s, %s available" % (
                    fmt.bold(installed_version),
                    fmt.style(remote_version, fg="yellow"),
                )
            else:
                ver = fmt.bold(installed_version)
            fmt.echo("  %-20s %s (%s)" % (fmt.bold(tk_name), description, ver))

    if listed_available:
        if listed_installed:
            fmt.echo("")
        fmt.echo("Available toolkits:")
        for tk in listed_available:
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
            "Toolkit %s not found in the workbench (missing %s)"
            % (fmt.bold(name), ".claude-plugin/plugin.json")
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
