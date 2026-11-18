import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple
import tomlkit
import tomlkit.exceptions
import yaml

from dlt.common.runtime import run_context
from dlt._workspace.access import FULL_ACCESS, format_access, parse_access
from dlt._workspace.run_context import format_display_path
from dlt._workspace.cli._repo_source import TRepoSource, format_repo_source
from dlt._workspace.cli import echo as fmt, utils
from dlt._workspace.cli.utils import (
    DEFAULT_MCP_FEATURES,
    add_mcp_features,
    mcp_stdio_args,
    resolve_features,
    split_mcp_features,
)
from dlt._workspace.cli.markdown import MarkdownDocument
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
from dlt._workspace.cli.dlthub.ai.typing import TAiStatusInfo, TToolkitInfo
from dlt._workspace.cli.dlthub.ai.utils import (
    DLTHUB_AGENTS_DIR,
    build_toolkits_dependency_map,
    compute_content_hash,
    extract_toolkit_info,
    fetch_ai_status,
    fetch_secrets_list,
    fetch_secrets_update_fragment,
    fetch_secrets_view_redacted,
    fetch_workbench_toolkit_info,
    fetch_workbench_base,
    get_recorded_harness_source,
    is_component_visible,
    is_toolkit_installed,
    is_toolkit_visible,
    load_toolkits_index,
    read_workbench_toolkit_mcp_servers,
    read_workbench_toolkit_combined_info,
    resolve_harness_source,
    resolve_toolkit_dependencies,
    safe_write_bytes,
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
    variant = _resolve_agent(agent, project_root)

    args = ["uv", "run", fmt.get_cli_host_name(), *mcp_stdio_args(features)]

    server_config: Dict[str, Any] = {"command": args[0], "args": args[1:], "type": "stdio"}
    new_servers = {name: server_config}

    for config_path in variant.mcp_config_paths(project_root):
        display_path = _format_component_path(config_path, project_root)
        existing_content = ""
        if config_path.is_file():
            existing_content = config_path.read_text(encoding="utf-8")

        if not overwrite:
            existing_servers = variant.parse_mcp_servers(existing_content)
            if name in existing_servers:
                fmt.echo("MCP server %s already configured in %s" % (fmt.bold(name), display_path))
                continue

        merged = variant.merge_mcp_servers(existing_content, new_servers)
        config_path.parent.mkdir(parents=True, exist_ok=True)
        config_path.write_text(merged, encoding="utf-8")
        fmt.echo("Installed MCP server %s in %s" % (fmt.bold(name), display_path))


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
        MarkdownDocument(md_file.read_text(encoding="utf-8")).split_frontmatter()
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
    previous_files: Optional[Dict[str, Any]] = None,
    toolkit_info: Optional[TToolkitInfo] = None,
    see_all: bool = False,
) -> Tuple[List["InstallAction"], List[str]]:
    """Scan toolkit directory and build install actions. Reads source files but does not
    write to project_root. Returns (actions, validation_warnings).

    Files in `previous_files` (index paths of an earlier install) that the toolkit no longer
    ships get a `remove` action. Components that `toolkit_info` hides from the agent are
    skipped unless `see_all`.
    """
    actions: List[InstallAction] = []
    warnings: List[str] = []

    def _visible(folder: str, name: str) -> bool:
        if see_all or toolkit_info is None:
            return True
        return is_component_visible(toolkit_info, folder + "/" + name, agent.name)

    # skills (directory-based, each with SKILL.md + optional files)
    skills_dir = toolkit_dir / "skills"
    if skills_dir.is_dir():
        for skill_path in sorted(skills_dir.iterdir()):
            if not skill_path.is_dir() or not (skill_path / COMPONENT_MARKERS["skill"]).exists():
                continue
            if not _visible("skills", skill_path.name):
                continue
            errors = _validate_skill_dir(skill_path)
            if errors:
                for err in errors:
                    warnings.append("Skipping skill %s: %s" % (skill_path.name, err))
                continue
            actions.extend(
                agent.install_actions(
                    "skill", skill_path, skill_path.name, toolkit_name, project_root, overwrite
                )
            )

    # agents (directory-based, each with AGENT.md + optional files), copied verbatim
    agents_dir = toolkit_dir / DLTHUB_AGENTS_DIR
    if agents_dir.is_dir():
        for agent_path in sorted(agents_dir.iterdir()):
            if not agent_path.is_dir() or not (agent_path / COMPONENT_MARKERS["agent"]).exists():
                continue
            if not _visible(DLTHUB_AGENTS_DIR, agent_path.name):
                continue
            errors = _validate_skill_dir(agent_path)
            if errors:
                for err in errors:
                    warnings.append("Skipping agent %s: %s" % (agent_path.name, err))
                continue
            actions.extend(
                agent.install_actions(
                    "agent", agent_path, agent_path.name, toolkit_name, project_root, overwrite
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
            if not _visible(dir_name, md_file.stem):
                continue
            err = _validate_md_frontmatter(md_file)
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

    # mcp servers, and features added to servers that other toolkits installed
    mcp_servers = {
        name: config
        for name, config in read_workbench_toolkit_mcp_servers(toolkit_dir).items()
        if _visible("mcp", name)
    }
    mcp_features = {
        name: features
        for name, features in (toolkit_info.get("mcp_features", {}) if toolkit_info else {}).items()
        if _visible("mcp", name)
    }
    for config_path in agent.mcp_config_paths(project_root) if mcp_servers or mcp_features else []:
        mcp_action = _plan_mcp_config(
            agent, config_path, mcp_servers, mcp_features, toolkit_name, overwrite, warnings
        )
        if mcp_action is not None:
            actions.append(mcp_action)

    actions = agent.finalize_actions(actions, project_root)
    if previous_files:
        actions.extend(_stale_file_actions(agent, project_root, previous_files, actions))
    actions.extend(agent.post_finalize_actions(actions, project_root))
    return actions, warnings


def _with_mcp_features(
    config: Dict[str, Any], features: Sequence[str], no_default_features: bool = False
) -> Dict[str, Any]:
    """`config` with `features` added to its args, and without the default features if asked."""
    args = [str(arg) for arg in config.get("args", [])]
    if no_default_features and "--no-default-features" not in args:
        args.append("--no-default-features")
    return {**config, "args": add_mcp_features(args, features)}


def _mcp_feature_selection(server: Dict[str, Any]) -> Tuple[List[str], bool]:
    """The `--features` tokens of `server` and whether it drops the default features."""
    other, tokens = split_mcp_features([str(arg) for arg in server.get("args", [])])
    return tokens, "--no-default-features" in other


def _format_feature_labels(tokens: Sequence[str]) -> str:
    """Feature tokens for display, each marked as added (`+name`) or removed (`-name`)."""
    return ", ".join(token if token[0] in "+-" else "+" + token for token in tokens)


def _plan_mcp_config(
    agent: "_AIAgent",
    config_path: Path,
    mcp_servers: Dict[str, Any],
    mcp_features: Dict[str, List[str]],
    toolkit_name: str,
    overwrite: bool,
    warnings: List[str],
) -> Optional["InstallAction"]:
    """Plans the update of one MCP config of the agent, `None` when it stays as is.

    `mcp_servers` are written when missing or with `overwrite`. `mcp_features` are added to the
    configured servers of the same name.
    """
    existing_content = config_path.read_text(encoding="utf-8") if config_path.is_file() else ""
    existing = agent.parse_mcp_servers(existing_content)
    written: Dict[str, Dict[str, Any]] = {}
    labels: Dict[str, str] = {}
    for name, config in mcp_servers.items():
        if name in existing and not overwrite:
            continue
        # a reinstalled server keeps the features other toolkits and the user selected
        written[name] = (
            _with_mcp_features(config, *_mcp_feature_selection(existing[name]))
            if name in existing
            else config
        )
        labels[name] = name
    provided = tuple(written)

    for name, features in mcp_features.items():
        target = written.get(name, existing.get(name))
        if target is None:
            warnings.append(
                "Toolkit %s adds feature groups to MCP server %s, but %s is not in %s. Install"
                " the toolkit that provides %s, then run %s."
                % (
                    toolkit_name,
                    name,
                    name,
                    config_path,
                    name,
                    fmt.cli_cmd("ai toolkit install %s --overwrite" % toolkit_name),
                )
            )
            continue
        if "command" not in target:
            warnings.append(
                "MCP server %s in %s has no command, so toolkit %s cannot add feature groups to it."
                % (name, config_path, toolkit_name)
            )
            continue
        updated = _with_mcp_features(target, features)
        if updated == target:
            continue
        before = _mcp_feature_selection(target)[0]
        added = [token for token in _mcp_feature_selection(updated)[0] if token not in before]
        written[name] = updated
        labels[name] = "%s (%s)" % (name, _format_feature_labels(added))

    if not written:
        return None
    return InstallAction(
        kind="mcp",
        source_name=", ".join(labels[name] for name in sorted(labels)),
        dest_path=config_path,
        op="save",
        content_or_path=agent.merge_mcp_servers(existing_content, written),
        conflict=False,
        mcp_servers=provided,
    )


def _stale_file_actions(
    agent: "_AIAgent",
    project_root: Path,
    previous_files: Dict[str, Any],
    actions: List["InstallAction"],
) -> List["InstallAction"]:
    """Remove actions for files a previous install recorded that no planned action writes."""
    planned = {a.dest_path for a in actions if a.kind != "mcp"}
    component_dirs = sorted(
        ((kind, agent.component_dir(kind, project_root)) for kind in agent._DIRS),
        key=lambda kind_dir: len(kind_dir[1].parts),
        reverse=True,
    )
    removals: List[InstallAction] = []
    for rel_path in sorted(previous_files):
        path = project_root / rel_path
        if path in planned:
            continue
        kind, base = next(
            ((kind, base) for kind, base in component_dirs if base in path.parents),
            ("ignore", project_root),
        )
        removals.append(
            InstallAction(
                kind=kind,
                source_name=str(path.relative_to(base)),
                dest_path=path,
                op="remove",
                content_or_path="",
                conflict=False,
                component_dir=base,
            )
        )
    return removals


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


def _execute_install(
    actions: List["InstallAction"],
    toolkit_info: Optional[TToolkitInfo] = None,
    agent_name: Optional[str] = None,
    project_root: Optional[Path] = None,
    see_all: bool = False,
    source: Optional[TRepoSource] = None,
) -> Tuple[int, List["InstallAction"]]:
    """Commits non-conflicting actions and the toolkit index. Returns count of items installed
    and the render actions that changed their file.

    All content is read before anything is written, so a missing source changes nothing on disk.
    Render actions run last and see the files the other actions wrote.
    """
    pending = [action for action in actions if not action.conflict]
    writes = [
        (action, _action_bytes(action)) for action in pending if action.op in ("copy", "save")
    ]
    removals = [action for action in pending if action.op == "remove"]
    renders = [action for action in pending if action.op == "render"]

    for action, data in writes:
        action.dest_path.parent.mkdir(parents=True, exist_ok=True)
        safe_write_bytes(action.dest_path, data)
    for action in removals:
        assert action.component_dir is not None
        _remove_installed_file(action.dest_path, action.component_dir)
    rendered: List[InstallAction] = []
    for action in renders:
        assert action.render is not None
        if (content := action.render()) is not None:
            action.dest_path.parent.mkdir(parents=True, exist_ok=True)
            safe_write_bytes(action.dest_path, content.encode("utf-8"))
            rendered.append(action)

    # write toolkit index
    installed = len({(a.kind, a.source_name, a.source_kind) for a, _ in writes})
    if (installed > 0 or removals) and toolkit_info is not None:
        tracked_files: Optional[Dict[str, Any]] = None
        if project_root is not None:
            tracked_files = {
                str(action.dest_path.relative_to(project_root)): {
                    "sha3_256": compute_content_hash(data)
                }
                for action, data in writes
                # shared merge targets (e.g. hooks.json) are not tracked per toolkit
                if action.kind != "mcp" and not action.skip_index
            }
        # each of the agent's MCP configs names the same servers
        mcp_servers = sorted({name for action, _ in writes for name in action.mcp_servers})
        save_toolkit_entry(
            toolkit_info,
            agent=agent_name,
            files=tracked_files,
            mcp_servers=mcp_servers or None,
            see_all=see_all,
            source=source,
        )
    return installed, rendered


def _resolve_agent(agent: Optional[str], project_root: Path) -> "_AIAgent":
    """Resolve an explicit agent name or auto-detect. Raises on failure."""
    if agent is not None:
        if agent not in AI_AGENTS:
            fmt.error("Unknown agent: %s" % agent)
            raise CliCommandException()
        return AI_AGENTS[agent]()

    if recorded := _AIAgent.recorded_agent_name():
        return AI_AGENTS[recorded]()

    best = _AIAgent.best_detected_agents(project_root)
    if not best:
        available = ", ".join(sorted(AI_AGENTS))
        fmt.error("Could not detect AI coding agent. Use --agent to specify one of: %s" % available)
        raise CliCommandException()
    if len(best) > 1:
        names = ", ".join(a.name for a in best)
        fmt.error("Multiple AI coding agents detected: %s. Use --agent to specify one." % names)
        raise CliCommandException()
    fmt.echo("Detected AI coding agent: %s" % fmt.bold(best[0].name))
    return best[0]


def _format_component_path(path: Any, project_root: Optional[Path]) -> str:
    if project_root is None:
        return str(path)
    return format_display_path(str(path), str(project_root))


def _report_and_execute(
    actions: List["InstallAction"],
    validation_warnings: List[str],
    strict: bool = False,
    toolkit_meta: Optional[TToolkitInfo] = None,
    agent_name: Optional[str] = None,
    project_root: Optional[Path] = None,
    see_all: bool = False,
    source: Optional[TRepoSource] = None,
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
    # a skill or agent folder is one component made of many file actions; a component saved
    # to several files (e.g. MCP configs) gets a line per file
    components: Dict[Tuple[str, str, Optional[str], Optional[Path]], List[InstallAction]] = {}
    for a in actions:
        if a.op == "remove":
            fmt.echo(
                "  - %s %s (%s)"
                % (
                    a.kind,
                    fmt.bold(a.source_name),
                    _format_component_path(a.dest_path, project_root),
                )
            )
        elif a.op != "render":
            dest_key = None if a.op == "copy" else a.dest_path
            components.setdefault((a.kind, a.source_name, a.source_kind, dest_key), []).append(a)
    for (kind, source_name, _, _), component_actions in components.items():
        dest_paths = [str(a.dest_path) for a in component_actions]
        if any(a.op == "copy" for a in component_actions):
            dest = (
                os.path.commonpath(dest_paths)
                if len(dest_paths) > 1
                else str(component_actions[0].dest_path.parent)
            )
        else:
            dest = dest_paths[0]
        dest = _format_component_path(dest, project_root)
        if component_actions[0].conflict:
            fmt.warning("  Skipping %s %s (already exists at %s)" % (kind, source_name, dest))
        else:
            fmt.echo("  + %s %s -> %s" % (kind, fmt.bold(source_name), dest))
    installed, rendered = _execute_install(
        actions,
        toolkit_info=toolkit_meta,
        agent_name=agent_name,
        project_root=project_root,
        see_all=see_all,
        source=source,
    )
    for a in rendered:
        fmt.echo(
            "  ~ %s %s -> %s (rendered from the installed files)"
            % (a.kind, fmt.bold(a.source_name), _format_component_path(a.dest_path, project_root))
        )
    # a running agent keeps the old server process until it reconnects
    if toolkit_meta and any(a.kind == "mcp" for a in actions):
        for server in toolkit_meta.get("mcp_features", {}):
            if not (see_all or is_component_visible(toolkit_meta, "mcp/" + server, agent_name)):
                continue
            fmt.warning(
                "MCP server %s changed: reconnect it in %s or restart the session to load its"
                " changed tools" % (fmt.bold(server), agent_name or "your AI coding agent")
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
    see_all: bool = False,
    source: Optional[TRepoSource] = None,
) -> int:
    """Core install logic: read plugin.json, version-check, plan, execute. Returns count of items
    installed."""
    toolkit_dir = base / name
    meta = read_workbench_toolkit_combined_info(toolkit_dir)
    if meta is None:
        fmt.warning(
            "Toolkit %s not found (missing %s)" % (fmt.bold(name), ".claude-plugin/plugin.json")
        )
        return 0

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
                "Toolkit %s %s is installed, version %s available. Use --overwrite to update."
                % (toolkit_name, local_version, toolkit_version)
            )
        if workflow_entry_skill := toolkit_meta.get("workflow_entry_skill"):
            fmt.echo("Use %s skill to start!" % fmt.bold(workflow_entry_skill))
        return 0

    # a file another toolkit also installed (e.g. the ignore file) is not ours to remove
    files_of_others = {
        path
        for other_name, entry in installed_index.items()
        if other_name != toolkit_name
        for path in entry.get("files", {})
    }
    previous_files = {
        path: entry
        for path, entry in (local.get("files", {}) if local else {}).items()
        if path not in files_of_others
    }
    actions, warnings = _plan_toolkit_install(
        toolkit_dir,
        agent,
        project_root,
        toolkit_name,
        overwrite=overwrite,
        previous_files=previous_files,
        toolkit_info=toolkit_meta,
        see_all=see_all,
    )
    # render actions only rewrite files from what the toolkit installs
    if not any(a.op != "render" for a in actions) and not warnings:
        fmt.echo("No components found in toolkit %s." % fmt.bold(name))
        return 0

    return _report_and_execute(
        actions,
        warnings,
        strict=strict,
        toolkit_meta=toolkit_meta,
        agent_name=agent.name,
        project_root=project_root,
        see_all=see_all,
        source=source,
    )


def _install_dependencies(
    name: str,
    toolkits: Dict[str, TToolkitInfo],
    base: Path,
    agent: "_AIAgent",
    project_root: Path,
    see_all: bool = False,
    source: Optional[TRepoSource] = None,
) -> None:
    """Install upstream dependencies for `name` that are not yet installed.

    Dependencies hidden from the agent are skipped with their own dependencies, unless `see_all`.
    """
    dep_map = build_toolkits_dependency_map(toolkits)
    hidden = (
        set()
        if see_all
        else {tk for tk, info in toolkits.items() if not is_toolkit_visible(info, agent.name)}
    )
    try:
        deps = resolve_toolkit_dependencies(name, dep_map, skip=hidden)
        skipped = [d for d in resolve_toolkit_dependencies(name, dep_map) if d not in deps]
    except ValueError as ex:
        fmt.warning(str(ex))
        return
    for dep in skipped:
        fmt.echo(
            "Skipping dependency %s: not available for %s. Use --see-all to install it."
            % (fmt.bold(dep), agent.name)
        )
    for dep in deps:
        if is_toolkit_installed(dep):
            continue
        _install_toolkit(dep, base, agent, project_root, see_all=see_all, source=source)


def _install_toolkit_with_dependencies(
    name: str,
    base: Path,
    agent: "_AIAgent",
    project_root: Path,
    overwrite: bool = False,
    strict: bool = False,
    see_all: bool = False,
    source: Optional[TRepoSource] = None,
) -> int:
    """Install toolkit `name` after its dependencies. Returns count of items installed for
    `name`."""
    toolkits, warnings = fetch_workbench_toolkits(base, strict=strict)
    for w in warnings:
        fmt.warning(w)
    # what an earlier --see-all install brought in stays on update
    installed = load_toolkits_index().get(name)
    see_all = see_all or bool(installed and installed.get("see_all"))
    if name in toolkits and not see_all and not is_toolkit_visible(toolkits[name], agent.name):
        fmt.error(
            "Toolkit %s is not available for %s. Use --see-all to install it anyway."
            % (fmt.bold(name), agent.name)
        )
        raise CliCommandException()
    _install_dependencies(name, toolkits, base, agent, project_root, see_all=see_all, source=source)
    return _install_toolkit(
        name,
        base,
        agent,
        project_root,
        overwrite=overwrite,
        strict=strict,
        see_all=see_all,
        source=source,
    )


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


@utils.track_command("ai", False, operation="rules")
def ai_rules_command(agent: Optional[str] = None) -> None:
    """Print the rules an AI coding agent gets when a session starts, nothing when there are none."""
    # runs from an agent's session start hook: no detection, nothing printed but the rules
    agent_name = agent or _AIAgent.recorded_agent_name()
    if agent_name not in AI_AGENTS:
        return
    context = AI_AGENTS[agent_name]().session_context(Path(run_context.active().run_dir))
    if context:
        fmt.echo(context)


@utils.track_command("ai", False, operation="init")
def ai_init_command(
    agent: Optional[str],
    location: Optional[str] = None,
    branch: Optional[str] = None,
    overwrite: bool = False,
) -> None:
    """Install the init toolkit into the current project.

    `location` and `branch` default to the ones the installed toolkits came from.
    """
    project_root = Path(run_context.active().run_dir)
    var = _resolve_agent(agent, project_root)
    location, branch = resolve_harness_source(location, branch)

    fmt.echo(
        "Initializing AI rules for %s from %s..."
        % (fmt.bold(var.name), format_repo_source(location, branch))
    )

    base = _fetch_workbench_base_cli(location, branch)
    if base is None:
        return

    installed = _install_toolkit_with_dependencies(
        _INIT_TOOLKIT,
        base,
        var,
        project_root,
        overwrite=overwrite,
        source=get_recorded_harness_source(location, branch),
    )
    if not installed:
        return

    status = fetch_ai_status(project_root)
    if "mcp_unavailable" in status["warnings"]:
        fmt.warning(f"MCP server cannot be started. Run `{fmt.cli_cmd('ai status')}` for details.")
    for note in var.install_notes():
        fmt.warning(note)

    if "no_toolkits" in status["warnings"]:
        fmt.echo()
        fmt.echo(
            f"Now you can install your first toolkit. Use `{fmt.cli_cmd('ai toolkit list')}` for"
            " more information."
        )


@utils.track_command("ai", False, "name", operation="toolkit.install")
def ai_toolkit_install_command(
    name: str,
    agent: Optional[str],
    location: Optional[str] = None,
    branch: Optional[str] = None,
    overwrite: bool = False,
    strict: bool = False,
    see_all: bool = False,
) -> None:
    """Install toolkit components into the current project.

    `location` and `branch` default to the ones the installed toolkits came from.
    """
    project_root = Path(run_context.active().run_dir)
    var = _resolve_agent(agent, project_root)
    location, branch = resolve_harness_source(location, branch)

    fmt.echo(
        "Installing toolkit %s for %s from %s..."
        % (fmt.bold(name), fmt.bold(var.name), format_repo_source(location, branch))
    )

    base = _fetch_workbench_base_cli(location, branch)
    if base is None:
        return

    _install_toolkit_with_dependencies(
        name,
        base,
        var,
        project_root,
        overwrite=overwrite,
        strict=strict,
        see_all=see_all,
        source=get_recorded_harness_source(location, branch),
    )


def _agent_scope(agent: Optional[str], see_all: bool) -> Optional[str]:
    """Agent whose toolkits are shown, `None` for all of them. Tells the user which."""
    if see_all:
        return None
    project_root = Path(run_context.active().run_dir)
    agent_name = _AIAgent.detect_agent_name(agent, project_root)
    if agent_name is not None:
        fmt.echo(
            "Showing what %s can install. Use --see-all to see everything." % fmt.bold(agent_name)
        )
    elif best := _AIAgent.best_detected_agents(project_root):
        fmt.echo(
            "Several AI coding agents detected (%s), showing what every agent can install. Use"
            " --agent to pick one."
            % ", ".join(a.name for a in best)
        )
    else:
        fmt.echo("No AI coding agent detected, showing what every agent can install.")
    return agent_name


@utils.track_command("ai", False, operation="toolkit.list")
def ai_toolkit_list_command(
    location: Optional[str] = None,
    branch: Optional[str] = None,
    agent: Optional[str] = None,
    see_all: bool = False,
) -> None:
    """List available toolkits with name and description."""
    location, branch = resolve_harness_source(location, branch)
    base = _fetch_workbench_base_cli(location, branch)
    if base is None:
        return
    agent_name = _agent_scope(agent, see_all)
    toolkits, warnings = fetch_workbench_toolkits(base, listed_only=True, agent_name=agent_name)
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
    location: Optional[str] = None,
    branch: Optional[str] = None,
    agent: Optional[str] = None,
    see_all: bool = False,
) -> None:
    """Show what's inside a toolkit."""
    location, branch = resolve_harness_source(location, branch)
    agent_name = _agent_scope(agent, see_all)
    info = fetch_workbench_toolkit_info(name, location, branch, agent_name=agent_name)
    if info is None:
        fmt.warning(
            "Toolkit %s not found (missing %s)" % (fmt.bold(name), ".claude-plugin/plugin.json")
        )
        return
    if not is_toolkit_visible(info, agent_name):
        fmt.warning(
            "Toolkit %s is not available for %s. Use --see-all to see it anyway."
            % (fmt.bold(name), agent_name)
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

    if info.get("mcp_features"):
        fmt.echo("\nMCP feature groups:")
        for srv_name, features in info["mcp_features"].items():
            fmt.echo("  %-20s %s" % (fmt.bold(srv_name), _format_feature_labels(features)))

    if info["has_ignore"]:
        fmt.echo("\nIgnore: .claudeignore")
