import os
import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import tomlkit
import tomlkit.exceptions

from dlt.common.json import json
from dlt.common.libs import git
from dlt.common.pipeline import get_dlt_repos_dir
from dlt.common.runtime import run_context
from dlt.common.configuration.providers.toml import SECRETS_TOML, SecretsTomlProvider

from dlt._workspace.cli import echo as fmt, utils
from dlt._workspace.cli._scaffold_api_client import get_scaffold_files_storage
from dlt._workspace.cli.exceptions import (
    CliCommandException,
    ScaffoldApiError,
    ScaffoldSourceNotFound,
)
from dlt._workspace.cli.ai.agents import AI_AGENTS, _AIAgent, InstallAction
from dlt._workspace.cli.ai.utils import (
    copy_repo_files,
    iter_toolkits,
    read_mcp_servers,
    read_plugin_meta,
    redact_toml_document,
)


DEFAULT_AI_WORKBENCH_REPO = "git@github.com:dlt-hub/dlthub-ai-workbench.git"
# DEFAULT_AI_WORKBENCH_REPO = "/home/rudolfix/src/dlt-ai-dev-kit"
DEFAULT_AI_WORKBENCH_BRANCH = None
AI_WORKBENCH_BASE_DIR = "workbench"
_INIT_TOOLKIT = "_init"


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


def _list_secrets_paths() -> List[str]:
    """Return project-scoped secrets.toml paths from TOML providers."""
    from dlt.common.configuration.providers.toml import SecretsTomlProvider
    from dlt._workspace.utils import get_provider_locations

    paths: List[str] = []
    for info in get_provider_locations():
        if not isinstance(info.provider, SecretsTomlProvider):
            continue
        project_locs = [loc for loc in info.locations if loc.scope == "project"]
        project_locs.sort(key=lambda loc: (loc.profile_name is None, loc.path))
        for loc in project_locs:
            paths.append(loc.path)
    return paths


def _default_secrets_path(path: Optional[str] = None) -> str:
    """Returns `path` if given, first project secrets path, or the settings default."""
    if path:
        return path
    paths = _list_secrets_paths()
    if paths:
        return paths[0]
    return utils.make_dlt_settings_path(SECRETS_TOML)


@utils.track_command("ai", False, operation="secrets_list")
def ai_secrets_list_command() -> None:
    """Lists project-scoped secret file locations from TOML providers."""
    from dlt.common.configuration.providers.toml import SecretsTomlProvider
    from dlt._workspace.utils import get_provider_locations

    fmt.echo("Secret file locations:")
    for info in get_provider_locations():
        if not isinstance(info.provider, SecretsTomlProvider):
            continue
        project_locs = [loc for loc in info.locations if loc.scope == "project"]
        project_locs.sort(key=lambda loc: (loc.profile_name is None, loc.path))
        if not project_locs:
            continue
        for loc in project_locs:
            tag = "profile: %s" % loc.profile_name if loc.profile_name else ""
            if tag:
                fmt.echo("  %s (%s)" % (loc.path, tag))
            else:
                fmt.echo("  %s" % loc.path)


@utils.track_command("ai", False, operation="secrets_view_redacted")
def ai_secrets_view_redacted_command(path: Optional[str] = None) -> None:
    """Prints a redacted version of a secrets TOML file."""
    resolved = _default_secrets_path(path)
    try:
        with open(resolved, "r", encoding="utf-8") as f:
            doc = tomlkit.load(f)
    except FileNotFoundError:
        fmt.warning("Secrets file not found: %s" % resolved)
        return
    redacted = redact_toml_document(doc)
    fmt.echo(tomlkit.dumps(redacted))


@utils.track_command("ai", False, operation="secrets_update_fragment")
def ai_secrets_update_fragment_command(fragment: str, path: Optional[str] = None) -> None:
    """Merges a TOML fragment into secrets file and prints the redacted result."""
    from dlt.common.utils import update_dict_nested

    resolved = _default_secrets_path(path)
    settings_dir = os.path.dirname(resolved)
    os.makedirs(settings_dir, exist_ok=True)
    if not os.path.isfile(resolved):
        with open(resolved, "w", encoding="utf-8") as f:
            f.write("")
    provider = SecretsTomlProvider(settings_dir=settings_dir)
    try:
        parsed = tomlkit.parse(fragment)
    except tomlkit.exceptions.TOMLKitError as ex:
        fmt.error("Invalid TOML fragment: %s" % str(ex))
        raise CliCommandException()
    update_dict_nested(provider._config_toml, parsed)
    provider.write_toml()
    redacted = redact_toml_document(provider._config_toml)
    fmt.echo(tomlkit.dumps(redacted))


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


ai_mcp_command = ai_mcp_run_command


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


def _fetch_workbench_base(location: str, branch: Optional[str]) -> Optional[Path]:
    """Fetch AI workbench repo and return base Path, or None with warning."""
    branch = branch or DEFAULT_AI_WORKBENCH_BRANCH
    src_storage = git.get_fresh_repo_files(location, get_dlt_repos_dir(), branch=branch)
    if not src_storage.has_folder(AI_WORKBENCH_BASE_DIR):
        fmt.warning(
            "AI workbench directory not found in repo %s branch %s"
            % (fmt.bold(location), fmt.bold(branch))
        )
        return None
    return Path(src_storage.make_full_path(AI_WORKBENCH_BASE_DIR))


def _plan_toolkit_install(
    toolkit_dir: Path,
    agent: "_AIAgent",
    project_root: Path,
    toolkit_name: str,
    overwrite: bool = False,
) -> List["InstallAction"]:
    """Scan toolkit directory and build install actions. Reads source files but does not
    write to project_root."""
    from dlt._workspace.cli.ai.agents import InstallAction, TComponentType

    actions: List[InstallAction] = []

    skills_dir = toolkit_dir / "skills"
    if skills_dir.is_dir():
        for skill_path in sorted(skills_dir.iterdir()):
            if not skill_path.is_dir() or not (skill_path / "SKILL.md").exists():
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

    component_types: List[Tuple[TComponentType, str]] = [("command", "commands"), ("rule", "rules")]
    for component_type, dir_name in component_types:
        src_dir = toolkit_dir / dir_name
        if not src_dir.is_dir():
            continue
        for md_file in sorted(src_dir.glob("*.md")):
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

    mcp_servers = read_mcp_servers(toolkit_dir)
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

    return actions


def _execute_install(actions: List["InstallAction"], overwrite: bool = False) -> int:
    """Write non-conflicting actions to disk. Returns count of items installed."""
    installed = 0
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
        installed += 1
    return installed


def _resolve_agent(agent: Optional[str], project_root: Path) -> "_AIAgent":
    """Resolve an explicit agent name or auto-detect. Raises on failure."""
    if agent is not None:
        if agent not in AI_AGENTS:
            fmt.error("Unknown agent: %s" % agent)
            raise CliCommandException()
        return AI_AGENTS[agent]()

    detected = _AIAgent.detect(project_root)
    if detected is None:
        fmt.error("Could not detect AI coding agent. Use --agent to specify one.")
        raise CliCommandException()
    fmt.echo("Detected AI coding agent: %s" % fmt.bold(detected.name))
    return detected


def _report_and_execute(actions: List["InstallAction"], overwrite: bool = False) -> int:
    """Print planned actions, skip conflicts, execute, return count installed."""
    for a in actions:
        if not a.conflict:
            fmt.echo("  + %s %s -> %s" % (a.kind, fmt.bold(a.source_name), a.dest_path))
    for a in actions:
        if a.conflict:
            fmt.warning(
                "  Skipping %s %s (already exists at %s)" % (a.kind, a.source_name, a.dest_path)
            )
    installed = _execute_install(actions, overwrite=overwrite)
    fmt.echo("%s item(s) installed." % fmt.bold(str(installed)))
    return installed


@utils.track_command("ai", False, operation="init")
def ai_init_command(
    agent: Optional[str],
    location: str,
    branch: Optional[str] = None,
) -> None:
    """Install the built-in _init toolkit into the current project."""
    project_root = Path(run_context.active().run_dir)
    var = _resolve_agent(agent, project_root)

    fmt.echo("Initializing AI rules for %s from %s..." % (fmt.bold(var.name), fmt.bold(location)))

    base = _fetch_workbench_base(location, branch)
    if base is None:
        return

    toolkit_dir = base / _INIT_TOOLKIT
    if not toolkit_dir.is_dir():
        fmt.warning("Init toolkit not found in %s" % fmt.bold(location))
        return

    actions = _plan_toolkit_install(toolkit_dir, var, project_root, _INIT_TOOLKIT)
    if not actions:
        fmt.echo("No components found in init toolkit.")
        return

    _report_and_execute(actions)


def _install_init_silently(base: Path, agent: "_AIAgent", project_root: Path) -> None:
    """Install the _init toolkit without overwrite and without conflict warnings."""
    init_dir = base / _INIT_TOOLKIT
    if not init_dir.is_dir():
        return
    actions = _plan_toolkit_install(init_dir, agent, project_root, _INIT_TOOLKIT)
    _execute_install(actions)


@utils.track_command("ai", False, operation="toolkit_install")
def ai_toolkit_install_command(
    name: str,
    agent: Optional[str],
    location: str,
    branch: Optional[str] = None,
    overwrite: bool = False,
) -> None:
    """Install toolkit components into the current project."""
    project_root = Path(run_context.active().run_dir)
    var = _resolve_agent(agent, project_root)

    fmt.echo(
        "Installing toolkit %s for %s from %s..."
        % (fmt.bold(name), fmt.bold(var.name), fmt.bold(location))
    )

    base = _fetch_workbench_base(location, branch)
    if base is None:
        return

    _install_init_silently(base, var, project_root)

    toolkit_dir = base / name
    meta_json = toolkit_dir / ".claude-plugin" / "plugin.json"
    if not meta_json.exists():
        fmt.warning(
            "Toolkit %s not found (missing %s)" % (fmt.bold(name), ".claude-plugin/plugin.json")
        )
        return

    try:
        meta = json.loads(meta_json.read_text(encoding="utf-8"))
    except (ValueError, OSError) as ex:
        fmt.warning("Invalid plugin.json: %s" % str(ex))
        return
    toolkit_name = meta.get("name", name)

    actions = _plan_toolkit_install(
        toolkit_dir, var, project_root, toolkit_name, overwrite=overwrite
    )
    if not actions:
        fmt.echo("No components found in toolkit %s." % fmt.bold(name))
        return

    _report_and_execute(actions, overwrite=overwrite)


@utils.track_command("ai", False, operation="toolkit_list")
def ai_toolkit_list_command(
    location: str,
    branch: Optional[str] = None,
) -> None:
    """List available toolkits with name and description."""
    base = _fetch_workbench_base(location, branch)
    if base is None:
        return

    toolkits = iter_toolkits(base)
    if not toolkits:
        fmt.echo("No toolkits found.")
        return

    fmt.echo("Available toolkits:")
    for dir_name, meta in toolkits:
        name = meta.get("name", dir_name)
        description = meta.get("description", "")
        fmt.echo("  %-20s %s" % (fmt.bold(name), description))


def _echo_md_components(label: str, md_files: List[Path]) -> None:
    """Print a labeled list of markdown components (skills, commands, rules)."""
    from dlt._workspace.cli.formatters import read_md_name_desc

    if not md_files:
        return
    fmt.echo("\n%s:" % label)
    for md_file in md_files:
        n, d = read_md_name_desc(md_file)
        fmt.echo("  %-20s %s" % (fmt.bold(n), d))


@utils.track_command("ai", False, operation="toolkit_info")
def ai_toolkit_info_command(
    name: str,
    location: str,
    branch: Optional[str] = None,
) -> None:
    """Show what's inside a toolkit."""
    base = _fetch_workbench_base(location, branch)
    if base is None:
        return

    toolkit_dir = base / name
    meta = read_plugin_meta(toolkit_dir)
    if meta is None:
        fmt.warning(
            "Toolkit %s not found (missing %s)" % (fmt.bold(name), ".claude-plugin/plugin.json")
        )
        return

    toolkit_name = meta.get("name", name)
    description = meta.get("description", "")
    fmt.echo("Toolkit: %s" % fmt.bold(toolkit_name))
    if description:
        fmt.echo("  %s" % description)

    skills_dir = toolkit_dir / "skills"
    if skills_dir.is_dir():
        _echo_md_components(
            "Skills",
            [
                p / "SKILL.md"
                for p in sorted(skills_dir.iterdir())
                if p.is_dir() and (p / "SKILL.md").exists()
            ],
        )

    cmd_dir = toolkit_dir / "commands"
    if cmd_dir.is_dir():
        _echo_md_components("Commands", sorted(cmd_dir.glob("*.md")))

    rules_dir = toolkit_dir / "rules"
    if rules_dir.is_dir():
        _echo_md_components("Rules", sorted(rules_dir.glob("*.md")))

    mcp_servers = read_mcp_servers(toolkit_dir)
    if mcp_servers:
        fmt.echo("\nMCP servers:")
        for srv_name, srv_config in sorted(mcp_servers.items()):
            cmd = srv_config.get("command", "")
            srv_args = " ".join(srv_config.get("args", []))
            fmt.echo("  %-20s %s %s" % (fmt.bold(srv_name), cmd, srv_args))

    if (toolkit_dir / ".claudeignore").is_file():
        fmt.echo("\nIgnore: .claudeignore")
