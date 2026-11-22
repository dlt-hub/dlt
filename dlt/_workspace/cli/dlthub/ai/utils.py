import hashlib
import os
import re
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Set, Tuple

import tomlkit
import yaml

from dlt.common.json import json
from dlt.common.utils import uniq_id, update_dict_nested
from dlt.common.configuration.const import TYPE_EXAMPLES
from dlt.common.configuration.providers.toml import SecretsTomlProvider, SettingsTomlProvider
from dlt.common.pipeline import get_dlt_repos_dir
from dlt.common.runtime.run_context import active as active_run_context
from dlt.version import __version__ as dlt_ver

from dlt._workspace._workspace_context import WorkspaceRunContext
from dlt._workspace.cli import _urls
from dlt._workspace.cli._repo_source import (
    TRepoSource,
    get_recorded_repo_source,
    resolve_repo_source,
)
from dlt._workspace.cli._urls import DEFAULT_AI_WORKBENCH_BRANCH  # noqa: F401
from dlt._workspace.cli._urls import DEFAULT_AI_WORKBENCH_REPO  # noqa: F401
from dlt._workspace.cli.dlthub.ai.typing import (
    TAgentVisibility,
    TAiStatusInfo,
    TAiStatusWarning,
    TToolkitIndexEntry,
    TToolkitInfo,
    TToolkitsIndex,
    TWorkbenchComponentInfo,
    TWorkbenchToolkitInfo,
)
from dlt._workspace.cli.markdown import MarkdownDocument
from dlt._workspace.cli.utils import get_provider_locations, make_dlt_settings_path
from dlt._workspace.exceptions import ToolkitsIndexEngineNoUpgradePath
from dlt._workspace.settings_files import load_yaml_file, save_yaml_file
from dlt._workspace.typing import TLocationInfo

AI_WORKBENCH_BASE_DIR = "workbench"
TOOLKITS_INDEX_FILE = ".toolkits"
TOOLKITS_INDEX_ENGINE_VERSION = 1
DLTHUB_AGENTS_DIR = "dlthub/agents"
"""Folder of dlt agents in a toolkit and in a host folder, apart from the host's own `agents`."""

MCP_FEATURE_TOKEN_RE = re.compile(r"[+-]?[A-Za-z0-9_][A-Za-z0-9_.-]*")
"""A feature group a toolkit adds (`name`, `+name`) or removes (`-name`), without commas."""

_workbench_lock = threading.Lock()  # lock git clone operation


def compute_file_hash(file_path: Path) -> str:
    """Return the SHA3-256 hex digest of a file's raw bytes."""
    return hashlib.sha3_256(file_path.read_bytes()).hexdigest()


def compute_content_hash(content: bytes) -> str:
    """Return the SHA3-256 hex digest of `content`."""
    return hashlib.sha3_256(content).hexdigest()


def home_dir() -> Optional[Path]:
    """Return the user home directory or None when unavailable."""
    home = os.path.expanduser("~")
    if home and home != "~" and os.path.isdir(home):
        return Path(home)
    return None


def _strip_mcp_type(servers: Dict[str, Any]) -> Dict[str, Any]:
    """Strip the `type` field from each server config (Claude -> other platform)."""
    return {name: {k: v for k, v in cfg.items() if k != "type"} for name, cfg in servers.items()}


def parse_json_mcp(content: str, top_key: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Parse a JSON MCP config file into (full_data, servers_dict)."""
    if not content.strip():
        return {}, {}
    data: Dict[str, Any] = json.loads(content)
    servers: Dict[str, Any] = data.get(top_key, {})
    return data, servers


def merge_json_mcp_servers(
    existing_content: str,
    new_servers: Dict[str, Any],
    top_key: str,
    strip_type: bool,
) -> str:
    """Merge new MCP server entries into a JSON config file."""
    data, servers = parse_json_mcp(existing_content, top_key)
    if not servers:
        servers = data.setdefault(top_key, {})
    servers.update(_strip_mcp_type(new_servers) if strip_type else new_servers)
    return json.dumps(data, pretty=True) + "\n"


def merge_json_hooks(
    existing_content: str, event: str, matcher_group: Dict[str, Any]
) -> Optional[str]:
    """Add a matcher group to `event` in a JSON hooks config. Returns `None` when every hook of
    the group already runs for the event.
    """
    data: Dict[str, Any] = json.loads(existing_content) if existing_content.strip() else {}
    groups: List[Dict[str, Any]] = data.setdefault("hooks", {}).setdefault(event, [])
    commands = {hook.get("command") for group in groups for hook in group.get("hooks", [])}
    if all(hook["command"] in commands for hook in matcher_group["hooks"]):
        return None
    groups.append(matcher_group)
    return json.dumps(data, pretty=True) + "\n"


def parse_toml_mcp(content: str, top_key: str) -> Tuple[tomlkit.TOMLDocument, Dict[str, Any]]:
    """Parse a TOML MCP config file into (document, servers_dict)."""
    if not content.strip():
        return tomlkit.document(), {}
    doc = tomlkit.parse(content)
    servers: Dict[str, Any] = doc[top_key].unwrap() if top_key in doc else {}
    return doc, servers


def merge_toml_mcp_servers(
    existing_content: str,
    new_servers: Dict[str, Any],
    top_key: str,
    defaults: Optional[Dict[str, Any]] = None,
) -> str:
    """Merge new MCP server entries into a TOML config file (always strips type). `defaults` are
    top-level settings written only when the file does not have them."""
    doc, _ = parse_toml_mcp(existing_content, top_key)
    for key, value in (defaults or {}).items():
        if key not in doc:
            doc[key] = value
    if top_key not in doc:
        doc.add(top_key, tomlkit.table())
    mcp: Any = doc[top_key]
    for name, config in _strip_mcp_type(new_servers).items():
        server = mcp.get(name)
        if not isinstance(server, dict):
            mcp[name] = config
            continue
        # update the existing table in place so the user's comments and dotted keys stay valid
        for key in [key for key in server if key not in config]:
            del server[key]
        for key, value in config.items():
            if key not in server or server[key] != value:
                server[key] = value
    return tomlkit.dumps(doc)


def read_rule_body(rule_file: Path) -> str:
    """Rule text without its frontmatter. A rule a user broke is still read, never raises."""
    text = rule_file.read_text(encoding="utf-8", errors="replace")
    return MarkdownDocument(text).body_text.strip("\n")


def strip_rule_frontmatter(content: str) -> str:
    """Strip frontmatter keys that Claude Code rules don't use (e.g. alwaysApply)."""
    fm, body = MarkdownDocument(content).split_frontmatter()
    if not fm:
        return content
    kept = {k: v for k, v in fm.items() if k in ("name", "description")}
    return str(MarkdownDocument.from_frontmatter(kept, body)) if kept else body


def ensure_cursor_rule_frontmatter(content: str) -> str:
    """Ensure Cursor rule has alwaysApply and description in frontmatter."""
    doc = MarkdownDocument(content)
    fm, body = doc.split_frontmatter()
    fm["alwaysApply"] = True
    if "description" not in fm and (heading := doc.find_first_heading()):
        fm["description"] = heading.text
    return str(MarkdownDocument.from_frontmatter(fm, body))


ALWAYS_APPLY_SKILL_PREFIX = "ALWAYS read and follow this skill before acting. "
"""Starts the description of the skill a rule is installed as, for agents without rules."""


def wrap_as_skill(content: str, skill_name: str, always_apply: bool = False) -> str:
    """Wrap content as a Codex SKILL.md with name/description frontmatter."""
    doc = MarkdownDocument(content)
    fm, body = doc.split_frontmatter()
    heading = doc.find_first_heading()
    skill_fm: Dict[str, Any] = {}
    skill_fm["name"] = fm.get("name", skill_name)
    desc = fm.get("description") or (heading.text if heading else None) or skill_name
    if always_apply:
        desc = ALWAYS_APPLY_SKILL_PREFIX + desc
    skill_fm["description"] = desc
    return str(MarkdownDocument.from_frontmatter(skill_fm, body))


def is_always_apply_skill(skill_md: Path) -> bool:
    """Tell whether a SKILL.md is a rule installed as an always-apply skill."""
    try:
        doc = MarkdownDocument(skill_md.read_text(encoding="utf-8"))
        description = doc.split_frontmatter()[0].get("description")
    except (yaml.YAMLError, UnicodeDecodeError):
        # a skill the user broke is not one of ours
        return False
    return isinstance(description, str) and description.startswith(ALWAYS_APPLY_SKILL_PREFIX)


def cap_skill_description(content: str, max_len: int) -> Optional[str]:
    """Truncate a SKILL.md frontmatter `description` to `max_len` characters.

    Some agents (e.g. Codex) silently drop a skill whose description exceeds a
    hard limit. Returns the rewritten content when truncation was applied, or
    None when the description already fits (or there is none).
    """
    fm, body = MarkdownDocument(content).split_frontmatter()
    desc = fm.get("description")
    if not isinstance(desc, str) or len(desc) <= max_len:
        return None
    ellipsis = "…"
    fm["description"] = desc[: max_len - len(ellipsis)].rstrip() + ellipsis
    return str(MarkdownDocument.from_frontmatter(fm, body))


def safe_write_bytes(dest: Path, data: bytes) -> None:
    """Write data to dest atomically via write-then-move.

    Writes to a uniquely-named temp sibling first, then uses os.replace()
    for an atomic rename on the same filesystem.
    """
    tmp = dest.parent / (dest.name + "." + uniq_id(8) + ".tmp")
    try:
        tmp.write_bytes(data)
        os.replace(tmp, dest)
    except BaseException:
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass
        raise


MIN_REDACT_STARS = 10
"""Minimum number of stars used when redacting a value, hides the length of short secrets."""


def redact_value(val: Any) -> str:
    """Replaces a leaf value with stars.

    Values shorter than MIN_REDACT_STARS always produce that many stars (hides
    length of short secrets). Longer values produce stars matching their length.
    Newlines are preserved.
    """
    s = str(val)
    if "\n" in s:
        return "\n".join(
            ("*" * max(MIN_REDACT_STARS, len(line)) if line else "") for line in s.split("\n")
        )
    return "*" * max(MIN_REDACT_STARS, len(s))


def redact_toml_document(doc: tomlkit.TOMLDocument) -> tomlkit.TOMLDocument:
    """Returns a deep copy of `doc` with all leaf values replaced by stars.

    Configuration placeholder values from TYPE_EXAMPLES (e.g. `<configure me>`)
    are preserved verbatim so the caller can see which fields still need to be set.
    """
    placeholders = set(TYPE_EXAMPLES.values())
    redacted = tomlkit.parse(tomlkit.dumps(doc))

    def _is_placeholder(val: Any) -> bool:
        try:
            return val in placeholders
        except TypeError:
            return False

    def _redact(container: Any) -> None:
        if isinstance(container, dict):
            for key in container:
                val = container[key]
                if isinstance(val, dict):
                    _redact(val)
                elif not _is_placeholder(val):
                    container[key] = redact_value(val)

    _redact(redacted)
    return redacted


def fetch_secrets_list() -> List[TLocationInfo]:
    """Return project-scoped secret file locations, profile-scoped first."""
    locations: List[TLocationInfo] = []
    for info in get_provider_locations():
        if not isinstance(info.provider, SecretsTomlProvider):
            continue
        project_locs = [loc for loc in info.locations if loc.scope == "project"]
        project_locs.sort(key=lambda loc: (loc.profile_name is None, loc.path))
        for loc in project_locs:
            entry = TLocationInfo(
                path=loc.path,
                present=loc.present,
                scope=loc.scope,
            )
            if loc.profile_name is not None:
                entry["profile_name"] = loc.profile_name
            locations.append(entry)
    return locations


def fetch_secrets_view_redacted(path: Optional[str] = None) -> Optional[str]:
    """Return redacted secrets TOML string, or None if not found.

    Without path, returns the unified merged view from SecretsTomlProvider.
    With path, returns that exact file redacted.
    """
    if path:
        try:
            with open(path, "r", encoding="utf-8") as f:
                doc = tomlkit.load(f)
        except FileNotFoundError:
            return None
    else:
        doc = None
        for info in get_provider_locations():
            if not isinstance(info.provider, SecretsTomlProvider):
                continue
            doc = info.provider._config_toml
            break
        if doc is None or len(doc.body) == 0:
            return None
    return tomlkit.dumps(redact_toml_document(doc))


def fetch_secrets_update_fragment(fragment: str, path: str) -> str:
    """Merge a TOML fragment into the secrets file at path.

    Creates the file if needed. Returns the redacted TOML after merge.
    Raises tomlkit.exceptions.TOMLKitError on invalid fragment.
    """
    settings_dir = os.path.dirname(path) or "."
    file_name = os.path.basename(path)
    os.makedirs(settings_dir, exist_ok=True)
    if not os.path.isfile(path):
        with open(path, "w", encoding="utf-8") as f:
            f.write("")
        # a new secrets file of a profile changes its `configured` flag
        ctx = active_run_context()
        if isinstance(ctx, WorkspaceRunContext):
            ctx.reload_workspace_definition()
    provider = SettingsTomlProvider(file_name, True, file_name, [settings_dir])
    # allow literal \n (two chars) as newline — agents on Windows/PowerShell
    # can't easily pass real newlines, so we accept the escaped form
    if "\\n" in fragment and "\n" not in fragment:
        fragment = fragment.replace("\\n", "\n")
    parsed = tomlkit.parse(fragment)
    update_dict_nested(provider._config_toml, parsed)
    provider.write_toml()
    return tomlkit.dumps(redact_toml_document(provider._config_toml))


def read_workbench_toolkit_combined_info(toolkit_dir: Path) -> Optional[Dict[str, Any]]:
    """Read plugin.json and toolkit.json from a toolkit directory.

    plugin.json must exist and is the base metadata (validated by Claude).
    toolkit.json is optional and carries dlt-specific fields (`listed`,
    `dependencies`) that are not part of the Claude plugin schema.  When
    present its keys are merged on top of plugin.json.

    Returns None when plugin.json is missing or unreadable.
    """
    plugin_dir = toolkit_dir / ".claude-plugin"
    meta_json = plugin_dir / "plugin.json"
    if not meta_json.exists():
        return None
    try:
        meta: Dict[str, Any] = json.loads(meta_json.read_text(encoding="utf-8"))
    except (ValueError, OSError):
        return None
    # merge dlt-specific toolkit.json when present
    toolkit_json = plugin_dir / "toolkit.json"
    if toolkit_json.is_file():
        extra = json.loads(toolkit_json.read_text(encoding="utf-8"))
        meta.update(extra)
    return meta


def read_workbench_toolkit_mcp_servers(toolkit_dir: Path) -> Dict[str, Any]:
    """Read MCP server definitions from plugin.json mcpServers or standalone .mcp.json/mcp.json."""
    meta = read_workbench_toolkit_combined_info(toolkit_dir)
    if meta and "mcpServers" in meta:
        servers: Dict[str, Any] = meta["mcpServers"]
        return servers
    # check standalone files — .mcp.json (Claude format) then mcp.json
    for name in (".mcp.json", "mcp.json"):
        mcp_file = toolkit_dir / name
        if mcp_file.is_file():
            data = json.loads(mcp_file.read_text(encoding="utf-8"))
            # .mcp.json uses {"mcpServers": {...}} wrapper
            if "mcpServers" in data:
                return data["mcpServers"]  # type: ignore[no-any-return]
            return data  # type: ignore[no-any-return]
    return {}


def build_toolkits_dependency_map(
    toolkits: Mapping[str, TToolkitInfo],
) -> Dict[str, List[str]]:
    """Build a map of toolkit name -> list of dependency toolkit names."""
    return {name: list(info.get("dependencies", [])) for name, info in toolkits.items()}


def resolve_toolkit_dependencies(
    name: str, dep_map: Dict[str, List[str]], skip: Optional[Set[str]] = None
) -> List[str]:
    """Return install-order list of dependencies for a toolkit.

    Args:
        name: Toolkit to resolve dependencies for.
        dep_map: Mapping of toolkit name to its direct dependencies.
        skip: Toolkits left out together with their own dependencies.

    Returns:
        Topologically sorted dependency names, excluding the toolkit itself.

    Raises:
        ValueError: On circular dependencies.
    """
    order: List[str] = []
    visited: Set[str] = set()
    path: Set[str] = set()
    skip = skip or set()

    def _visit(n: str) -> None:
        if n in path:
            raise ValueError("Circular dependency: %s" % " -> ".join([*path, n]))
        if n in visited or (n in skip and n != name):
            return
        path.add(n)
        for dep in dep_map.get(n, []):
            _visit(dep)
        path.discard(n)
        visited.add(n)
        order.append(n)

    _visit(name)
    # remove the toolkit itself — caller installs it separately
    order.remove(name)
    return order


def fetch_workbench_base(location: str, branch: Optional[str]) -> Path:
    """Fetch the dltHub AI Harness repo and return its base path.

    Thread-safe: git operations on the shared repo directory are serialized.

    Raises:
        FileNotFoundError: When the `workbench` directory is missing from the repo.
    """
    from dlt.common.libs import git

    branch = branch or DEFAULT_AI_WORKBENCH_BRANCH
    with _workbench_lock:
        src_storage = git.get_fresh_repo_files(
            location, get_dlt_repos_dir(), branch=branch, path=AI_WORKBENCH_BASE_DIR
        )
    if not src_storage.has_folder(AI_WORKBENCH_BASE_DIR):
        raise FileNotFoundError(
            "dltHub AI Harness directory '%s' not found in repo %s"
            % (AI_WORKBENCH_BASE_DIR, location)
        )
    return Path(src_storage.make_full_path(AI_WORKBENCH_BASE_DIR))


def is_visible(visibility: Optional[TAgentVisibility], agent_name: Optional[str]) -> bool:
    """Tell whether `agent_name` sees an item with `visibility`. Without an agent all is visible."""
    if not visibility or agent_name is None:
        return True
    if "include" in visibility:
        return agent_name in visibility["include"]
    return agent_name not in visibility.get("exclude", [])


def is_toolkit_visible(info: TToolkitInfo, agent_name: Optional[str]) -> bool:
    """Tell whether `agent_name` sees the toolkit."""
    return is_visible(info.get("agent_visibility"), agent_name)


def is_component_visible(info: TToolkitInfo, component_key: str, agent_name: Optional[str]) -> bool:
    """Tell whether `agent_name` sees a component, keyed by source folder and name (`skills/x`)."""
    return is_visible(info.get("component_visibility", {}).get(component_key), agent_name)


def fetch_workbench_toolkits(
    base: Path,
    listed_only: bool = False,
    strict: bool = False,
    agent_name: Optional[str] = None,
) -> Tuple[Dict[str, TToolkitInfo], List[str]]:
    """Scan dltHub AI Harness `workbench` directory and return mapping of toolkit name -> TToolkitInfo.

    Args:
        base: Root directory of the harness repo.
        listed_only: When True, skip toolkits with `listed: false` in metadata.
        strict: When True, raise on invalid toolkit metadata instead of warning.
        agent_name: Skip toolkits this agent does not see. `None` keeps all of them.

    Returns:
        Tuple of (toolkits mapping, warnings). Warnings contain messages for
        toolkits with invalid metadata that were skipped.

    Raises:
        ValueError: When `strict` is True and a toolkit has invalid metadata.
    """
    result: Dict[str, TToolkitInfo] = {}
    warnings: List[str] = []
    for entry in sorted(base.iterdir()):
        if not entry.is_dir():
            continue
        meta = read_workbench_toolkit_combined_info(entry)
        if meta is None:
            continue
        if listed_only and meta.get("listed", True) is False:
            continue
        try:
            info = extract_toolkit_info(meta, entry.name)
        except ValueError as ex:
            if strict:
                raise
            warnings.append(str(ex))
            continue
        if not is_toolkit_visible(info, agent_name):
            continue
        result[info["name"]] = info
    return result, warnings


def _parse_visibility(value: Any, where: str) -> TAgentVisibility:
    """Validate an `{"include": [...]}` or `{"exclude": [...]}` visibility entry."""
    if (
        not isinstance(value, dict)
        or len(value) != 1
        or not set(value) <= {"include", "exclude"}
        or not isinstance(next(iter(value.values())), list)
    ):
        raise ValueError(
            "%s: agent visibility must be either `include` or `exclude` with a list of agent names"
            % where
        )
    return value  # type: ignore[return-value]


def extract_toolkit_info(meta: Dict[str, Any], fallback_name: str) -> TToolkitInfo:
    """Build a TToolkitInfo from a raw metadata dict.

    Args:
        meta: Raw metadata dictionary with toolkit fields.
        fallback_name: Name used in error messages when validation fails.

    Returns:
        Validated toolkit info.

    Raises:
        ValueError: When name, description, or version is missing or empty.
    """
    missing = [k for k in ("name", "description", "version") if not meta.get(k)]
    if missing:
        raise ValueError(
            "plugin.json for %s missing required fields: %s" % (fallback_name, ", ".join(missing))
        )
    tk_meta = TToolkitInfo(
        name=meta["name"],
        version=meta["version"],
        description=meta["description"],
        tags=list(meta.get("keywords", [])),
    )
    if deps := meta.get("dependencies"):
        tk_meta["dependencies"] = list(deps)
    if wes := meta.get("workflow_entry_skill"):
        tk_meta["workflow_entry_skill"] = wes
    if "agents" in meta:
        tk_meta["agent_visibility"] = _parse_visibility(
            meta["agents"], "toolkit.json for %s" % fallback_name
        )
    if components := meta.get("components"):
        if not isinstance(components, dict):
            raise ValueError(
                "toolkit.json for %s: `components` must map component keys to their settings"
                % fallback_name
            )
        tk_meta["component_visibility"] = {
            key: _parse_visibility(
                entry.get("agents") if isinstance(entry, dict) else None,
                "toolkit.json for %s, component %s" % (fallback_name, key),
            )
            for key, entry in components.items()
        }
    if mcp_features := meta.get("mcp_features"):
        if not isinstance(mcp_features, dict) or not all(
            isinstance(features, list)
            and features
            and all(isinstance(f, str) and MCP_FEATURE_TOKEN_RE.fullmatch(f) for f in features)
            for features in mcp_features.values()
        ):
            raise ValueError(
                "toolkit.json for %s: `mcp_features` must map MCP server names to lists of"
                " feature groups, like `dlthub.jobs`, `+dlthub.jobs` or `-secrets`" % fallback_name
            )
        tk_meta["mcp_features"] = {name: list(features) for name, features in mcp_features.items()}
    return tk_meta


def fetch_workbench_toolkit_info(
    name: str,
    location: str,
    branch: Optional[str] = None,
    agent_name: Optional[str] = None,
) -> Optional[TWorkbenchToolkitInfo]:
    """Return detailed toolkit info from the dltHub AI Harness repo.

    Args:
        name: Toolkit name to look up.
        location: Harness git repo URL or local path.
        branch: Git branch to fetch. Uses default harness branch when None.
        agent_name: Leave out the components this agent does not see. `None` keeps all of them.

    Returns:
        Toolkit info with skills, commands, rules, and agents, or None if not found.
    """
    base = fetch_workbench_base(location, branch)
    toolkit_dir = base / name
    meta = read_workbench_toolkit_combined_info(toolkit_dir)
    if meta is None:
        return None

    tk_meta = extract_toolkit_info(meta, name)

    def _visible(folder: str, component_name: str) -> bool:
        return is_component_visible(tk_meta, folder + "/" + component_name, agent_name)

    def _components(
        md_files: List[Path], folder: str, folder_named: bool = False
    ) -> List[TWorkbenchComponentInfo]:
        # a skill or an agent is its folder, so a missing `name` falls back to the folder name
        return [
            TWorkbenchComponentInfo(name=n, description=d)
            for n, d in (
                MarkdownDocument.read_name_desc(f, f.parent.name if folder_named else None)
                for f in md_files
                if _visible(folder, f.parent.name if folder_named else f.stem)
            )
        ]

    skills: List[TWorkbenchComponentInfo] = []
    skills_dir = toolkit_dir / "skills"
    if skills_dir.is_dir():
        skills = _components(
            [
                p / "SKILL.md"
                for p in sorted(skills_dir.iterdir())
                if p.is_dir() and (p / "SKILL.md").exists()
            ],
            "skills",
            folder_named=True,
        )

    commands: List[TWorkbenchComponentInfo] = []
    cmd_dir = toolkit_dir / "commands"
    if cmd_dir.is_dir():
        commands = _components(sorted(cmd_dir.glob("*.md")), "commands")

    rules: List[TWorkbenchComponentInfo] = []
    rules_dir = toolkit_dir / "rules"
    if rules_dir.is_dir():
        rules = _components(sorted(rules_dir.glob("*.md")), "rules")

    agents: List[TWorkbenchComponentInfo] = []
    agents_dir = toolkit_dir / DLTHUB_AGENTS_DIR
    if agents_dir.is_dir():
        agents = _components(
            [
                p / "AGENT.md"
                for p in sorted(agents_dir.iterdir())
                if p.is_dir() and (p / "AGENT.md").exists()
            ],
            DLTHUB_AGENTS_DIR,
            folder_named=True,
        )

    servers = {
        server: config
        for server, config in read_workbench_toolkit_mcp_servers(toolkit_dir).items()
        if _visible("mcp", server)
    }

    info = TWorkbenchToolkitInfo(
        **tk_meta,
        skills=skills,
        commands=commands,
        rules=rules,
        agents=agents,
        has_ignore=(toolkit_dir / ".claudeignore").is_file(),
    )
    if servers:
        info["mcp_servers"] = servers
    features = {
        server: names
        for server, names in tk_meta.get("mcp_features", {}).items()
        if _visible("mcp", server)
    }
    if features:
        info["mcp_features"] = features
    else:
        info.pop("mcp_features", None)
    return info


_INIT_TOOLKIT = "init"


def _toolkits_index_path() -> str:
    return make_dlt_settings_path(TOOLKITS_INDEX_FILE)


def _load_toolkits_file() -> TToolkitsIndex:
    """Loads `.dlt/.toolkits`, migrated to the current engine version and saved when migrated.

    Raises:
        ToolkitsIndexEngineNoUpgradePath: A newer dlt wrote the file.
    """
    path = _toolkits_index_path()
    try:
        doc: Dict[str, Any] = load_yaml_file(path)
    except (ValueError, OSError):
        doc = {}
    if not doc:
        return {"engine_version": TOOLKITS_INDEX_ENGINE_VERSION, "toolkits": {}}
    from_engine = doc.get("engine_version", 0)
    if from_engine == TOOLKITS_INDEX_ENGINE_VERSION:
        return doc  # type: ignore[return-value]
    if from_engine != 0:
        raise ToolkitsIndexEngineNoUpgradePath(path, from_engine, TOOLKITS_INDEX_ENGINE_VERSION)
    index = migrate_toolkits_index(doc)
    # a read-only settings dir keeps the old file, the next load migrates again
    try:
        save_yaml_file(path, index)
    except OSError:
        pass
    return index


def migrate_toolkits_index(doc: Dict[str, Any]) -> TToolkitsIndex:
    """Migrates the flat map of toolkits that dlt 1.33.0a2 and earlier wrote to engine 1."""
    index: TToolkitsIndex = {"engine_version": TOOLKITS_INDEX_ENGINE_VERSION, "toolkits": doc}
    for name, entry in doc.items():
        if not isinstance(entry, dict):
            continue
        source = {key: entry.pop(key) for key in ("location", "branch") if key in entry}
        # 1.33.0a1 and a2 recorded the harness source on the toolkits they installed
        if name == _INIT_TOOLKIT:
            index.update(source)  # type: ignore[typeddict-item]
    return index


def _save_toolkits_file(index: TToolkitsIndex) -> None:
    """Saves `index` without the names of the toolkits, which are their keys."""
    toolkits = {
        key: {k: v for k, v in entry.items() if k != "name"}
        for key, entry in index["toolkits"].items()
    }
    save_yaml_file(_toolkits_index_path(), {**index, "toolkits": toolkits})


def load_toolkits_index(hide_init: bool = False) -> Dict[str, TToolkitIndexEntry]:
    """Load the installed toolkits index from the workspace settings directory. `hide_init`
    leaves out `init` and the toolkits it depends on, which set up the agent."""
    data = _load_toolkits_file()["toolkits"]
    # restore name from the YAML key (stripped on write)
    for key, entry in data.items():
        entry["name"] = key
    if hide_init:
        try:
            init_deps = resolve_toolkit_dependencies(
                _INIT_TOOLKIT, build_toolkits_dependency_map(data)
            )
        except ValueError:
            init_deps = []
        data = {k: v for k, v in data.items() if k not in {_INIT_TOOLKIT, *init_deps}}
    return data


def is_toolkit_installed(name: str) -> bool:
    """Check whether a toolkit is recorded in the index."""
    return name in load_toolkits_index()


def save_toolkit_entry(
    toolkit_meta: TToolkitInfo,
    agent: Optional[str] = None,
    files: Optional[Dict[str, Any]] = None,
    mcp_servers: Optional[List[str]] = None,
    see_all: bool = False,
) -> None:
    """Record that a toolkit was installed or updated in the toolkits index.

    Args:
        toolkit_meta: Core toolkit metadata to persist.
        agent: Name of the AI agent that triggered the install.
        files: Mapping of installed file paths to their content hashes.
        mcp_servers: Names of MCP servers provided by the toolkit.
        see_all: Whether the toolkit was installed with `--see-all`.
    """

    index = _load_toolkits_file()
    entry: Dict[str, Any] = dict(toolkit_meta)
    name = entry.pop("name")
    # visibility and MCP features describe the harness, not what got installed
    entry.pop("agent_visibility", None)
    entry.pop("component_visibility", None)
    entry.pop("mcp_features", None)
    entry["installed_at"] = datetime.now(timezone.utc).isoformat()
    if agent:
        entry["agent"] = agent
    if files:
        entry["files"] = files
    if mcp_servers:
        entry["mcp_servers"] = mcp_servers
    if see_all:
        entry["see_all"] = True
    index["toolkits"][name] = entry  # type: ignore[assignment]
    _save_toolkits_file(index)


def save_harness_source(location: str, branch: Optional[str]) -> None:
    """Records the harness `location` and `branch` that the next commands default to. Defaults are
    not recorded, so a reset removes them."""
    source = get_recorded_harness_source(location, branch)
    if not source and not os.path.isfile(_toolkits_index_path()):
        return
    index = _load_toolkits_file()
    index.pop("location", None)
    index.pop("branch", None)
    index.update(source)
    _save_toolkits_file(index)


def resolve_harness_source(
    location: Optional[str] = None, branch: Optional[str] = None
) -> Tuple[str, Optional[str]]:
    """Harness location and branch to fetch from: given, else given last to a command, else the
    defaults. An empty value resets."""
    index = _load_toolkits_file()
    recorded: TRepoSource = {}
    if "location" in index:
        recorded["location"] = index["location"]
    if "branch" in index:
        recorded["branch"] = index["branch"]
    return resolve_repo_source(
        location,
        branch,
        recorded,
        _urls.DEFAULT_AI_WORKBENCH_REPO,
        _urls.DEFAULT_AI_WORKBENCH_BRANCH,
    )


def get_recorded_harness_source(location: str, branch: Optional[str]) -> TRepoSource:
    """The part of the harness `location` and `branch` that the toolkits index records."""
    return get_recorded_repo_source(
        location, branch, _urls.DEFAULT_AI_WORKBENCH_REPO, _urls.DEFAULT_AI_WORKBENCH_BRANCH
    )


def fetch_ai_status(project_root: Path) -> TAiStatusInfo:
    """Collect AI setup status for the current workspace.

    Args:
        project_root: Path to the project root for agent detection.

    Returns:
        Status info including dlt version, detected agent, installed toolkits,
        and any readiness warnings.
    """
    from dlt._workspace.cli.dlthub.ai.agents import _AIAgent  # circular

    index = load_toolkits_index()
    warnings: List[TAiStatusWarning] = []

    # detect agent
    agent_name: Optional[str] = None
    init_entry = index.get(_INIT_TOOLKIT)
    if isinstance(init_entry, dict):
        agent_name = init_entry.get("agent")
    if not agent_name:
        detected = _AIAgent.detect_all(project_root)
        if detected:
            agent_name = detected[0][0].name

    # workspace initialized?
    initialized = os.path.isfile(make_dlt_settings_path("config.toml"))
    if not initialized:
        warnings.append("not_initialized")

    # init toolkit installed?
    has_init = _INIT_TOOLKIT in index
    if not has_init:
        warnings.append("no_init_toolkit")

    toolkits = load_toolkits_index(hide_init=True)
    if not toolkits:
        warnings.append("no_toolkits")

    # check MCP availability
    status = TAiStatusInfo(
        dlt_version=dlt_ver,
        agent_name=agent_name,
        initialized=initialized,
        has_init_toolkit=has_init,
        toolkits=toolkits,
        warnings=warnings,
    )
    try:
        from dlt._workspace.mcp import WorkspaceMCP  # noqa: F401

        WorkspaceMCP("dlt")
    except Exception as ex:
        warnings.append("mcp_unavailable")
        status["mcp_error"] = str(ex)

    return status
