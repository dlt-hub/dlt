import hashlib
import os
import shutil
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import tomlkit
import yaml

from dlt.common.json import json
from dlt.common.pendulum import pendulum
from dlt.common.utils import update_dict_nested
from dlt.common.configuration.const import TYPE_EXAMPLES
from dlt.common.configuration.providers.toml import SecretsTomlProvider, SettingsTomlProvider

from dlt._workspace.typing import TWorkbenchComponentInfo, TWorkbenchMcpServerInfo
from dlt._workspace.cli import echo as fmt
from dlt._workspace.cli.formatters import (
    extract_first_heading,
    parse_frontmatter,
    render_frontmatter,
    read_md_name_desc,
)
from dlt._workspace.cli.utils import get_provider_locations
from dlt._workspace.typing import (
    TLocationInfo,
    TToolkitIndexEntry,
    TToolkitInfo,
    TWorkbenchToolkitInfo,
)


DEFAULT_AI_WORKBENCH_REPO = "https://github.com/dlt-hub/dlthub-ai-workbench.git"
# DEFAULT_AI_WORKBENCH_REPO = "/home/rudolfix/src/dlt-ai-dev-kit"
DEFAULT_AI_WORKBENCH_BRANCH: Optional[str] = "rfix/more-workbench"
AI_WORKBENCH_BASE_DIR = "workbench"
TOOLKITS_INDEX_FILE = ".toolkits"

_workbench_lock = threading.Lock()  # lock git clone operation


def compute_file_hash(file_path: Path) -> str:
    """Return the SHA3-256 hex digest of a file's raw bytes."""
    return hashlib.sha3_256(file_path.read_bytes()).hexdigest()


def compute_content_hash(content: str) -> str:
    """Return the SHA3-256 hex digest of a string encoded as UTF-8."""
    return hashlib.sha3_256(content.encode("utf-8")).hexdigest()


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


def parse_toml_mcp(content: str, top_key: str) -> Tuple[tomlkit.TOMLDocument, Dict[str, Any]]:
    """Parse a TOML MCP config file into (document, servers_dict)."""
    if not content.strip():
        return tomlkit.document(), {}
    doc = tomlkit.parse(content)
    servers: Dict[str, Any] = dict(doc.get(top_key, {}))
    return doc, servers


def merge_toml_mcp_servers(
    existing_content: str,
    new_servers: Dict[str, Any],
    top_key: str,
) -> str:
    """Merge new MCP server entries into a TOML config file (always strips type)."""
    doc, _ = parse_toml_mcp(existing_content, top_key)
    if top_key not in doc:
        doc.add(top_key, tomlkit.table())
    mcp: Any = doc[top_key]
    for name, config in _strip_mcp_type(new_servers).items():
        mcp[name] = config
    return tomlkit.dumps(doc)


def strip_rule_frontmatter(content: str) -> str:
    """Strip frontmatter keys that Claude Code rules don't use (e.g. alwaysApply)."""
    fm, body = parse_frontmatter(content)
    if not fm:
        return content
    kept = {k: v for k, v in fm.items() if k in ("name", "description")}
    return render_frontmatter(kept, body) if kept else body


def ensure_cursor_rule_frontmatter(content: str) -> str:
    """Ensure Cursor rule has alwaysApply and description in frontmatter."""
    fm, body = parse_frontmatter(content)
    fm["alwaysApply"] = True
    if "description" not in fm:
        heading = extract_first_heading(body)
        if heading:
            fm["description"] = heading
    return render_frontmatter(fm, body)


def wrap_as_skill(content: str, skill_name: str, always_apply: bool = False) -> str:
    """Wrap content as a Codex SKILL.md with name/description frontmatter."""
    fm, body = parse_frontmatter(content)
    skill_fm: Dict[str, Any] = {}
    skill_fm["name"] = fm.get("name", skill_name)
    desc = fm.get("description") or extract_first_heading(body) or skill_name
    if always_apply:
        desc = "ALWAYS read and follow this skill before acting. " + desc
    skill_fm["description"] = desc
    return render_frontmatter(skill_fm, body)


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
        try:
            extra = json.loads(toolkit_json.read_text(encoding="utf-8"))
            meta.update(extra)
        except (ValueError, OSError):
            pass
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
            try:
                data = json.loads(mcp_file.read_text(encoding="utf-8"))
                # .mcp.json uses {"mcpServers": {...}} wrapper
                if "mcpServers" in data:
                    return data["mcpServers"]  # type: ignore[no-any-return]
                return data  # type: ignore[no-any-return]
            except (ValueError, OSError):
                pass
    return {}


def build_toolkits_dependency_map(
    toolkits: Dict[str, TToolkitInfo],
) -> Dict[str, List[str]]:
    """Build a map of toolkit name -> list of dependency toolkit names."""
    return {name: list(info.get("dependencies", [])) for name, info in toolkits.items()}


def resolve_toolkit_dependencies(name: str, dep_map: Dict[str, List[str]]) -> List[str]:
    """Return install-order list of dependencies for `name` (excluding `name` itself).

    Raises:
        ValueError: On circular dependencies.
    """
    order: List[str] = []
    visited: Set[str] = set()
    path: Set[str] = set()

    def _visit(n: str) -> None:
        if n in path:
            raise ValueError("Circular dependency: %s" % " -> ".join([*path, n]))
        if n in visited:
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
    """Fetch AI workbench repo and return base Path.

    Thread-safe: git operations on the shared repo directory are serialized.

    Raises:
        FileNotFoundError: When the workbench directory is missing from the repo.
    """
    from dlt.common.libs import git
    from dlt.common.pipeline import get_dlt_repos_dir

    branch = branch or DEFAULT_AI_WORKBENCH_BRANCH
    with _workbench_lock:
        src_storage = git.get_fresh_repo_files(location, get_dlt_repos_dir(), branch=branch)
    if not src_storage.has_folder(AI_WORKBENCH_BASE_DIR):
        raise FileNotFoundError(
            "Workbench directory '%s' not found in repo %s" % (AI_WORKBENCH_BASE_DIR, location)
        )
    return Path(src_storage.make_full_path(AI_WORKBENCH_BASE_DIR))


def scan_workbench_toolkits(base: Path, listed_only: bool = False) -> Dict[str, TToolkitInfo]:
    """Scan workbench directory and return mapping of toolkit name -> TToolkitInfo.

    Reads and validates plugin.json for every toolkit subdirectory.
    Toolkits with invalid metadata are skipped with a warning.
    """
    result: Dict[str, TToolkitInfo] = {}
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
            fmt.warning(str(ex))
            continue
        result[info["name"]] = info
    return result


def extract_toolkit_info(meta: Dict[str, Any], fallback_name: str) -> TToolkitInfo:
    """Build a `TToolkitInfo` from raw plugin.json / toolkit.json dict.

    Raises ``ValueError`` when ``name``, ``description``, or ``version`` is
    missing or empty.
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
    return tk_meta


def fetch_workbench_toolkit_info(
    name: str, location: str, branch: Optional[str] = None
) -> Optional[TWorkbenchToolkitInfo]:
    """Return detailed toolkit info, or None if not found."""
    base = fetch_workbench_base(location, branch)
    toolkit_dir = base / name
    meta = read_workbench_toolkit_combined_info(toolkit_dir)
    if meta is None:
        return None

    tk_meta = extract_toolkit_info(meta, name)

    def _components(md_files: List[Path]) -> List[TWorkbenchComponentInfo]:
        return [
            TWorkbenchComponentInfo(name=n, description=d)
            for n, d in (read_md_name_desc(f) for f in md_files)
        ]

    skills: List[TWorkbenchComponentInfo] = []
    skills_dir = toolkit_dir / "skills"
    if skills_dir.is_dir():
        skills = _components(
            [
                p / "SKILL.md"
                for p in sorted(skills_dir.iterdir())
                if p.is_dir() and (p / "SKILL.md").exists()
            ]
        )

    commands: List[TWorkbenchComponentInfo] = []
    cmd_dir = toolkit_dir / "commands"
    if cmd_dir.is_dir():
        commands = _components(sorted(cmd_dir.glob("*.md")))

    rules: List[TWorkbenchComponentInfo] = []
    rules_dir = toolkit_dir / "rules"
    if rules_dir.is_dir():
        rules = _components(sorted(rules_dir.glob("*.md")))

    servers = read_workbench_toolkit_mcp_servers(toolkit_dir)

    info = TWorkbenchToolkitInfo(
        **tk_meta,
        skills=skills,
        commands=commands,
        rules=rules,
        has_ignore=(toolkit_dir / ".claudeignore").is_file(),
    )
    if servers:
        info["mcp_servers"] = {
            srv: TWorkbenchMcpServerInfo(command=cfg.get("command", ""), args=cfg.get("args", []))
            for srv, cfg in sorted(servers.items())
        }
    return info


def copy_repo_files(
    src_dir: Path, dest_dir: Path, warn_on_overwrite: bool = True
) -> Tuple[List[str], int]:
    """Copy files from src_dir into dest_dir, skipping existing files.

    `.message` files are echoed instead of copied.  Returns (copied_names, total_count).
    """
    copied_files: List[str] = []
    count_files = 0

    for src_sub_path in src_dir.rglob("*"):
        if src_sub_path.is_dir():
            continue

        if src_sub_path.name == ".message":
            fmt.echo(src_sub_path.read_text(encoding="utf-8"))
            continue

        count_files += 1
        dest_file_path = dest_dir / src_sub_path.relative_to(src_dir)
        if dest_file_path.exists():
            if warn_on_overwrite:
                fmt.warning(f"Existing rules file found at {dest_file_path.absolute()}; Skipping.")
            continue

        dest_file_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src_sub_path, dest_file_path)
        copied_files.append(src_sub_path.name)

    return copied_files, count_files


def _toolkits_index_path() -> str:
    from dlt._workspace.cli.utils import make_dlt_settings_path

    return make_dlt_settings_path(TOOLKITS_INDEX_FILE)


def load_toolkits_index() -> Dict[str, TToolkitIndexEntry]:
    """Load the installed toolkits index from .dlt/.toolkits."""
    path = _toolkits_index_path()
    if not os.path.isfile(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data: Dict[str, TToolkitIndexEntry] = yaml.safe_load(f)
        if not data:
            return {}
        # restore name from the YAML key (stripped on write)
        for key, entry in data.items():
            entry["name"] = key
        return data
    except (yaml.YAMLError, OSError):
        return {}


def is_toolkit_installed(name: str) -> bool:
    """Check whether a toolkit is recorded in the index."""
    return name in load_toolkits_index()


def save_toolkit_entry(
    toolkit_meta: TToolkitInfo,
    agent: Optional[str] = None,
    files: Optional[Dict[str, Any]] = None,
    mcp_servers: Optional[List[str]] = None,
) -> None:
    """Record that a toolkit was installed (or updated)."""

    index = load_toolkits_index()
    entry: Dict[str, Any] = dict(toolkit_meta)
    name = entry.pop("name")
    entry["installed_at"] = pendulum.now("UTC").isoformat()
    if agent:
        entry["agent"] = agent
    if files:
        entry["files"] = files
    if mcp_servers:
        entry["mcp_servers"] = mcp_servers
    index[name] = entry  # type: ignore[assignment]
    path = _toolkits_index_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        yaml.dump(index, f, allow_unicode=True, default_flow_style=False, sort_keys=False)
