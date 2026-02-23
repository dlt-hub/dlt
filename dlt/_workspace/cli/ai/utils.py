import os
import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import tomlkit

from dlt.common.json import json
from dlt.common.configuration.const import TYPE_EXAMPLES
from dlt._workspace.cli import echo as fmt
from dlt._workspace.cli.formatters import (
    extract_first_heading,
    parse_frontmatter,
    render_frontmatter,
)


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


def strip_non_claude_frontmatter(content: str) -> str:
    """Remove frontmatter that is not relevant to Claude."""
    fm, body = parse_frontmatter(content)
    if not fm:
        return content
    claude_keys = {"name", "description"}
    kept = {k: v for k, v in fm.items() if k in claude_keys}
    return render_frontmatter(kept, body)


def ensure_cursor_frontmatter(content: str) -> str:
    """Ensure Cursor-compatible frontmatter with alwaysApply and description."""
    fm, body = parse_frontmatter(content)
    fm["alwaysApply"] = True
    if "description" not in fm:
        heading = extract_first_heading(body)
        if heading:
            fm["description"] = heading
    return render_frontmatter(fm, body)


def wrap_as_skill(content: str, skill_name: str) -> str:
    """Wrap content as a Codex SKILL.md with name/description frontmatter."""
    fm, body = parse_frontmatter(content)
    skill_fm: Dict[str, Any] = {}
    skill_fm["name"] = fm.get("name", skill_name)
    skill_fm["description"] = fm.get("description") or extract_first_heading(body) or skill_name
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

    Configuration placeholder values from TYPE_EXAMPLES (e.g. ``<configure me>``)
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


def read_plugin_meta(toolkit_dir: Path) -> Optional[Dict[str, Any]]:
    """Read plugin.json from a toolkit directory. Returns None on failure."""
    meta_json = toolkit_dir / ".claude-plugin" / "plugin.json"
    if not meta_json.exists():
        return None
    try:
        return json.loads(meta_json.read_text(encoding="utf-8"))  # type: ignore[no-any-return]
    except (ValueError, OSError):
        return None


def read_mcp_servers(toolkit_dir: Path) -> Dict[str, Any]:
    """Read MCP server definitions from plugin.json mcpServers or standalone .mcp.json/mcp.json."""
    meta = read_plugin_meta(toolkit_dir)
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


def iter_toolkits(base: Path) -> List[Tuple[str, Dict[str, Any]]]:
    """Iterate toolkit subdirs in base, skipping names starting with ``_``.

    Returns sorted list of (dir_name, plugin_meta) tuples.
    """
    results: List[Tuple[str, Dict[str, Any]]] = []
    for entry in sorted(base.iterdir()):
        if not entry.is_dir() or entry.name.startswith("_"):
            continue
        meta = read_plugin_meta(entry)
        if meta is not None:
            results.append((entry.name, meta))
    return results


def copy_repo_files(
    src_dir: Path, dest_dir: Path, warn_on_overwrite: bool = True
) -> Tuple[List[str], int]:
    """Copy files from src_dir into dest_dir, skipping existing files.

    ``.message`` files are echoed instead of copied.  Returns (copied_names, total_count).
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
