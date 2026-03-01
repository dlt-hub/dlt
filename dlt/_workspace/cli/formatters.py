import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import yaml

from dlt.common.json import custom_encode, json


def parse_frontmatter(text: str) -> Tuple[Dict[str, Any], str]:
    """Split YAML frontmatter (`---` delimited) from markdown body."""
    lines = text.split("\n")
    if not lines or lines[0].rstrip("\r") != "---":
        return {}, text
    close = None
    for i in range(1, len(lines)):
        if lines[i].rstrip("\r") == "---":
            close = i
            break
    if close is None:
        return {}, text
    fm_str = "\n".join(lines[1:close])
    body = "\n".join(lines[close + 1 :])
    data: Dict[str, Any] = yaml.safe_load(fm_str) or {}
    if not isinstance(data, dict):
        return {}, text
    return data, body


def render_frontmatter(data: Dict[str, Any], body: str) -> str:
    """Combine frontmatter dict and body into `---\\nyaml\\n---\\nbody`."""
    if not data:
        return body
    fm = yaml.dump(data, default_flow_style=False, sort_keys=False).rstrip("\n")
    return "---\n" + fm + "\n---\n" + body


def extract_first_heading(body: str) -> Optional[str]:
    """Return text of the first markdown heading, or None."""
    for line in body.splitlines():
        stripped = line.strip()
        if stripped.startswith("#"):
            return stripped.lstrip("#").strip()
    return None


def read_md_name_desc(path: Path) -> Tuple[str, str]:
    """Read a markdown file and return (name, description) from its frontmatter."""
    text = path.read_text(encoding="utf-8")
    try:
        fm, body = parse_frontmatter(text)
    except yaml.YAMLError:
        fm, body = {}, text
    name = fm.get("name", path.stem)
    desc = fm.get("description") or extract_first_heading(body) or ""
    return name, desc


# c0 controls (except tab 0x09, LF 0x0A, CR 0x0D), DEL, c1 controls
_CONTROL_CHAR_RE = re.compile("[\x00-\x08\x0b\x0c\x0e-\x1f\x7f\x80-\x9f]")


def sanitize_string(s: str) -> str:
    """Strip control characters and lone surrogates."""
    s = _CONTROL_CHAR_RE.sub("", s)
    s = s.encode("utf-8", errors="surrogatepass").decode("utf-8", errors="ignore")
    return s


def sanitize_value(val: Any) -> str:
    """Coerce a value to a sanitized string representation."""
    if val is None:
        return ""
    if isinstance(val, str):
        return sanitize_string(val)
    try:
        encoded = custom_encode(val)
    except TypeError:
        encoded = val
    return sanitize_string(str(encoded))


def _sanitize_json_value(val: Any) -> Any:
    """Sanitize a value for JSON output, preserving native JSON types."""
    if val is None:
        return None
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float)):
        return val
    if isinstance(val, str):
        return sanitize_string(val)
    try:
        encoded = custom_encode(val)
    except TypeError:
        encoded = val
    if isinstance(encoded, (dict, list)):
        return encoded
    return sanitize_string(str(encoded))


def md_table(
    columns: Sequence[str],
    rows: Sequence[Tuple[Any, ...]],
) -> str:
    """Render columns and rows as a Markdown table with sanitized values."""
    if not columns:
        return "*(empty result)*"

    headers = [sanitize_string(c) for c in columns]
    str_rows: List[List[str]] = [[sanitize_value(v) for v in row] for row in rows]

    lines = ["| " + " | ".join(headers) + " |"]
    lines.append("| " + " | ".join("---" for _ in headers) + " |")
    for str_row in str_rows:
        lines.append("| " + " | ".join(str_row) + " |")

    lines.append(f"\n*({len(str_rows)} row(s))*")
    return "\n".join(lines)


def jsonl(
    columns: Sequence[str],
    rows: Sequence[Tuple[Any, ...]],
) -> str:
    """Render columns and rows as JSON-lines (one JSON object per row)."""
    if not columns:
        return ""

    lines: List[str] = []
    col_list = list(columns)
    for row in rows:
        obj: Dict[str, Any] = {}
        for i, val in enumerate(row):
            if i < len(col_list):
                obj[col_list[i]] = _sanitize_json_value(val)
        lines.append(json.dumps(obj))
    return "\n".join(lines)
