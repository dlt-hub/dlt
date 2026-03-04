import re
from pathlib import Path
from typing import Any, Dict, List, NamedTuple, Optional, Sequence, Set, Tuple

import yaml

from dlt.common.json import custom_encode, json

_HEADING_RE = re.compile(r"^(#{1,6})\s+(.*)")


class MarkdownHeading(NamedTuple):
    line_index: int  # line index in document
    depth: int  # number of # (1-6)
    text: str  # heading text after # chars


class MarkdownDocument:
    """Simple line-based markdown document for heading lookup, frontmatter, and line insertion."""

    def __init__(self, text: str) -> None:
        self._lines: List[str] = text.split("\n")

    def __str__(self) -> str:
        return "\n".join(self._lines)

    @property
    def lines(self) -> List[str]:
        return self._lines

    @property
    def is_empty(self) -> bool:
        return self._lines == [""]

    def find_headings(self, depth: Optional[int] = None) -> List[MarkdownHeading]:
        """Return all headings, optionally filtered by depth (1-6)."""
        result: List[MarkdownHeading] = []
        for i, line in enumerate(self._lines):
            m = _HEADING_RE.match(line.strip())
            if m:
                d = len(m.group(1))
                if depth is None or d == depth:
                    result.append(MarkdownHeading(i, d, m.group(2).strip()))
        return result

    def find_first_heading(self) -> Optional[MarkdownHeading]:
        """Return the first heading or None."""
        for i, line in enumerate(self._lines):
            m = _HEADING_RE.match(line.strip())
            if m:
                return MarkdownHeading(i, len(m.group(1)), m.group(2).strip())
        return None

    def find_line(self, text: str) -> Optional[int]:
        """Return index of first line whose stripped content equals *text*, or None."""
        for i, line in enumerate(self._lines):
            if line.strip() == text:
                return i
        return None

    def search(self, pattern: str) -> List[Tuple[int, "re.Match[str]"]]:
        """Return ``(index, match)`` for every line matching *pattern*."""
        compiled = re.compile(pattern)
        return [(i, m) for i, line in enumerate(self._lines) if (m := compiled.search(line))]

    def insert_lines(self, index: int, new_lines: List[str]) -> None:
        """Insert *new_lines* before *index*, mutating the document."""
        for j, entry in enumerate(new_lines):
            self._lines.insert(index + j, entry)

    def insert_md(self, index: Optional[int], other: "MarkdownDocument") -> None:
        """Insert all lines of *other* before *index*. ``None`` appends at end."""
        if index is None:
            index = len(self._lines)
        self.insert_lines(index, other.lines)

    def frontmatter(self) -> Tuple[Dict[str, Any], int]:
        """Parse ``---`` delimited YAML frontmatter.

        Returns ``(data_dict, body_start_index)``. When there is no valid
        frontmatter, returns ``({}, 0)``.
        """
        if not self._lines or self._lines[0].rstrip("\r") != "---":
            return {}, 0
        close = None
        for i in range(1, len(self._lines)):
            if self._lines[i].rstrip("\r") == "---":
                close = i
                break
        if close is None:
            return {}, 0
        fm_str = "\n".join(self._lines[1:close])
        data: Dict[str, Any] = yaml.safe_load(fm_str) or {}
        if not isinstance(data, dict):
            return {}, 0
        return data, close + 1

    @property
    def body_text(self) -> str:
        """Text after frontmatter (or full text when there is none)."""
        _, start = self.frontmatter()
        return "\n".join(self._lines[start:])

    @staticmethod
    def from_frontmatter(data: Dict[str, Any], body: str) -> "MarkdownDocument":
        """Create a document from a frontmatter dict and body text."""
        if not data:
            return MarkdownDocument(body)
        fm = yaml.dump(data, default_flow_style=False, sort_keys=False).rstrip("\n")
        return MarkdownDocument("---\n" + fm + "\n---\n" + body)


def parse_frontmatter(text: str) -> Tuple[Dict[str, Any], str]:
    """Split YAML frontmatter (`---` delimited) from markdown body."""
    doc = MarkdownDocument(text)
    data, body_start = doc.frontmatter()
    if not data and body_start == 0 and text:
        # no valid frontmatter — return original text as body
        return {}, text
    return data, doc.body_text


def render_frontmatter(data: Dict[str, Any], body: str) -> str:
    """Combine frontmatter dict and body into `---\\nyaml\\n---\\nbody`."""
    return str(MarkdownDocument.from_frontmatter(data, body))


def extract_first_heading(body: str) -> Optional[str]:
    """Return text of the first markdown heading, or None."""
    h = MarkdownDocument(body).find_first_heading()
    return h.text if h else None


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


def render_md_table(
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


def render_jsonl(
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


def merge_agents_md_skills(existing: str, skill_names: List[str], template: str) -> str:
    """Merge always-activate skill entries into an AGENTS.md file.

    *template* is the AGENTS.md snippet from the init toolkit whose first
    heading defines the section to merge into.  When the heading already
    exists in *existing*, new entries are inserted right after it (and its
    first non-heading line).  Otherwise the whole template plus entries are
    appended.

    Raises ``ValueError`` when *template* has no markdown heading.

    For each skill, checks if it's already listed (`` `skill_name` `` present).
    Preserves all user content.
    """
    # deduplicate while preserving order, skip already-present skills
    to_add: List[str] = []
    seen: Set[str] = set()
    for name in skill_names:
        if name in seen:
            continue
        seen.add(name)
        if ("`%s`" % name) in existing:
            continue
        to_add.append(name)

    if not to_add:
        return existing

    # parse template heading + subheading
    tpl = MarkdownDocument(template)
    tpl_heading = tpl.find_first_heading()
    if tpl_heading is None:
        raise ValueError("AGENTS.md template must contain a markdown heading")

    heading = tpl.lines[tpl_heading.line_index]
    sub_idx = tpl_heading.line_index + 1
    subheading = tpl.lines[sub_idx].strip() if sub_idx < len(tpl.lines) else None

    new_lines = ["- `%s`" % name for name in to_add]

    doc = MarkdownDocument(existing) if existing else MarkdownDocument("")
    heading_idx = doc.find_line(heading)

    if heading_idx is not None:
        insert_idx = heading_idx + 1
        if (
            subheading
            and insert_idx < len(doc.lines)
            and doc.lines[insert_idx].strip() == subheading
        ):
            insert_idx += 1
        doc.insert_lines(insert_idx, new_lines)
        return str(doc)
    else:
        section_lines = list(tpl.lines)
        section_lines.extend(new_lines)
        section_doc = MarkdownDocument("\n".join(section_lines) + "\n")
        if existing and not existing.endswith("\n"):
            doc.insert_lines(len(doc.lines), [""])
        doc.insert_md(None, MarkdownDocument("\n" + str(section_doc)))
        return str(doc)
