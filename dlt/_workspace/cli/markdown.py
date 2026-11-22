"""Markdown documents: YAML frontmatter, headings, and the sections dlt rewrites."""

from pathlib import Path
from typing import Any, Dict, List, NamedTuple, Optional, Tuple

import yaml
from markdown_it import MarkdownIt
from markdown_it.token import Token

_PARSER = MarkdownIt("commonmark")
_SENTINEL_HEADING = "# end"
_HTML_BLOCK_CLOSERS = ("-->", "?>", "]]>", ">", "</pre>", "</script>", "</style>", "</textarea>")
"""Lines that end the CommonMark HTML blocks that do not end at a blank line."""


class MarkdownHeading(NamedTuple):
    line_index: int  # line index in document
    depth: int  # 1-6
    text: str  # heading text without its markup
    level: int  # nesting level: 0 at the top, more inside quotes and lists
    end_index: int  # line index after the heading, past the underline of a setext heading


class MarkdownDocument:
    """Markdown text split into lines, with its YAML frontmatter and the block structure of
    the body. Code blocks are never mistaken for headings.
    """

    def __init__(self, text: str) -> None:
        # editors on Windows may save a byte order mark that hides the frontmatter and headings
        if text.startswith("﻿"):
            text = text[1:]
        self._lines: List[str] = text.split("\n")
        self._tokens: Optional[List[Token]] = None

    def __str__(self) -> str:
        return "\n".join(self._lines)

    @staticmethod
    def from_frontmatter(data: Dict[str, Any], body: str) -> "MarkdownDocument":
        """Create a document from a frontmatter dict and body text."""
        if not data:
            return MarkdownDocument(body)
        fm = yaml.dump(data, default_flow_style=False, sort_keys=False).rstrip("\n")
        return MarkdownDocument("---\n" + fm + "\n---\n" + body)

    @staticmethod
    def read_name_desc(path: Path, default_name: Optional[str] = None) -> Tuple[str, str]:
        """Read a markdown file and return (name, description) from its frontmatter.

        Without a `name` in the frontmatter, `default_name` is used, else the file stem. Without
        a `description`, the first heading is used.
        """
        doc = MarkdownDocument(path.read_text(encoding="utf-8"))
        try:
            fm, _ = doc.split_frontmatter()
        except yaml.YAMLError:
            fm = {}
        heading = doc.find_first_heading()
        name = fm.get("name") or default_name or path.stem
        return name, fm.get("description") or (heading.text if heading else "")

    def _find_frontmatter_close(self) -> Optional[int]:
        """Line index of the `---` that closes a frontmatter block at the top, or None."""
        if not self._lines or self._lines[0].rstrip("\r") != "---":
            return None
        return next(
            (i for i in range(1, len(self._lines)) if self._lines[i].rstrip("\r") == "---"), None
        )

    def split_frontmatter(self) -> Tuple[Dict[str, Any], str]:
        """Parse `---` delimited YAML frontmatter and return it with the body text.

        Without valid frontmatter returns an empty dict and the whole text.

        Raises:
            yaml.YAMLError: When the frontmatter is not valid YAML.
        """
        close = self._find_frontmatter_close()
        if close is None:
            return {}, str(self)
        data = yaml.safe_load("\n".join(self._lines[1:close])) or {}
        if not isinstance(data, dict):
            return {}, str(self)
        return data, self.body_text

    @property
    def body_text(self) -> str:
        """Text after the delimited frontmatter block, whether or not its YAML parses."""
        close = self._find_frontmatter_close()
        return "\n".join(self._lines[0 if close is None else close + 1 :])

    def _parse_body_tokens(self) -> Tuple[List[Token], int]:
        """Block tokens of the body and the line index the body starts at."""
        # a delimited block at the top is never body, even when its YAML does not parse
        close = self._find_frontmatter_close()
        start = 0 if close is None else close + 1
        if self._tokens is None:
            self._tokens = _PARSER.parse("\n".join(self._lines[start:]))
        return self._tokens, start

    def find_headings(self) -> List[MarkdownHeading]:
        """Return all headings of the body, ATX and setext."""
        tokens, start = self._parse_body_tokens()
        return [
            MarkdownHeading(
                start + token.map[0],
                int(token.tag[1]),
                tokens[i + 1].content,
                token.level,
                start + token.map[1],
            )
            for i, token in enumerate(tokens)
            if token.type == "heading_open" and token.map is not None
        ]

    def find_first_heading(self) -> Optional[MarkdownHeading]:
        """Return the first heading or None."""
        headings = self.find_headings()
        return headings[0] if headings else None

    def find_top_heading(self, line: str) -> Optional[MarkdownHeading]:
        """Return the first top-level heading written exactly as `line`, or None."""
        return next(
            (
                h
                for h in self.find_headings()
                if h.level == 0 and self._lines[h.line_index].strip() == line
            ),
            None,
        )

    def section_end(self, heading: MarkdownHeading) -> int:
        """Line index where the section of `heading` ends: the next top-level heading of the
        same or a higher level, or the end of the document.
        """
        return next(
            (
                h.line_index
                for h in self.find_headings()
                if h.line_index > heading.line_index and h.level == 0 and h.depth <= heading.depth
            ),
            len(self._lines),
        )

    def replace_lines(self, start: int, end: int, new_lines: List[str]) -> None:
        """Replace lines `start` up to `end` (exclusive) with `new_lines`."""
        self._lines[start:end] = new_lines
        self._tokens = None

    def demote_headings(self) -> None:
        """Move every top-level heading one level down, to level 6 at most. A setext heading
        becomes an ATX one. Headings in quotes and lists stay as they are.
        """
        for heading in reversed(self.find_headings()):
            if heading.level != 0:
                continue
            if heading.end_index - heading.line_index > 1:
                atx = "#" * min(heading.depth + 1, 6) + " " + " ".join(heading.text.split("\n"))
                self._lines[heading.line_index : heading.end_index] = [atx]
            elif heading.depth < 6:
                line = self._lines[heading.line_index]
                self._lines[heading.line_index] = line.replace("#", "##", 1)
        self._tokens = None

    def close_open_blocks(self) -> None:
        """Close a code fence or an HTML block the document leaves open, so it does not swallow
        text appended after it. Leaves the document as is when no closing line helps.
        """
        text = str(self)
        if self._blocks_closed(text):
            return
        fences = [t for t in _PARSER.parse(text) if t.type == "fence" and t.level == 0]
        for closer in ([fences[-1].markup] if fences else []) + list(_HTML_BLOCK_CLOSERS):
            if self._blocks_closed(text + "\n" + closer):
                self.replace_lines(len(self._lines), len(self._lines), [closer])
                return

    def replace_section(self, heading: str, section: str) -> None:
        """Replace the section that starts with the top-level `heading` line, or append it.

        The section runs to the next top-level heading of the same or a higher level, or to the
        end of the document. Text outside it is kept. `section` is the full new section,
        starting with `heading`. The document then ends with a single newline.
        """
        section = section.strip("\n")
        owned = self.find_top_heading(heading)
        if owned is None:
            kept = str(self).rstrip("\n")
            text = (kept + "\n\n" if kept.strip() else "") + section
        else:
            after = self._lines[self.section_end(owned) :]
            lines = self._lines[: owned.line_index] + [section] + ([""] + after if after else [])
            text = "\n".join(lines).rstrip("\n")
        self.replace_lines(0, len(self._lines), (text + "\n").split("\n"))

    @staticmethod
    def _blocks_closed(text: str) -> bool:
        """Tell whether a heading placed after `text` starts a new top-level block."""
        tokens = _PARSER.parse(text + "\n\n" + _SENTINEL_HEADING)
        last = next((t for t in reversed(tokens) if t.type == "heading_open"), None)
        if last is None or last.map is None or last.level != 0:
            return False
        # the sentinel sits after the text and the blank line
        return last.map[0] == len(text.split("\n")) + 1
