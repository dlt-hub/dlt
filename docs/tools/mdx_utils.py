"""Helpers shared by the generators that write MDX pages."""

import re
from typing import List


def escape_mdx(text: str) -> str:
    """Escapes characters MDX would parse as JSX or emphasis outside of code fences and spans."""
    out: List[str] = []
    in_fence = False
    for line in text.split("\n"):
        if line.lstrip().startswith("```"):
            in_fence = not in_fence
            out.append(line)
            continue
        if in_fence:
            out.append(line)
            continue
        parts = re.split(r"(`[^`]*`)", line)
        for i in range(0, len(parts), 2):
            parts[i] = (
                parts[i]
                .replace("{", "\\{")
                .replace("}", "\\}")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("__", "\\_\\_")
            )
        out.append("".join(parts))
    return "\n".join(out)


def one_line(text: str) -> str:
    """Joins `text` into one line and escapes the pipe that would end a table cell."""
    return " ".join(text.split()).replace("|", "\\|")


def table(rows: List[List[str]], header: List[str]) -> str:
    lines = ["| " + " | ".join(header) + " |", "|" + " --- |" * len(header)]
    lines += ["| " + " | ".join(row) + " |" for row in rows]
    return "\n".join(lines) + "\n\n"


def frontmatter(title: str, description: str, keywords: List[str]) -> str:
    return (
        f"---\ntitle: {title}\ndescription: {description}\n"
        f"keywords: [{', '.join(keywords)}]\n---\n"
    )
