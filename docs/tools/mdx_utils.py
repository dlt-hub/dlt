"""Helpers shared by the generators that write MDX pages."""

import re
from typing import List


def escape_mdx(text: str) -> str:
    """Escapes characters MDX would parse as JSX outside of code fences and code spans."""
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
            )
        out.append("".join(parts))
    return "\n".join(out)
