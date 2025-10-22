"""
Snippet processing for documentation.
"""

import os
from textwrap import dedent
from typing import List, Tuple, Optional, Dict

from .constants import (
    SNIPPETS_FILE_SUFFIX,
    SNIPPET_MARKER,
    SNIPPET_START_MARKER,
    SNIPPET_END_MARKER,
)
from .utils import extract_marker_content


def build_snippet_map(lines: List[str], file_name: str) -> Dict[str, Dict[str, int]]:
    """Build a map of snippet names to their start and end line numbers."""
    snippet_map = {}
    for line_index, line in enumerate(lines):
        line = line.rstrip()
        snippet_name = extract_marker_content(SNIPPET_START_MARKER, line)
        if snippet_name:
            snippet_map[snippet_name] = {"start": line_index}

        snippet_name = extract_marker_content(SNIPPET_END_MARKER, line)
        if not snippet_name:
            continue
        if snippet_name in snippet_map:
            snippet_map[snippet_name]["end"] = line_index
        else:
            raise ValueError(
                f'Found end tag for snippet "{snippet_name}" but start tag not found! '
                f"File {file_name}."
            )
    return snippet_map


def get_snippet_from_file(snippets_file_name: str, snippet_name: str) -> Optional[List[str]]:
    """Get a snippet from a file."""
    try:
        with open(snippets_file_name, "r", encoding="utf-8") as f:
            lines = f.read().split("\n")
    except FileNotFoundError:
        return None

    snippet_map = build_snippet_map(lines, snippets_file_name)

    if snippet_name not in snippet_map:
        return None

    start = snippet_map[snippet_name]["start"]
    end = snippet_map[snippet_name]["end"]
    result = lines[start + 1 : end]

    # Dedent the snippet
    result_str = "\n".join(result)
    result_str = dedent(result_str)
    result = result_str.split("\n")

    return result


def get_snippet(file_name: str, snippet_name: str) -> List[str]:
    """Get a snippet and wrap it in code blocks."""
    ext = str(os.path.splitext(file_name)[1])
    snippet_parts = snippet_name.split("::")

    # Regular snippet
    snippets_file_name = file_name[: -len(ext)] + SNIPPETS_FILE_SUFFIX
    if len(snippet_parts) > 1:
        snippets_file_name = os.path.join(os.path.dirname(file_name), snippet_parts[0])
        snippet_name = snippet_parts[1]

    snippet = get_snippet_from_file(snippets_file_name, snippet_name)
    if not snippet:
        raise ValueError(
            f'Could not find requested snippet "{snippet_name}" requested in file '
            f"{file_name} in file {snippets_file_name}."
        )

    code_type = os.path.splitext(snippets_file_name)[1].lstrip(".")
    snippet.insert(0, f"```{code_type}")
    snippet.append("```")

    return snippet


def insert_snippets(file_name: str, lines: List[str]) -> Tuple[int, List[str]]:
    """Insert snippets into the markdown file."""
    result = []
    snippet_count = 0

    for line in lines:
        snippet_name = extract_marker_content(SNIPPET_MARKER, line)
        if SNIPPET_MARKER not in line or not snippet_name:
            result.append(line)
            continue

        snippet = get_snippet(file_name, snippet_name)
        result.extend(snippet)
        snippet_count += 1
        result.append(line)

    return snippet_count, result
