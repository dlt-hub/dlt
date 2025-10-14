"""
Utility functions for documentation preprocessing.
"""

import os
from typing import List, Optional, Iterator

from .constants import DLT_MARKER


def walk_sync(directory: str) -> Iterator[str]:
    """Recursively yield all files in directory."""
    try:
        files = os.listdir(directory)
    except FileNotFoundError:
        return

    for file in files:
        full_path = os.path.join(directory, file)
        if os.path.isdir(full_path):
            yield from walk_sync(full_path)
        else:
            yield full_path


def list_dirs_sync(directory: str) -> Iterator[str]:
    """List all directories in directory."""
    try:
        files = os.listdir(directory)
    except FileNotFoundError:
        return

    for file in files:
        full_path = os.path.join(directory, file)
        if os.path.isdir(full_path):
            yield full_path


def extract_marker_content(tag: str, line: str) -> Optional[str]:
    """Extract the snippet or tuba tag name from a line."""
    if not line or tag not in line:
        return None
    line = line.replace("-->", "").replace("<!--", "")
    words = line.split()
    try:
        tag_index = words.index(tag)
        return words[tag_index + 1].strip()
    except (ValueError, IndexError):
        print(f"Error: Could not extract {tag} from line: {line}")
        return None


def remove_remaining_markers(lines: List[str]) -> List[str]:
    """Remove all lines that contain a DLT_MARKER except for CAPABILITIES_MARKER."""
    return [line for line in lines if DLT_MARKER not in line]


def trim_array(lines: List[str]) -> List[str]:
    """Trim empty lines from start and end of list."""
    if not lines:
        return lines

    while lines and not lines[0].strip():
        lines.pop(0)
    while lines and not lines[-1].strip():
        lines.pop()

    return lines
