"""
Utility functions for documentation preprocessing.
"""

import os
from typing import List, Optional, Iterator

from docs.website.tools.constants import DLT_MARKER, CAPABILITIES_MARKER


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
    return [line for line in lines if DLT_MARKER not in line or CAPABILITIES_MARKER in line]
