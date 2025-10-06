"""
Utility functions for documentation preprocessing.
"""

from typing import Optional


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
