"""
Tuba links processing for documentation.
"""

import random
from typing import List, Tuple, Dict, Optional

import requests

from constants import NUM_TUBA_LINKS, TUBA_MARKER


def fetch_tuba_config() -> List[Dict]:
    """Fetch tuba config from remote URL."""
    try:
        response = requests.get(
            "https://dlthub.com/docs/pipelines/links.json",
            headers={"Accept": "application/vnd.citationstyles.csl+json"},
        )
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error: Could not fetch tuba config: {e}")
        return []


def extract_marker_content(tag: str, line: str) -> Optional[str]:
    """Extract the snippet or tuba tag name from a line."""
    if not line or not tag in line:
        return None
    line = line.replace("-->", "").replace("<!--", "")
    words = line.split(" ")
    try:
        tag_index = words.index(tag)
        return words[tag_index + 1].strip()
    except (ValueError, IndexError):
        print(f"Error: Could not extract tuba tag from line: {line}")
        return None


def format_tuba_links_section(links: List[Dict]) -> List[str]:
    result = []
    result.append("## Additional Setup guides")

    random.shuffle(links)

    for i, link in enumerate(links):
        if i >= NUM_TUBA_LINKS:
            break
        result.append(f"- [{link['title']}]({link['public_url']})")

    return result


def insert_tuba_links(tuba_config: List[Dict], lines: List[str]) -> Tuple[int, List[str]]:
    """Insert tuba links into the markdown file."""
    result = []
    tuba_count = 0

    for line in lines:
        if not TUBA_MARKER in line:
            result.append(line)
            continue

        tuba_tag = extract_marker_content(TUBA_MARKER, line)
        links = [link for link in tuba_config if tuba_tag in link.get("tags", [])]

        if not len(links):
            tuba_count += 1
            result.append(line)
            continue

        tuba_count += 1
        result.extend(format_tuba_links_section(links))
        result.append(line)

    return tuba_count, result
