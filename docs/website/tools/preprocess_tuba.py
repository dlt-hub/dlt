"""
Tuba links processing for documentation.
"""

import random
from typing import Any, Dict, List, Optional, Tuple
from datetime import date

import requests

from .constants import NUM_TUBA_LINKS, TUBA_MARKER
from .utils import extract_marker_content


# Cache for tuba config to avoid repeated network requests
_tuba_config_cache: Optional[List[Dict[str, Any]]] = None


def fetch_tuba_config() -> List[Dict[str, Any]]:
    """Fetch tuba config from remote URL (cached after first call)."""
    global _tuba_config_cache

    # Return cached config if available
    if _tuba_config_cache is not None:
        return _tuba_config_cache

    try:
        response = requests.get(
            "https://dlthub.com/docs/pipelines/links.json",
            headers={"Accept": "application/vnd.citationstyles.csl+json"},
        )
        response.raise_for_status()
        _tuba_config_cache = response.json()
        return _tuba_config_cache
    except Exception as e:
        print(f"Error: Could not fetch tuba config: {e}")
        return []


def format_tuba_links_section(links: List[Dict[str, Any]]) -> List[str]:
    """Format tuba links into markdown lines."""
    result = []
    result.append("## Additional Setup guides")

    # see per day, so there are no constant changes when regenerating the docs locally
    today = date.today()
    seed = int(today.strftime("%Y%m%d"))
    random.seed(seed)
    random.shuffle(links)

    for i, link in enumerate(links):
        if i >= NUM_TUBA_LINKS:
            break
        result.append(f"- [{link['title']}]({link['public_url']})")

    return result


def insert_tuba_links(tuba_config: List[Dict[str, Any]], lines: List[str]) -> Tuple[int, List[str]]:
    """Insert tuba links into the markdown file."""
    result = []
    tuba_count = 0

    for line in lines:
        if TUBA_MARKER not in line:
            result.append(line)
            continue

        tuba_tag = extract_marker_content(TUBA_MARKER, line)
        links = [link for link in tuba_config if tuba_tag in link.get("tags", [])]

        if not links:
            tuba_count += 1
            result.append(line)
            continue

        result.extend(format_tuba_links_section(links))
        tuba_count += 1
        result.append(line)  # Add the original line back

    return tuba_count, result
