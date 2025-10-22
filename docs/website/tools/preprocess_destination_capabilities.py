"""
Destination capabilities processing for documentation.
"""

import os
import re
from typing import Any, List, Optional, Tuple, cast

from dlt.common.destination.capabilities import DestinationCapabilitiesContext
from dlt.common.destination.reference import Destination

from .constants import (
    CAPABILITIES_MARKER,
    DESTINATION_CAPABILITIES_SOURCE_DIR,
    DESTINATION_NAME_PATTERN,
    CAPABILITIES_TABLE_HEADER,
    SELECTED_CAPABILITIES_ATTRIBUTES,
    CAPABILITIES_DOC_LINK_PATTERNS,
    CAPABILITIES_DATA_TYPES_DOC_LINK,
)


# Cache for destination capabilities to avoid repeated lookups
_capabilities_cache: dict[str, Optional[DestinationCapabilitiesContext]] = {}
# Cache for impl destinations list
_impl_destinations_cache: Optional[set[str]] = None


def get_impl_destination_names() -> set[str]:
    """Get a set of supported destination names from /dlt/destinations/impl (cached)."""
    global _impl_destinations_cache

    if _impl_destinations_cache is not None:
        return _impl_destinations_cache

    try:
        _impl_destinations_cache = {
            d
            for d in os.listdir(DESTINATION_CAPABILITIES_SOURCE_DIR)
            if os.path.isdir(os.path.join(DESTINATION_CAPABILITIES_SOURCE_DIR, d))
        }
        return _impl_destinations_cache
    except OSError as e:
        print(f"Error: Could not read source directory {DESTINATION_CAPABILITIES_SOURCE_DIR}: {e}")
        return set()


def get_raw_capabilities(destination_name: str) -> Optional[DestinationCapabilitiesContext]:
    """Get destination capabilities (cached after first call)."""
    if destination_name in _capabilities_cache:
        return _capabilities_cache[destination_name]

    try:
        dest = Destination.from_reference(destination_name)
        caps = dest._raw_capabilities()
        if not isinstance(caps, DestinationCapabilitiesContext):
            print(f"Error: Invalid capabilities type for {destination_name}: {type(caps)}")
            return None
        _capabilities_cache[destination_name] = caps
        return caps
    except Exception as e:
        print(f"Error: Could not get capabilities for {destination_name}: {e}")
        return None


def _format_value(value: Any) -> str:
    """Format value based on its type."""
    if isinstance(value, list):
        return ", ".join(str(v) for v in value)
    elif hasattr(value, "__name__") and isinstance(value.__name__, str):
        return value.__name__
    return str(value)


def _generate_doc_link(attr_name: str) -> str:
    """Generate documentation link based on attribute name pattern."""
    lower = attr_name.lower()
    for key, link in CAPABILITIES_DOC_LINK_PATTERNS:
        if key in lower:
            return _format_doc_link_for_key(key, link)

    # Fallback to data types link when nothing matches
    return CAPABILITIES_DATA_TYPES_DOC_LINK


def _format_doc_link_for_key(key: str, link: str) -> str:
    """Format the markdown link for a capability mapping key and link.

    Centralizes any human-friendly label overrides.
    """
    if key == "merge":
        return f"[Merge strategy]({link})"
    if key == "replace":
        return f"[Replace strategy]({link})"
    if key == "time":
        return f"[Timestamps and Timezones]({link})"
    if key == "dialect":
        return f"[Dataset access]({link})"

    section_name = link.strip("/").split("/")[-1].split("#")[0].replace("-", " ").capitalize()
    return f"[{section_name}]({link})"


def _is_relevant_capability(attr_name: str, value: Any) -> bool:
    """Check if capability should be included in table."""
    if value is None or attr_name not in SELECTED_CAPABILITIES_ATTRIBUTES:
        return False

    value_str = str(value)
    return not (value_str.startswith("<") and value_str.endswith(">"))


def _format_capability_row(attr_name: str, value: Any) -> Optional[str]:
    """Format a single capability attribute into a table row."""
    feature_name = attr_name.replace("_", " ").capitalize()
    if not feature_name.strip():
        return None

    formatted_value = _format_value(value)
    if not formatted_value.strip():
        return None

    doc_link = _generate_doc_link(attr_name)
    return f"| {feature_name} | {formatted_value} | {doc_link} |"


def generate_capabilities_table(destination_name: str) -> List[str]:
    """Generate a markdown table for destination capabilities."""
    caps = get_raw_capabilities(destination_name)

    if caps is None:
        return []

    table_lines = [
        "## Destination capabilities",
        (
            "The following table shows the capabilities of the"
            f" {destination_name.title()} destination:"
        ),
        "",
        CAPABILITIES_TABLE_HEADER.rstrip("\n"),
        "|---------|-------|------|",
    ]

    attrs = {k: v for k, v in vars(caps).items() if _is_relevant_capability(k, v)}

    for attr_name, value in attrs.items():
        row = _format_capability_row(attr_name, value)
        if row:
            table_lines.append(row)

    table_lines.append("")
    table_lines.append(
        f"*This table shows the supported features of the {destination_name.title()} destination in"
        " dlt.*"
    )
    table_lines.append("")

    return table_lines


def insert_destination_capabilities(lines: List[str]) -> Tuple[int, List[str]]:
    """
    Insert destination capabilities tables into the markdown file.
    """
    impl_destinations = get_impl_destination_names()
    result = []
    marker_count = 0

    for line in lines:
        if CAPABILITIES_MARKER not in line:
            result.append(line)
            continue

        match = re.search(rf"{re.escape(CAPABILITIES_MARKER)}\s+{DESTINATION_NAME_PATTERN}", line)
        if not match or match.group(1) not in impl_destinations:
            result.append(line)
            continue

        destination_name = match.group(1)
        table_lines = generate_capabilities_table(destination_name)
        result.extend(table_lines)
        marker_count += 1

    return marker_count, result
