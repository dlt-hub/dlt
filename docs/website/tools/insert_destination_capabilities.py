"""
Script to insert destination capabilities tables into processed documentation files.
Similar structure to preprocess_docs.js
"""

import os
import re
from typing import Any, Generator, List, Optional
from dlt.common.destination.capabilities import DestinationCapabilitiesContext
from dlt.common.destination.reference import Destination

MD_TARGET_DIR = "./docs_processed/dlt-ecosystem/destinations"
MD_SOURCE_DIR = "../../dlt/destinations/impl"
DOCS_EXTENSIONS = [".md", ".mdx"]

DLT_MARKER = "@@@DLT"
CAPABILITIES_MARKER = f"{DLT_MARKER}_DESTINATION_CAPABILITIES"

DESTINATION_NAME_PATTERN = r"([a-z0-9_-]+?)(?:--|$)"

TABLE_HEADER = "| Feature | Value | More |\n"

SELECTED_ATTRIBUTES = {
    "preferred_loader_file_format",
    "supported_loader_file_formats",
    "preferred_staging_file_format",
    "supported_staging_file_formats",
    "has_case_sensitive_identifiers",
    "supported_merge_strategies",
    "supported_replace_strategies",
    "supports_tz_aware_datetime",
    "supports_naive_datetime",
}

DOC_LINK_PATTERNS = [
    ("_formats", "../file-formats/"),
    ("_strategies", "../../general-usage/merge-loading#merge-strategies"),
]

DATA_TYPES_DOC_LINK = "[Data Types](../../general-usage/schema#data-types)"


def _format_value(value: Any) -> str:
    """Format value based on its type."""
    if isinstance(value, list):
        return ", ".join(str(v) for v in value)
    elif hasattr(value, "__name__"):
        return value.__name__
    return str(value)


def _generate_doc_link(attr_name: str) -> str:
    """Generate documentation link based on attribute name pattern."""
    for pattern, link in DOC_LINK_PATTERNS:
        if attr_name.endswith(pattern):
            section_name = link.strip("/").split("/")[-1].split("#")[0].replace("-", " ").title()
            return f"[{section_name}]({link})"
    return DATA_TYPES_DOC_LINK


def get_raw_capabilities(destination_name: str) -> Optional[DestinationCapabilitiesContext]:
    try:
        dest = Destination.from_reference(destination_name)
        caps = dest._raw_capabilities()
        if not isinstance(caps, DestinationCapabilitiesContext):
            print(f"Error: Invalid capabilities type for {destination_name}: {type(caps)}")
            return None
        return caps
    except Exception as e:
        print(f"Error: Could not get capabilities for {destination_name}: {e}")
        return None


def walk_files_recursively(directory: str) -> Generator[str, None, None]:
    """Yield all files in directory recursively."""
    for root, _, files in os.walk(directory):
        for file in files:
            yield os.path.join(root, file)


def get_impl_destination_names() -> set[str]:
    """Get a set of supported destination names from /dlt/destinations/impl."""
    try:
        return {
            d for d in os.listdir(MD_SOURCE_DIR) if os.path.isdir(os.path.join(MD_SOURCE_DIR, d))
        }
    except OSError as e:
        print(f"Error: Could not read source directory {MD_SOURCE_DIR}: {e}")
        return set()


def should_process_file(file_name: str, impl_destinations: set[str]) -> bool:
    """Check if file should be processed based on various criteria."""
    if not file_name.endswith(tuple(DOCS_EXTENSIONS)):
        return False

    destination_name = os.path.splitext(file_name)[0]
    return destination_name in impl_destinations


def _is_relevant_capability(attr_name: str, value: Any) -> bool:
    """Check if capability should be included in table."""
    if value is None or attr_name not in SELECTED_ATTRIBUTES:
        return False

    value_str = str(value)
    return not (value_str.startswith("<") and value_str.endswith(">"))


def _format_capability_row(attr_name: str, value: Any) -> Optional[str]:
    """Format a single capability attribute into a table row. Returns None if any column is missing."""
    feature_name = attr_name.replace("_", " ").title()
    if not feature_name.strip():
        return None

    formatted_value = _format_value(value)
    if not formatted_value.strip():
        return None

    doc_link = _generate_doc_link(attr_name)
    return f"| {feature_name} | {formatted_value} | {doc_link} |\n"


def generate_capabilities_table(destination_name: str) -> List[str]:
    """
    Generate a markdown table for destination capabilities using dynamic data.

    Returns list of lines for the table.
    """
    caps = get_raw_capabilities(destination_name)

    if caps is None:
        return []

    table_lines = [TABLE_HEADER, "|---------|-------|------|\n"]

    attrs = {k: v for k, v in vars(caps).items() if _is_relevant_capability(k, v)}

    for attr_name, value in attrs.items():
        row = _format_capability_row(attr_name, value)
        if row:
            table_lines.append(row)

    table_lines.append(
        f"\n*This table shows the supported features of the {destination_name.title()} destination"
        " in dlt.*\n\n"
    )

    return table_lines


def read_file_content(file_path: str) -> Optional[List[str]]:
    """Read file content and return lines, or None if error."""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return f.readlines()
    except Exception as e:
        print(f"Error: reading file {file_path}: {e}")
        return None


def write_file_content(file_path: str, lines: List[str]) -> bool:
    """Write content to file. Returns True if successful."""
    try:
        with open(file_path, "w", encoding="utf-8") as f:
            f.writelines(lines)
        print(f"Processed: {file_path} (markers replaced)")
        return True
    except Exception as e:
        print(f"Error: writing file {file_path}: {e}")
        return False


def process_markers(file_path: str, lines: List[str], impl_destinations: set[str]) -> bool:
    """Process capabilities markers in file lines. Returns True if markers were found."""
    new_lines = []
    marker_found = False

    for line in lines:
        if CAPABILITIES_MARKER not in line:
            new_lines.append(line)
            continue

        match = re.search(rf"{re.escape(CAPABILITIES_MARKER)}\s+{DESTINATION_NAME_PATTERN}", line)
        if not match or match.group(1) not in impl_destinations:
            new_lines.append(line)
            continue

        destination_name = match.group(1)
        print(f"Found capabilities marker for: {destination_name}")

        table_lines = generate_capabilities_table(destination_name)
        new_lines.extend(table_lines)
        marker_found = True

    if marker_found:
        return write_file_content(file_path, new_lines)

    return False


def process_doc_file(file_path: str, impl_destinations: set[str]) -> bool:
    """
    Process a single documentation file.

    Returns True if file was successfully processed.
    """
    lines = read_file_content(file_path)
    return process_markers(file_path, lines, impl_destinations)


def insert_destination_capabilities(impl_destinations: set[str]) -> None:
    """Process all docs with progress feedback."""
    print("Inserting destination capabilities...")

    if not impl_destinations:
        print("No available destinations found. Skipping.")
        return

    print(f"Found {len(impl_destinations)} available destinations: {sorted(impl_destinations)}")

    processed_files = 0

    for file_path in walk_files_recursively(MD_TARGET_DIR):
        file_name = os.path.basename(file_path)
        if not should_process_file(file_name, impl_destinations):
            continue

        print(f"Processing {file_name}")
        if process_doc_file(file_path, impl_destinations):
            processed_files += 1

    print(f"Processed {processed_files} files for destination capabilities.")


def main() -> None:
    """Main function."""
    if not os.path.exists(MD_SOURCE_DIR):
        print(f"Source directory {MD_SOURCE_DIR} does not exist. Exiting.")
        return

    if not os.path.exists(MD_TARGET_DIR):
        print(f"Target directory {MD_TARGET_DIR} does not exist. Exiting.")
        return

    impl_destinations = get_impl_destination_names()
    insert_destination_capabilities(impl_destinations)


if __name__ == "__main__":
    main()
