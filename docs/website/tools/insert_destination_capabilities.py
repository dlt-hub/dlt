"""
Script to insert destination capabilities tables into processed documentation files.
Similar structure to preprocess_docs.js
"""

import os
import re
import sys
from typing import Any, Generator, List, Optional

# Add dlt to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../"))

from dlt.common.destination.capabilities import DestinationCapabilitiesContext
from dlt.common.destination.reference import Destination

# Constants
MD_TARGET_DIR = "./docs_processed/dlt-ecosystem/destinations"
MD_SOURCE_DIR = "../../dlt/destinations/impl"
DOCS_EXTENSIONS = [".md", ".mdx"]

# Markers
DLT_MARKER = "@@@DLT"
CAPABILITIES_MARKER = f"{DLT_MARKER}_DESTINATION_CAPABILITIES"

# Table formatting
TABLE_HEADER = "| Feature | Value | More |\n"
TABLE_SEPARATOR = "|---------|-------|------|\n"
DOC_LINK_PLACEHOLDER = "(link to feature info in docs)"


def get_raw_capabilities(destination_name: str) -> DestinationCapabilitiesContext:
    """Get raw capabilities for a destination using Destination.from_reference()."""
    try:
        dest = Destination.from_reference(destination_name)
        return dest._raw_capabilities()
    except Exception as e:
        print(f"Error getting capabilities for {destination_name}: {e}")
        # Return empty capabilities context as fallback
        return DestinationCapabilitiesContext()


def walk_files_recursively(directory: str) -> Generator[str, None, None]:
    """Yield all files in directory recursively."""
    for root, _, files in os.walk(directory):
        for file in files:
            yield os.path.join(root, file)


def get_impl_destination_names() -> set[str]:
    """Get a set of supported destination names from /dlt/destinations/impl."""
    try:
        source_dirs = [
            d
            for d in os.listdir(MD_SOURCE_DIR)
            if os.path.isdir(os.path.join(MD_SOURCE_DIR, d))
        ]
        return set(source_dirs)
    except OSError as e:
        print(f"Error reading source directory {MD_SOURCE_DIR}: {e}")
        return set()


def should_process_file(file_name: str, impl_destinations: set[str]) -> bool:
    """Check if file should be processed based on various criteria."""
    if not file_name.endswith(tuple(DOCS_EXTENSIONS)):
        return False

    # Extract destination name from filename (remove .md/.mdx extension)
    destination_name = os.path.splitext(file_name)[0]

    # Check if destination exists in available destinations
    if destination_name not in impl_destinations:
        print(
            f"Skipping {file_name} - no matching destination directory "
            f"'{destination_name}' in {MD_SOURCE_DIR}"
        )
        return False

    return True


def _format_capability_row(attr_name: str, value: Any) -> str:
    """Format a single capability attribute into a table row."""
    # Convert attribute name to readable format
    feature_name = attr_name.replace("_", " ").title()

    # Format the value based on its type
    if isinstance(value, list):
        formatted_value = ", ".join(str(v) for v in value)
    elif hasattr(value, "__name__"):  # For function/class objects
        formatted_value = value.__name__
    else:
        formatted_value = str(value)

    return f"| {feature_name} | {formatted_value} | {DOC_LINK_PLACEHOLDER} |\n"


def generate_capabilities_table(destination_name: str) -> List[str]:
    """
    Generate a markdown table for destination capabilities using dynamic data.

    Returns list of lines for the table.
    """
    # Get actual capabilities from destination implementation
    caps = get_raw_capabilities(destination_name)

    # Start building the table
    table_lines = [TABLE_HEADER, TABLE_SEPARATOR]

    # Add dynamic data from actual capabilities
    # Get all attributes that have values
    non_empty_attrs = {
        k: v for k, v in vars(caps).items()
        if v is not None and not str(v).startswith('<') and not str(v).endswith('>')
    }

    # Generate rows dynamically for all set attributes
    for attr_name, value in non_empty_attrs.items():
        print(f"Adding capability: {attr_name} = {value}")
        table_lines.append(_format_capability_row(attr_name, value))

    # Add footer
    table_lines.extend([
        "\n",
        f"*This table shows the supported features of the {destination_name} destination in dlt.*\n",
        "\n",
    ])

    return table_lines


def read_file_content(file_path: str) -> Optional[List[str]]:
    """Read file content and return lines, or None if error."""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return f.readlines()
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
        return None


def write_file_content(file_path: str, lines: List[str]) -> bool:
    """Write content to file. Returns True if successful."""
    try:
        with open(file_path, "w", encoding="utf-8") as f:
            f.writelines(lines)
        print(f"Processed: {file_path} (markers replaced)")
        return True
    except Exception as e:
        print(f"Error writing file {file_path}: {e}")
        return False


def process_markers(file_path: str, lines: List[str], impl_destinations: set[str]) -> bool:
    """Process capabilities markers in file lines. Returns True if markers were found."""
    new_lines = []
    marker_found = False

    for line in lines:
        # Check if this line contains the capabilities marker
        if CAPABILITIES_MARKER not in line:
            new_lines.append(line)
            continue

        # Extract destination name from marker
        # Format: <!--@@@DLT_DESTINATION_CAPABILITIES destination_name-->
        # Match destination names: lowercase letters, digits, underscores, hyphens (but not trailing dashes)
        match = re.search(rf"{re.escape(CAPABILITIES_MARKER)}\s+([a-z0-9_-]+?)(?:--|$)", line)
        if not match:
            new_lines.append(line)
            continue

        destination_name = match.group(1)

        # Validate that destination exists in impl_destinations
        if destination_name not in impl_destinations:
            print(
                f"Warning: Destination '{destination_name}' from marker not found in "
                f"impl_destinations"
            )
            new_lines.append(line)  # Keep original line
            continue

        print(f"Found capabilities marker for: {destination_name}")

        # Generate capability tables and replace the marker line
        table_lines = generate_capabilities_table(destination_name)
        new_lines.extend(table_lines)
        marker_found = True
        # Continue processing remaining lines after the marker

    # Write the processed content back to the file
    if marker_found:
        return write_file_content(file_path, new_lines)

    return False


def process_doc_file(file_path: str, impl_destinations: set[str]) -> bool:
    """
    Process a single documentation file.

    Returns True if file was successfully processed.
    """
    # 1. Read file content
    lines = read_file_content(file_path)
    if lines is None:
        return False

    # 2. Find DESTINATION_CAPABILITIES markers and process them
    return process_markers(file_path, lines, impl_destinations)


def insert_destination_capabilities(impl_destinations: set[str]) -> None:
    """Process all docs in the processed docs folder."""
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

        if process_doc_file(file_path, impl_destinations):
            processed_files += 1

    print(f"Processed {processed_files} files for destination capabilities.")


def main() -> None:
    """Main function."""
    # Pre-check directories exist before processing
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