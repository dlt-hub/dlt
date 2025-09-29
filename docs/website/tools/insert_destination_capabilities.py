"""
Script to insert destination capabilities tables into processed documentation files.
Similar structure to preprocess_docs.js
"""

import os
import re
from typing import List, Optional

# constants
MD_TARGET_DIR = "./docs_processed/dlt-ecosystem/destinations"
MD_SOURCE_DIR = "../../dlt/destinations/impl"
DOCS_EXTENSIONS = [".md", ".mdx"]

# markers
DLT_MARKER = "@@@DLT"
CAPABILITIES_MARKER = f"{DLT_MARKER}_DESTINATION_CAPABILITIES"

# Configuration data for destination capabilities
# This can be easily modified or loaded from external sources later
DESTINATION_CAPABILITIES_DATA = [
    ("File Formats", "JSON, Parquet, CSV", "Supported file formats"),
    ("Max Query Length", "32MB", "Maximum query length supported"),
    ("Transactions", "Yes", "Supports database transactions"),
    ("Case Sensitive", "No", "Identifiers are case insensitive"),
    ("Staging", "Yes", "Supports staging tables"),
    ("Merge Strategies", "Append, Merge", "Available merge strategies"),
    ("Schema Evolution", "Yes", "Supports schema changes"),
    ("Incremental Loading", "Yes", "Supports incremental data loading"),
    ("Data Types", "All Standard", "Supports all standard SQL data types"),
    ("Performance", "High", "Optimized for high-performance operations"),
    ("Concurrency", "Yes", "Supports concurrent data loading"),
    ("Error Handling", "Robust", "Comprehensive error handling"),
    ("Monitoring", "Yes", "Built-in monitoring and logging"),
    ("Scalability", "High", "Designed for high-scale processing")
]


def walk_sync(directory: str):
    """Yield all files in directory recursively"""
    for root, _ , files in os.walk(directory):
        for file in files:
            yield os.path.join(root, file)


def get_impl_destination_names() -> set[str]:
    """Get a set of supported destination names from /dlt/destinations/impl, we are only going to create capabilities for those destinations."""
    if not os.path.exists(MD_SOURCE_DIR):
        print(f"Source directory {MD_SOURCE_DIR} does not exist")
        return set()

    try:
        source_dirs = [d for d in os.listdir(MD_SOURCE_DIR)
                      if os.path.isdir(os.path.join(MD_SOURCE_DIR, d))]
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
        print(f"Skipping {file_name} - no matching destination directory '{destination_name}' in {MD_SOURCE_DIR}")
        return False

    return True


def generate_capabilities_table(destination_name: str) -> List[str]:
    """
    Generate a markdown table for destination capabilities using dynamic data.
    Returns list of lines for the table.
    """
    # Start building the table
    table_lines = [
        "| Capability | Value | Description |\n",
        "|------------|-------|-------------|\n"
    ]

    # Add data rows dynamically
    for capability, value, description in DESTINATION_CAPABILITIES_DATA:
        # Format description to include destination name
        formatted_description = f"{description} for {destination_name} destination"
        table_lines.append(f"| {capability} | {value} | {formatted_description} |\n")

    # Add footer
    table_lines.extend([
        "\n",
        f"*This table shows the supported features of the {destination_name} destination in dlt.*\n",
        "\n"
    ])

    return table_lines


def read_file_content(file_path: str) -> Optional[List[str]]:
    """Read file content and return lines, or None if error."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return f.readlines()
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
        return None


def write_file_content(file_path: str, lines: List[str]) -> bool:
    """Write content to file. Returns True if successful."""
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.writelines(lines)
        print(f"Processed: {file_path} (markers replaced)")
        return True
    except Exception as e:
        print(f"Error writing file {file_path}: {e}")
        return False

def process_markers(file_path: str, lines: List[str]) -> bool:
    """Process capabilities markers in file lines. Returns True if markers were found."""
    new_lines = []
    marker_found = False

    for line in lines:
        # Check if this line contains the capabilities marker
        if not CAPABILITIES_MARKER in line:
            new_lines.append(line)
            continue
        # Extract destination name from marker
        # Format: <!--@@@DLT_DESTINATION_CAPABILITIES destination_name-->
        match = re.search(rf'{re.escape(CAPABILITIES_MARKER)}\s+(\w+)', line)
        if not match:
            new_lines.append(line)
            continue
        destination_name = match.group(1)
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
    file_name = os.path.basename(file_path)
    if not should_process_file(file_name, impl_destinations):
        return False

    # 1. Read file content
    lines = read_file_content(file_path)
    if lines is None:
        return False

    # 2. Find DESTINATION_CAPABILITIES markers and process them
    return process_markers(file_path, lines)


def insert_destination_capabilities(impl_destinations: set[str]):
    """Process all docs in the processed docs folder"""
    print("Inserting destination capabilities...")
    if not os.path.exists(MD_TARGET_DIR):
        print(f"Target directory {MD_TARGET_DIR} does not exist. Skipping.")
        return

    if not impl_destinations:
        print("No available destinations found. Skipping.")
        return

    print(f"Found {len(impl_destinations)} available destinations: {sorted(impl_destinations)}")

    processed_files = 0

    for file_path in walk_sync(MD_TARGET_DIR):
        if process_doc_file(file_path, impl_destinations):
            processed_files += 1

    print(f"Processed {processed_files} files for destination capabilities.")


def main():
    """Main function"""
    impl_destinations = get_impl_destination_names()
    insert_destination_capabilities(impl_destinations)


if __name__ == "__main__":
    main()
