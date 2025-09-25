#!/usr/bin/env python3
"""
Script to insert destination capabilities tables into processed documentation files.
Similar structure to preprocess_docs.js
"""

import os
import re
import sys
import time
import argparse
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# constants
MD_TARGET_DIR = "docs_processed/"
DOCS_EXTENSIONS = [".md", ".mdx"]

# markers
DLT_MARKER = "@@@DLT"
CAPABILITIES_MARKER = f"{DLT_MARKER}_DESTINATION_CAPABILITIES"

def walk_sync(dir_path: Path):
    """Yield all files in docs dir recursively"""
    if not dir_path.exists():
        return

    for item in dir_path.iterdir():
        if item.is_dir():
            yield from walk_sync(item)
        else:
            yield item

def extract_marker_content(tag: str, line: str) -> Optional[str]:
    """Extract the tag name from a line"""
    if line and tag in line:
        # strip out md start and end comments
        line = line.replace("-->", "")
        line = line.replace("<!--", "")
        words = line.split()
        try:
            tag_index = words.index(tag)
            if tag_index + 1 < len(words):
                return words[tag_index + 1].strip()
        except ValueError:
            pass
    return None

def get_destination_capabilities(destination_name: str) -> Dict[str, str]:
    """Get capabilities for a specific destination"""
    # Default capabilities
    capabilities = {
        "Example Write Disposition": "append, replace, merge",
        "File Formats": "parquet, jsonl, csv",
        "Staging Support": "Yes",
        "State Sync": "Yes",
        "dbt Support": "Yes",
        "Primary Key Support": "Yes",
        "Unique Constraints": "Yes",
        "Partitioning": "Yes",
        "Clustering": "Yes",
        "HARD_CODED": "YES"
    }


    return capabilities

def create_capabilities_table(capabilities: Dict[str, str]) -> List[str]:
    """Create a markdown table from capabilities dictionary"""
    if not capabilities:
        return []

    table_lines = [
        "## Capabilities",
        "",
        "| Capability | Support |",
        "|------------|---------|"
    ]

    for capability, support in capabilities.items():
        table_lines.append(f"| {capability} | {support} |")

    table_lines.append("")  # Add empty line at end
    return table_lines

def insert_capabilities_tables(lines: List[str]) -> Tuple[int, List[str]]:
    """Insert capabilities tables into the markdown file"""
    result = []
    capabilities_count = 0

    for line in lines:
        if not CAPABILITIES_MARKER in line:
            continue
        destination_name = extract_marker_content(CAPABILITIES_MARKER, line)
        if destination_name:
            capabilities = get_destination_capabilities(destination_name)
            table_lines = create_capabilities_table(capabilities)
            result.extend(table_lines)
            capabilities_count += 1
            print(f"Inserted capabilities table for {destination_name}")
        else:
            result.append(line)

    return [capabilities_count, result]

def remove_remaining_markers(lines: List[str]) -> List[str]:
    """Remove all lines that contain a DLT_MARKER"""
    return [line for line in lines if DLT_MARKER not in line]

def process_doc_file(file_path: Path) -> Tuple[int, bool]:
    """Process a single documentation file"""
    if file_path.suffix not in DOCS_EXTENSIONS:
        return [0, False]

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.read().split('\n')

        capabilities_count, lines = insert_capabilities_tables(lines)
        lines = remove_remaining_markers(lines)

        with open(file_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(lines))

        return [capabilities_count, True]

    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return [0, False]

def preprocess_docs():
    """Preprocess all docs in the docs_processed folder"""
    print("Processing destination capabilities...")

    processed_files = 0
    inserted_capabilities = 0

    # Target directory for processed docs
    target_dir = Path(MD_TARGET_DIR) / "dlt-ecosystem" / "destinations"

    if not target_dir.exists():
        print(f"Target directory not found: {target_dir}")
        return

    for file_path in walk_sync(target_dir):
        capabilities_count, processed = process_doc_file(file_path)
        if not processed:
            continue

        processed_files += 1
        inserted_capabilities += capabilities_count

    print(f"Processed {processed_files} files.")
    print(f"Inserted {inserted_capabilities} capabilities tables.")

def watch_and_process():
    """Watch for changes and process files continuously"""
    print("Watching for changes in destination capabilities...")
    last_processed = 0

    while True:
        try:
            # Check if docs_processed directory exists and has been modified
            target_dir = Path(MD_TARGET_DIR) / "dlt-ecosystem" / "destinations"
            if target_dir.exists():
                # Get the most recent modification time of any file in the directory
                max_mtime = 0
                for file_path in walk_sync(target_dir):
                    if file_path.suffix in DOCS_EXTENSIONS:
                        max_mtime = max(max_mtime, file_path.stat().st_mtime)

                # Only process if files have been modified since last run
                if max_mtime > last_processed:
                    print(f"Files modified, processing destination capabilities...")
                    preprocess_docs()
                    last_processed = max_mtime
                    print("Watching for changes...")

            time.sleep(1)  # Check every second

        except KeyboardInterrupt:
            print("\nStopping destination capabilities watcher...")
            break
        except Exception as e:
            print(f"Error in watch mode: {e}")
            time.sleep(5)  # Wait 5 seconds before retrying

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="Insert destination capabilities into docs")
    parser.add_argument('--watch', action='store_true', help='Watch for file changes and process continuously')
    args = parser.parse_args()

    if args.watch:
        watch_and_process()
    else:
        print("Processing destination capabilities...")
        preprocess_docs()

if __name__ == "__main__":
    main()
