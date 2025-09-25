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
MD_TARGET_DIR = "docs_processed/dlt-ecosystem/destinations"
DOCS_EXTENSIONS = [".md", ".mdx"]

# markers
DLT_MARKER = "@@@DLT"
CAPABILITIES_MARKER = f"{DLT_MARKER}_DESTINATION_CAPABILITIES"


def walk_sync(directory: str):
    """Yield all files in directory recursively"""
    for root, dirs, files in os.walk(directory):
        for file in files:
            yield os.path.join(root, file)


def process_doc_file(file_path: str) -> Tuple[int, bool]:
    """
    Process a single documentation file.
    Returns (processed_count, was_processed)
    """
    if not file_path.endswith(tuple(DOCS_EXTENSIONS)):
        return 0, False

    # Check if file is in target directory
    if not file_path.startswith(MD_TARGET_DIR):
        return 0, False

    # Placeholder: just count files that would be processed
    # In real implementation, this would:
    # 1. Read file content
    # 2. Find DESTINATION_CAPABILITIES markers
    # 3. Generate capability tables
    # 4. Replace markers with generated content
    # 5. Write processed file

    print(f"Processing: {file_path}")
    return 1, True


def insert_destination_capabilities():
    """Process all docs in the processed docs folder"""
    print("Inserting destination capabilities...")

    if not os.path.exists(MD_TARGET_DIR):
        print(f"Target directory {MD_TARGET_DIR} does not exist. Skipping.")
        return

    processed_files = 0

    for file_path in walk_sync(MD_TARGET_DIR):
        count, processed = process_doc_file(file_path)
        if processed:
            processed_files += count

    print(f"Processed {processed_files} files for destination capabilities.")


def main():
    """Main function"""
    insert_destination_capabilities()


if __name__ == "__main__":
    main()
