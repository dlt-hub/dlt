#!/usr/bin/env python3
"""
Documentation preprocessor for dlt docs.

This script processes markdown files by:
- Inserting code snippets from Python files
- Inserting tuba links from remote config
- Syncing examples from ../examples/ directory
- Checking for absolute/http links in docs
- Executing destination capabilities script
- Optionally watching for file changes
"""

import os
import sys
import time
import shutil
import subprocess
from typing import List, Tuple
import argparse

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from debouncer import DebounceOptions, debounce

from constants import (
    MD_SOURCE_DIR,
    MD_TARGET_DIR,
    MOVE_FILES_EXTENSION,
    DOCS_EXTENSIONS,
    WATCH_EXTENSIONS,
    DEBOUNCE_INTERVAL_MS,
    EXAMPLES_DESTINATION_DIR,
    EXAMPLES_SOURCE_DIR,
    HTTP_LINK,
    ABS_LINK,
    ABS_IMG_LINK,
)
from preprocess_tuba import insert_tuba_links, fetch_tuba_config
from preprocess_snippets import insert_snippets
from process_examples import sync_examples, build_example_doc
from utils import walk_sync, remove_remaining_markers


class SimpleEventHandler(FileSystemEventHandler):
    """Simple event handler for file watching."""

    def on_modified(self, event):
        if not event.is_directory:
            handle_change(event.src_path)

    def on_created(self, event):
        if not event.is_directory:
            handle_change(event.src_path)


def process_doc_file(file_name: str) -> Tuple[int, int, bool]:
    """Process a single documentation file."""
    ext = os.path.splitext(file_name)[1]
    if ext not in MOVE_FILES_EXTENSION:
        return 0, 0, False

    target_file_name = file_name.replace(MD_SOURCE_DIR, MD_TARGET_DIR)
    os.makedirs(os.path.dirname(target_file_name), exist_ok=True)

    if ext not in DOCS_EXTENSIONS:
        shutil.copyfile(file_name, target_file_name)
        return 0, 0, True

    try:
        with open(file_name, "r", encoding="utf-8") as f:
            lines = f.read().splitlines()
    except FileNotFoundError:
        return 0, 0, False

    snippet_count, lines = insert_snippets(file_name, lines)
    tuba_count, lines = insert_tuba_links(fetch_tuba_config(), lines)
    lines = remove_remaining_markers(lines)

    with open(target_file_name, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    return snippet_count, tuba_count, True


def preprocess_docs():
    """Preprocess all docs in the docs folder."""
    print("Processing docs...")
    processed_files = 0
    inserted_snippets = 0
    processed_tuba_blocks = 0

    for file_name in walk_sync(MD_SOURCE_DIR):
        snippet_count, tuba_count, processed = process_doc_file(file_name)
        if not processed:
            continue
        processed_files += 1
        inserted_snippets += snippet_count
        processed_tuba_blocks += tuba_count

    print(f"Processed {processed_files} files.")
    print(f"Inserted {inserted_snippets} snippets.")
    print(f"Processed {processed_tuba_blocks} tuba blocks.")


def check_file_links(file_name: str, lines: List[str]) -> bool:
    """Check a single file for problematic links."""
    found_error = False

    for index, line in enumerate(lines):
        line_no = index + 1
        line_lower = line.lower()

        if ABS_LINK in line_lower and ABS_IMG_LINK not in line_lower:
            found_error = True
            print(f"Found absolute md link in file {file_name}, line {line_no}")

        if HTTP_LINK in line_lower:
            found_error = True
            print(f"Found http md link referencing these docs in file {file_name}, line {line_no}")

    return found_error


def check_docs():
    """Inspect all md files and run some checks."""
    found_error = False

    for file_name in walk_sync(MD_SOURCE_DIR):
        ext = os.path.splitext(file_name)[1]
        if ext not in DOCS_EXTENSIONS:
            continue

        try:
            with open(file_name, "r", encoding="utf-8") as f:
                lines = f.read().splitlines()
        except Exception:
            continue

        if check_file_links(file_name, lines):
            found_error = True

    if found_error:
        raise ValueError("Found one or more errors while checking docs.")
    print("Found no errors in md files")


def execute_destination_capabilities():
    """Execute Python script for destination capabilities."""
    print("Inserting destination capabilities...")

    script_dir = os.path.dirname(os.path.abspath(__file__))
    python_script = os.path.join(script_dir, "insert_destination_capabilities.py")
    command = ["uv", "run", "python", python_script]

    try:
        result = subprocess.run(command, capture_output=True, text=True, check=True)

        if result.stdout and result.stdout.strip():
            print(f"Python script output: {result.stdout.strip()}")

        print("Destination capabilities inserted successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error executing destination capabilities script: {e}")
        if e.stdout:
            print(f"Python script stdout: {e.stdout}")
        if e.stderr:
            print(f"Python script stderr: {e.stderr}")
        raise


def should_process(file_path: str) -> bool:
    """Determine if a file should be processed."""
    ext = os.path.splitext(file_path)[1]

    if file_path.startswith(MD_SOURCE_DIR):
        return ext in WATCH_EXTENSIONS
    elif file_path.startswith(EXAMPLES_SOURCE_DIR):
        return ext in WATCH_EXTENSIONS
    else:
        return False


@debounce(
    wait=DEBOUNCE_INTERVAL_MS, options=DebounceOptions(trailing=True, leading=False, time_window=3)
)
def handle_change(file_path: str):
    """Handle a file change event."""
    print(f"{file_path} modified.", file=sys.stderr)

    if not should_process(file_path):
        print(f"Skipping {file_path}.", file=sys.stderr)
        return

    try:
        if file_path.startswith(MD_SOURCE_DIR):
            process_doc_file(file_path)
            print(f"{file_path} processed.")
        elif file_path.startswith(EXAMPLES_SOURCE_DIR):
            example_name = os.path.splitext(os.path.basename(file_path))[0]
            print(example_name)
            if build_example_doc(example_name):
                target_file_name = f"{EXAMPLES_DESTINATION_DIR}/{example_name}.md"
                process_doc_file(target_file_name)
                print(f"{file_path} processed.")
        elif file_path.endswith("snippets.toml"):
            preprocess_docs()
            print(f"{file_path} processed.")

        execute_destination_capabilities()
        check_docs()
    except Exception as e:
        print(f"Error processing {file_path}: {e}")


def process_docs():
    """Main processing function."""
    if os.path.exists(MD_TARGET_DIR):
        shutil.rmtree(MD_TARGET_DIR)

    sync_examples()
    preprocess_docs()
    execute_destination_capabilities()
    check_docs()


def watch():
    """Start watching for file changes."""
    print("Watching for file changes...")

    event_handler = SimpleEventHandler()
    observer = Observer()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    watch_dirs = [MD_SOURCE_DIR, EXAMPLES_SOURCE_DIR, script_dir]
    for watch_dir in watch_dirs:
        if os.path.exists(watch_dir):
            observer.schedule(event_handler, watch_dir, recursive=True)
            print(f"Watching directory: {watch_dir}")

    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        print("\nStopped watching.")

    observer.join()


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Preprocess dlt documentation files")
    parser.add_argument(
        "--watch", action="store_true", help="Watch for file changes and reprocess automatically"
    )

    args = parser.parse_args()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    website_dir = os.path.dirname(script_dir)
    os.chdir(website_dir)

    process_docs()

    if args.watch:
        watch()


if __name__ == "__main__":
    main()
