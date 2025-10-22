#!/usr/bin/env python3
"""
Documentation preprocessor for dlt docs.

This script processes markdown files by:
- Inserting code snippets from Python files
- Inserting tuba links from remote config
- Syncing examples from tools.examples/ directory
- Checking for absolute/http links in docs
- Executing destination capabilities script
- Optionally watching for file changes
"""

import os
import shutil
import threading
from typing import List, Tuple
import argparse

from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer

from .constants import (
    EXAMPLES_SOURCE_DIR,
    MD_SOURCE_DIR,
    MD_TARGET_DIR,
    MOVE_FILES_EXTENSION,
    DOCS_EXTENSIONS,
    EXAMPLES_DESTINATION_DIR,
    HTTP_LINK,
    ABS_LINK,
    ABS_IMG_LINK,
    WATCH_EXTENSIONS,
)
from .utils import walk_sync, remove_remaining_markers
from .preprocess_snippets import insert_snippets
from .preprocess_tuba import insert_tuba_links, fetch_tuba_config
from .preprocess_destination_capabilities import insert_destination_capabilities
from .preprocess_examples import build_example_doc, sync_examples

_processing_lock: threading.Lock = threading.Lock()
_pending_changes: bool = False


class SimpleEventHandler(FileSystemEventHandler):
    """Simple event handler for file watching."""

    def on_modified(self, event: FileSystemEvent) -> None:
        if not event.is_directory:
            handle_change(str(event.src_path))

    def on_created(self, event: FileSystemEvent) -> None:
        if not event.is_directory and MD_TARGET_DIR not in event.src_path:
            handle_change(str(event.src_path))


def handle_change(file_path: str) -> None:
    """Handle a file change by rebuilding docs if needed."""
    print(f"Handling change in: {file_path}")

    global _processing_lock, _pending_changes

    rel_path = os.path.relpath(file_path)
    ext = os.path.splitext(rel_path)[1]

    should_process = (
        rel_path.startswith(MD_SOURCE_DIR) or rel_path.startswith(EXAMPLES_SOURCE_DIR)
    ) and ext in WATCH_EXTENSIONS

    if not should_process:
        print(f"Skipping change in: {rel_path}")
        return

    if _processing_lock.locked():
        print(f"Found a change in: {rel_path}, but processing is locked, adding to pending changes")
        _pending_changes = True
        return

    _pending_changes = True
    while _pending_changes:
        _pending_changes = False
        try:
            with _processing_lock:
                process_docs(incremental_run=True)
        except Exception as e:
            print(f"Error rebuilding docs: {e}")
            raise e


def watch() -> None:
    """Start watching for file changes."""
    event_handler = SimpleEventHandler()
    observer = Observer()

    watch_dirs = [MD_SOURCE_DIR, EXAMPLES_SOURCE_DIR]
    for watch_dir in watch_dirs:
        if os.path.exists(watch_dir):
            observer.schedule(event_handler, watch_dir, recursive=True)
            print(f"Watching directory: {watch_dir}")

    observer.start()

    try:
        observer.join()
    except KeyboardInterrupt:
        print("\nStopping file watcher...")
        observer.stop()
        observer.join()
        print("File watcher stopped.")


def process_doc_file(file_name: str) -> Tuple[int, int, int, bool]:
    """Process a single documentation file."""
    ext = os.path.splitext(file_name)[1]
    if ext not in MOVE_FILES_EXTENSION:
        return 0, 0, 0, False

    if os.path.isabs(file_name):
        file_name = os.path.relpath(file_name)

    target_file_name = file_name.replace(MD_SOURCE_DIR, MD_TARGET_DIR)
    os.makedirs(os.path.dirname(target_file_name), exist_ok=True)

    if ext not in DOCS_EXTENSIONS:
        shutil.copyfile(file_name, target_file_name)
        return 0, 0, 0, True

    try:
        with open(file_name, "r", encoding="utf-8") as f:
            content = f.read()
            lines = content.split("\n")
    except FileNotFoundError:
        return 0, 0, 0, False

    snippet_count, lines = insert_snippets(file_name, lines)
    tuba_count, lines = insert_tuba_links(fetch_tuba_config(), lines)
    capabilities_count, lines = insert_destination_capabilities(lines)
    lines = remove_remaining_markers(lines)

    existing_file_content = ""
    if os.path.exists(target_file_name):
        with open(target_file_name, "r", encoding="utf-8") as f:
            existing_file_content = f.read()

    new_file_content = "\n".join(lines)

    if existing_file_content != new_file_content:
        print(f"Updating {target_file_name}")
        with open(target_file_name, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))

    return snippet_count, tuba_count, capabilities_count, True


def preprocess_docs() -> Tuple[int, int, int, int]:
    """Preprocess all docs in the docs folder."""
    print("Processing docs...")
    processed_files = 0
    inserted_snippets = 0
    processed_tuba_blocks = 0
    processed_capabilities_blocks = 0

    for file_name in walk_sync(MD_SOURCE_DIR):
        snippet_count, tuba_count, capabilities_count, processed = process_doc_file(file_name)
        if not processed:
            continue
        processed_files += 1
        inserted_snippets += snippet_count
        processed_tuba_blocks += tuba_count
        processed_capabilities_blocks += capabilities_count

    print(f"Processed {processed_files} files.")
    print(f"Inserted {inserted_snippets} snippets.")
    print(f"Processed {processed_tuba_blocks} tuba blocks.")
    print(f"Processed {processed_capabilities_blocks} capabilities blocks.")

    return processed_files, inserted_snippets, processed_tuba_blocks, processed_capabilities_blocks


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


def check_docs() -> None:
    """Inspect all md files and run some checks."""
    found_error = False

    for file_name in walk_sync(MD_SOURCE_DIR):
        ext = os.path.splitext(file_name)[1]
        if ext not in DOCS_EXTENSIONS:
            continue

        try:
            with open(file_name, "r", encoding="utf-8") as f:
                lines = f.read().split("\n")
        except Exception:
            continue

        if check_file_links(file_name, lines):
            found_error = True

    if found_error:
        raise ValueError("Found one or more errors while checking docs.")
    print("Found no errors in md files")


def process_example_change(file_path: str) -> None:
    """Process an example file change."""
    example_name = os.path.splitext(os.path.basename(file_path))[0]
    if build_example_doc(example_name):
        target_file_name = f"{EXAMPLES_DESTINATION_DIR}/{example_name}.md"
        process_doc_file(target_file_name)


def process_docs(incremental_run: bool = False) -> None:
    """Main processing function."""
    if not incremental_run and os.path.exists(MD_TARGET_DIR):
        shutil.rmtree(MD_TARGET_DIR)

    sync_examples()
    preprocess_docs()
    check_docs()


def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Preprocess dlt documentation files")
    parser.add_argument(
        "--watch", action="store_true", help="Watch for file changes and reprocess automatically"
    )
    print("Parsing args")
    args = parser.parse_args()
    print("Args parsed")
    script_dir = os.path.dirname(os.path.abspath(__file__))
    website_dir = os.path.dirname(script_dir)
    print("Changing directory to", website_dir)
    os.chdir(website_dir)

    if args.watch:
        print("Watching for file changes...")
        watch()
    else:
        process_docs()


if __name__ == "__main__":
    main()
