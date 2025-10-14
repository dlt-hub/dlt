from dataclasses import dataclass
from datetime import datetime
import os
from queue import Queue, Empty
import threading
import time
from typing import Optional, Set, Any

from watchdog.events import FileSystemEventHandler, FileSystemEvent
from watchdog.observers import Observer


@dataclass
class FileChangeEvent:
    """Represents a file change event."""

    file_path: str
    timestamp: datetime


_processing_files: Set[str] = set()
_processing_lock: threading.Lock = threading.Lock()
_change_queue: Queue[FileChangeEvent] = Queue()
_worker_thread: Optional[threading.Thread] = None
_worker_running: bool = True


class SimpleEventHandler(FileSystemEventHandler):
    """Simple event handler for file watching."""

    def __init__(self) -> None:
        super().__init__()

    def on_modified(self, event: FileSystemEvent) -> None:
        if not event.is_directory:
            handle_change(str(event.src_path))

    def on_created(self, event: FileSystemEvent) -> None:
        if not event.is_directory:
            handle_change(str(event.src_path))


def process_change_events() -> None:
    """Worker function to process events from the queue."""
    while _worker_running:
        try:
            event = _change_queue.get(timeout=1.0)
            print(f"Processing change for {event.file_path}")
            _handle_change(event.file_path)
            _change_queue.task_done()
        except Empty:
            continue
        except Exception as e:
            print(f"Error processing change: {e}")


def handle_change(file_path: str) -> None:
    """Queue a file change event for processing."""
    with _processing_lock:
        if file_path in _processing_files:
            print(f"File {file_path} already queued/processing, skipping")
            return
        _processing_files.add(file_path)

    event = FileChangeEvent(file_path=file_path, timestamp=datetime.now())
    _change_queue.put(event)
    print(f"Queued change for: {file_path}")


def _handle_change(file_path: str) -> None:
    """Process a single file change."""
    try:
        from tools.preprocess_docs import (
            preprocess_docs,
            check_docs,
            process_doc_file,
            process_example_change,
        )
        from tools.constants import WATCH_EXTENSIONS

        ext = os.path.splitext(file_path)[1]
        if file_path.endswith("snippets.toml"):
            preprocess_docs()
            check_docs()
        elif "examples" in file_path and ext in WATCH_EXTENSIONS:
            process_example_change(file_path)
        elif "docs" in file_path and ext in WATCH_EXTENSIONS:
            process_doc_file(file_path)
    finally:
        with _processing_lock:
            _processing_files.discard(file_path)


def watch() -> None:
    """Start watching for file changes."""
    global _worker_running, _worker_thread

    _worker_running = True
    _worker_thread = threading.Thread(target=process_change_events, daemon=True)
    _worker_thread.start()

    event_handler = SimpleEventHandler()
    observer = Observer()

    from tools.constants import EXAMPLES_SOURCE_DIR, MD_SOURCE_DIR

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
        # Stop the worker thread
        _worker_running = False
        if _worker_thread:
            _worker_thread.join(timeout=1)
        # Stop the observer
        observer.stop()
        observer.join()
        print("File watcher stopped.")
