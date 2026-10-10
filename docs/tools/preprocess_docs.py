# /// script
# requires-python = ">=3.10,<3.13"
# dependencies = [
#     "dlt",
#     "requests>=2.30.5",
# ]
#
# [tool.uv.sources]
# dlt = { path = "../..", editable = true }
# ///
"""Documentation preprocessor for dlt docs.

Copies `website/docs` to `website/docs_processed`, replacing `@@@DLT_*` markers
on the way:

- `@@@DLT_TUBA <tag>`: insert a list of setup guides fetched from dlthub.com
- `@@@DLT_DESTINATION_CAPABILITIES <destination>`: insert a capabilities table
  generated from the destination in the dlt codebase

Any remaining `@@@DLT` marker lines are dropped. Non-markdown assets are copied
verbatim. Files are only written when their content actually changes, so this
can be re-run cheaply (the docusaurus dev server does so on every source edit,
see `website/plugins/preprocess-docs.js`).

Usage:
    uv run --script tools/preprocess_docs.py [--incremental] [-v]
"""

from __future__ import annotations

import argparse
import random
import re
import shutil
from datetime import date
from pathlib import Path
from typing import Any

import requests

# --- Configuration ----------------------------------------------------------

DOCS_DIR = Path(__file__).resolve().parents[1]
REPO_ROOT = DOCS_DIR.parent
WEBSITE_ROOT = DOCS_DIR / "website"
MD_SOURCE_DIR = WEBSITE_ROOT / "docs"
MD_TARGET_DIR = WEBSITE_ROOT / "docs_processed"

# extensions copied to the target dir, and the subset that gets preprocessed
MOVE_FILES_EXTENSIONS = [".md", ".mdx", ".py", ".png", ".jpg", ".jpeg"]
DOCS_EXTENSIONS = [".md", ".mdx"]

DLT_MARKER = "@@@DLT"
TUBA_MARKER = f"{DLT_MARKER}_TUBA"
CAPABILITIES_MARKER = f"{DLT_MARKER}_DESTINATION_CAPABILITIES"

# tuba links
TUBA_CONFIG_URL = "https://dlthub.com/docs/pipelines/links.json"
NUM_TUBA_LINKS = 10

# destination capabilities
DESTINATION_CAPABILITIES_SOURCE_DIR = REPO_ROOT / "dlt" / "destinations" / "impl"
DESTINATION_NAME_PATTERN = r"([a-z0-9_-]+?)(?:--|$)"
CAPABILITIES_TABLE_HEADER = "| Feature | Value | More |"

SELECTED_CAPABILITIES_ATTRIBUTES = {
    "preferred_loader_file_format",
    "supported_loader_file_formats",
    "preferred_staging_file_format",
    "supported_staging_file_formats",
    "has_case_sensitive_identifiers",
    "supported_merge_strategies",
    "supported_replace_strategies",
    "supports_tz_aware_datetime",
    "supports_naive_datetime",
    "sqlglot_dialect",
    "preferred_table_format",
    "supported_table_formats",
}

# (substring of the capability name, doc link, link label)
CAPABILITIES_DOC_LINKS = [
    ("file_format", "../file-formats/", "File formats"),
    (
        "merge",
        "../../general-usage/merge-loading#merge-strategies",
        "Merge strategy",
    ),
    (
        "replace",
        "../../general-usage/full-loading#choosing-the-correct-replace-strategy-for-your-full-load",
        "Replace strategy",
    ),
    (
        "time",
        "../../general-usage/schema#handling-of-timestamp-and-time-zones",
        "Timestamps and Timezones",
    ),
    (
        "dialect",
        "../../general-usage/dataset-access/dataset",
        "Dataset access",
    ),
    (
        "identifier",
        "../../general-usage/naming-convention#case-sensitive-and-insensitive-destinations",
        "Naming convention",
    ),
]
CAPABILITIES_DEFAULT_DOC_LINK = "[Data types](../../general-usage/schema#data-types)"


# --- Marker helpers ---------------------------------------------------------


def extract_marker_content(marker: str, line: str) -> str | None:
    """Extract the value following `marker` on `line`."""
    words = line.replace("<!--", "").replace("-->", "").split()
    try:
        return words[words.index(marker) + 1].strip()
    except (ValueError, IndexError):
        print(f"Error: Could not extract {marker} from line: {line}")
        return None


# --- Tuba links -------------------------------------------------------------

_tuba_config: list[dict[str, Any]] | None = None


def fetch_tuba_config() -> list[dict[str, Any]]:
    """Fetch the tuba links config from dlthub.com (cached per process)."""
    global _tuba_config

    if _tuba_config is None:
        try:
            response = requests.get(
                TUBA_CONFIG_URL,
                headers={"Accept": "application/vnd.citationstyles.csl+json"},
            )
            response.raise_for_status()
            _tuba_config = response.json()
        except Exception as e:
            print(f"Error: Could not fetch tuba config: {e}")
            _tuba_config = []

    return _tuba_config


def format_tuba_links_section(links: list[dict[str, Any]]) -> list[str]:
    """Format a stable (per day) random selection of tuba links as markdown."""
    # seed per day so regenerating the docs locally does not shuffle the links
    random.seed(int(date.today().strftime("%Y%m%d")))
    random.shuffle(links)

    return ["## Additional Setup guides"] + [
        f"- [{link['title']}]({link['public_url']})" for link in links[:NUM_TUBA_LINKS]
    ]


def insert_tuba_links(lines: list[str]) -> tuple[int, list[str]]:
    """Insert tuba links sections above their markers."""
    result = []
    count = 0

    for line in lines:
        if TUBA_MARKER not in line:
            result.append(line)
            continue

        tuba_tag = extract_marker_content(TUBA_MARKER, line)
        links = [
            link for link in fetch_tuba_config() if tuba_tag in link.get("tags", [])
        ]
        if links:
            result.extend(format_tuba_links_section(links))
        # the marker line itself is dropped with all leftover markers below
        result.append(line)
        count += 1

    return count, result


# --- Destination capabilities ----------------------------------------------

_impl_destinations: set[str] | None = None


def get_impl_destination_names() -> set[str]:
    """List destination names implemented in `dlt/destinations/impl` (cached)."""
    global _impl_destinations

    if _impl_destinations is None:
        try:
            _impl_destinations = {
                entry.name
                for entry in DESTINATION_CAPABILITIES_SOURCE_DIR.iterdir()
                if entry.is_dir()
            }
        except OSError as e:
            print(f"Error: Could not read {DESTINATION_CAPABILITIES_SOURCE_DIR}: {e}")
            _impl_destinations = set()

    return _impl_destinations


def get_raw_capabilities(destination_name: str) -> Any | None:
    """Get the raw capabilities of a destination, or None if unavailable."""
    # imported lazily: importing dlt is slow and only needed for destination pages
    from dlt.common.destination.capabilities import DestinationCapabilitiesContext
    from dlt.common.destination.reference import Destination

    try:
        caps = Destination.from_reference(destination_name)._raw_capabilities()
    except Exception as e:
        print(f"Error: Could not get capabilities for {destination_name}: {e}")
        return None

    if not isinstance(caps, DestinationCapabilitiesContext):
        print(f"Error: Invalid capabilities type for {destination_name}: {type(caps)}")
        return None

    return caps


def _format_value(value: Any) -> str:
    if isinstance(value, list):
        return ", ".join(str(v) for v in value)
    if hasattr(value, "__name__") and isinstance(value.__name__, str):
        return value.__name__
    return str(value)


def _doc_link(attr_name: str) -> str:
    """Get the "More" doc link for a capability."""
    for key, link, label in CAPABILITIES_DOC_LINKS:
        if key in attr_name.lower():
            return f"[{label}]({link})"
    return CAPABILITIES_DEFAULT_DOC_LINK


def _is_relevant_capability(attr_name: str, value: Any) -> bool:
    if value is None or attr_name not in SELECTED_CAPABILITIES_ATTRIBUTES:
        return False
    # skip reprs of objects, ie. `<function ...>`
    value_str = str(value)
    return not (value_str.startswith("<") and value_str.endswith(">"))


def generate_capabilities_table(destination_name: str) -> list[str]:
    """Generate a markdown capabilities table for a destination."""
    caps = get_raw_capabilities(destination_name)
    if caps is None:
        return []

    title = destination_name.title()
    lines = [
        "## Destination capabilities",
        f"The following table shows the capabilities of the {title} destination:",
        "",
        CAPABILITIES_TABLE_HEADER,
        "|---------|-------|------|",
    ]

    for attr_name, value in vars(caps).items():
        if not _is_relevant_capability(attr_name, value):
            continue
        feature = attr_name.replace("_", " ").capitalize()
        formatted_value = _format_value(value)
        if feature.strip() and formatted_value.strip():
            lines.append(f"| {feature} | {formatted_value} | {_doc_link(attr_name)} |")

    lines.append("")
    lines.append(
        f"*This table shows the supported features of the {title} destination in dlt.*"
    )
    lines.append("")

    return lines


def insert_destination_capabilities(lines: list[str]) -> tuple[int, list[str]]:
    """Replace destination capabilities markers with generated tables."""
    result = []
    count = 0

    for line in lines:
        if CAPABILITIES_MARKER not in line:
            result.append(line)
            continue

        match = re.search(
            rf"{re.escape(CAPABILITIES_MARKER)}\s+{DESTINATION_NAME_PATTERN}", line
        )
        if not match or match.group(1) not in get_impl_destination_names():
            result.append(line)
            continue

        result.extend(generate_capabilities_table(match.group(1)))
        count += 1

    return count, result


# --- Processing -------------------------------------------------------------


def process_doc_file(source_file: Path, verbose: bool = False) -> tuple[int, int, bool]:
    """Process a single file. Returns (tuba blocks, capabilities blocks, processed)."""
    ext = source_file.suffix
    if ext not in MOVE_FILES_EXTENSIONS:
        return 0, 0, False

    target_file = MD_TARGET_DIR / source_file.relative_to(MD_SOURCE_DIR)
    target_file.parent.mkdir(parents=True, exist_ok=True)

    if ext not in DOCS_EXTENSIONS:
        shutil.copyfile(source_file, target_file)
        return 0, 0, True

    try:
        lines = source_file.read_text(encoding="utf-8").split("\n")
    except FileNotFoundError:
        return 0, 0, False

    tuba_count, lines = insert_tuba_links(lines)
    capabilities_count, lines = insert_destination_capabilities(lines)
    # drop all leftover marker lines
    lines = [line for line in lines if DLT_MARKER not in line]
    new_content = "\n".join(lines)

    existing_content = None
    if target_file.exists():
        existing_content = target_file.read_text(encoding="utf-8")

    # only write on change, the docusaurus dev server watches the target dir
    if existing_content != new_content:
        if verbose:
            print(f"Updating {target_file}")
        target_file.write_text(new_content, encoding="utf-8")

    return tuba_count, capabilities_count, True


def preprocess_docs(clean: bool = True, verbose: bool = False) -> tuple[int, int, int]:
    """Preprocess all docs pages into the target dir."""
    if clean and MD_TARGET_DIR.exists():
        shutil.rmtree(MD_TARGET_DIR)

    print("Processing docs...")
    processed_files = tuba_blocks = capabilities_blocks = 0

    for source_file in sorted(MD_SOURCE_DIR.rglob("*")):
        if not source_file.is_file():
            continue
        if "jaffle_shop" in str(source_file) or source_file.suffix == ".py":
            continue
        if verbose:
            print(f"Processing file: {source_file}")
        tuba_count, capabilities_count, processed = process_doc_file(
            source_file, verbose
        )
        if not processed:
            continue
        processed_files += 1
        tuba_blocks += tuba_count
        capabilities_blocks += capabilities_count

    print(f"Processed {processed_files} files.")
    print(f"Processed {tuba_blocks} tuba blocks.")
    print(f"Processed {capabilities_blocks} capabilities blocks.")

    return processed_files, tuba_blocks, capabilities_blocks


def main() -> None:
    parser = argparse.ArgumentParser(description="Preprocess dlt documentation files")
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Keep the target folder and only rewrite files whose content changed",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Print per-file processing and update messages",
    )
    args = parser.parse_args()

    preprocess_docs(clean=not args.incremental, verbose=args.verbose)


if __name__ == "__main__":
    main()
