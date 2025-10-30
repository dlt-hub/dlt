"""Simplify labels in the sidebar"""

from typing import List, Any
import json  # noqa: I251

from docs_tools.const import API_REFERENCE_DIR

SIDEBAR_PATH = f"{API_REFERENCE_DIR}/sidebar.json"


def process_items(items: List[Any]) -> None:
    for item in items:
        if isinstance(item, str):
            continue
        if "items" in item:
            process_items(item["items"])
        if "label" in item:
            item["label"] = item["label"].split(".")[-1]


def main() -> None:
    # clean sidebar
    with open(SIDEBAR_PATH, "r", encoding="utf-8") as f:
        sidebar = json.load(f)

    process_items(sidebar["items"])

    with open(SIDEBAR_PATH, "w", encoding="utf-8") as f:
        json.dump(sidebar, f, indent=2)

    # change init file title
    with open(f"{API_REFERENCE_DIR}/dlt/__init__.md", "r", encoding="utf-8") as f:
        content = f.read()

    content = content.replace("sidebar_label: dlt", "sidebar_label: __init__")

    with open(f"{API_REFERENCE_DIR}/dlt/__init__.md", "w", encoding="utf-8") as f:
        f.write(content)
