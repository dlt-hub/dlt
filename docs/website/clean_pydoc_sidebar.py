"""Simplify labels in the sidebar"""
import json

SIDEBAR_PATH = "docs_processed/api_reference/sidebar.json"


def process_items(items: list) -> list:
    for item in items:
        if isinstance(item, str):
            continue
        if "items" in item:
            process_items(item["items"])
        if "label" in item:
            item["label"] = item["label"].split(".")[-1]


if __name__ == "__main__":
    # clean sidebar
    with open(SIDEBAR_PATH, "r") as f:
        sidebar = json.load(f)

    process_items(sidebar["items"])

    with open(SIDEBAR_PATH, "w") as f:
        json.dump(sidebar, f, indent=2)

    # change init file title
    with open("docs_processed/api_reference/dlt/__init__.md", "r") as f:
        content = f.read()

    content = content.replace("sidebar_label: dlt", "sidebar_label: __init__")

    with open("docs_processed/api_reference/dlt/__init__.md", "w") as f:
        f.write(content)
