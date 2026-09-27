# /// script
# requires-python = ">=3.10,<3.13"
# dependencies = [
#     "pydoc-markdown>=4.8.2,<5",
#     "databind>=4.5.2",
#     "docstring-parser>=0.11",
# ]
# ///
"""Standalone tool that generates the API reference docs from the `dlt` source code.

The module docstrings are parsed statically (no `dlt` import is required), processed
for MDX compatibility and rendered as Docusaurus pages plus a sidebar.

Usage:
    uv run --script tools/generate_api_ref.py [--output-dir DIR]
"""

from __future__ import annotations

import argparse
import json
import pathlib
import re
from functools import partial
from typing import Any

from pydoc_markdown import PydocMarkdown
from pydoc_markdown.contrib.processors.smart import SmartProcessor

DOCS_DIR = pathlib.Path(__file__).resolve().parents[1]
REPO_ROOT = DOCS_DIR.parent
DEFAULT_OUTPUT_DIR = DOCS_DIR / "website" / "docs_processed"
RELATIVE_OUTPUT_PATH = "api_reference"
SIDEBAR_FILE_NAME = "sidebar.json"

#: modules that are internal and should not show up in the API reference
EXCLUDED_MODULES = (
    "dlt._workspace.cli",
    "dlt.normalize",
    "dlt.load",
    "dlt.reflection",
)

#: modules under any of these path segments are implementation details: their pages stay
#: in the reference and the sidebar but are kept out of search engine indexes
NOINDEX_MODULE_SEGMENTS = frozenset(
    {
        "__main__",
        "data_writers",
        "exceptions",
        "impl",
        "libs",
        "normalizers",
        "runners",
        "runtime",
        "storages",
        "typing",
        "utils",
        "warnings",
    }
)

NOINDEX_HEAD = '<head>\n  <meta name="robots" content="noindex, follow" />\n</head>\n'

sub = partial(re.sub, flags=re.M)


def is_private_module(module_name: str) -> bool:
    """Tells if any part of a dotted module name is private, e.g. `dlt._workspace.helpers`."""
    return any(
        part.startswith("_") and not part.startswith("__") for part in module_name.split(".")
    )


def is_noindex_module(module_name: str) -> bool:
    """Tells if a module is an implementation detail that search engines should not index."""
    return any(part in NOINDEX_MODULE_SEGMENTS for part in module_name.split("."))


class DltProcessor(SmartProcessor):
    """Makes `dlt` docstrings render correctly as MDX."""

    def _process(self, node: Any) -> Any:
        if not getattr(node, "docstring", None):
            return None

        # join long lines ending in escape (\)
        c = sub(r"\\\n\s*", "", node.docstring.content)

        # remove markdown headers
        c = sub(r"^#### (.*?)$", r"\1", c)

        # convert REPL code blocks to code
        c = sub(r"^(\s*>>>|\.\.\.)(.*?)$", r"```\n\1\2\n```", c)
        c = sub(r"^(\s*>>>|\.\.\.)(.*?)\n```\n```\n(\s*>>>|\.\.\.)", r"\1\2\n\3", c)
        c = sub(r"^(\s*>>>|\.\.\.)(.*?)\n```\n```\n(\s*>>>|\.\.\.)", r"\1\2\n\3", c)
        c = sub(r"^(\s*```)(\n\s*>>>) ", r"\1py\2", c)
        c = sub(r"(\n\s*)(>>> ?)", r"\1", c)

        # escape characters that are special characters in mdx
        c = c.replace("<", "&lt;")
        c = c.replace(">", "&gt;")
        c = c.replace("{", "&#123;")
        c = c.replace("}", "&#125;")

        node.docstring.content = c

        return super()._process(node)


def build_config(output_dir: pathlib.Path) -> dict[str, Any]:
    """Builds the pydoc-markdown configuration (the former `pydoc-markdown.yml`)."""
    filter_expression = (
        " and ".join(f'not name.startswith("{module}")' for module in EXCLUDED_MODULES)
        + " and default()"
    )
    return {
        "loaders": [
            {"type": "python", "search_path": [str(REPO_ROOT)], "packages": ["dlt"]},
        ],
        "processors": [
            {"type": "filter", "expression": filter_expression},
        ],
        "renderer": {
            "type": "docusaurus",
            "docs_base_path": str(output_dir),
            "relative_output_path": RELATIVE_OUTPUT_PATH,
            "relative_sidebar_path": SIDEBAR_FILE_NAME,
            "sidebar_top_level_label": "API Reference",
            "markdown": {
                "use_fixed_header_levels": False,
                "escape_html_in_docstring": False,
                "classdef_with_decorators": True,
                "signature_with_decorators": True,
                "format_code": True,
                "source_linker": {
                    "type": "github",
                    "repo": "dlt-hub/dlt",
                    "root": str(REPO_ROOT),
                },
                "source_format": "[View source on GitHub]({url})",
            },
        },
    }


def render_api_reference(output_dir: pathlib.Path) -> None:
    """Loads the `dlt` modules and renders them into `output_dir`."""
    output_dir.mkdir(parents=True, exist_ok=True)

    session = PydocMarkdown()
    session.load_config(build_config(output_dir))
    # the custom processor can't be referenced from the config because it's not a
    # registered pydoc-markdown plugin; it replaces the default `SmartProcessor`
    session.processors.append(DltProcessor())

    # private modules are not part of the public API, so they get no pages at all
    modules = [module for module in session.load_modules() if not is_private_module(module.name)]
    session.process(modules)
    session.render(modules)


def module_name_from_path(api_reference_dir: pathlib.Path, page: pathlib.Path) -> str:
    """Maps a rendered page back to its module, e.g. `dlt/pipeline/__init__.md` -> `dlt.pipeline`."""
    parts = list(page.relative_to(api_reference_dir).with_suffix("").parts)
    if parts[-1] == "__init__":
        parts.pop()
    return ".".join(parts)


def mark_noindex_pages(api_reference_dir: pathlib.Path) -> None:
    """Adds a robots `noindex` tag to implementation-detail pages.

    Docusaurus also leaves pages with this tag out of the sitemap.
    """
    for page in sorted(api_reference_dir.rglob("*.md")):
        if not is_noindex_module(module_name_from_path(api_reference_dir, page)):
            continue
        content = page.read_text(encoding="utf-8")
        # the head block goes right after the front matter
        _, front_matter, body = content.split("---\n", 2)
        page.write_text(f"---\n{front_matter}---\n\n{NOINDEX_HEAD}{body}", encoding="utf-8")


def simplify_sidebar_labels(items: list[Any]) -> None:
    """Shortens fully qualified module labels, e.g. `dlt.extract.decorators` -> `decorators`."""
    for item in items:
        if isinstance(item, str):
            continue

        if "items" in item:
            simplify_sidebar_labels(item["items"])

        if "label" in item:
            item["label"] = item["label"].split(".")[-1]


def clean_sidebar(api_reference_dir: pathlib.Path) -> None:
    """Simplifies the generated sidebar labels and the title of the `dlt` root page."""
    sidebar_path = api_reference_dir / SIDEBAR_FILE_NAME
    sidebar = json.loads(sidebar_path.read_text(encoding="utf-8"))
    simplify_sidebar_labels(sidebar["items"])
    sidebar_path.write_text(json.dumps(sidebar, indent=2), encoding="utf-8")

    init_path = api_reference_dir / "dlt" / "__init__.md"
    content = init_path.read_text(encoding="utf-8")
    init_path.write_text(
        content.replace("sidebar_label: dlt", "sidebar_label: __init__"),
        encoding="utf-8",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        type=pathlib.Path,
        default=DEFAULT_OUTPUT_DIR,
        help=(
            "Docs base directory; the reference is written to its `api_reference` subdirectory"
            f" (default: {DEFAULT_OUTPUT_DIR})"
        ),
    )
    args = parser.parse_args()

    output_dir = args.output_dir.resolve()
    render_api_reference(output_dir)
    clean_sidebar(output_dir / RELATIVE_OUTPUT_PATH)
    mark_noindex_pages(output_dir / RELATIVE_OUTPUT_PATH)


if __name__ == "__main__":
    main()
