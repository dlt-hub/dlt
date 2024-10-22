from typing import List
import os
import glob

import dlt.cli.echo as fmt


DOCS_DIR = "../website/docs"


def collect_markdown_files(verbose: bool) -> List[str]:
    """
    Discovers all docs markdown files
    """

    # collect docs pages
    markdown_files: List[str] = []
    for filepath in glob.glob(f"{DOCS_DIR}/**/*.md", recursive=True):
        if "api_reference" in filepath:
            continue
        if "jaffle_shop" in filepath:
            continue
        markdown_files.append(filepath)
        if verbose:
            fmt.echo(f"Discovered {filepath}")

    if len(markdown_files) < 50:  # sanity check
        fmt.error("Found too few files. Something went wrong.")
        exit(1)

    fmt.note(f"Discovered {len(markdown_files)} markdown files")

    return markdown_files
