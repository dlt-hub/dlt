from typing import List
import os

import dlt.cli.echo as fmt


DOCS_DIR = "../website/docs"


def collect_markdown_files(verbose: bool) -> List[str]:
    """
    Discovers all docs markdown files
    """
    markdown_files: List[str] = []
    for path, _, files in os.walk(DOCS_DIR):
        if "api_reference" in path:
            continue
        if "jaffle_shop" in path:
            continue
        for file in files:
            if file.endswith(".md"):
                markdown_files.append(os.path.join(path, file))
                if verbose:
                    fmt.echo(f"Discovered {os.path.join(path, file)}")

    if len(markdown_files) < 50:  # sanity check
        fmt.error("Found too few files. Something went wrong.")
        exit(1)

    fmt.note(f"Discovered {len(markdown_files)} markdown files")

    return markdown_files
