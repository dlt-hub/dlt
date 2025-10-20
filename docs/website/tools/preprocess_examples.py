"""
Example processing for documentation.
"""

import os
import sys

from .constants import EXAMPLES_DESTINATION_DIR, EXAMPLES_SOURCE_DIR, EXAMPLES_EXCLUSIONS
from .utils import list_dirs_sync, trim_array


def build_example_doc(example_name: str) -> bool:
    """Build example documentation from example Python file."""
    if any(example_name.startswith(ex) for ex in EXAMPLES_EXCLUSIONS):
        print(f"Skipping {example_name}. Is excluded example.", file=sys.stderr)
        return False

    example_file = f"{EXAMPLES_SOURCE_DIR}{example_name}/{example_name}.py"
    if not os.path.exists(example_file):
        print(f"Skipping {example_file}. File doesn't exist.", file=sys.stderr)
        return False

    target_file_name = f"{EXAMPLES_DESTINATION_DIR}/{example_name}.md"

    try:
        with open(example_file, "r", encoding="utf-8") as f:
            lines = f.read().splitlines()
    except Exception as e:
        print(f"Error reading {example_file}: {e}", file=sys.stderr)
        return False

    comment_count = 0
    header_count = 0

    header = []
    markdown = []
    code = []

    for line in lines:
        if line.startswith('"""'):
            comment_count += 1
            if comment_count > 2:
                raise ValueError(f"Too many comment blocks in {example_file}")
            continue

        if line.startswith("---"):
            header_count += 1
            if header_count > 2:
                raise ValueError(f"Too many header blocks in {example_file}")
            continue

        if header_count == 1:
            header.append(line)
        elif comment_count == 1:
            markdown.append(line)
        elif comment_count == 2:
            code.append(line)

    if header_count == 0:
        print(f"Aborting {example_file}. No header found.", file=sys.stderr)
        return False

    output = []
    output.append("---")
    output.extend(header)
    output.append("---")

    output.append(":::info")
    url = f"https://github.com/dlt-hub/dlt/tree/devel/docs/examples/{example_name}"
    output.append("The source code for this example can be found in our repository at: ")
    output.append(url)
    output.append(":::")

    output.append("## About this Example")
    output.extend(trim_array(markdown))

    output.append("### Full source code")
    output.append("```py")
    output.extend(trim_array(code))
    output.append("```")

    os.makedirs(os.path.dirname(target_file_name), exist_ok=True)
    with open(target_file_name, "w", encoding="utf-8") as f:
        f.write("\n".join(output))

    print(f"{target_file_name} generated.", file=sys.stderr)
    return True


def sync_examples() -> None:
    """Sync examples into docs."""
    count = 0
    for example_dir in list_dirs_sync(EXAMPLES_SOURCE_DIR):
        example_name = os.path.basename(example_dir)
        if build_example_doc(example_name):
            count += 1
    print(f"Synced {count} examples")
