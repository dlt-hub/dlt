"""
Creates the pytest files for our examples tests. These will not be committed
"""
import os
import argparse

import dlt.cli.echo as fmt

EXAMPLES_DIR = "../examples"

# settings
SKIP_FOLDERS = ["archive", ".", "_", "local_cache"]

# the entry point for the script
MAIN_CLAUSE = 'if __name__ == "__main__":'

# some stuff to insert for setting up and tearing down fixtures
TEST_HEADER = """
from tests.utils import skipifgithubfork

"""


if __name__ == "__main__":
    # setup cli
    parser = argparse.ArgumentParser(
        description="Prepares examples in docs/examples for testing.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "-c", "--clear", help="Remove all generated test files", action="store_true"
    )

    # get args
    args = parser.parse_args()

    count = 0
    for example in next(os.walk(EXAMPLES_DIR))[1]:
        # skip some
        if any(map(lambda skip: example.startswith(skip), SKIP_FOLDERS)):
            continue

        count += 1
        example_file = f"{EXAMPLES_DIR}/{example}/{example}.py"
        test_example_file = f"{EXAMPLES_DIR}/{example}/test_{example}.py"

        if args.clear:
            os.unlink(test_example_file)
            continue

        with open(example_file, "r", encoding="utf-8") as f:
            lines = f.read().split("\n")

        processed_lines = TEST_HEADER.split("\n")
        main_clause_found = False

        for line in lines:
            # convert the main clause to a test function
            if line.startswith(MAIN_CLAUSE):
                main_clause_found = True
                processed_lines.append("@skipifgithubfork")
                processed_lines.append(f"def test_{example}():")
            else:
                processed_lines.append(line)

        if not main_clause_found:
            fmt.error(f"No main clause defined for example {example}")
            exit(1)

        with open(test_example_file, "w", encoding="utf-8") as f:
            f.write("\n".join(processed_lines))

    if args.clear:
        fmt.note("Cleared generated test files.")
    else:
        fmt.note(f"Prepared {count} examples for testing.")
