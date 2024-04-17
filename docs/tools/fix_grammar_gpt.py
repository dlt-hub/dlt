"""
Fixes the grammar of all the markdown files in the docs/website/docs directory.
Required openai package to be installed, and an .env file with the open ai api key to be present in the root directory:
OPENAI_API_KEY="..."
"""
import os
import argparse

from openai import OpenAI
from dotenv import load_dotenv

import dlt.cli.echo as fmt

from utils import collect_markdown_files

# constants
BASE_DIR = "../website/docs"
GPT_MODEL = "gpt-3.5-turbo-0125"

SYSTEM_PROMPT = """\
You are a grammar checker. Every message you get will be a document that is to be grammarchecked and returned as such.
You will not change the markdown syntax. You will only fix the grammar. You will not change the code snippets except for the comments therein.
You will not modify the header section which is enclosed by two occurences of "---".
Do not change the spelling or casing of these words: dlt, sdf, dbt
"""

if __name__ == "__main__":
    load_dotenv()

    fmt.note("Welcome to Grammar Fixer 3000, run 'python fix_grammar_gpt.py --help' for help.")

    # setup cli
    parser = argparse.ArgumentParser(
        description=(
            "Fixes the grammar of our docs with open ai. Requires an .env file with the open ai"
            " key."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("-v", "--verbose", help="Increase output verbosity", action="store_true")
    parser.add_argument(
        "-f",
        "--files",
        help=(
            "Specify the file name. Grammar Checker will filter all .md files containing this"
            " string in the filepath."
        ),
        type=str,
    )

    # get args
    args = parser.parse_args()

    # find all files
    markdown_files = collect_markdown_files(args.verbose)

    # filter files
    if args.files:
        markdown_files = [f for f in markdown_files if args.files in f]

    # run grammar check
    count = 0
    for file_path in markdown_files:
        count += 1

        fmt.note(f"Fixing grammar for file {file_path} ({count} of {len(markdown_files)})")

        with open(file_path, "r", encoding="utf-8") as f:
            doc = f.readlines()

        client = OpenAI()
        response = client.chat.completions.create(
            model=GPT_MODEL,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": "".join(doc)},
            ],
            temperature=0,
        )

        fixed_doc = response.choices[0].message.content

        with open(file_path, "w", encoding="utf-8") as f:
            f.writelines(fixed_doc)

    if count == 0:
        fmt.warning("No files selected for grammar check.")
    else:
        fmt.note(f"Fixed grammar for {count} files.")
