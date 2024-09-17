"""
Fixes the grammar of all the markdown files in the docs/website/docs directory.
Required openai package to be installed, and an .env file with the open ai api key to be present in the root directory:
OPENAI_API_KEY="..."
"""
import os
import functools
import argparse
from typing import List

from openai import OpenAI
from dotenv import load_dotenv

import dlt.cli.echo as fmt

from utils import collect_markdown_files

# constants
BASE_DIR = "../website/docs"
GPT_MODEL = "gpt-4-turbo"
MAX_CHUNK_SIZE = 4000  # make sure that this is below the context window size of the model to not have cut off files

SYSTEM_PROMPT = """\
You are a grammar checker. Every message you get will be a document that is to be grammarchecked and returned as such.
You will not change the markdown syntax. You will only fix the grammar. You will not change the code snippets except for the comments therein.
You will not modify the header section which is enclosed by two occurences of "---".
Make sure all headings use the Sentence case.
Never insert any codeblock start or end statements such as "```"
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

    parser.add_argument(
        "-o",
        "--offset",
        help="File count offset from where to start fixing",
        default=0,
        type=int,
    )

    parser.add_argument(
        "-l",
        "--limit",
        help="File count limit, how many files to process",
        default=100000,
        type=int,
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
    processed = 0
    for file_path in markdown_files:
        count += 1

        if count <= args.offset:
            continue

        if processed >= args.limit:
            break

        processed += 1

        fmt.note(f"Fixing grammar for file {file_path} ({count} of {len(markdown_files)})")

        with open(file_path, "r", encoding="utf-8") as f:
            doc = f.readlines()

        with open(file_path, "r", encoding="utf-8") as f:
            doc_length = len(f.read())

        def get_chunk_length(chunk: List[str]) -> int:
            count = 0
            for line in chunk:
                count += len(line)
            return count

        # cut file into sections
        sections: List[List[str]] = []
        current_section: List[str] = []
        is_in_code_block: bool = False
        for line in doc:
            if "```" in line:
                is_in_code_block = not is_in_code_block
            if line.startswith("#") and not is_in_code_block:
                if current_section:
                    sections.append(current_section)
                current_section = [line]
            else:
                current_section.append(line)
        sections.append(current_section)

        # build chunks from sections
        chunks: List[List[str]] = []
        current_chunk: List[str] = []

        for section in sections:
            # we can extend the chunk if the size is small enough
            if get_chunk_length(current_chunk + section) < MAX_CHUNK_SIZE:
                current_chunk += section
            # start a new one
            else:
                chunks.append(current_chunk)
                current_chunk = section
        chunks.append(current_chunk)

        # sanity test, make sure we still have the full doc
        assert doc == functools.reduce(lambda a, b: a + b, chunks)

        # count chars in doc
        fmt.note(f"Created {len(chunks)} chunks for {doc_length} chars")

        fixed_chunks: List[str] = []
        for chunk in chunks:
            client = OpenAI()
            in_string = "".join(chunk)
            response = client.chat.completions.create(
                seed=123981298,
                model=GPT_MODEL,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": in_string},
                ],
                temperature=0,
            )
            fixed_chunks.append(response.choices[0].message.content)

        # here we check that no part of the doc was swallowed by gpt
        fixed_doc_length = functools.reduce(
            lambda count, chunk: count + len(chunk), fixed_chunks, 0
        )
        if fixed_doc_length / doc_length < 0.9:
            fmt.error(
                "Doc length reduced too much during processing, skipping saving, please check"
                " manually"
            )
            continue

        with open(file_path, "w", encoding="utf-8") as f:
            for c in fixed_chunks:
                f.write(c)
                f.write("\n")
                f.write("\n")

    if count == 0:
        fmt.warning("No files selected for grammar check.")
    else:
        fmt.note(f"Fixed grammar for {processed} files.")
