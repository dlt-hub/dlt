"""
Walks through all markdown files, finds all code snippets, and checks wether they are parseable.
"""
import os
from typing import TypedDict, List
import ast
from textwrap import dedent
import tomlkit
import json
import yaml

DOCS_DIR = "../website/docs"

SNIPPET_MARKER = "```"


class Snippet(TypedDict):
    language: str
    code: str
    file: str
    line: int


if __name__ == "__main__":
    # discover all markdown files to be processed
    markdown_files = []
    for path, directories, files in os.walk(DOCS_DIR):
        if "api_reference" in path:
            continue
        if "jaffle_shop" in path:
            continue
        for file in files:
            if file.endswith(".md"):
                markdown_files.append(os.path.join(path, file))

    # extract snippets from markdown files
    snippets: List[Snippet] = []
    for file in markdown_files:
        print(f"Processing file {file}")

        # go line by line and find all code  blocks
        with open(file, "r") as f:
            current_snippet: Snippet = None
            lint_count = 0
            for line in f.readlines():
                lint_count += 1
                if line.strip().startswith(SNIPPET_MARKER):
                    if current_snippet:
                        # process snippet
                        snippets.append(current_snippet)
                        current_snippet["code"] = dedent(current_snippet["code"])
                        current_snippet = None
                    else:
                        # start new snippet
                        current_snippet = {
                            "language": line.strip().split(SNIPPET_MARKER)[1],
                            "code": "",
                            "file": file,
                            "line": lint_count,
                        }
                elif current_snippet:
                    current_snippet["code"] += line
        assert not current_snippet

    # parse python snippets for now
    count = {}
    total = 0
    failed_count = {}
    for snippet in snippets:
        language = snippet["language"] or "unknown"
        code = snippet["code"]
        total += 1
        count[language] = count.get(language, 0) + 1

        # print(
        #     "Processing snippet no",
        #     total,
        #     "at line",
        #     snippet["line"],
        #     "in file",
        #     snippet["file"],
        #     "with language",
        #     language,
        # )

        # parse snippet by type
        try:
            if language in ["python", "py"]:
                ast.parse(code)
            elif language in ["toml"]:
                tomlkit.loads(code)
            elif language in ["json"]:
                json.loads(snippet["code"])
            elif language in ["yaml"]:
                yaml.safe_load(code)
            # ignore text and shell scripts
            elif language in ["text", "shell", "bat"]:
                pass
            elif language in ["sql"]:
                pass
            else:
                assert False, "Unknown language. Please choose the correct language for the snippet: py, toml, json, yaml, text, shell, bat or sql."

        except Exception as e:
            print(
                "---\n"
                "Failed parsing snippet no",
                total,
                "at line",
                snippet["line"],
                "in file",
                snippet["file"],
                "with language",
                language,
                "\n---"
            )
            raise
            failed_count[language] = failed_count.get(language, 0) + 1

    assert len(snippets) > 100, "Found too few snippets. Something went wrong."  # sanity check

    print(count)
    print(failed_count)
