"""
Walks through all markdown files, finds all code snippets, and checks wether they are parseable.
"""
import os
from typing import TypedDict, List
import ast
from textwrap import dedent
import tomlkit

DOCS_DIR = "./docs"

SNIPPET_MARKER = "```"

class Snippet(TypedDict):
    language: str
    code: str
    file: str
    line: int

if __name__ == "__main__":

    markdown_files = []
    for path, directories, files in os.walk(DOCS_DIR):
        for file in files:
            if file.endswith(".md"):
                markdown_files.append(os.path.join(path, file))

    # extract snippets from markdown files
    snippets: List[Snippet] = []
    for file in markdown_files:
        print(f"Processing file {file}")
        with open(file, "r") as f:
            current_snippet: Snippet = None
            lint_count = 0
            for line in f.readlines():
                lint_count += 1
                if line.strip().startswith(SNIPPET_MARKER):
                    if current_snippet:
                        # process snippet
                        snippets.append(current_snippet)
                        current_snippet = None
                    else:
                        # start new snippet
                        current_snippet = {
                            "language": line.strip().split(SNIPPET_MARKER)[1],
                            "code": "",
                            "file": file,
                            "line": lint_count
                        }
                elif current_snippet:
                    current_snippet["code"] += line
        assert not current_snippet

    # parse python snippets for now
    count = 0
    for snippet in snippets:
        print("Processing snippet no", count, " at line", snippet["line"], "in file", snippet["file"])
        if snippet["language"] in ["python", "py"]:
            ast.parse(dedent(snippet["code"]))
        if snippet["language"] in ["toml"]:
            tomlkit.loads(snippet["code"])


        count += 1

    print(count)


    print(f"Found {len(snippets)} snippets")

    print(snippets[500]["code"])

