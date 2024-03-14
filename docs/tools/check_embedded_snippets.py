"""
Walks through all markdown files, finds all code snippets, and checks wether they are parseable.
"""
from typing import TypedDict, List, Dict

import os, ast, json, yaml, tomlkit  # noqa: I251
from textwrap import dedent

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
    for path, _, files in os.walk(DOCS_DIR):
        if "api_reference" in path:
            continue
        if "jaffle_shop" in path:
            continue
        for file in files:
            if file.endswith(".md"):
                markdown_files.append(os.path.join(path, file))

    # extract snippets from markdown files
    snippets: List[Snippet] = []
    count: Dict[str, int] = {}
    failed_count: Dict[str, int] = {}

    for file in markdown_files:
        # go line by line and find all code  blocks
        with open(file, "r", encoding="utf-8") as f:
            current_snippet: Snippet = None
            lint_count = 0
            for line in f.readlines():
                lint_count += 1
                if line.strip().startswith(SNIPPET_MARKER):
                    if current_snippet:
                        # process snippet
                        snippets.append(current_snippet)
                        current_snippet["code"] = dedent(current_snippet["code"])
                        language = current_snippet["language"] or "unknown"
                        count[language] = count.get(language, 0) + 1
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
        assert not current_snippet, (
            "It seems that the last snippet in the file was not closed. Please check the file "
            + file
        )

    print("Found", len(snippets), "snippets", "in", len(markdown_files), "markdown files: ")
    print(count)
    print(
        "Snippets of the types py, yaml, toml and json will now be parsed. All other snippets will"
        " be accepted as they are."
    )
    print("---")

    assert len(snippets) > 100, "Found too few snippets. Something went wrong."  # sanity check
    assert (
        len(markdown_files) > 50
    ), "Found too few markdown files. Something went wrong."  # sanity check

    # parse python snippets for now
    total = 0
    for snippet in snippets:
        language = snippet["language"] or "unknown"
        code = snippet["code"]
        total += 1

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
                raise AssertionError("""
Unknown language. Please choose the correct language for the snippet: py, toml, json, yaml, text, shell, bat or sql."
All shell commands, except for windows, should be marked as shell.
All code blocks that are not a specific (markup-) language should be marked as text.
""")
        except Exception:
            print(
                "---\nFailed parsing snippet no",
                total,
                "at line",
                snippet["line"],
                "in file",
                snippet["file"],
                "with language",
                language,
                "\n---",
            )
            failed_count[language] = failed_count.get(language, 0) + 1

            raise  # you can comment this out to see all failed snippets

    if failed_count:
        print("Failed to parse the following amount of snippets")
        print(failed_count)
    else:
        print("All snippets could be parsed.")
