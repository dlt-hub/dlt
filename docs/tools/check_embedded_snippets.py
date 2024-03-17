"""
Walks through all markdown files, finds all code snippets, and checks wether they are parseable.
"""
from typing import List, Dict, Optional

import os, ast, json, yaml, tomlkit, subprocess, argparse  # noqa: I251
from dataclasses import dataclass
from textwrap import dedent

import dlt.cli.echo as fmt

DOCS_DIR = "../website/docs"

SNIPPET_MARKER = "```"
ALLOWED_LANGUAGES = ["py", "toml", "json", "yaml", "text", "sh", "bat", "sql"]

LINT_TEMPLATE = "./lint_setup/template.py"
LINT_FILE = "./lint_setup/lint_me.py"

ENABLE_MYPY = False


@dataclass
class Snippet:
    index: int
    language: str
    code: str
    file: str
    line: int

    def __str__(self) -> str:
        return (
            f"Snippet No. {self.index} in {self.file} at line {self.line} with language"
            f" {self.language}"
        )


def collect_markdown_files() -> List[str]:
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

    if len(markdown_files) < 50:  # sanity check
        fmt.error("Found too few files. Something went wrong.")
        exit(1)

    fmt.note(f"Discovered {len(markdown_files)} markdown files")

    return markdown_files


def collect_snippets(markdown_files: List[str]) -> List[Snippet]:
    """
    Extract all snippets from markdown files
    """
    snippets: List[Snippet] = []
    index = 0
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
                        current_snippet.code = dedent(current_snippet.code)
                        current_snippet = None
                    else:
                        # start new snippet
                        index += 1
                        current_snippet = Snippet(
                            index=index,
                            language=line.strip().split(SNIPPET_MARKER)[1] or "unknown",
                            code="",
                            file=file,
                            line=lint_count,
                        )
                elif current_snippet:
                    current_snippet.code += line
        assert not current_snippet, (
            "It seems that the last snippet in the file was not closed. Please check the file "
            + file
        )

    fmt.note(f"Discovered {len(snippets)} snippets")
    if len(snippets) < 100:  # sanity check
        fmt.error("Found too few snippets. Something went wrong.")
        exit(1)
    return snippets


def filter_snippets(snippets: List[Snippet], files: str, snippet_numbers: str) -> List[Snippet]:
    """
    Filter out snippets based on file or snippet number
    """
    fmt.echo("Filtering Snippets")
    filtered_snippets: List[Snippet] = []
    filtered_count = 0
    for snippet in snippets:
        if files and (files not in snippet.file):
            filtered_count += 1
            continue
        elif snippet_numbers and (str(snippet.index) not in snippet_numbers):
            filtered_count += 1
            continue
        filtered_snippets.append(snippet)
    if filtered_count:
        fmt.note(
            f"{filtered_count} Snippets skipped based on file and snippet number settings."
            f" {len(filtered_snippets)} snippets remaining."
        )
    else:
        fmt.note("0 Snippets skipped based on file and snippet number settings")

    if len(filtered_snippets) == 0:  # sanity check
        fmt.error("Filtered out all snippets, nothing to check.")
        exit(1)
    return filtered_snippets


def check_language(snippets: List[Snippet]) -> None:
    """
    Check if the language is allowed
    """
    fmt.echo("Checking snippets language")
    failed_count = 0
    for snippet in snippets:
        if snippet.language not in ALLOWED_LANGUAGES:
            fmt.warning(f"{str(snippet)} has an invalid language {snippet.language} setting.")
            failed_count += 1

    if failed_count:
        fmt.error(f"""\
Found {failed_count} snippets with invalid language settings.
* Please choose the correct language for your snippets: {ALLOWED_LANGUAGES}"
* All sh commands, except for windows (bat), should be marked as sh.
* All code blocks that are not a specific (markup-) language should be marked as text.\
""")
        exit(1)
    else:
        fmt.note("All snippets have valid language settings")


def parse_snippets(snippets: List[Snippet]) -> None:
    """
    Parse all snippets with the respective parser library
    """
    fmt.echo("Parsing snippets")
    failed_count = 0
    for snippet in snippets:
        # parse snippet by type
        try:
            if snippet.language == "py":
                ast.parse(snippet.code)
            elif snippet.language == "toml":
                tomlkit.loads(snippet.code)
            elif snippet.language == "json":
                json.loads(snippet.code)
            elif snippet.language == "yaml":
                yaml.safe_load(snippet.code)
            # ignore text and sh scripts
            elif snippet.language in ["text", "sh", "bat", "sql"]:
                pass
            else:
                raise ValueError(f"Unknown language {snippet.language}")
        except Exception:
            failed_count += 1

    if failed_count:
        fmt.error(f"Failed to parse {failed_count} snippets")
        exit(1)
    else:
        fmt.note("All snippets could be parsed")


def prepare_for_linting(snippet: Snippet) -> None:
    """
    Prepare the lintme file with the snippet code and the template header
    """
    with open(LINT_TEMPLATE, "r", encoding="utf-8") as f:
        lint_template = f.read()
    with open(LINT_FILE, "w", encoding="utf-8") as f:
        f.write(lint_template)
        f.write("# Snippet start\n\n")
        f.write(snippet.code)


def lint_snippets(snippets: List[Snippet]) -> None:
    """
    Lint all python snippets with ruff
    """
    fmt.echo("Linting python snippets")
    failed_count = 0
    for snippet in snippets:
        if snippet.language != "py":
            continue
        prepare_for_linting(snippet)
        result = subprocess.run(["ruff", "check", LINT_FILE], capture_output=True, text=True)
        if "error" in result.stdout.lower():
            failed_count += 1
            fmt.warning(f"Failed to lint {str(snippet)}")
            fmt.echo(result.stdout.strip())

    if failed_count:
        fmt.error(f"Failed to lint {failed_count} snippets")
        exit(1)
    else:
        fmt.note("All snippets could be linted")


def typecheck_snippets(snippets: List[Snippet]) -> None:
    """
    TODO: Type check all python snippets with mypy
    """
    fmt.echo("Type checking python snippets")
    failed_count = 0
    for snippet in snippets:
        if snippet.language != "py":
            continue
        prepare_for_linting(snippet)
        result = subprocess.run(["mypy", LINT_FILE], capture_output=True, text=True)
        if "no issues found" not in result.stdout.lower():
            failed_count += 1
            fmt.warning(f"Failed to type check {str(snippet)}")
            fmt.echo(result.stdout.strip())

    if failed_count:
        fmt.error(f"Failed to type check {failed_count} snippets")
        exit(1)
    else:
        fmt.note("All snippets passed type checking")


if __name__ == "__main__":
    fmt.note(
        "Welcome to Snippet Checker 3000, run 'python check_embedded_snippets.py --help' for help."
    )

    # setup cli
    parser = argparse.ArgumentParser(
        description=(
            "Check embedded snippets. Discover, parse, lint, and type check all code snippets in"
            " the docs."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "command",
        help=(
            'Which checks to run. "full" will run all checks, parse, lint or typecheck will only'
            " run that specific step"
        ),
        choices=["full", "parse", "lint", "typecheck"],
        default="full",
    )
    parser.add_argument("-v", "--verbose", help="Increase output verbosity", action="store_true")
    parser.add_argument(
        "-f",
        "--files",
        help="Filter .md files to files containing this string in filename",
        type=str,
    )
    parser.add_argument(
        "-sn",
        "--snippet-number",
        help=(
            "Filter checked snippets to snippetnumbers contained in this string, example:"
            ' "13,412,345"'
        ),
        type=lambda i: i.split(","),
        default=None,
    )

    args = parser.parse_args()

    fmt.echo("Discovering snippets")

    # find all markdown files and collect all snippets
    markdown_files = collect_markdown_files()
    snippets = collect_snippets(markdown_files)

    # check language settings
    check_language(snippets)

    # filter snippets
    filtered_snippets = filter_snippets(snippets, args.files, args.snippet_number)

    if args.command in ["parse", "full"]:
        parse_snippets(filtered_snippets)
    if args.command in ["lint", "full"]:
        lint_snippets(filtered_snippets)
    if ENABLE_MYPY and args.command in ["typecheck", "full"]:
        typecheck_snippets(filtered_snippets)
