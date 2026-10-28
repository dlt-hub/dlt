# ruff: noqa: T201
# flake8: noqa: T201
# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "dlt[cli]",
#   "duckdb>=0.9",
#   "ibis-framework>=12.0.0",
#   "pyarrow>=16.0.0",
#   "marimo>=0.14.5",
#   "fastmcp>=3.0.0",
#   "mowidgets>=0.2.1 ; python_version >= '3.11'",
#   "pathspec>=0.11.2",
#   "pydbml>=1.2.0",
#   "croniter>=6.0.0",
#   "s3fs>=2022.4.0",
# ]
#
# [tool.uv.sources]
# dlt = { path = "../..", editable = true }
# ///
"""Standalone tool for generating and checking CLI reference docs.

Usage:
    uv run --script tools/generate_cli_docs.py <file_name> [--commands CMD ...] [--compare]
"""

import argparse
import difflib
import os
import re
import sys
import textwrap
from importlib.metadata import version
from typing import List, Optional

from mdx_utils import escape_mdx
from reference_constants import (
    CLI_DEFAULT_TITLE,
    CLI_HEADER,
    CLI_HEADER_TEMPLATE,
    CLI_INHERITS_TEMPLATE,
    CLI_VERSIONS_TEMPLATE,
)

# Developer NOTE: This generation is based on parsing the output of the help string in argparse.
# It works very well at the moment, but there may be cases where it will break due to unanticipated
# argparse help output. The cleaner solution would be to implement an argparse help formatter,
# but this would require more work and also some not very nice parsing.


class _WidthFormatter(argparse.RawTextHelpFormatter):
    def __init__(self, prog: str) -> None:
        super().__init__(prog, width=99999, max_help_position=99999)


def render_argparse_markdown(
    name: str,
    parser: argparse.ArgumentParser,
    /,
    *,
    header: str = CLI_HEADER,
    commands: Optional[List[str]] = None,
    sort_commands: bool = False,
) -> str:
    def get_parser_help_recursive(
        parser: argparse.ArgumentParser,
        cmd: str = "",
        parent: str = "",
        nesting: int = 0,
        help_string: Optional[str] = None,
        commands: Optional[List[str]] = None,
    ) -> str:
        markdown = ""

        # prevent wrapping in help output for better parseability
        parser.formatter_class = _WidthFormatter
        if parser.description:
            # remove markdown from description
            parser.description = parser.description.markup  # type: ignore
        help_output = parser.format_help()
        sections = help_output.split("\n\n")

        def _text_wrap_line(line: str, indent: int = 4) -> str:
            return "\n".join(
                textwrap.wrap(
                    line,
                    width=80,
                    break_on_hyphens=False,
                    subsequent_indent=" " * indent,
                    break_long_words=False,
                )
            )

        # extract usage section denoted by "usage: "
        usage = sections[0]
        usage = usage.replace("usage: ", "")
        if sort_commands:
            usage = re.sub(
                r"{([\w,-]+)}",
                lambda m: "{" + ",".join(sorted(m[1].split(","))) + "}",
                usage,
            )
        usage = _text_wrap_line(usage)

        # get description or use help passed down from choices
        help_string = help_string or ""
        help_string = help_string.strip().strip(" .") + "."
        description = parser.description or help_string or ""
        description = description.strip().strip(" .") + "."

        if nesting == 0:
            help_string = description
            description = None

        # MDX parses `<word>` as a JSX tag; escape angle brackets in prose.
        # Usage (inside ```cli) and backtick-wrapped tokens are left untouched.
        help_string = escape_mdx(help_string) if help_string else help_string
        description = escape_mdx(description) if description else description

        if not parser.description:
            print(
                f"WARNING: No description found for {cmd}, please consider providing one.",
                file=sys.stderr,
            )

        inherits_from = ""
        if parent:
            parent_slug = parent.lower().replace(" ", "-")
            inherits_from = CLI_INHERITS_TEMPLATE.format(
                parent=parent, anchor=parent_slug
            )

        # extract all other sections
        # here we remove excess information and style the args nicely
        # into markdown lists
        extracted_sections = []
        for section in sections[1:]:
            section_lines = section.splitlines()

            # detect full sections
            if not section_lines[0].endswith(":"):
                continue

            if len(section_lines[0]) > 30:
                continue

            # remove first line with header and empty lines
            header = section_lines[0].replace(":", "")

            if header.lower() not in [
                "available subcommands",
                "positional arguments",
                "options",
            ]:
                print(
                    f"WARNING: Skipping unknown section {header} of {cmd}.",
                    file=sys.stderr,
                )
                continue

            section_lines = section_lines[1:]
            section_lines = [line for line in section_lines if line]
            section_lines = [
                line for line in section_lines if not line.strip().startswith("{")
            ]
            section = textwrap.dedent(os.linesep.join(section_lines))

            # NOTE: this is based on convention to name the subcommands section
            # "available subcommands"
            is_subcommands_list = "subcommands" in header

            # an indented line continues the help of the entry above it
            entries: List[str] = []
            for line in section.splitlines():
                if entries and line[:1].isspace():
                    entries[-1] += " " + line.strip()
                else:
                    entries.append(line)
            if is_subcommands_list and sort_commands:
                entries.sort()
            # make markdown list of args
            section = ""
            for line in entries:
                line = line.strip()
                line_elements = re.split(r"\s{2,}|\n+", line)
                arg_title = line_elements[0]
                arg_help = line_elements[1] if len(line_elements) > 1 else ""
                if not arg_help:
                    print(
                        f"WARNING: Missing helpstring for argument '{arg_title}' in section"
                        f" '{header}' of command '{cmd}'.",
                        file=sys.stderr,
                    )

                quoted_title = ""
                if is_subcommands_list:
                    # skip unwanted commands
                    if not commands or arg_title in commands:
                        full_command = f"{cmd} {arg_title}"
                        anchor_slug = full_command.lower().replace(" ", "-")
                        quoted_title = f"[`{arg_title}`](#{anchor_slug})"
                else:
                    quoted_title = f"`{arg_title}`"
                if quoted_title:
                    section += (
                        f"* {quoted_title} - {escape_mdx(arg_help.capitalize())}\n"
                    )

            extracted_sections.append(
                {"header": header.capitalize(), "section": section}
            )

        heading = "##" if nesting < 2 else "###"
        markdown += f"{heading} `{cmd}`\n\n"
        if help_string:
            markdown += f"{help_string}\n\n"
        markdown += "**Usage**\n"
        # `cli` is highlighted by the grammar in website/src/theme/prism-include-languages.js
        markdown += "```cli\n"
        markdown += f"{usage}\n"
        markdown += "```\n\n"

        if description:
            markdown += "**Description**\n\n"
            markdown += f"{description}\n\n"

        markdown += "<details>\n\n"
        markdown += "<summary>Show Arguments and Options</summary>\n\n"

        if inherits_from:
            markdown += f"{inherits_from}\n\n"

        for es in extracted_sections:
            markdown += f"**{es['header']}**\n"
            markdown += f"{es['section']}\n"

        markdown += "</details>\n\n"

        # traverse the subparsers and forward help strings to the recursive function
        commands_found = []
        for action in parser._actions:
            if isinstance(action, argparse._SubParsersAction):
                subactions = action._get_subactions()
                if sort_commands:
                    subactions = sorted(subactions, key=lambda a: a.dest)
                for subaction in subactions:
                    subparser = action._name_parser_map[subaction.dest]
                    # skip unwanted commands
                    if commands and subaction.dest not in commands:
                        continue
                    commands_found.append(subaction.dest)
                    assert (
                        subaction.help
                    ), f"Subparser help string of {subaction.dest} is empty, please provide one."
                    markdown += get_parser_help_recursive(
                        subparser,
                        f"{cmd} {subaction.dest}",
                        parent=cmd,
                        nesting=nesting + 1,
                        help_string=subaction.help,
                        commands=None,
                    )
        if commands and set(commands_found) != set(commands):
            raise RuntimeError(
                f"Following commands were expected:: {commands} but found only {commands_found}."
                " Typically this means that dlthub was not installed or workspace context was "
                " not found."
            )

        return markdown

    markdown = get_parser_help_recursive(parser, name, commands=commands)

    return header + markdown


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate or check CLI reference docs."
    )
    parser.add_argument("file_name", help="Output file name")
    parser.add_argument(
        "--commands",
        nargs="*",
        help="List of command names to render (optional)",
        default=None,
    )
    parser.add_argument(
        "--compare",
        default=False,
        action="store_true",
        help="Compare and raise if output would be updated",
    )
    parser.add_argument(
        "--executable-name",
        default="dlt",
        help="Name of the executable shown in generated docs (default: dlt)",
    )
    parser.add_argument(
        "--title",
        default=CLI_DEFAULT_TITLE,
        help=f"Page title of the generated docs (default: {CLI_DEFAULT_TITLE})",
    )
    parser.add_argument(
        "--sort-commands",
        default=False,
        action="store_true",
        help="Sort subcommands by name instead of registration order, which depends on plugins",
    )
    parser.add_argument(
        "--show-versions",
        nargs="+",
        default=None,
        metavar="DISTRIBUTION",
        help="State the installed versions of these packages on the page",
    )
    args = parser.parse_args()

    header = CLI_HEADER_TEMPLATE.format(title=args.title)
    if args.show_versions:
        versions = " and ".join(
            f"`{name}` {version(name)}" for name in args.show_versions
        )
        header += CLI_VERSIONS_TEMPLATE.format(versions=versions)

    from dlt._workspace.cli._dlt import _create_parser

    cli_parser, _, _ = _create_parser(host=args.executable_name)
    result = render_argparse_markdown(
        args.executable_name,
        cli_parser,
        header=header,
        commands=args.commands,
        sort_commands=args.sort_commands,
    )

    if args.compare:
        with open(args.file_name, "r", encoding="utf-8") as f:
            existing = f.read()
            if result != existing:
                diff = difflib.unified_diff(
                    existing.splitlines(keepends=True),
                    result.splitlines(keepends=True),
                    fromfile=args.file_name,
                    tofile=args.file_name + " (generated)",
                )
                print(
                    "ERROR: CLI docs out of date, please run"
                    " `make cli-generate` in docs/ and commit your changes.\n"
                    "Diff:\n" + "".join(diff),
                    file=sys.stderr,
                )
                sys.exit(1)

        print("Docs page up to date.")
    else:
        with open(args.file_name, "w", encoding="utf-8") as f:
            f.write(result)
        print("Docs page updated.")


if __name__ == "__main__":
    main()
