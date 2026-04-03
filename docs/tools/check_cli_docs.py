# ruff: noqa: T201
# flake8: noqa: T201
"""Standalone tool for generating and checking CLI reference docs.

Usage:
    uv run python tools/check_cli_docs.py <file_name> [--commands CMD ...] [--compare]
"""

import argparse
import difflib
import os
import re
import sys
import textwrap
from typing import List, Optional

HEADER = """---
title: Command Line Interface
description: Command line interface (CLI) full reference of dlt
keywords: [command line interface, cli, dlt init]
---


# Command Line Interface Reference

<!-- this page is fully generated from the argparse object of dlt, run make update-cli-docs to update it -->

This page contains all commands available in the dlt CLI and is generated
automatically from the fully populated python argparse object of dlt.
:::note
Flags and positional commands are inherited from the parent command. Position within the command string
is important. For example if you want to enable debug mode on the pipeline command, you need to add the
debug flag to the base dlt command:

```sh
dlt --debug pipeline
```

Adding the flag after the pipeline keyword will not work.
:::

"""

# Developer NOTE: This generation is based on parsing the output of the help string in argparse.
# It works very well at the moment, but there may be cases where it will break due to unanticipated
# argparse help output. The cleaner solution would be to implement an argparse help formatter,
# but this would require more work and also some not very nice parsing.


class _WidthFormatter(argparse.RawTextHelpFormatter):
    def __init__(self, prog: str) -> None:
        super().__init__(prog, width=99999, max_help_position=99999)


def _set_prog_recursive(
    parser: argparse.ArgumentParser, old_prog: str, new_prog: str
) -> None:
    """Propagates prog to a parser and all nested subparsers."""
    parser.prog = parser.prog.replace(old_prog, new_prog, 1)
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            for subparser in action._name_parser_map.values():
                _set_prog_recursive(subparser, old_prog, new_prog)


def render_argparse_markdown(
    name: str,
    parser: argparse.ArgumentParser,
    /,
    *,
    header: str = HEADER,
    commands: Optional[List[str]] = None,
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
        usage = _text_wrap_line(usage)

        # get description or use help passed down from choices
        help_string = help_string or ""
        help_string = help_string.strip().strip(" .") + "."
        description = parser.description or help_string or ""
        description = description.strip().strip(" .") + "."

        if nesting == 0:
            help_string = description
            description = None

        if not parser.description:
            print(
                f"WARNING: No description found for {cmd}, please consider providing one.",
                file=sys.stderr,
            )

        inherits_from = ""
        if parent:
            parent_slug = parent.lower().replace(" ", "-")
            inherits_from = f"Inherits arguments from [`{parent}`](#{parent_slug})."

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

            if header.lower() not in ["available subcommands", "positional arguments", "options"]:
                print(
                    f"WARNING: Skipping unknown section {header} of {cmd}.",
                    file=sys.stderr,
                )
                continue

            section_lines = section_lines[1:]
            section_lines = [line for line in section_lines if line]
            section_lines = [line for line in section_lines if not line.strip().startswith("{")]
            section = textwrap.dedent(os.linesep.join(section_lines))

            # NOTE: this is based on convention to name the subcommands section
            # "available subcommands"
            is_subcommands_list = "subcommands" in header

            # split section and remove lines starting with {
            section_lines = section.splitlines()
            # make markdown list of args
            section = ""
            for line in section_lines:
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
                    section += f"* {quoted_title} - {arg_help.capitalize()}\n"

            extracted_sections.append({"header": header.capitalize(), "section": section})

        heading = "##" if nesting < 2 else "###"
        markdown += f"{heading} `{cmd}`\n\n"
        if help_string:
            markdown += f"{help_string}\n\n"
        markdown += "**Usage**\n"
        markdown += "```sh\n"
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
                for subaction in action._get_subactions():
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
    parser = argparse.ArgumentParser(description="Generate or check CLI reference docs.")
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
    args = parser.parse_args()

    from dlt._workspace.cli._dlt import _create_parser

    cli_parser, _ = _create_parser()
    _set_prog_recursive(cli_parser, cli_parser.prog, args.executable_name)
    result = render_argparse_markdown(args.executable_name, cli_parser, commands=args.commands)

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
                    " update-cli-docs from the main Makefile and commit your changes.\n"
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
