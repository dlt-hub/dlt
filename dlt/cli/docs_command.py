"""Cli for rendering markdown docs"""

import argparse
import textwrap
import os
import re

import dlt.cli.echo as fmt

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


def render_argparse_markdown(
    name: str,
    parser: argparse.ArgumentParser,
    header: str = HEADER,
) -> str:
    def get_parser_help_recursive(
        parser: argparse.ArgumentParser,
        cmd: str = "",
        parent: str = "",
        nesting: int = 0,
        help_string: str = None,
    ) -> str:
        markdown = ""

        # Prevent wrapping in help output for better parseability
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
            fmt.warning(f"No description found for {cmd}, please consider providing one.")

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
                fmt.warning(f"Skipping unknown section {header} of {cmd}.")
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
                    fmt.warning(
                        f"Missing helpstring for argument '{arg_title}' in section '{header}' of"
                        f" command '{cmd}'."
                    )
                if is_subcommands_list:
                    full_command = f"{cmd} {arg_title}"
                    anchor_slug = full_command.lower().replace(" ", "-")
                    arg_title = f"[`{arg_title}`](#{anchor_slug})"
                else:
                    arg_title = f"`{arg_title}`"
                section += f"* {arg_title} - {arg_help.capitalize()}\n"

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
        for action in parser._actions:
            if isinstance(action, argparse._SubParsersAction):
                for subaction in action._get_subactions():
                    subparser = action._name_parser_map[subaction.dest]
                    assert (
                        subaction.help
                    ), f"Subparser help string of {subaction.dest} is empty, please provide one."
                    markdown += get_parser_help_recursive(
                        subparser,
                        f"{cmd} {subaction.dest}",
                        parent=cmd,
                        nesting=nesting + 1,
                        help_string=subaction.help,
                    )
        return markdown

    markdown = get_parser_help_recursive(parser, name)

    return header + markdown
