"""Cli for rendering markdown docs"""

import argparse
import textwrap
import os
import re

HEADER = """---
title: Full CLI Reference
description: Command line interface (CLI) of dlt
keywords: [command line interface, cli, dlt init]
---
# Full CLI Reference

This page contains all commands available in the dlt CLI if dlt+ is installed and is generated
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
        super().__init__(prog, width=99999)


def render_argparse_markdown(
    name: str,
    parser: argparse.ArgumentParser,
    header: str = HEADER,
) -> str:
    def get_parser_help_recursive(
        parser: argparse.ArgumentParser,
        cmd: str = "",
        root: bool = True,
        nesting: int = 0,
        help_string: str = None,
    ) -> str:
        markdown = ""

        # Prevent wrapping in help output for better parseability
        parser.formatter_class = _WidthFormatter
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
        description = parser.description or help_string or ""

        # extract all other sections
        # here we remove excess information and style the args nicely
        # into markdown lists
        extracted_sections = []
        for section in sections[1:]:
            section_lines = section.splitlines()

            # detect full sections
            if not section_lines[0].endswith(":"):
                continue

            # remove first line with header and empty lines
            header = section_lines[0].replace(":", "")
            section_lines = section_lines[1:]
            section_lines = [line for line in section_lines if line]
            section = textwrap.dedent(os.linesep.join(section_lines))

            # split args into array and remove more unneeded lines
            section_lines = re.split(r"\s{2,}|\n+", section)
            section_lines = [line for line in section_lines if not line.startswith("{")]
            assert len(section_lines) % 2 == 0, (
                f"Expected even number of lines, args and descriptions in section {header} of"
                f" {cmd}. Possible problems are a different help formatter or arguments that are"
                " missing help strings."
            )

            # make markdown list of args
            section = ""
            for x in range(0, len(section_lines), 2):
                section += f"* `{section_lines[x]}` - {section_lines[x+1].capitalize()}\n"

            extracted_sections.append({"header": header.capitalize(), "section": section})

        heading = "##" if nesting < 2 else "###"
        markdown += f"{heading} `{cmd}`\n\n"
        if description:
            markdown += f"{description}\n\n"
        markdown += "**Usage**\n"
        markdown += "```sh\n"
        markdown += f"{usage}\n"
        markdown += "```\n\n"

        for es in extracted_sections:
            markdown += f"**{es['header']}**\n"
            markdown += f"{es['section']}\n"

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
                        root=False,
                        nesting=nesting + 1,
                        help_string=subaction.help,
                    )
        return markdown

    markdown = get_parser_help_recursive(parser, name)

    return header + markdown
