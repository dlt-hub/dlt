"""Cli for rendering markdown docs"""

import argparse
import textwrap
import os

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

```bash
dlt --debug pipeline
```

Adding the flag after the pipeline keyword will not work.
:::

"""


class _WidthFormatter(argparse.RawTextHelpFormatter):
    def __init__(self, prog: str) -> None:
        super().__init__(prog, width=99999)


def render_argparse_markdown(
    name: str,
    parser: argparse.ArgumentParser,
    wrap: int = 80,
    wrap_indent: int = 8,
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

        # no linebreaking in help output
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

        # extract usage section
        usage = sections[0]
        usage = usage.replace("usage: ", "")
        usage = _text_wrap_line(usage)

        # get description or use help passed down from choices
        description = parser.description or help_string or ""

        # extract all other sections
        extracted_sections = []
        for section in sections[1:]:
            section_lines = section.splitlines()

            # detect full sections
            if not section_lines[0].endswith(":"):
                continue

            # remove first line
            header = section_lines[0].replace(":", "")
            section_lines = section_lines[1:]

            # remove empty lines
            section_lines = [line for line in section_lines if line]

            # dedent lines
            section_lines = textwrap.dedent(os.linesep.join(section_lines)).splitlines()

            # wrap
            section_lines = [_text_wrap_line(line, indent=20) for line in section_lines]

            # join lines
            section = os.linesep.join(section_lines)

            # dedent
            section = textwrap.dedent(section)

            extracted_sections.append({"header": header.capitalize(), "section": section})

        heading = "##" if nesting < 2 else "###"
        markdown += f"{heading} `{cmd}`\n\n"
        if description:
            markdown += f"{description}\n\n"
        markdown += "**Usage**\n"
        markdown += "```bash\n"
        markdown += f"{usage}\n"
        markdown += "```\n\n"

        for es in extracted_sections:
            markdown += f"**{es['header']}**\n"
            markdown += "```sh\n"
            markdown += f"{es['section']}\n"
            markdown += "```\n\n"

        for action in parser._actions:
            if isinstance(action, argparse._SubParsersAction):
                for subaction in action._get_subactions():
                    subparser = action._name_parser_map[subaction.dest]
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
