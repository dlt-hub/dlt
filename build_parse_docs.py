import textwrap
import argparse

from dlt.cli._dlt import _build_parser


HEADER = """---
title: Command line interface
description: Command line interface (CLI) of dlt
keywords: [command line interface, cli, dlt init]
---

# Full CLI Reference

This page contains all commands available in the dlt CLI and is generated automatically from the argparse object.

"""


def get_argparse_help_string(
    name: str, parser: argparse.ArgumentParser, wrap: int = 80, wrap_indent: int = 8
) -> str:
    """Generate a docstring for an argparse parser that shows the help for the parser and all subparsers, recursively.

    Based on an idea from <https://github.com/pdoc3/pdoc/issues/89>

    Arguments:
    * `name`: The name of the program
    * `parser`: The parser
    * `wrap`: The number of characters to wrap the help text to (0 to disable)
    * `wrap_indent`: The number of characters to indent the wrapped text
    """

    def get_parser_help_recursive(
        parser: argparse.ArgumentParser, cmd: str = "", root: bool = True, nesting: int = 0
    ):
        markdown = ""

        if not parser.description:
            raise ValueError(f"Parser at command '{cmd}' has no description. This means you have probably added a new parser without adding a description property.")
        
        heading = "##" if nesting < 2 else "###"
        markdown += f"{heading} `{cmd}`\n\n"
        markdown += f"{parser.description}\n\n"
        markdown += f"**Help command**\n"
        markdown += f"```sh\n"
        markdown += f"{cmd} --help\n"
        markdown += f"```\n\n"
        markdown += f"**Help output**\n"
        markdown += f"```sh\n"
        markdown += parser.format_help()
        markdown += f"```\n"

        markdown += "\n\n"
        # markdown += parser.format_usage()

        for action in parser._actions:
            if isinstance(action, argparse._SubParsersAction):
                for subcmd, subparser in action.choices.items():
                    markdown += get_parser_help_recursive(
                        subparser, f"{cmd} {subcmd}", root=False, nesting=nesting+1
                    )
        return markdown

    markdown = get_parser_help_recursive(parser, name)
    
    if wrap > 0:
        wrapped = []
        # From the textwrap docs:
        # > If replace_whitespace is false,
        # > newlines may appear in the middle of a line and cause strange output.
        # > For this reason, text should be split into paragraphs
        # > (using str.splitlines() or similar) which are wrapped separately.
        for line in markdown.splitlines():
            if line:
                wrapped += textwrap.wrap(
                    line,
                    width=wrap,
                    replace_whitespace=False,
                    subsequent_indent=" " * wrap_indent,
                )
            else:
                wrapped += [""]
        return "\n".join(wrapped)
    else:
        return markdown

if __name__ == "__main__":
    parser, _ = _build_parser()
    
    with open("docs/website/docs/reference/cli.md", "w") as f:
        f.write(HEADER + get_argparse_help_string("dlt", parser))
