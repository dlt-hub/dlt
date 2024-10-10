from typing import Protocol

import argparse


class SupportsCliCommand(Protocol):
    """Protocol for defining one dlt cli command"""

    command: str
    help_string: str

    def configure_parser(self, parser: argparse.ArgumentParser) -> None:
        """Configures the parser for the given argument"""
        ...

    def execute(self, args: argparse.Namespace) -> int:
        """Executes the command with the given arguments"""
        ...
