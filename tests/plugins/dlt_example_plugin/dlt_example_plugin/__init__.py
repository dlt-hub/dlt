import os
import argparse

from typing import ClassVar, Type

from dlt.common.configuration import plugins
from dlt.common.configuration.specs.pluggable_run_context import SupportsRunContext
from dlt.cli import SupportsCliCommand
from dlt.common.runtime.run_context import RunContext, DOT_DLT

from tests.utils import TEST_STORAGE_ROOT


class RunContextTest(RunContext):
    CONTEXT_NAME: ClassVar[str] = "dlt-test"

    @property
    def run_dir(self) -> str:
        return os.path.abspath("tests")

    @property
    def settings_dir(self) -> str:
        return os.path.join(self.run_dir, DOT_DLT)

    @property
    def data_dir(self) -> str:
        return os.path.abspath(TEST_STORAGE_ROOT)


@plugins.hookimpl(specname="plug_run_context")
def plug_run_context_impl() -> SupportsRunContext:
    return RunContextTest()


class ExampleCommand(SupportsCliCommand):
    command: str = "example"
    help_string: str = "Example command"

    def configure_parser(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--name", type=str, help="Name to print")

    def execute(self, args: argparse.Namespace) -> int:
        print(f"Example command executed with name: {args.name}")
        return 33


class InitCommand(SupportsCliCommand):
    command: str = "init"
    help_string: str = "Init command"

    def configure_parser(self, parser: argparse.ArgumentParser) -> None:
        pass

    def execute(self, args: argparse.Namespace) -> int:
        print("Plugin overwrote init command")
        return 55


@plugins.hookimpl(specname="plug_cli")
def plug_cli_example() -> Type[SupportsCliCommand]:
    return ExampleCommand


@plugins.hookimpl(specname="plug_cli")
def plug_cli_init_new() -> Type[SupportsCliCommand]:
    return InitCommand
