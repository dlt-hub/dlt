import os
import argparse
from typing import Any, ClassVar, Dict, Optional, Type

from dlt.common.configuration import plugins
from dlt.common.configuration.specs.pluggable_run_context import RunContextBase
from dlt._workspace.cli import SupportsCliCommand
from dlt.common.runtime.run_context import RunContext, DOT_DLT

from tests.utils import TEST_STORAGE_ROOT
from dlt._workspace.cli.exceptions import CliCommandException


class RunContextTest(RunContext):
    @property
    def run_dir(self) -> str:
        # use the location of __init__ as run dir so we are running inside the Python module
        # and we can obtain it via `module` property of run_context
        return os.path.dirname(__file__)

    @property
    def settings_dir(self) -> str:
        return os.path.join(self.run_dir, DOT_DLT)

    @property
    def data_dir(self) -> str:
        return os.path.abspath(TEST_STORAGE_ROOT)

    @property
    def local_dir(self) -> str:
        return os.path.join(self.data_dir, "tmp")

    @property
    def runtime_kwargs(self) -> Dict[str, Any]:
        return {"profile": "dev"}

    @property
    def name(self) -> str:
        return "dlt-test"


@plugins.hookimpl(specname="plug_run_context", tryfirst=True)
def plug_run_context_impl(
    run_dir: Optional[str], runtime_kwargs: Optional[Dict[str, Any]]
) -> Optional[RunContextBase]:
    print("PLUG TEST")
    # test fallback to OSS
    if (runtime_kwargs or {}).get("passthrough"):
        return None
    return RunContextTest(run_dir)


class ExampleException(Exception):
    pass


class ExampleCommand(SupportsCliCommand):
    command: str = "example"
    help_string: str = "Example command"
    docs_url: str = "DEFAULT_DOCS_URL"

    def configure_parser(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--name", type=str, help="Name to print")
        parser.add_argument("--result", type=str, help="How to result")

    def execute(self, args: argparse.Namespace) -> None:
        print(f"Example command executed with name: {args.name}")

        # pass without return
        if args.result == "pass":
            pass
        if args.result == "known_error":
            raise CliCommandException(error_code=-33, docs_url="MODIFIED_DOCS_URL")
        if args.result == "unknown_error":
            raise ExampleException("No one knows what is going on")


class InitCommand(SupportsCliCommand):
    command: str = "init"
    help_string: str = "Init command"
    docs_url: str = "INIT_DOCS_URL"

    def configure_parser(self, parser: argparse.ArgumentParser) -> None:
        pass

    def execute(self, args: argparse.Namespace) -> None:
        print("Plugin overwrote init command")
        raise CliCommandException(error_code=-55)


@plugins.hookimpl(specname="plug_cli")
def plug_cli_example() -> Type[SupportsCliCommand]:
    return ExampleCommand


@plugins.hookimpl(specname="plug_cli", tryfirst=True)
def plug_cli_init_new() -> Type[SupportsCliCommand]:
    # should be executed before dlt command got plugged in (tryfirst) to override it
    return InitCommand
