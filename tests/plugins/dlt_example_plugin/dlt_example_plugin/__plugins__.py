import os
import argparse
from typing import Any, ClassVar, Dict, Optional, Type

from dlt.common.configuration import plugins
from dlt.common.configuration.specs.pluggable_run_context import RunContextBase
from dlt._workspace.cli import SupportsCliCommand
from dlt.common.runtime.run_context import RunContext, DOT_DLT

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
        from tests.utils import get_test_storage_root

        return os.path.abspath(get_test_storage_root())

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


class LegacyCommand(SupportsCliCommand):
    command: str = "legacy"
    help_string: str = "Legacy command registered without host parameter"
    docs_url: str = "LEGACY_DOCS_URL"

    def configure_parser(self, parser: argparse.ArgumentParser) -> None:
        pass

    def execute(self, args: argparse.Namespace) -> None:
        print("Legacy command executed")


@plugins.hookimpl(specname="plug_cli")
def plug_cli_example(host: str) -> Optional[Type[SupportsCliCommand]]:
    if host not in ("dlt", "dlthub"):
        return None
    return ExampleCommand


@plugins.hookimpl(specname="plug_cli", tryfirst=True)
def plug_cli_init_new(host: str) -> Optional[Type[SupportsCliCommand]]:
    # should be executed before dlt command got plugged in (tryfirst) to override it.
    # `init` is a dlt-only built-in, so the override is also dlt-only.
    if host != "dlt":
        return None
    return InitCommand


@plugins.hookimpl(specname="plug_cli")
def plug_cli_legacy() -> Type[SupportsCliCommand]:
    # legacy hookimpl without `host` parameter; pluggy keeps calling it because
    # it discovers args by introspection. Verifies backward compatibility.
    return LegacyCommand


class TInfoACommand(SupportsCliCommand):
    """Top-level extend, first plugin — defines parser args and executes."""

    command: str = "t_info"
    help_string: str = "Show info (combined from multiple plugins)"
    compose = "extend"

    def configure_parser(self, parser: argparse.ArgumentParser) -> None:
        # `--detailed` (not `--verbose`) so it doesn't clash with the global `-v/--verbose`
        # consumed by the top-level pre-parser before subparsers see argv.
        parser.add_argument("--detailed", action="store_true", help="Detailed output")

    def execute(self, args: argparse.Namespace) -> None:
        print(f"t_info A: detailed={args.detailed}")


class TInfoBCommand(SupportsCliCommand):
    """Top-level extend, second plugin — only its execute fires; configure_parser is skipped.

    Declares the same `--detailed` arg as `TInfoACommand`. If the dispatcher ever calls
    both plugins' `configure_parser` (violating the extend contract), argparse will
    raise `ArgumentError: conflicting option string --detailed` and the test will fail.
    """

    command: str = "t_info"
    help_string: str = "Show info (combined from multiple plugins)"
    compose = "extend"

    def configure_parser(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--detailed", action="store_true", help="Detailed output")

    def execute(self, args: argparse.Namespace) -> None:
        print(f"t_info B: detailed={args.detailed}")


class TWorkspaceCommand(SupportsCliCommand):
    """Top-level additive — base for sub-subcommand additions."""

    command: str = "t_workspace"
    help_string: str = "Test workspace (additive base for subcommand tests)"
    compose = "additive"

    def configure_parser(self, parser: argparse.ArgumentParser) -> None:
        sub = parser.add_subparsers(dest="t_workspace_op")
        info = sub.add_parser("info", help="Show test workspace info")
        info.set_defaults(func=lambda args: print("t_workspace info"))

    def execute(self, args: argparse.Namespace) -> None:
        # fallback when no sub-subcommand selected
        print("t_workspace (no subcommand)")


class TWorkspaceSwitchCommand(SupportsCliCommand):
    """Sub-subcommand — adds `switch` under `t_workspace` from a separate plugin."""

    parent: str = "t_workspace"
    command: str = "switch"
    help_string: str = "Switch workspaces (added from addon plugin)"

    def configure_parser(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--target", type=str, help="Target workspace")

    def execute(self, args: argparse.Namespace) -> None:
        print(f"t_workspace switch: target={args.target}")


@plugins.hookimpl(specname="plug_cli")
def plug_cli_t_info_a(host: str) -> Optional[Type[SupportsCliCommand]]:
    if host != "dlthub":
        return None
    return TInfoACommand


@plugins.hookimpl(specname="plug_cli")
def plug_cli_t_info_b(host: str) -> Optional[Type[SupportsCliCommand]]:
    if host != "dlthub":
        return None
    return TInfoBCommand


@plugins.hookimpl(specname="plug_cli")
def plug_cli_t_workspace(host: str) -> Optional[Type[SupportsCliCommand]]:
    if host != "dlthub":
        return None
    return TWorkspaceCommand


@plugins.hookimpl(specname="plug_cli")
def plug_cli_t_workspace_switch(host: str) -> Optional[Type[SupportsCliCommand]]:
    if host != "dlthub":
        return None
    return TWorkspaceSwitchCommand
