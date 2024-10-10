from typing import Any, Sequence, Type, cast, List, Dict
import argparse

from dlt.version import __version__
from dlt.common.runners import Venv
from dlt.cli import SupportsCliCommand

import dlt.cli.echo as fmt

from dlt.cli.command_wrappers import (
    deploy_command_wrapper,
    telemetry_change_status_command_wrapper,
)
from dlt.cli import debug


ACTION_EXECUTED = False


def print_help(parser: argparse.ArgumentParser) -> None:
    if not ACTION_EXECUTED:
        parser.print_help()


class TelemetryAction(argparse.Action):
    def __init__(
        self,
        option_strings: Sequence[str],
        dest: Any = argparse.SUPPRESS,
        default: Any = argparse.SUPPRESS,
        help: str = None,  # noqa
    ) -> None:
        super(TelemetryAction, self).__init__(
            option_strings=option_strings, dest=dest, default=default, nargs=0, help=help
        )

    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: Any,
        option_string: str = None,
    ) -> None:
        global ACTION_EXECUTED

        ACTION_EXECUTED = True
        telemetry_change_status_command_wrapper(option_string == "--enable-telemetry")


class NonInteractiveAction(argparse.Action):
    def __init__(
        self,
        option_strings: Sequence[str],
        dest: Any = argparse.SUPPRESS,
        default: Any = argparse.SUPPRESS,
        help: str = None,  # noqa
    ) -> None:
        super(NonInteractiveAction, self).__init__(
            option_strings=option_strings, dest=dest, default=default, nargs=0, help=help
        )

    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: Any,
        option_string: str = None,
    ) -> None:
        fmt.ALWAYS_CHOOSE_DEFAULT = True
        fmt.note(
            "Non interactive mode. Default choices are automatically made for confirmations and"
            " prompts."
        )


class DebugAction(argparse.Action):
    def __init__(
        self,
        option_strings: Sequence[str],
        dest: Any = argparse.SUPPRESS,
        default: Any = argparse.SUPPRESS,
        help: str = None,  # noqa
    ) -> None:
        super(DebugAction, self).__init__(
            option_strings=option_strings, dest=dest, default=default, nargs=0, help=help
        )

    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: Any,
        option_string: str = None,
    ) -> None:
        # will show stack traces (and maybe more debug things)
        debug.enable_debug()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Creates, adds, inspects and deploys dlt pipelines.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--version", action="version", version="%(prog)s {version}".format(version=__version__)
    )
    parser.add_argument(
        "--disable-telemetry",
        action=TelemetryAction,
        help="Disables telemetry before command is executed",
    )
    parser.add_argument(
        "--enable-telemetry",
        action=TelemetryAction,
        help="Enables telemetry before command is executed",
    )
    parser.add_argument(
        "--non-interactive",
        action=NonInteractiveAction,
        help=(
            "Non interactive mode. Default choices are automatically made for confirmations and"
            " prompts."
        ),
    )
    parser.add_argument(
        "--debug", action=DebugAction, help="Displays full stack traces on exceptions."
    )
    subparsers = parser.add_subparsers(dest="command")

    # load plugins
    from dlt.common.configuration import plugins

    m = plugins.manager()
    commands = cast(List[Type[SupportsCliCommand]], m.hook.plug_cli())
    # NOTE: plugin commands are added in reverse order so that the last added command (coming from external plugin)
    # overwrites internal commands
    commands.reverse()

    # install available commands
    installed_commands: Dict[str, SupportsCliCommand] = {}
    for c in commands:
        command = c()
        command_parser = subparsers.add_parser(command.command, help=command.help_string)
        command.configure_parser(command_parser)
        installed_commands[command.command] = command

    args = parser.parse_args()

    if Venv.is_virtual_env() and not Venv.is_venv_activated():
        fmt.warning(
            "You are running dlt installed in the global environment, however you have virtual"
            " environment activated. The dlt command will not see dependencies from virtual"
            " environment. You should uninstall the dlt from global environment and install it in"
            " the current virtual environment instead."
        )

    if args.command in installed_commands:
        return installed_commands[args.command].execute(args)
    else:
        print_help(parser)
        return -1


def _main() -> None:
    """Script entry point"""
    exit(main())


if __name__ == "__main__":
    exit(main())
