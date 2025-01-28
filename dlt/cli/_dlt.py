from typing import Any, Sequence, Type, cast, List, Dict, Tuple
import argparse
import click
import rich_argparse
from rich.markdown import Markdown

from dlt.version import __version__
from dlt.common.runners import Venv
from dlt.cli import SupportsCliCommand

import dlt.cli.echo as fmt
from dlt.cli.exceptions import CliCommandException

from dlt.cli.command_wrappers import (
    deploy_command_wrapper,
    telemetry_change_status_command_wrapper,
)
from dlt.cli import debug


ACTION_EXECUTED = False
DEFAULT_DOCS_URL = "https://dlthub.com/docs/intro"


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


def _create_parser() -> Tuple[argparse.ArgumentParser, Dict[str, SupportsCliCommand]]:
    parser = argparse.ArgumentParser(
        description=(
            "Creates, adds, inspects and deploys dlt pipelines. Further help is available at"
            " https://dlthub.com/docs/reference/command-line-interface."
        ),
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
        "--debug",
        action=DebugAction,
        help=(
            "Displays full stack traces on exceptions. Useful for debugging if the output is not"
            " clear enough."
        ),
    )
    subparsers = parser.add_subparsers(title="Available subcommands", dest="command")

    # load plugins
    from dlt.common.configuration import plugins

    m = plugins.manager()
    commands = cast(List[Type[SupportsCliCommand]], m.hook.plug_cli())

    # install Available subcommands
    installed_commands: Dict[str, SupportsCliCommand] = {}
    for c in commands:
        command = c()
        if command.command in installed_commands.keys():
            continue
        command_parser = subparsers.add_parser(
            command.command,
            help=command.help_string,
            description=command.description if hasattr(command, "description") else None,
        )
        command.configure_parser(command_parser)
        installed_commands[command.command] = command

    # recursively add formatter class
    def add_formatter_class(parser: argparse.ArgumentParser) -> None:
        parser.formatter_class = rich_argparse.RichHelpFormatter

        # NOTE: make markup available for console output
        if parser.description:
            parser.description = Markdown(parser.description, style="argparse.text")  # type: ignore
        for action in parser._actions:
            if isinstance(action, argparse._SubParsersAction):
                for _subcmd, subparser in action.choices.items():
                    add_formatter_class(subparser)

    add_formatter_class(parser)

    return parser, installed_commands


def main() -> int:
    parser, installed_commands = _create_parser()
    args = parser.parse_args()

    if Venv.is_virtual_env() and not Venv.is_venv_activated():
        fmt.warning(
            "You are running dlt installed in the global environment, however you have virtual"
            " environment activated. The dlt command will not see dependencies from virtual"
            " environment. You should uninstall the dlt from global environment and install it in"
            " the current virtual environment instead."
        )

    if cmd := installed_commands.get(args.command):
        try:
            cmd.execute(args)
        except Exception as ex:
            docs_url = cmd.docs_url if hasattr(cmd, "docs_url") else DEFAULT_DOCS_URL
            error_code = -1
            raiseable_exception = ex

            # overwrite some values if this is a CliCommandException
            if isinstance(ex, CliCommandException):
                error_code = ex.error_code
                docs_url = ex.docs_url or docs_url
                raiseable_exception = ex.raiseable_exception

            # print exception if available
            if raiseable_exception:
                click.secho(str(ex), err=True, fg="red")

            fmt.note("Please refer to our docs at '%s' for further assistance." % docs_url)
            if debug.is_debug_enabled() and raiseable_exception:
                raise raiseable_exception

            return error_code
    else:
        print_help(parser)
        return -1

    return 0


def _main() -> None:
    """Script entry point"""
    exit(main())


if __name__ == "__main__":
    exit(main())
