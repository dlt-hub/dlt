import sys
import warnings
from typing import TYPE_CHECKING, Any, Optional, Sequence, Type, cast, List, Dict, Tuple

if TYPE_CHECKING:
    from _typeshed import SupportsWrite
import argparse

from dlt.version import __version__
from dlt.common.runners import Venv

from dlt._workspace.cli import SupportsCliCommand, echo as fmt, _debug
from dlt._workspace.cli import compose as _compose
from dlt._workspace.cli.exceptions import CliCommandException
from dlt._workspace.cli._telemetry_command import (
    telemetry_change_status_command_wrapper,
)
from dlt._workspace.cli.echo import maybe_no_stdin

ACTION_EXECUTED = False
DEFAULT_DOCS_URL = "https://dlthub.com/docs/intro"


class _LazyMarkdown:
    """Renderable wrapper that defers `rich.markdown.Markdown` instantiation"""

    def __init__(self, text: str, **kwargs: Any) -> None:
        self._text = text
        self._kwargs = kwargs

    @property
    def markup(self) -> str:
        """Original markdown source; mirrors `rich.markdown.Markdown.markup`."""
        return self._text

    def __rich__(self) -> Any:
        from rich.markdown import Markdown

        return Markdown(self._text, **self._kwargs)

    def __str__(self) -> str:
        return self._text


def is_workspace_active() -> bool:
    import dlt

    ctx = dlt.current.run_context()
    return ctx.__class__.__name__ == "WorkspaceRunContext"


def print_help(host: str, parser: argparse.ArgumentParser) -> None:
    if not ACTION_EXECUTED:
        parser.print_help()


def _print_dlthub_workspace_hint(file: Any = None) -> None:
    """Print the 'commands not visible' note after `dlthub --help` outside a workspace."""
    if is_workspace_active():
        return
    fmt.echo(file=file)
    fmt.secho(
        "NOTE: Not all dlthub commands are visible. "
        "Run %s to initialize workspace or %s for coding agent assist."
        % (fmt.bold("dlthub init"), fmt.bold("dlthub ai init")),
        fg="green",
        file=file,
    )


class _DlthubArgumentParser(argparse.ArgumentParser):
    """ArgumentParser that appends the workspace-hint after `--help`. Only used for the `dlthub` host."""

    def print_help(self, file: "Optional[SupportsWrite[str]]" = None) -> None:
        super().print_help(file)
        _print_dlthub_workspace_hint(file=file)


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
        fmt.set_non_interactive(True)


class YesAction(argparse.Action):
    def __init__(
        self,
        option_strings: Sequence[str],
        dest: Any = argparse.SUPPRESS,
        default: Any = argparse.SUPPRESS,
        help: str = None,  # noqa
    ) -> None:
        super(YesAction, self).__init__(
            option_strings=option_strings, dest=dest, default=default, nargs=0, help=help
        )

    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: Any,
        option_string: str = None,
    ) -> None:
        fmt.set_auto_yes(True)


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
        _debug.enable_debug()


def _create_pre_parser() -> argparse.ArgumentParser:
    """Builds the pre-parser holding flags allowed at any argv position."""

    pre_parser = argparse.ArgumentParser(add_help=False)
    pre_parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        dest="verbosity",
        help="Increase verbosity. Repeat for more (-v, -vv, -vvv).",
    )
    pre_parser.add_argument(
        "--non-interactive",
        action=NonInteractiveAction,
        help="Use prompt defaults; fail if a prompt has none. Implied when stdin is not a tty.",
    )
    pre_parser.add_argument(
        "-y",
        "--yes",
        action=YesAction,
        help="Auto-accept confirmations. Free-form prompts still need defaults.",
    )
    pre_parser.add_argument(
        "--debug",
        action=DebugAction,
        help="Show full stack traces on exceptions.",
    )
    return pre_parser


def _create_parser(
    host: str = "dlt",
) -> Tuple[
    argparse.ArgumentParser, argparse.ArgumentParser, Dict[str, _compose.ComposedExecutable]
]:
    pre_parser = _create_pre_parser()
    parser_cls = _DlthubArgumentParser if host == "dlthub" else argparse.ArgumentParser
    parser = parser_cls(
        prog=host,
        parents=[pre_parser],
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
        "--no-pwd",
        default=False,
        action="store_true",
        help=(
            "Do not add current working directory to sys.path. By default $pwd is added to "
            "reproduce Python behavior when running scripts."
        ),
    )
    subparsers = parser.add_subparsers(title="Available subcommands", dest="command")

    from dlt.common.configuration import plugins

    m = plugins.manager()
    # load cli commands for `host`
    results = cast(List[Optional[Type[SupportsCliCommand]]], m.hook.plug_cli(host=host))
    top_groups, sub_groups = _compose.group_commands(results)

    installed_commands: Dict[str, _compose.ComposedExecutable] = {}

    # install top level commands
    for name, group in top_groups.items():
        command_parser = subparsers.add_parser(
            name,
            help=group[0].help_string,
            description=getattr(group[0], "description", None),
        )
        installed_commands[name] = _compose.configure_parser(command_parser, group)

    # attach sub commands to commands
    for (parent_name, sub_name), sub_group in sub_groups.items():
        if parent_name not in installed_commands:
            warnings.warn(
                f"sub-subcommand {sub_name!r} skipped: parent {parent_name!r} is not"
                f" registered for host {host!r}",
                stacklevel=2,
            )
            continue
        parent_node = installed_commands[parent_name]
        if parent_node.compose != "additive":
            raise CliCommandException(
                error_code=-1,
                raiseable_exception=ValueError(
                    f"cannot register sub-subcommand {sub_name!r} under {parent_name!r}:"
                    f" parent's `compose` is {parent_node.compose!r}, must be 'additive'."
                ),
            )
        # get parser from top level command and attach subcommand
        parent_subparsers = _compose.get_existing_subparsers_action(parent_node.parser)
        if parent_subparsers is None:
            raise CliCommandException(
                error_code=-1,
                raiseable_exception=ValueError(
                    f"cannot register sub-subcommand {sub_name!r}: {parent_name!r}"
                    ".configure_parser did not call add_subparsers despite"
                    " compose='additive'."
                ),
            )
        sub_parser = parent_subparsers.add_parser(
            sub_name,
            help=sub_group[0].help_string,
            description=getattr(sub_group[0], "description", None),
        )
        sub_node = _compose.configure_parser(sub_parser, sub_group)
        sub_parser.set_defaults(execute=sub_node.execute)

    # recursively add formatter class
    def add_formatter_class(parser: argparse.ArgumentParser) -> None:
        import rich_argparse

        parser.formatter_class = rich_argparse.RichHelpFormatter

        if parser.description and isinstance(parser.description, str):
            parser.description = _LazyMarkdown(parser.description, style="argparse.text")  # type: ignore[assignment]
        for action in parser._actions:
            if isinstance(action, argparse._SubParsersAction):
                for _subcmd, subparser in action.choices.items():
                    add_formatter_class(subparser)

    add_formatter_class(parser)

    return parser, pre_parser, installed_commands


def main(host: str = "dlt") -> int:
    fmt.set_cli_host_name(host)
    try:
        parser, pre_parser, installed_commands = _create_parser(host)
    except ValueError as ex:
        fmt.secho(str(ex), err=True, fg="red")
        fmt.note("Please refer to our docs at '%s' for further assistance." % DEFAULT_DOCS_URL)
        return -1
    # pre-pass extracts global flags at any argv position; main parse uses namespace=ns to keep them
    ns, remaining = pre_parser.parse_known_args(sys.argv[1:])
    try:
        args = parser.parse_args(remaining, namespace=ns)
    except SystemExit as ex:
        # argparse exits with code 2 on errors
        if ex.code == 2 and host == "dlthub":
            _print_dlthub_workspace_hint()
        raise

    if Venv.is_virtual_env() and not Venv.is_venv_activated():
        fmt.warning(
            "You are running dlt installed in the global environment, however you have virtual"
            " environment activated. The dlt command will not see dependencies from virtual"
            " environment. You should uninstall the dlt from global environment and install it in"
            " the current virtual environment instead."
        )

    if cmd := installed_commands.get(args.command):
        try:
            # switch to non-interactive if tty not connected
            with maybe_no_stdin():
                if not args.no_pwd:
                    if "" not in sys.path:
                        sys.path.insert(0, "")
                cmd.execute(args)
        except Exception as ex:
            docs_url = getattr(cmd, "docs_url", None) or DEFAULT_DOCS_URL
            error_code = -1
            raiseable_exception = ex

            if isinstance(ex, CliCommandException):
                error_code = ex.error_code
                docs_url = ex.docs_url or docs_url
                raiseable_exception = ex.raiseable_exception

            if raiseable_exception:
                fmt.secho(str(raiseable_exception) or str(ex), err=True, fg="red")

            fmt.note("Please refer to our docs at '%s' for further assistance." % docs_url)
            if _debug.is_debug_enabled() and raiseable_exception:
                raise raiseable_exception

            return error_code
    else:
        print_help(host, parser)
        return -1

    return 0


def _main() -> None:
    """Entry point for the `dlt` console script."""
    # when workspace is active, dlt commands mirrors dlthub
    if is_workspace_active():
        host = "dlthub"
        fmt.note(
            "Please use %s as top level command. Check `%s` for former dlt commands. "
            "Falling back to dlthub command set."
            % (fmt.bold("dlthub"), fmt.bold("dlthub local --help"))
        )
        fmt.echo()
    else:
        host = "dlt"
    exit(main(host))


def _main_dlthub() -> None:
    """Entry point for the `dlthub` console script."""
    exit(main("dlthub"))


if __name__ == "__main__":
    exit(main("dlt"))
