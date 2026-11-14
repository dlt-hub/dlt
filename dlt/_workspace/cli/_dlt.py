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
from dlt._workspace.environment import ENVIRONMENT_ARGUMENT_HELP
from dlt._workspace.exceptions import WorkspaceException
from dlt._workspace.profile import PROFILE_ARGUMENT_HELP, activate_profile

PROFILE_COMMAND = "profile"
"""`dlthub profile` and `dlthub local profile` manage profiles and take no `--profile`."""

ENVIRONMENT_DEST = "environment"
"""`dest` of the `--environment` option: a command that has it takes no `--profile`."""

ACTION_EXECUTED = False


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

    # in a workspace every dlthub command takes --profile, before or after the command
    if host == "dlthub" and is_workspace_active():
        _add_profile_argument(parser)

    return parser, pre_parser, installed_commands


def add_environment_argument(parser: argparse.ArgumentParser) -> None:
    """Adds `--environment` to the parser of a command that deploys or works with a deployment.
    Such a command takes no `--profile`. It activates the environment with `activate_environment`
    before it generates a manifest or works with the deployment."""
    parser.add_argument(
        "--environment",
        metavar="NAME",
        default=None,
        dest=ENVIRONMENT_DEST,
        help=ENVIRONMENT_ARGUMENT_HELP,
    )


def _add_profile_argument(parser: argparse.ArgumentParser) -> None:
    """Adds `--profile` to `parser` and to every command under it that has no `profile` option,
    except the commands that manage profiles. A command that takes `--environment` gets a
    hidden `--profile`, so that `main` can name the option to use instead."""
    dests = {a.dest for a in parser._actions}
    if "profile" not in dests:
        # SUPPRESS so a command parser does not reset a value given before the command
        parser.add_argument(
            "--profile",
            metavar="NAME",
            default=argparse.SUPPRESS,
            help=argparse.SUPPRESS if ENVIRONMENT_DEST in dests else PROFILE_ARGUMENT_HELP,
        )
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            for name, subparser in action.choices.items():
                if name != PROFILE_COMMAND:
                    _add_profile_argument(subparser)


def _manages_profiles(args: argparse.Namespace) -> bool:
    command: Optional[str] = getattr(args, "command", None)
    local_op: Optional[str] = getattr(args, "local_op", None)
    return command == PROFILE_COMMAND or (command == "local" and local_op == PROFILE_COMMAND)


def _takes_environment(args: argparse.Namespace) -> bool:
    return ENVIRONMENT_DEST in vars(args)


def main(host: str = "dlt") -> int:
    fmt.set_cli_host_name(host)
    try:
        parser, pre_parser, installed_commands = _create_parser(host)
    except (ValueError, WorkspaceException) as ex:
        # the workspace loads here: an invalid `.workspace` or an undeclared profile stops dlt
        fmt.secho(str(ex), err=True, fg="red")
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
    profile = getattr(args, "profile", None)
    if profile and _manages_profiles(args):
        parser.error(
            "`dlthub profile` and `dlthub local profile` take no --profile. To show another"
            " profile with `dlthub profile info`, make it active first with `dlthub local"
            " profile use NAME`."
        )
    if profile and _takes_environment(args):
        parser.error(
            "this command deploys or works with a deployment and takes no --profile. Name the"
            " environment with --environment NAME."
        )

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
                if profile:
                    activate_profile(profile)
                cmd.execute(args)
        except Exception as ex:
            docs_url = getattr(cmd, "docs_url", None)
            error_code = -1
            raiseable_exception = ex

            if isinstance(ex, CliCommandException):
                error_code = ex.error_code
                docs_url = ex.docs_url or docs_url
                raiseable_exception = ex.raiseable_exception

            if raiseable_exception:
                fmt.secho(str(raiseable_exception) or str(ex), err=True, fg="red")

            # only point to docs when the command or exception provides a specific
            # link; the generic intro-page footer was removed (#4126)
            if docs_url:
                fmt.note("Please refer to our docs at '%s' for further assistance." % docs_url)
            if _debug.is_debug_enabled() and raiseable_exception:
                raise raiseable_exception

            return error_code
    else:
        print_help(host, parser)
        return -1

    return 0


def _print_use_dlthub_note(command: Optional[str]) -> None:
    """Print a note pointing the user to the `dlthub` replacement of the attempted `dlt` command."""
    if replacement := fmt.DLT_TO_DLTHUB_COMMANDS.get(command or ""):
        fmt.echo(
            "`dlt %s` is not available in an active dltHub Workspace. Use %s instead."
            % (command, fmt.bold("dlthub " + replacement)),
            err=True,
        )
    else:
        fmt.echo(
            "Use %s as the top level command in an active dltHub Workspace. Check %s and %s"
            " for former dlt commands."
            % (fmt.bold("dlthub"), fmt.bold("dlthub --help"), fmt.bold("dlthub local --help")),
            err=True,
        )


def _main() -> None:
    """Entry point for the `dlt` console script."""
    # when workspace is active, dlt commands do not execute - the user is pointed to dlthub.
    # only `dlt --version` still dispatches
    try:
        workspace_active = is_workspace_active()
    except (ValueError, WorkspaceException) as ex:
        fmt.secho(str(ex), err=True, fg="red")
        exit(-1)
    if workspace_active and "--version" not in sys.argv[1:]:
        command = next((a for a in sys.argv[1:] if not a.startswith("-")), None)
        _print_use_dlthub_note(command)
        exit(-1)
    exit(main("dlt"))


def _main_dlthub() -> None:
    """Entry point for the `dlthub` console script."""
    exit(main("dlthub"))


if __name__ == "__main__":
    exit(main("dlt"))
