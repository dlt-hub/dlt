"""The `--profile`, `--environment` and `--workspace` options of `dlthub` commands. A command
declares the ones it takes in `profile_options` of its `SupportsCliCommand`."""

import argparse
from contextlib import nullcontext
from typing import Any, ContextManager, Dict, Mapping, Optional, Sequence, Set, Tuple

from dlt.common.configuration.plugins import SupportsCliCommand

from dlt._workspace.cli.dlthub.typing import TProfileOptions, TProfileOptionsTakes
from dlt._workspace.environment import (
    ENVIRONMENT_ARGUMENT_HELP,
    ENVIRONMENT_PROFILE_ARGUMENT_HELP,
    WORKSPACE_ARGUMENT_HELP,
    use_profile_options,
)
from dlt._workspace.profile import PROFILE_ARGUMENT_HELP

PROFILE_DEST = "profile"
"""`dest` of the `--profile` option."""

ENVIRONMENT_DEST = "environment"
"""`dest` of the `--environment` and `--workspace` options: `None` when neither is given, `""`
for `--workspace`, the environment of the active profile, else the default one."""

PROFILE_OPTIONS_DEST = "_profile_options"
"""Hidden default that holds the `TProfileOptions` of a command and of the commands under it."""

DEFAULT_PROFILE_OPTIONS: TProfileOptions = {"takes": "profile"}
"""A command that declares no profile options takes `--profile`."""

NO_PROFILE_OPTIONS: TProfileOptions = {"takes": "none"}
"""The declaration of a command that uses no profile."""

TAKEN_OPTIONS: Dict[TProfileOptionsTakes, Tuple[str, ...]] = {
    "profile": ("--profile",),
    "environment": ("--environment",),
    "both": ("--environment", "--workspace", "--profile"),
    "none": (),
}
"""The options that each kind of command takes."""

MANAGES_PROFILES_REFUSAL = (
    "`dlthub profile` and `dlthub local profile` take no --profile. To show another profile with"
    " `dlthub profile info`, make it active first with `dlthub local profile use NAME`."
)
"""Why the commands that manage profiles take no `--profile`."""


def add_profile_options(
    parser: argparse.ArgumentParser,
    commands: Sequence[Tuple[argparse.ArgumentParser, SupportsCliCommand]],
) -> None:
    """Gives every command under `parser` the profile options that it declares.

    Args:
        parser (argparse.ArgumentParser): The parser of the host.
        commands (Sequence[Tuple[argparse.ArgumentParser, SupportsCliCommand]]): The parser of
            each command with the command whose `profile_options` declare its options. A later
            command overrides the declaration of an earlier one for its parser.

    Raises:
        ValueError: A declaration names a subcommand that the command does not have.
    """
    for command_parser, command in commands:
        options = getattr(command, "profile_options", None)
        if options is not None:
            _declare_profile_options(command_parser, options)
    for subparser in _get_subparsers(parser).values():
        _add_command_profile_options(subparser, DEFAULT_PROFILE_OPTIONS)


def use_command_profile(
    parser: argparse.ArgumentParser, args: argparse.Namespace
) -> ContextManager[Any]:
    """Checks the profile options given to the command of `args` and returns the context that runs
    the command with its profile, see `use_profile_options`. Exits through `parser.error` when the
    command takes none of the given options, or not together."""
    profile: Optional[str] = getattr(args, PROFILE_DEST, None)
    _check_profile_options(parser, args, profile)
    takes = _get_profile_options(args).get("takes", "profile")
    # an environment command group without its subcommand only prints its help
    if takes in ("environment", "both") and _is_group_without_subcommand(parser, args):
        return nullcontext()
    return use_profile_options(takes, getattr(args, ENVIRONMENT_DEST, None), profile)


def _declare_profile_options(parser: argparse.ArgumentParser, options: Mapping[str, Any]) -> None:
    declared: TProfileOptions = {k: v for k, v in options.items() if k != "subcommands"}  # type: ignore[assignment]
    # a declaration of subcommands only keeps the options of the command
    if declared:
        if declared.get("takes") == "none" and not declared.get("refusal"):
            declared["refusal"] = f"`{parser.prog}` does not use a profile and takes no --profile."
        parser.set_defaults(**{PROFILE_OPTIONS_DEST: declared})
    subparsers = _get_subparsers(parser)
    for name, sub_options in (options.get("subcommands") or {}).items():
        if name not in subparsers:
            raise ValueError(
                f"the profile options of `{parser.prog}` declare the subcommand `{name}`, which"
                f" `{parser.prog}` does not have"
            )
        _declare_profile_options(subparsers[name], sub_options)


def _get_subparsers(parser: argparse.ArgumentParser) -> Dict[str, argparse.ArgumentParser]:
    subparsers: Dict[str, argparse.ArgumentParser] = {}
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            subparsers.update(action.choices)
    return subparsers


def _add_command_profile_options(
    parser: argparse.ArgumentParser, parent_options: TProfileOptions
) -> Set[str]:
    """Adds the profile options to the command of `parser` and to every command under it, and
    returns the options its help shows: the ones the command takes, for a group the ones it
    declares itself and the ones every command under it takes. `--profile` and `--environment`
    are hidden where the command does not take them, so `use_command_profile` refuses them with
    the remedy."""
    options: TProfileOptions = parser._defaults.get(PROFILE_OPTIONS_DEST, parent_options)
    takes = options.get("takes", "profile")
    taken = TAKEN_OPTIONS[takes]
    # an alias maps to the parser of its command
    subparsers = {id(p): p for p in _get_subparsers(parser).values()}
    if subparsers:
        shown = set.intersection(
            *(_add_command_profile_options(subparser, options) for subparser in subparsers.values())
        )
        # a group that runs a command of its own, as `dlthub ai mcp` does, declares its options
        if "takes" in parser._defaults.get(PROFILE_OPTIONS_DEST, {}):
            shown |= set(taken)
    else:
        shown = set(taken)
    for option in TAKEN_OPTIONS["both"]:
        if option == "--workspace" and option not in taken and option not in shown:
            continue
        # a command may declare the option itself, as `dlthub profile add` does `--environment`
        if option in parser._option_string_actions:
            continue
        help_text = argparse.SUPPRESS
        if option in shown:
            help_text = options.get("option_help", {}).get(option) or _get_default_option_help(
                option, takes
            )
        if option == "--workspace":
            parser.add_argument(
                option,
                action="store_const",
                const="",
                dest=ENVIRONMENT_DEST,
                default=argparse.SUPPRESS,
                help=help_text,
            )
        else:
            parser.add_argument(
                option,
                metavar="NAME",
                dest=PROFILE_DEST if option == "--profile" else ENVIRONMENT_DEST,
                default=argparse.SUPPRESS,
                help=help_text,
            )
    return shown


def _get_default_option_help(option: str, takes: TProfileOptionsTakes) -> str:
    if option == "--profile":
        return ENVIRONMENT_PROFILE_ARGUMENT_HELP if takes == "both" else PROFILE_ARGUMENT_HELP
    if option == "--workspace":
        return WORKSPACE_ARGUMENT_HELP
    return ENVIRONMENT_ARGUMENT_HELP


def _get_profile_options(args: argparse.Namespace) -> TProfileOptions:
    options: TProfileOptions = getattr(args, PROFILE_OPTIONS_DEST, DEFAULT_PROFILE_OPTIONS)
    return options


def _check_profile_options(
    parser: argparse.ArgumentParser, args: argparse.Namespace, profile: Optional[str]
) -> None:
    """Refuses `--profile`, `--environment` and `--workspace` where the command of `args` takes
    none of them, or not together. `profile` is the one given to dlt. Exits through
    `parser.error`."""
    options = _get_profile_options(args)
    takes = options.get("takes", "profile")
    environment = getattr(args, ENVIRONMENT_DEST, None)
    if profile and takes == "none":
        parser.error(options.get("refusal") or "this command takes no --profile.")
    if takes not in ("environment", "both"):
        if environment is not None:
            parser.error(
                "this command does not deploy or work with a deployment and takes no"
                " --environment or --workspace."
            )
        return
    if profile and takes == "environment":
        parser.error(
            "this command deploys or works with a deployment and takes no --profile. Name the"
            " environment with --environment NAME."
        )
    if profile and environment == "":
        parser.error(
            "--workspace names no environment and takes no --profile: give one of them. A"
            " profile picks its own environment."
        )
    if options.get("required") and not profile and environment is None:
        parser.error(
            "this command needs --profile NAME, or the environment with --environment NAME or"
            " --workspace."
        )


def _is_group_without_subcommand(parser: argparse.ArgumentParser, args: argparse.Namespace) -> bool:
    """Whether `args` runs a command group without its subcommand."""
    while True:
        action = next(
            (a for a in parser._actions if isinstance(a, argparse._SubParsersAction)), None
        )
        if action is None or action.dest == argparse.SUPPRESS:
            return False
        name = getattr(args, action.dest, None)
        if name is None:
            return True
        parser = action.choices[name]
