import os
import argparse

from dlt._workspace._workspace_context import WorkspaceRunContext, active
from dlt._workspace.profile import (
    BUILT_IN_PROFILES,
    get_profile_pin_file,
    read_profile_pin,
    save_profile_pin,
)
from dlt.cli import SupportsCliCommand, echo as fmt


class ProfileCommand(SupportsCliCommand):
    command = "profile"
    help_string = "Manage Workspace built-in profiles"
    description = """
Commands to list and pin profiles
Run without arguments to list all profiles, the default profile and the
pinned profile in current project.
"""

    def configure_parser(self, parser: argparse.ArgumentParser) -> None:
        self.parser = parser

        parser.add_argument("profile_name", help="Name of the profile", nargs="?")

        subparsers = parser.add_subparsers(
            title="Available subcommands", dest="profile_command", required=False
        )

        subparsers.add_parser(
            "info",
            help="Show information about the current profile.",
            description="Show information about the current profile.",
        )

        subparsers.add_parser(
            "list",
            help="Show list of built-in profiles.",
            description="Show list of built-in profiles.",
        )

        subparsers.add_parser(
            "pin",
            help="Pin a profile to the Workspace.",
            description="""
Pin a profile to the Workspace, this will be the new default profile while it is pinned.
""",
        )

    def execute(self, args: argparse.Namespace) -> None:
        workspace_context = active()

        if args.profile_command == "info" or not args.profile_command:
            print_profile_info(workspace_context)
        elif args.profile_command == "list":
            list_profiles(workspace_context)
        elif args.profile_command == "pin":
            pin_profile(workspace_context, args.profile_name)
        else:
            self.parser.print_usage()


def print_profile_info(workspace_run_context: WorkspaceRunContext) -> None:
    fmt.echo("Current profile: %s" % fmt.bold(workspace_run_context.profile))
    if pinned_profile := read_profile_pin(workspace_run_context):
        fmt.echo("Pinned profile: %s" % fmt.bold(pinned_profile))


def list_profiles(workspace_run_context: WorkspaceRunContext) -> None:
    fmt.echo("Available profiles:")
    for profile in workspace_run_context.available_profiles():
        desc = BUILT_IN_PROFILES.get(profile, "Pinned custom profile")
        fmt.echo("* %s - %s" % (fmt.bold(profile), desc))


def pin_profile(workspace_run_context: WorkspaceRunContext, profile_name: str) -> None:
    if not profile_name:
        pinned_profile = read_profile_pin(workspace_run_context)
        if pinned_profile:
            pin_file = get_profile_pin_file(workspace_run_context)
            fmt.echo(
                "Currently pinned profile is: %s. To unpin remove %s file."
                % (fmt.bold(pinned_profile), fmt.bold(os.path.relpath(pin_file)))
            )
        else:
            fmt.echo("No pinned profile.")
    else:
        fmt.echo("Will pin the profile %s to current Workspace." % fmt.bold(profile_name))
        if not fmt.confirm("Do you want to proceed?", default=True):
            # TODO: raise exception that will exit with all required cleanups
            exit(0)
        save_profile_pin(workspace_run_context, profile_name)
