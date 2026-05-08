"""DlthubProfileCommand — single additive class for `dlthub profile`."""
import argparse
from typing import Optional

from dlt._workspace.cli import SupportsCliCommand
from dlt.common.configuration.plugins import TCliCommandCompose


class ProfileCommand(SupportsCliCommand):
    """`dlthub profile` — additive shell with inline info / list / use."""

    command = "profile"
    compose: TCliCommandCompose = "additive"
    help_string = "Manage Workspace built-in profiles"
    description = "Show, switch, and list workspace profiles."
    docs_url: Optional[str] = None

    def configure_parser(self, parser: argparse.ArgumentParser) -> None:
        self.parser = parser
        # additive parent declares the subparsers action so plugin sub-subcommands can find it
        sub = parser.add_subparsers(title="Available subcommands", dest="operation", required=False)

        sub.add_parser(
            "info",
            help="Display the active profile (paths, providers, pinned status)",
        )
        sub.add_parser("list", help="List all available profiles")
        use_p = sub.add_parser(
            "use",
            help="Pin a profile so subsequent commands use it by default",
        )
        use_p.add_argument("profile_name", help="Profile name to pin")

    def execute(self, args: argparse.Namespace) -> None:
        # plugin sub-subcommands are dispatched by the composer via `args.execute`;
        # only inline operations are handled here. Default (no operation) shows info.
        op = getattr(args, "operation", None)
        if op == "list":
            self._list(args)
        elif op == "use":
            self._use(args)
        else:
            self._info(args)

    def _info(self, args: argparse.Namespace) -> None:
        from dlt._workspace.cli.utils import fetch_profile_info
        from dlt._workspace.cli._profile_command import print_profile_info

        info = fetch_profile_info()
        if info is None:
            from dlt._workspace.cli import echo as fmt

            fmt.warning("No active profile (not running inside a workspace).")
            return
        print_profile_info(info, getattr(args, "verbosity", 0))

    def _list(self, args: argparse.Namespace) -> None:
        from dlt._workspace._workspace_context import active
        from dlt._workspace.cli._profile_command import list_profiles

        list_profiles(active())

    def _use(self, args: argparse.Namespace) -> None:
        from dlt._workspace._workspace_context import active
        from dlt._workspace.cli._profile_command import pin_profile

        pin_profile(active(), args.profile_name)
