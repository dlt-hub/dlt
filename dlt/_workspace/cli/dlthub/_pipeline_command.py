"""DlthubPipelineCommand — single additive class for `dlthub pipeline`.

Inlines `init` (the OSS-side dlt init wired as `dlthub pipeline init`).
Cloud verbs (run / list / info) are registered as separate plugin commands
with `parent="pipeline"` and dispatched by the host via `args.execute`.
"""
import argparse
from typing import Optional

from dlt._workspace.cli import SupportsCliCommand
from dlt._workspace.cli.commands import InitCommand
from dlt.common.configuration.plugins import TCliCommandCompose


class PipelineCommand(SupportsCliCommand):
    command = "pipeline"
    compose: TCliCommandCompose = "additive"
    help_string = "Interact with pipelines running in dlthub"
    description = """Create, run, inspect and monitor pipelines at dltHub"""
    docs_url: Optional[str] = None

    def configure_parser(self, parser: argparse.ArgumentParser) -> None:
        self.parser = parser
        # additive parent declares the subparsers action so plugin sub-subcommands can find it
        sub = parser.add_subparsers(title="Available subcommands", dest="operation", required=False)

        init_p = sub.add_parser(
            "init",
            help=InitCommand.help_string,
            description=InitCommand.description,
        )
        self._init_cmd = InitCommand()
        self._init_cmd.configure_parser(init_p)

    def execute(self, args: argparse.Namespace) -> None:
        # plugin sub-subcommands are dispatched by the composer via `args.execute`;
        # only inline operations are handled here.
        if args.operation == "init":
            self._init_cmd.execute(args)
            return
        self.parser.print_usage()
