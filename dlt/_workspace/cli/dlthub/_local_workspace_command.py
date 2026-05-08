"""DlthubLocalWorkspaceCommand — single replace-mode class for `dlthub local`.

Hosts run / info / show / clean / schema / telemetry / pipeline as inline
argparse subparsers. `pipeline` reuses `PipelineCommand._add_operation_subparsers`
to expose the dlt OSS pipeline verb-first layout.
"""
import argparse
import sys
from typing import Dict, List, Optional, Tuple

from dlt._workspace.cli import echo as fmt
from dlt._workspace.cli import SupportsCliCommand
from dlt._workspace.cli.commands import PipelineCommand, SchemaCommand, TelemetryCommand
from dlt._workspace.deployment.typing import DEFAULT_DEPLOYMENT_MODULE
from dlt.common.configuration.plugins import TCliCommandCompose


class LocalWorkspaceCommand(SupportsCliCommand):
    command = "local"
    compose: TCliCommandCompose = "replace"
    help_string = (
        "Operations on the local Workspace (run, info, show, clean, schema, telemetry, pipeline)"
    )
    description = "Local-only operations on the current workspace."
    docs_url: Optional[str] = None

    def configure_parser(self, parser: argparse.ArgumentParser) -> None:
        self.parser = parser
        # `dest="local_op"` keeps the inner pipeline subparsers' `dest="operation"` from clashing
        sub = parser.add_subparsers(title="Available subcommands", dest="local_op", required=False)

        sub.add_parser("info", help="Display detailed local workspace info")

        show_p = sub.add_parser("show", help="Show workspace dashboard")
        show_p.add_argument(
            "--edit",
            action="store_true",
            help="Eject Dashboard and start editable version",
            default=None,
        )

        run_p = sub.add_parser(
            "run",
            help="Run a single workspace job locally",
            description=(
                "Run a single job from a deployment module locally. Loads the manifest,"
                " matches exactly one job by selector or job reference, builds a runtime"
                " entry point, and spawns the launcher subprocess."
            ),
        )
        run_p.add_argument(
            "selector_or_job_ref",
            nargs="?",
            default=None,
            help=(
                "Job reference (backfill, batch.backfill), trigger selector"
                " (tag:backfill, schedule:*), or a .py file path (auto-promoted"
                " to --file). If omitted, the job's default trigger is used."
            ),
        )
        run_p.add_argument(
            "--file",
            "-f",
            default=None,
            metavar="FILE",
            help=(
                "Path to a .py deployment module. If omitted, loads the default"
                f" {DEFAULT_DEPLOYMENT_MODULE!r} module from the workspace."
            ),
        )
        run_p.add_argument(
            "--profile",
            default=None,
            metavar="NAME",
            help="Override require.profile and the workspace pinned profile.",
        )
        run_p.add_argument(
            "--start",
            default=None,
            metavar="ISO",
            help="Override interval start (ISO 8601). Naive values use the job's timezone.",
        )
        run_p.add_argument(
            "--end",
            default=None,
            metavar="ISO",
            help="Override interval end (ISO 8601). Defaults to now if --start is set.",
        )
        run_p.add_argument(
            "--dry-run",
            action="store_true",
            help="Resolve the job and print the entry point without launching",
        )
        run_p.add_argument(
            "-c",
            "--config",
            action="append",
            default=[],
            metavar="KEY=VALUE",
            help="Config key=value pairs passed to the job (repeatable)",
        )
        run_p.add_argument(
            "--refresh",
            action="store_true",
            help=(
                "Request a refresh run. Respects TJobDefinition.refresh:"
                " `always` forces refresh regardless, `block` ignores the flag"
                " with a warning (run proceeds), `auto` honors it."
            ),
        )

        clean_p = sub.add_parser(
            "clean",
            help=(
                "Clean local data for the current profile. Locally loaded data and pipelines"
                " working dirs are deleted by default. Remote destinations are not affected."
            ),
        )
        clean_p.add_argument(
            "--skip-data-dir",
            action="store_true",
            default=False,
            help="Do not delete pipelines working dir.",
        )

        # delegate parser definition; `execute` re-instantiates fresh — these classes
        # don't reach for `self.parser` after configure_parser
        SchemaCommand().configure_parser(
            sub.add_parser(
                "schema",
                help=SchemaCommand.help_string,
                description=SchemaCommand.description,
            )
        )
        TelemetryCommand().configure_parser(
            sub.add_parser(
                "telemetry",
                help=TelemetryCommand.help_string,
                description=TelemetryCommand.description,
            )
        )

        # dlt OSS pipeline verbs — verb-first form: `pipeline <verb> <pipeline_name>`
        pipeline_p = sub.add_parser(
            "pipeline",
            help="Local pipeline operations (info, drop, sync, load-package, etc.)",
        )
        pipeline_p.add_argument("--pipelines-dir", help="Pipelines working directory", default=None)
        # PipelineCommand.execute reads list_pipelines and pipeline_name even when no verb is set
        pipeline_p.set_defaults(list_pipelines=False, pipeline_name=None)
        pipeline_sub = pipeline_p.add_subparsers(
            title="Available subcommands", dest="operation", required=False
        )
        pipeline_sub.add_parser(
            "list",
            help="List local pipelines",
            description="List pipelines in the working directory.",
        )
        # stash so `execute` reuses the same instance with `self.parser` set —
        # PipelineCommand.execute calls `self.parser.print_usage()` when pipeline_name is missing
        self._pipeline_cmd = PipelineCommand()
        self._pipeline_cmd.parser = pipeline_p
        self._pipeline_cmd._add_operation_subparsers(
            pipeline_sub, pre_positional_callback=_add_pipeline_name
        )

    def execute(self, args: argparse.Namespace) -> None:
        op = getattr(args, "local_op", None)
        if op == "pipeline":
            self._pipeline_cmd.execute(args)
            return
        if op == "schema":
            SchemaCommand().execute(args)
            return
        if op == "telemetry":
            TelemetryCommand().execute(args)
            return
        if op == "run":
            self._execute_run(args)
            return
        if op == "show":
            from dlt._workspace._workspace_context import active
            from dlt._workspace.cli._workspace_command import show_workspace

            show_workspace(active(), args.edit)
            return
        if op == "clean":
            from dlt._workspace._workspace_context import active
            from dlt._workspace.cli._workspace_command import clean_workspace

            clean_workspace(active(), args)
            return
        # default and explicit `info` both render the workspace overview
        from dlt._workspace._workspace_context import active
        from dlt._workspace.cli._workspace_command import print_workspace_info

        print_workspace_info(active(), getattr(args, "verbosity", 0))

    def _execute_run(self, args: argparse.Namespace) -> None:
        from dlt.common import json

        from dlt._workspace.cli._run_command import (
            fetch_run_info,
            print_run_plan,
            print_run_starting,
            print_run_warnings,
        )
        from dlt._workspace.deployment._job_ref import format_job_label
        from dlt._workspace.deployment.launchers._launcher import exec_process
        from dlt._workspace.deployment.typing import TJobDefinition, TTrigger

        def _pick(
            candidates: List[Tuple["TJobDefinition", "TTrigger"]],
        ) -> Tuple["TJobDefinition", "TTrigger"]:
            if len(candidates) == 1:
                return candidates[0]

            def _label(j: "TJobDefinition") -> str:
                return format_job_label(j["job_ref"], j.get("expose"), j.get("deliver"))

            labels = [f"{i}-{_label(j)}" for i, (j, _) in enumerate(candidates, 1)]
            fmt.echo(f"{len(candidates)} jobs match:")
            for i, (j, t) in enumerate(candidates, 1):
                fmt.echo(f"  {i}. {_label(j)}  (trigger: {t})")
            choice = fmt.prompt(
                "Pick a job: " + ", ".join(labels),
                choices=[str(i) for i in range(1, len(candidates) + 1)],
                default="1",
            )
            return candidates[int(choice) - 1]

        cli_config = _parse_config_args(args.config) if args.config else {}
        info = fetch_run_info(
            selector=args.selector_or_job_ref,
            file=args.file,
            user_profile=args.profile,
            user_start=args.start,
            user_end=args.end,
            user_refresh=args.refresh,
            cli_config=cli_config,
            pick=_pick,
        )
        if info is None:
            fmt.echo("No jobs found in manifest.")
            return

        print_run_warnings(info)

        if getattr(args, "verbosity", 0) or args.dry_run:
            print_run_plan(info)
        if args.dry_run:
            fmt.echo("--dry-run: not launching")
            return

        print_run_starting(info)
        exec_process(
            [
                sys.executable,
                "-u",
                "-m",
                info["launcher"],
                "--run-id",
                info["run_id"],
                "--trigger",
                info["trigger"],
                "--entry-point",
                json.typed_dumps(info["entry_point"]),
            ]
        )


class InfoSubCommand(SupportsCliCommand):
    command = "info"
    compose: TCliCommandCompose = "extend"
    help_string = "Display combined workspace info (local + cloud)"
    description: Optional[str] = None
    docs_url: Optional[str] = None

    def configure_parser(self, parser: argparse.ArgumentParser) -> None:
        # `-v/--verbose` is global (declared on the top-level pre-parser)
        pass

    def execute(self, args: argparse.Namespace) -> None:
        # dlt-side contribution; dlthub-client extends with cloud
        from dlt._workspace._workspace_context import active
        from dlt._workspace.cli._workspace_command import print_workspace_info

        print_workspace_info(active(), getattr(args, "verbosity", 0))


def _add_pipeline_name(parser: argparse.ArgumentParser, _op: str) -> None:
    parser.add_argument("pipeline_name", nargs="?", help="Pipeline name")


def _parse_config_args(pairs: List[str]) -> Dict[str, str]:
    config: Dict[str, str] = {}
    for pair in pairs:
        if "=" not in pair:
            raise ValueError(f"config must be KEY=VALUE, got: {pair!r}")
        key, value = pair.split("=", 1)
        config[key] = value
    return config
