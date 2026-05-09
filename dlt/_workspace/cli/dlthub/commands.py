"""Parser classes for the `dlthub` CLI host."""

import argparse
import sys
from typing import List, Optional

from dlt.common.configuration.plugins import TCliCommandCompose

from dlt._workspace.cli import echo as fmt
from dlt._workspace.cli import SupportsCliCommand
from dlt._workspace.cli.exceptions import CliCommandException
from dlt._workspace.cli.utils import add_mcp_arg_parser, make_mcp_run_flags
from dlt._workspace.cli.commands import (
    InitCommand,
    PipelineCommand as DltPipelineCommand,
    SchemaCommand,
    TelemetryCommand,
)
from dlt._workspace.deployment.typing import DEFAULT_DEPLOYMENT_MODULE


class AiCommand(SupportsCliCommand):
    command = "ai"
    help_string = "Use AI-powered development tools and utilities"
    # docs_url =
    description = (
        "The `dlt ai` command provides commands to configure your LLM-enabled IDE and MCP server."
    )

    def configure_parser(self, ai_cmd: argparse.ArgumentParser) -> None:
        self.parser = ai_cmd

        ai_subparsers = ai_cmd.add_subparsers(
            title="Available subcommands", dest="operation", required=False
        )

        # status command
        ai_subparsers.add_parser(
            "status",
            help="Show AI setup status: dlt version, agent, toolkits, readiness checks",
        )

        # init command
        from dlt._workspace.cli._urls import (
            DEFAULT_AI_WORKBENCH_BRANCH,
            DEFAULT_AI_WORKBENCH_REPO,
        )

        init_cmd = ai_subparsers.add_parser(
            "init",
            help="Install initial AI rules and skills for your AI coding agent",
        )
        init_cmd.add_argument(
            "--agent",
            choices=["claude", "cursor", "codex"],
            default=None,
            help="AI coding agent to install for. Auto-detected if omitted.",
        )
        init_cmd.add_argument(
            "--location",
            default=DEFAULT_AI_WORKBENCH_REPO,
            help="Advanced. Git URL or local path to AI workbench repository.",
        )
        init_cmd.add_argument(
            "--branch",
            default=DEFAULT_AI_WORKBENCH_BRANCH,
            help="Advanced. Git branch to fetch from.",
        )
        init_cmd.add_argument(
            "--overwrite",
            default=False,
            action="store_true",
            help="Overwrite existing files instead of skipping them.",
        )

        # secrets command group
        secrets_cmd = ai_subparsers.add_parser(
            "secrets",
            help="Manage secrets files used by dlt",
            description="List, view (redacted), or update secret files used by dlt providers.",
        )
        secrets_subparsers = secrets_cmd.add_subparsers(
            title="Available subcommands", dest="secrets_operation", required=False
        )
        secrets_subparsers.add_parser(
            "list",
            help="List secret file locations from providers",
        )
        view_cmd = secrets_subparsers.add_parser(
            "view-redacted",
            help="Print secrets TOML with all values replaced by '***'",
            description=(
                "Without --path, shows the unified view merged from all project"
                " secret files. With --path, shows that exact file."
            ),
        )
        view_cmd.add_argument(
            "--path",
            default=None,
            help="Show this exact file instead of the unified provider view",
        )
        update_cmd = secrets_subparsers.add_parser(
            "update-fragment",
            help="Merge a TOML fragment into the secrets file",
        )
        update_cmd.add_argument(
            "fragment",
            nargs="?",
            default=None,
            help="TOML fragment string to merge; reads from stdin if omitted",
        )
        update_cmd.add_argument(
            "--path",
            required=True,
            help="Path to the secrets TOML file to write to",
        )

        # toolkit command group — verb-first form: `ai toolkit <verb> <name>`
        toolkit_cmd = ai_subparsers.add_parser(
            "toolkit",
            help="Manage AI toolkit plugins (list, info, install)",
        )
        toolkit_sub = toolkit_cmd.add_subparsers(dest="toolkit_operation", required=False)

        # shared parent with --location and --branch
        toolkit_common = argparse.ArgumentParser(add_help=False)
        toolkit_common.add_argument(
            "--location",
            default=DEFAULT_AI_WORKBENCH_REPO,
            help="Advanced. Git URL or local path to toolkit repository.",
        )
        toolkit_common.add_argument(
            "--branch",
            default=DEFAULT_AI_WORKBENCH_BRANCH,
            help="Advanced. Git branch to fetch toolkit from.",
        )

        toolkit_sub.add_parser(
            "list",
            help="List available toolkits",
            parents=[toolkit_common],
        )
        info_cmd = toolkit_sub.add_parser(
            "info",
            help="Show toolkit contents and components",
            parents=[toolkit_common],
        )
        info_cmd.add_argument("name", help="Toolkit name")
        install_cmd = toolkit_sub.add_parser(
            "install",
            help="Install toolkit components into project",
            parents=[toolkit_common],
        )
        install_cmd.add_argument("name", help="Toolkit name")
        install_cmd.add_argument(
            "--agent",
            choices=["claude", "cursor", "codex"],
            default=None,
            help="AI coding agent to install for. Auto-detected if omitted.",
        )
        install_cmd.add_argument(
            "--overwrite",
            default=False,
            action="store_true",
            help="Overwrite existing files instead of skipping them.",
        )
        install_cmd.add_argument(
            "--strict",
            default=False,
            action="store_true",
            help="Fail on validation warnings (invalid frontmatter, etc.).",
        )

        # shared run flags — used by both `dlt ai mcp [flags]` and `dlt ai mcp run [flags]`
        mcp_run_flags = make_mcp_run_flags(default_port=8000)

        mcp_cmd = ai_subparsers.add_parser(
            "mcp",
            help="Run or install the dlt MCP server",
            parents=[mcp_run_flags],
        )
        mcp_sub = mcp_cmd.add_subparsers(dest="mcp_operation", required=False)

        mcp_sub.add_parser("run", help="Start the MCP server (default)", parents=[mcp_run_flags])

        mcp_install_cmd = mcp_sub.add_parser(
            "install",
            help="Install MCP server config into the current project",
        )
        mcp_install_cmd.add_argument(
            "--agent",
            choices=["claude", "cursor", "codex"],
            default=None,
            help="AI coding agent to install for. Auto-detected if omitted.",
        )
        mcp_install_cmd.add_argument(
            "--features",
            nargs="*",
            default=None,
            help="MCP feature sets to include in the server config",
        )
        mcp_install_cmd.add_argument(
            "--name",
            default="dlt-workspace",
            help="Server name in the MCP config (default: dlt-workspace)",
        )
        mcp_install_cmd.add_argument(
            "--overwrite",
            default=False,
            action="store_true",
            help="Overwrite existing server config instead of skipping.",
        )

    def execute(self, args: argparse.Namespace) -> None:
        from dlt._workspace.cli._urls import (
            DEFAULT_AI_WORKBENCH_BRANCH,
            DEFAULT_AI_WORKBENCH_REPO,
        )
        from dlt._workspace.cli.dlthub.ai import (
            ai_status_command,
            ai_init_command,
            ai_mcp_run_command,
            ai_mcp_install_command,
            ai_secrets_list_command,
            ai_secrets_view_redacted_command,
            ai_secrets_update_fragment_command,
            ai_toolkit_install_command,
            ai_toolkit_list_command,
            ai_toolkit_info_command,
        )

        if args.operation == "status":
            ai_status_command()
        elif args.operation == "init":
            ai_init_command(
                agent=args.agent,
                location=args.location,
                branch=args.branch,
                overwrite=args.overwrite,
            )
        elif args.operation == "secrets":
            op = getattr(args, "secrets_operation", None)
            if op == "view-redacted":
                ai_secrets_view_redacted_command(path=args.path)
            elif op == "update-fragment":
                fragment = args.fragment or sys.stdin.read()
                ai_secrets_update_fragment_command(fragment=fragment, path=args.path)
            else:
                ai_secrets_list_command()
        elif args.operation == "toolkit":
            tk_op = getattr(args, "toolkit_operation", None)
            if tk_op == "list":
                ai_toolkit_list_command(
                    location=args.location,
                    branch=args.branch,
                )
            elif tk_op == "info":
                if not args.name:
                    fmt.error("Toolkit name is required for 'info'.")
                    raise CliCommandException()
                ai_toolkit_info_command(
                    name=args.name,
                    location=args.location,
                    branch=args.branch,
                )
            elif tk_op == "install":
                if not args.name:
                    fmt.error("Toolkit name is required for 'install'.")
                    raise CliCommandException()
                ai_toolkit_install_command(
                    name=args.name,
                    agent=args.agent,
                    location=args.location,
                    branch=args.branch,
                    overwrite=args.overwrite,
                    strict=args.strict,
                )
            else:
                # default: list toolkits
                ai_toolkit_list_command(
                    location=getattr(args, "location", DEFAULT_AI_WORKBENCH_REPO),
                    branch=getattr(args, "branch", DEFAULT_AI_WORKBENCH_BRANCH),
                )
        elif args.operation == "mcp":
            mcp_op = getattr(args, "mcp_operation", None)
            if mcp_op == "install":
                ai_mcp_install_command(
                    agent=args.agent,
                    features=args.features,
                    name=args.name,
                    overwrite=args.overwrite,
                )
            else:
                # default: run
                ai_mcp_run_command(
                    port=getattr(args, "port", 8000),
                    stdio=getattr(args, "stdio", False),
                    sse=getattr(args, "sse", False),
                    features=getattr(args, "features", None),
                )
        else:
            self.parser.print_usage()


class LocalWorkspaceCommand(SupportsCliCommand):
    """`dlthub local` — replace-mode shell hosting run/info/show/clean/schema/telemetry/pipeline."""

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
        from dlt._workspace.cli.dlthub._local_workspace_command import _add_pipeline_name

        self._pipeline_cmd = DltPipelineCommand()
        self._pipeline_cmd.parser = pipeline_p
        self._pipeline_cmd._add_operation_subparsers(
            pipeline_sub, pre_positional_callback=_add_pipeline_name
        )

    def execute(self, args: argparse.Namespace) -> None:
        from dlt._workspace._workspace_context import active
        from dlt._workspace.cli.dlthub._local_workspace_command import (
            clean_workspace,
            execute_run,
            print_workspace_info,
            show_workspace,
        )

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
            execute_run(args)
            return
        if op == "show":
            show_workspace(active(), args.edit)
            return
        if op == "clean":
            clean_workspace(active(), args)
            return
        # default and explicit `info` both render the workspace overview
        print_workspace_info(active(), getattr(args, "verbosity", 0))


class InfoSubCommand(SupportsCliCommand):
    """`dlthub info` — dlt-side workspace overview; cloud sibling extends with hub state."""

    command = "info"
    compose: TCliCommandCompose = "extend"
    help_string = "Display combined workspace info (local + cloud)"
    description: Optional[str] = None
    docs_url: Optional[str] = None

    def configure_parser(self, parser: argparse.ArgumentParser) -> None:
        # `-v/--verbose` is global (declared on the top-level pre-parser)
        pass

    def execute(self, args: argparse.Namespace) -> None:
        from dlt._workspace._workspace_context import active
        from dlt._workspace.cli.dlthub._local_workspace_command import print_workspace_info

        print_workspace_info(active(), getattr(args, "verbosity", 0))


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
        from dlt._workspace.cli.dlthub._profile_command import print_profile_info
        from dlt._workspace.cli.dlthub.utils import fetch_profile_info

        info = fetch_profile_info()
        if info is None:
            fmt.warning("No active profile (not running inside a workspace).")
            return
        print_profile_info(info, getattr(args, "verbosity", 0))

    def _list(self, args: argparse.Namespace) -> None:
        from dlt._workspace._workspace_context import active
        from dlt._workspace.cli.dlthub._profile_command import list_profiles

        list_profiles(active())

    def _use(self, args: argparse.Namespace) -> None:
        from dlt._workspace._workspace_context import active
        from dlt._workspace.cli.dlthub._profile_command import pin_profile

        pin_profile(active(), args.profile_name)


class PipelineCommand(SupportsCliCommand):
    """`dlthub pipeline` — additive shell. Inlines `init`; cloud verbs are sibling plugins."""

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
