import argparse
import os
from typing import Callable, Dict, List, Optional

from dlt._workspace.cli import echo as fmt, utils
from dlt._workspace.cli import SupportsCliCommand, DEFAULT_VERIFIED_SOURCES_REPO
from dlt.common.configuration.plugins import TCliCommandCompose
from dlt._workspace.cli.exceptions import CliCommandException
from dlt._workspace.cli.utils import add_mcp_arg_parser, is_hub_available
from dlt._workspace.cli._urls import (
    DLT_INIT_DOCS_URL,
    DLT_PIPELINE_COMMAND_DOCS_URL,
    DLT_TELEMETRY_DOCS_URL,
    DLT_DEPLOY_DOCS_URL,
)
from dlt.common.storages.configuration import TSchemaFileFormat

# NOTE: do not add command specific import here - do that inline to reduce import time


class InitCommand(SupportsCliCommand):
    command = "init"
    help_string = (
        "Creates a pipeline in the current folder by adding existing verified source or"
        " creating a new one from template."
    )
    docs_url = DLT_INIT_DOCS_URL
    description = """
This command creates a new dlt pipeline script that loads data from `source` to `destination`. When you run the command, several things happen:

1. Creates a basic project structure if the current folder is empty by adding `.dlt/config.toml`, `.dlt/secrets.toml`, and `.gitignore` files.
2. Checks if the `source` argument matches one of our verified sources and, if so, adds it to your project.
3. If the `source` is unknown, uses a generic template to get you started.
4. Rewrites the pipeline scripts to use your `destination`.
5. Creates sample config and credentials in `secrets.toml` and `config.toml` for the specified source and destination.
6. Creates `requirements.txt` with dependencies required by the source and destination. If one exists, prints instructions on what to add to it.

This command can be used several times in the same folder to add more sources, destinations, and pipelines. It will also update the verified source code to the newest
version if run again with an existing `source` name. You will be warned if files will be overwritten or if the `dlt` version needs an upgrade to run a particular pipeline.
    """

    def configure_parser(self, parser: argparse.ArgumentParser) -> None:
        self.parser = parser

        parser.add_argument(
            "--list-sources",
            "-l",
            default=False,
            action="store_true",
            help=(
                "Shows all available verified sources and their short descriptions. For each"
                " source, it checks if your local `dlt` version requires an update and prints the"
                " relevant warning."
            ),
        )
        parser.add_argument(
            "--list-destinations",
            default=False,
            action="store_true",
            help="Shows the name of all core dlt destinations.",
        )
        parser.add_argument(
            "source",
            nargs="?",
            help=(
                "Name of data source for which to create a pipeline. Adds existing verified"
                " source or creates a new pipeline template if verified source for your data"
                " source is not yet implemented."
            ),
        )
        parser.add_argument(
            "destination", nargs="?", help="Name of a destination i.e. bigquery or redshift"
        )
        parser.add_argument(
            "--location",
            default=DEFAULT_VERIFIED_SOURCES_REPO,
            help="Advanced. Uses a specific url or local path to verified sources repository.",
        )
        parser.add_argument(
            "--branch",
            default=None,
            help=(
                "Advanced. Uses specific branch of the verified sources repository to fetch the"
                " template."
            ),
        )

        parser.add_argument(
            "--eject",
            default=False,
            action="store_true",
            help=(
                "Ejects the source code of the core source like sql_database or rest_api so they"
                " will be editable by you."
            ),
        )

    def execute(self, args: argparse.Namespace) -> None:
        from dlt._workspace.cli._init_command import (
            list_destinations_command_wrapper,
            list_sources_command_wrapper,
            init_command_wrapper,
        )

        if args.list_sources:
            list_sources_command_wrapper(args.location, args.branch)
        elif args.list_destinations:
            list_destinations_command_wrapper()
        else:
            if not args.source or not args.destination:
                self.parser.print_usage()
                raise CliCommandException()
            # event is "pipeline.init" when reached via `dlthub pipeline init`, else "init"
            event = "pipeline.init" if fmt.get_cli_host_name() == "dlthub" else "init"
            utils.track_command(event, False, "source_name", "destination_type")(
                init_command_wrapper
            )(
                args.source,
                args.destination,
                args.location,
                args.branch,
                args.eject,
            )


class PipelineCommand(SupportsCliCommand):
    command = "pipeline"
    compose: TCliCommandCompose = "additive"
    help_string = "Inspects pipeline state, trace, load packages, provides basic maintenance"
    docs_url = DLT_PIPELINE_COMMAND_DOCS_URL
    description = """
Provides tools to inspect the pipeline working directory, tables, and data in the destination, and to check for problems encountered during data loading.
    """

    def configure_parser(self, pipe_cmd: argparse.ArgumentParser) -> None:
        self.parser = pipe_cmd

        pipe_cmd.add_argument(
            "--list-pipelines",
            "-l",
            default=False,
            action="store_true",
            help="List local pipelines",
        )
        pipe_cmd.add_argument("pipeline_name", nargs="?", help="Pipeline name")
        pipe_cmd.add_argument("--pipelines-dir", help="Pipelines working directory", default=None)

        pipeline_subparsers = pipe_cmd.add_subparsers(
            title="Available subcommands", dest="operation", required=False
        )
        self._add_operation_subparsers(pipeline_subparsers)

    def _add_operation_subparsers(
        self,
        pipeline_subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]",
        pre_positional_callback: Optional[Callable[[argparse.ArgumentParser, str], None]] = None,
    ) -> None:
        """Builds the per-operation subparsers (info/show/sync/drop/load-package/etc.)."""
        # keep here to avoid importing schema storages at cli startup
        from dlt.common.storages.configuration import SCHEMA_FILES_EXTENSIONS

        # `pre_positional_callback(parser, op_name)` runs BEFORE each subparser's own args
        # are added. subclasses (e.g. DlthubPipelineCommand) use it to register a positional
        # (`pipeline_name`) ahead of the operation-specific args, achieving the verb-first
        # form `<verb> <pipeline> [args]` while reusing this builder.
        def _pre(p: argparse.ArgumentParser, op: str) -> None:
            if pre_positional_callback is not None:
                pre_positional_callback(p, op)

        pipe_cmd_sync_parent = argparse.ArgumentParser(add_help=False)
        pipe_cmd_sync_parent.add_argument(
            "--destination", help="Sync from this destination when local pipeline state is missing."
        )
        pipe_cmd_sync_parent.add_argument(
            "--dataset-name", help="Dataset name to sync from when local pipeline state is missing."
        )

        info_cmd = pipeline_subparsers.add_parser(
            "info",
            help="Displays state of the pipeline, use -v or -vv for more info",
            description="""
Displays the content of the working directory of the pipeline: dataset name, destination, list of
schemas, resources in schemas, list of completed and normalized load packages, and optionally a
pipeline state set by the resources during the extraction process.
""",
        )
        _pre(info_cmd, "info")
        show_cmd = pipeline_subparsers.add_parser(
            "show",
            help=(
                "Generates and launches workspace dashboard with the loading status and dataset"
                " explorer"
            ),
            description="""
Launches the workspace dashboard with a comprehensive interface to inspect the pipeline state, schemas, and data in the destination.

This dashboard should be executed from the same folder from which you ran the pipeline script to be able access destination credentials.

If the --edit flag is used, will launch the editable version of the dashboard if it exists in the current directory, or create this version and launch it in edit mode.

Requires `marimo` to be installed in the current environment: `pip install marimo`.
""",
        )
        _pre(show_cmd, "show")
        show_cmd.add_argument(
            "--edit",
            default=False,
            action="store_true",
            help=(
                "Creates editable version of workspace dashboard in current directory if it does"
                " not exist there yet and launches it in edit mode."
            ),
        )
        failed_jobs_cmd = pipeline_subparsers.add_parser(
            "failed-jobs",
            help=(
                "Displays information on all the failed loads in all completed packages, failed"
                " jobs and associated error messages"
            ),
            description="""
This command scans all the load packages looking for failed jobs and then displays information on
files that got loaded and the failure message from the destination.
""",
        )
        _pre(failed_jobs_cmd, "failed-jobs")
        drop_pending_cmd = pipeline_subparsers.add_parser(
            "drop-pending-packages",
            help=(
                "Deletes all extracted and normalized packages including those that are partially"
                " loaded."
            ),
            description="""
Removes all extracted and normalized packages in the pipeline's working dir.
`dlt` keeps extracted and normalized load packages in the pipeline working directory. When the `run` method is called, it will attempt to normalize and load
pending packages first. This command removes such packages. Note that **pipeline state** is not reverted to the state at which the deleted packages
were created. Using the `sync` sub-command is recommended if your destination supports state sync.
""",
        )
        _pre(drop_pending_cmd, "drop-pending-packages")
        sync_cmd = pipeline_subparsers.add_parser(
            "sync",
            help=(
                "Drops the local state of the pipeline and resets all the schemas and restores it"
                " from destination. The destination state, data and schemas are left intact."
            ),
            description="""
This command will remove the pipeline working directory with all pending packages, not synchronized
state changes, and schemas and retrieve the last synchronized data from the destination. If you drop
the dataset the pipeline is loading to, this command results in a complete reset of the pipeline state.

In case of a pipeline without a working directory, this command may be used to create one from the
destination. In order to do that, you need to pass the dataset name and destination name to the CLI
and provide the credentials to connect to the destination (i.e., in `.dlt/secrets.toml`) placed in the
folder where you run it.
""",
            parents=[pipe_cmd_sync_parent],
        )
        _pre(sync_cmd, "sync")
        trace_cmd = pipeline_subparsers.add_parser(
            "trace",
            help="Displays last run trace, use -v or -vv for more info",
            description="""
Displays the trace of the last pipeline run containing the start date of the run, elapsed time, and the
same information for all the steps (`extract`, `normalize`, and `load`). If any of the steps failed,
you'll see the message of the exceptions that caused that problem. Successful `load` and `run` steps
will display the load info instead.
""",
        )
        _pre(trace_cmd, "trace")
        pipe_cmd_schema = pipeline_subparsers.add_parser(
            "schema",
            help="Displays default schema",
            description="Displays the default schema for the selected pipeline.",
        )
        _pre(pipe_cmd_schema, "schema")
        pipe_cmd_schema.add_argument(
            "--format",
            choices=SCHEMA_FILES_EXTENSIONS,
            default="yaml",
            help="Display schema in this format",
        )
        pipe_cmd_schema.add_argument(
            "--remove-defaults",
            action="store_true",
            help="Does not show default hint values",
            default=True,
        )

        pipe_cmd_drop = pipeline_subparsers.add_parser(
            "drop",
            help="Selectively drop tables and reset state",
            description="""
Selectively drop tables and reset state.

```sh
dlt pipeline <pipeline name> drop [resource_1] [resource_2]
```

Drops tables generated by selected resources and resets the state associated with them. Mainly used
to force a full refresh on selected tables. In the example below, we drop all tables generated by
the `repo_events` resource in the GitHub pipeline:

```sh
dlt pipeline github_events drop repo_events
```

`dlt` will inform you of the names of dropped tables and the resource state slots that will be
reset:

```text
About to drop the following data in dataset airflow_events_1 in destination dlt.destinations.duckdb:
Selected schema:: github_repo_events
Selected resource(s):: ['repo_events']
Table(s) to drop:: ['issues_event', 'fork_event', 'pull_request_event', 'pull_request_review_event', 'pull_request_review_comment_event', 'watch_event', 'issue_comment_event', 'push_event__payload__commits', 'push_event']
Resource(s) state to reset:: ['repo_events']
Source state path(s) to reset:: []
Do you want to apply these changes? [y/N]
```

As a result of the command above the following will happen:

1. All the indicated tables will be dropped in the destination. Note that `dlt` drops the nested
   tables as well.
2. All the indicated tables will be removed from the indicated schema.
3. The state for the resource `repo_events` was found and will be reset.
4. New schema and state will be stored in the destination.

The `drop` command accepts several advanced settings:

1. You can use regexes to select resources. Prepend the `re:` string to indicate a regex pattern. The example
   below will select all resources starting with `repo`:

```sh
dlt pipeline github_events drop "re:^repo"
```

2. You can drop all tables in the indicated schema:

```sh
dlt pipeline chess drop --drop-all
```

3. You can indicate additional state slots to reset by passing JsonPath to the source state. In the example
   below, we reset the `archives` slot in the source state:

```sh
dlt pipeline chess_pipeline drop --state-paths archives
```

This will select the `archives` key in the `chess` source.

```json
{
  "sources":{
    "chess": {
      "archives": [
        "https://api.chess.com/pub/player/magnuscarlsen/games/2022/05"
      ]
    }
  }
}
```

**This command is still experimental** and the interface will most probably change.

""",
            parents=[pipe_cmd_sync_parent],
            epilog=(
                f"See {DLT_PIPELINE_COMMAND_DOCS_URL}#selectively-drop-tables-and-reset-state for"
                " more info"
            ),
        )
        _pre(pipe_cmd_drop, "drop")
        pipe_cmd_drop.add_argument(
            "resources",
            nargs="*",
            help=(
                "One or more resources to drop. Can be exact resource name(s) or regex pattern(s)."
                " Regex patterns must start with re:"
            ),
        )
        pipe_cmd_drop.add_argument(
            "--drop-all",
            action="store_true",
            default=False,
            help="Drop all resources found in schema. Supersedes [resources] argument.",
        )
        pipe_cmd_drop.add_argument(
            "--state-paths", nargs="*", help="State keys or json paths to drop", default=()
        )
        pipe_cmd_drop.add_argument(
            "--schema",
            help="Schema name to drop from (if other than default schema).",
            dest="schema_name",
        )
        pipe_cmd_drop.add_argument(
            "--state-only",
            action="store_true",
            help="Only wipe state for matching resources without dropping tables.",
            default=False,
        )

        pipe_cmd_package = pipeline_subparsers.add_parser(
            "load-package",
            help="Displays information on load package, use -v or -vv for more info",
            description="""
Shows information on a load package with a given `load_id`. The `load_id` parameter defaults to the
most recent package. Package information includes its state (`COMPLETED/PROCESSED`) and list of all
jobs in a package with their statuses, file sizes, types, and in case of failed jobs—the error
messages from the destination. With the verbose flag set (`-v`), you can also see the
list of all tables and columns created at the destination during the loading of that package.
""",
        )
        _pre(pipe_cmd_package, "load-package")
        pipe_cmd_package.add_argument(
            "load_id",
            metavar="load-id",
            nargs="?",
            help="Load id of completed or normalized package. Defaults to the most recent package.",
        )

        DEFAULT_PIPELINE_MCP_PORT = 43656
        add_mcp_arg_parser(
            pipeline_subparsers,
            "This MCP facilitates schema and data exploration for the dataset created with this"
            " pipeline",
            "Launch MCP server attached to this pipeline",
            DEFAULT_PIPELINE_MCP_PORT,
        )
        # add_mcp_arg_parser is a no-op when fastmcp isn't installed; the lookup below
        # may yield None in that case, which the callback can ignore
        mcp_parser = pipeline_subparsers.choices.get("mcp")
        if mcp_parser is not None:
            _pre(mcp_parser, "mcp")

    def execute(self, args: argparse.Namespace) -> None:
        from dlt._workspace.cli._pipeline_command import pipeline_command_wrapper

        # event is "local.pipeline" when reached via `dlthub local pipeline`, else "pipeline"
        event = "local.pipeline" if fmt.get_cli_host_name() == "dlthub" else "pipeline"
        tracked = utils.track_command(event, True, "operation")(pipeline_command_wrapper)

        if (
            args.list_pipelines
            or args.operation == "list"
            or (not args.pipeline_name and not args.operation)
        ):
            # Always use max verbosity (1) for dlt pipeline list - show full details
            tracked("list", "-", args.pipelines_dir, 1)
        else:
            command_kwargs = dict(args._get_kwargs())
            if not command_kwargs.get("pipeline_name"):
                self.parser.print_usage()
                raise CliCommandException(error_code=-1)
            command_kwargs["operation"] = args.operation or "info"
            del command_kwargs["command"]
            del command_kwargs["list_pipelines"]
            tracked(**command_kwargs)


class SchemaCommand(SupportsCliCommand):
    command = "schema"
    help_string = "Shows, converts and upgrades schemas"
    docs_url = "https://dlthub.com/docs/reference/command-line-interface#dlt-schema"
    description = """
Loads, validates and prints out a dlt schema from a yaml or json file.
    """

    def configure_parser(self, parser: argparse.ArgumentParser) -> None:
        # keep here to avoid importing schema storages at cli startup
        from dlt.common.storages.configuration import SCHEMA_FILES_EXTENSIONS

        self.parser = parser

        parser.add_argument(
            "file",
            help="Schema file name, in yaml or json format, will autodetect based on extension",
        )
        parser.add_argument(
            "--format",
            choices=SCHEMA_FILES_EXTENSIONS,
            default="yaml",
            help="Display schema in this format",
        )
        parser.add_argument(
            "--remove-defaults",
            action="store_true",
            help="Does not show default hint values",
            default=True,
        )

    def execute(self, args: argparse.Namespace) -> None:
        # keep here to avoid importing schema/yaml at cli startup
        import yaml

        from dlt.common import json
        from dlt.common.schema.schema import Schema
        from dlt.common.typing import DictStrAny

        def schema_command_wrapper(
            file_path: str, format_: TSchemaFileFormat, remove_defaults: bool
        ) -> None:
            with open(file_path, "rb") as f:
                if os.path.splitext(file_path)[1][1:] == "json":
                    schema_dict: DictStrAny = json.load(f)
                else:
                    schema_dict = yaml.safe_load(f)
            s = Schema.from_dict(schema_dict)
            export = utils.fetch_schema_export(s, format_=format_, remove_defaults=remove_defaults)
            fmt.echo(export["content"])

        event = "local.schema" if fmt.get_cli_host_name() == "dlthub" else "schema"
        utils.track_command(event, False, "format_")(schema_command_wrapper)(
            args.file, args.format, args.remove_defaults
        )


class DashboardCommand(SupportsCliCommand):
    command = "dashboard"
    help_string = "Shows the dlthub workspace dashboard"
    description = """
This command shows the dlt workspace dashboard. You can use the dashboard:

* to list and inspect local pipelines
* browse the full pipeline schema and all hints
* browse the data in the destination
* inspect the pipeline state

    """

    def configure_parser(self, parser: argparse.ArgumentParser) -> None:
        self.parser = parser
        self.parser.add_argument(
            "--pipelines-dir", help="Pipelines working directory", default=None
        )
        self.parser.add_argument(
            "--edit",
            action="store_true",
            help="Eject Dashboard and start editable version",
            default=None,
        )

    def execute(self, args: argparse.Namespace) -> None:
        if not is_hub_available():
            return

        @utils.track_command("dashboard", True)
        def dashboard_command_wrapper(pipelines_dir: Optional[str], edit: bool) -> None:
            from dlt._workspace.helpers.dashboard.runner import run_dashboard

            run_dashboard(pipelines_dir=pipelines_dir, edit=edit)

        dashboard_command_wrapper(pipelines_dir=args.pipelines_dir, edit=args.edit)


class TelemetryCommand(SupportsCliCommand):
    command = "telemetry"
    help_string = "Shows telemetry status"
    docs_url = DLT_TELEMETRY_DOCS_URL
    description = """
Shows the current status of dlt telemetry. Learn more about telemetry and what we send in our telemetry docs.
    """

    def configure_parser(self, parser: argparse.ArgumentParser) -> None:
        self.parser = parser

    def execute(self, args: argparse.Namespace) -> None:
        from dlt._workspace.cli._telemetry_command import telemetry_status_command_wrapper

        event = "local.telemetry" if fmt.get_cli_host_name() == "dlthub" else "telemetry"
        utils.track_command(event, False)(telemetry_status_command_wrapper)()


class DeployCommand(SupportsCliCommand):
    command = "deploy"
    help_string = "Creates a deployment package for a selected pipeline script"
    docs_url = DLT_DEPLOY_DOCS_URL
    description = """
Prepares your pipeline for deployment and gives you step-by-step instructions on how to accomplish it. To enable this functionality, please first execute `pip install "dlt[cli]"` which adds additional packages to the current environment.
    """

    def configure_parser(self, parser: argparse.ArgumentParser) -> None:
        # keep here to avoid importing heavy deploy deps at cli startup
        from dlt._workspace.cli._deploy_command import (
            DeploymentMethods,
            COMMAND_DEPLOY_REPO_LOCATION,
            SecretFormats,
        )

        self.parser = parser
        deploy_cmd = parser
        deploy_comm = argparse.ArgumentParser(
            formatter_class=argparse.ArgumentDefaultsHelpFormatter, add_help=False
        )

        deploy_cmd.add_argument(
            "pipeline_script_path", metavar="pipeline-script-path", help="Path to a pipeline script"
        )

        deploy_comm.add_argument(
            "--location",
            default=COMMAND_DEPLOY_REPO_LOCATION,
            help="Advanced. Uses a specific url or local path to pipelines repository.",
        )
        deploy_comm.add_argument(
            "--branch",
            help="Advanced. Uses specific branch of the deploy repository to fetch the template.",
        )

        deploy_sub_parsers = deploy_cmd.add_subparsers(
            title="Available subcommands", dest="deployment_method"
        )

        # deploy github actions
        deploy_github_cmd = deploy_sub_parsers.add_parser(
            DeploymentMethods.github_actions.value,
            help="Deploys the pipeline to Github Actions",
            parents=[deploy_comm],
            description="""
Deploys the pipeline to GitHub Actions.

GitHub Actions (https://github.com/features/actions) is a CI/CD runner with a large free tier which you can use to run your pipelines.

You must specify when the GitHub Action should run using a cron schedule expression. The command also takes additional flags:
`--run-on-push` (default is False) and `--run-manually` (default is True). Remember to put the cron
schedule expression in quotation marks.

For the chess.com API example from our docs, you can deploy it with `dlt deploy chess.py github-action --schedule "*/30 * * * *"`.

Follow the guide on how to deploy a pipeline with GitHub Actions in our documentation for more information.
""",
        )
        deploy_github_cmd.add_argument(
            "--schedule",
            required=True,
            help=(
                "A schedule with which to run the pipeline, in cron format. Example: '*/30 * * * *'"
                " will run the pipeline every 30 minutes. Remember to enclose the scheduler"
                " expression in quotation marks!"
            ),
        )
        deploy_github_cmd.add_argument(
            "--run-manually",
            default=True,
            action="store_true",
            help="Allows the pipeline to be run manually form Github Actions UI.",
        )
        deploy_github_cmd.add_argument(
            "--run-on-push",
            default=False,
            action="store_true",
            help="Runs the pipeline with every push to the repository.",
        )

        # deploy airflow composer
        deploy_airflow_cmd = deploy_sub_parsers.add_parser(
            DeploymentMethods.airflow_composer.value,
            help="Deploys the pipeline to Airflow",
            parents=[deploy_comm],
            description="""
Google Composer (https://cloud.google.com/composer?hl=en) is a managed Airflow environment provided by Google. Follow the guide in our docs on how to deploy a pipeline with Airflow to learn more. This command will:


* create an Airflow DAG for your pipeline script that you can customize. The DAG uses
the `dlt` Airflow wrapper (https://github.com/dlt-hub/dlt/blob/devel/dlt/helpers/airflow_helper.py#L37) to make this process trivial.

* provide you with the environment variables and secrets that you must add to Airflow.

* provide you with a cloudbuild file to sync your GitHub repository with the `dag` folder of your Airflow Composer instance.
""",
        )
        deploy_airflow_cmd.add_argument(
            "--secrets-format",
            default=SecretFormats.toml.value,
            choices=[v.value for v in SecretFormats],
            required=False,
            help="Format of the secrets",
        )

    def execute(self, args: argparse.Namespace) -> None:
        # keep here: pipdeptree scans all installed packages on import
        try:
            import pipdeptree  # noqa: F401
            import cron_descriptor  # noqa: F401

            deploy_command_available = True
        except ImportError:
            deploy_command_available = False
        except Exception:
            # beartype import hook can break pip._vendor.distlib on Windows;
            # pipdeptree is installed but cannot be imported — let deploy proceed,
            # generate_pip_freeze will handle the failure gracefully
            deploy_command_available = True

        # exit if deploy command is not available
        if not deploy_command_available:
            fmt.warning(
                "Please install additional command line dependencies to use deploy command:"
            )
            fmt.secho('pip install "dlt[cli]"', bold=True)
            fmt.echo(
                "We ask you to install those dependencies separately to keep our core library small"
                " and make it work everywhere."
            )
            raise CliCommandException()

        deploy_args = vars(args)
        if deploy_args.get("deployment_method") is None:
            self.parser.print_help()
            raise CliCommandException()
        else:
            from dlt._workspace.cli._deploy_command import deploy_command_wrapper

            # global flags are not part of the deploy_command surface; strip before forwarding
            deploy_args.pop("verbosity", None)
            deploy_command_wrapper(
                pipeline_script_path=deploy_args.pop("pipeline_script_path"),
                deployment_method=deploy_args.pop("deployment_method"),
                repo_location=deploy_args.pop("location"),
                branch=deploy_args.pop("branch"),
                **deploy_args,
            )


def make_moved_to_dlthub_command(cmd: str, new_cmd: str) -> "type[SupportsCliCommand]":
    """Builds a stub `dlt <cmd>` that redirects users to `dlthub <new_cmd>`."""

    msg = f"`{cmd}` command moved to dlthub, pip install dlt[hub] and `dlthub {new_cmd}` to use"

    class _Moved(SupportsCliCommand):
        command = cmd
        help_string = f"Moved to `dlthub {new_cmd}` (run `pip install dlt[hub]`)"
        description = msg
        docs_url: Optional[str] = None

        def configure_parser(self, parser: argparse.ArgumentParser) -> None:
            parser.add_argument("args", nargs=argparse.REMAINDER, help=argparse.SUPPRESS)

        def execute(self, args: argparse.Namespace) -> None:
            fmt.warning(msg)
            raise CliCommandException()

    _Moved.__name__ = f"Moved_{cmd}_To_Dlthub_{new_cmd}"
    return _Moved
