import argparse
import os
from typing import Optional

import yaml

from dlt.common import json
from dlt.common.schema.schema import Schema
from dlt.common.storages.configuration import SCHEMA_FILES_EXTENSIONS
from dlt.common.typing import DictStrAny

from dlt._workspace.cli import echo as fmt, utils
from dlt._workspace.cli import SupportsCliCommand, DEFAULT_VERIFIED_SOURCES_REPO
from dlt._workspace.cli.exceptions import CliCommandException
from dlt._workspace.cli.utils import add_mcp_arg_parser
from dlt._workspace.cli._ai_command import SUPPORTED_IDES
from dlt._workspace.cli._pipeline_command import DLT_PIPELINE_COMMAND_DOCS_URL
from dlt._workspace.cli._init_command import DLT_INIT_DOCS_URL
from dlt._workspace.cli._telemetry_command import DLT_TELEMETRY_DOCS_URL

from dlt._workspace.cli._deploy_command import (
    DeploymentMethods,
    COMMAND_DEPLOY_REPO_LOCATION,
    SecretFormats,
    DLT_DEPLOY_DOCS_URL,
)

try:
    import pipdeptree
    import cron_descriptor

    deploy_command_available = True
except ImportError:
    deploy_command_available = False


class InitCommand(SupportsCliCommand):
    command = "init"
    help_string = (
        "Creates a pipeline project in the current folder by adding existing verified source or"
        " creating a new one from template."
    )
    docs_url = DLT_INIT_DOCS_URL
    description = """
The `dlt init` command creates a new dlt pipeline script that loads data from `source` to `destination`. When you run the command, several things happen:

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
            "destination", nargs="?", help="Name of a destination ie. bigquery or redshift"
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
            else:
                init_command_wrapper(
                    args.source,
                    args.destination,
                    args.location,
                    args.branch,
                    args.eject,
                )


class PipelineCommand(SupportsCliCommand):
    command = "pipeline"
    help_string = "Operations on pipelines that were ran locally"
    docs_url = DLT_PIPELINE_COMMAND_DOCS_URL
    description = """
The `dlt pipeline` command provides a set of commands to inspect the pipeline working directory, tables, and data in the destination and check for problems encountered during data loading.
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
        pipe_cmd.add_argument(
            "--verbose",
            "-v",
            action="count",
            default=0,
            help="Provides more information for certain commands.",
            dest="verbosity",
        )

        pipeline_subparsers = pipe_cmd.add_subparsers(
            title="Available subcommands", dest="operation", required=False
        )

        pipe_cmd_sync_parent = argparse.ArgumentParser(add_help=False)
        pipe_cmd_sync_parent.add_argument(
            "--destination", help="Sync from this destination when local pipeline state is missing."
        )
        pipe_cmd_sync_parent.add_argument(
            "--dataset-name", help="Dataset name to sync from when local pipeline state is missing."
        )

        pipeline_subparsers.add_parser(
            "info",
            help="Displays state of the pipeline, use -v or -vv for more info",
            description="""
Displays the content of the working directory of the pipeline: dataset name, destination, list of
schemas, resources in schemas, list of completed and normalized load packages, and optionally a
pipeline state set by the resources during the extraction process.
""",
        )
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

Requires `marimo` to be installed in the current environment: `pip install marimo`. Use the --streamlit flag to launch the legacy streamlit app.
""",
        )
        show_cmd.add_argument(
            "--streamlit",
            default=False,
            action="store_true",
            help="Launch the legacy Streamlit dashboard instead of the new workspace dashboard. ",
        )
        show_cmd.add_argument(
            "--edit",
            default=False,
            action="store_true",
            help=(
                "Creates editable version of workspace dashboard in current directory if it does"
                " not exist there yet and launches it in edit mode. Will have no effect when using"
                " the streamlit flag."
            ),
        )
        pipeline_subparsers.add_parser(
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
        pipeline_subparsers.add_parser(
            "drop-pending-packages",
            help=(
                "Deletes all extracted and normalized packages including those that are partially"
                " loaded."
            ),
            description="""
Removes all extracted and normalized packages in the pipeline's working dir.
`dlt` keeps extracted and normalized load packages in the pipeline working directory. When the `run` method is called, it will attempt to normalize and load
pending packages first. The command above removes such packages. Note that **pipeline state** is not reverted to the state at which the deleted packages
were created. Using `dlt pipeline ... sync` is recommended if your destination supports state sync.
""",
        )
        pipeline_subparsers.add_parser(
            "sync",
            help=(
                "Drops the local state of the pipeline and resets all the schemas and restores it"
                " from destination. The destination state, data and schemas are left intact."
            ),
            description="""
This command will remove the pipeline working directory with all pending packages, not synchronized
state changes, and schemas and retrieve the last synchronized data from the destination. If you drop
the dataset the pipeline is loading to, this command results in a complete reset of the pipeline state.

In case of a pipeline without a working directory, the command may be used to create one from the
destination. In order to do that, you need to pass the dataset name and destination name to the CLI
and provide the credentials to connect to the destination (i.e., in `.dlt/secrets.toml`) placed in the
folder where you execute the `pipeline sync` command.
""",
            parents=[pipe_cmd_sync_parent],
        )
        pipeline_subparsers.add_parser(
            "trace",
            help="Displays last run trace, use -v or -vv for more info",
            description="""
Displays the trace of the last pipeline run containing the start date of the run, elapsed time, and the
same information for all the steps (`extract`, `normalize`, and `load`). If any of the steps failed,
you'll see the message of the exceptions that caused that problem. Successful `load` and `run` steps
will display the load info instead.
""",
        )
        pipe_cmd_schema = pipeline_subparsers.add_parser(
            "schema",
            help="Displays default schema",
            description="Displays the default schema for the selected pipeline.",
        )
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
messages from the destination. With the verbose flag set `dlt pipeline -v ...`, you can also see the
list of all tables and columns created at the destination during the loading of that package.
""",
        )
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
            "Launch MCP server attached to this pipeline in SSE transport mode",
            DEFAULT_PIPELINE_MCP_PORT,
        )

    def execute(self, args: argparse.Namespace) -> None:
        from dlt._workspace.cli._pipeline_command import pipeline_command_wrapper

        if (
            args.list_pipelines
            or args.operation == "list"
            or (not args.pipeline_name and not args.operation)
        ):
            # Always use max verbosity (1) for dlt pipeline list - show full details
            pipeline_command_wrapper("list", "-", args.pipelines_dir, 1)
        else:
            command_kwargs = dict(args._get_kwargs())
            if not command_kwargs.get("pipeline_name"):
                self.parser.print_usage()
                raise CliCommandException(error_code=-1)
            command_kwargs["operation"] = args.operation or "info"
            del command_kwargs["command"]
            del command_kwargs["list_pipelines"]
            pipeline_command_wrapper(**command_kwargs)


class SchemaCommand(SupportsCliCommand):
    command = "schema"
    help_string = "Shows, converts and upgrades schemas"
    docs_url = "https://dlthub.com/docs/reference/command-line-interface#dlt-schema"
    description = """
The `dlt schema` command will load, validate and print out a dlt schema: `dlt schema path/to/my_schema_file.yaml`.
    """

    def configure_parser(self, parser: argparse.ArgumentParser) -> None:
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
        @utils.track_command("schema", False, "format_")
        def schema_command_wrapper(file_path: str, format_: str, remove_defaults: bool) -> None:
            with open(file_path, "rb") as f:
                if os.path.splitext(file_path)[1][1:] == "json":
                    schema_dict: DictStrAny = json.load(f)
                else:
                    schema_dict = yaml.safe_load(f)
            s = Schema.from_dict(schema_dict)
            if format_ == "json":
                schema_str = s.to_pretty_json(remove_defaults=remove_defaults)
            elif format_ == "yaml":
                schema_str = s.to_pretty_yaml(remove_defaults=remove_defaults)
            elif format_ == "dbml":
                schema_str = s.to_dbml()
            elif format_ == "dot":
                schema_str = s.to_dot()
            elif format == "mermaid":
                schema_str = s.to_mermaid()
            else:
                schema_str = s.to_pretty_yaml(remove_defaults=remove_defaults)

            fmt.echo(schema_str)

        schema_command_wrapper(args.file, args.format, args.remove_defaults)


class DashboardCommand(SupportsCliCommand):
    command = "dashboard"
    help_string = "Starts the dlt workspace dashboard"
    description = """
The `dlt dashboard` command starts the dlt workspace dashboard. You can use the dashboard:

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
The `dlt telemetry` command shows the current status of dlt telemetry. Learn more about telemetry and what we send in our telemetry docs.
    """

    def configure_parser(self, parser: argparse.ArgumentParser) -> None:
        self.parser = parser

    def execute(self, args: argparse.Namespace) -> None:
        from dlt._workspace.cli._telemetry_command import telemetry_status_command_wrapper

        telemetry_status_command_wrapper()


class DeployCommand(SupportsCliCommand):
    command = "deploy"
    help_string = "Creates a deployment package for a selected pipeline script"
    docs_url = DLT_DEPLOY_DOCS_URL
    description = """
The `dlt deploy` command prepares your pipeline for deployment and gives you step-by-step instructions on how to accomplish it. To enable this functionality, please first execute `pip install "dlt[cli]"` which will add additional packages to the current environment.
    """

    def configure_parser(self, parser: argparse.ArgumentParser) -> None:
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
        from rich.markdown import Markdown

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

            deploy_command_wrapper(
                pipeline_script_path=deploy_args.pop("pipeline_script_path"),
                deployment_method=deploy_args.pop("deployment_method"),
                repo_location=deploy_args.pop("location"),
                branch=deploy_args.pop("branch"),
                **deploy_args,
            )


# NOTE: we should port this as a command or as a tool to the core repo
class CliDocsCommand(SupportsCliCommand):
    command = "render-docs"
    help_string = "Renders markdown version of cli docs"
    description = """
The `dlt render-docs` command renders markdown version of cli docs by parsing the argparse help output and generating a markdown file.
If you are reading this on the docs website, you are looking at the rendered version of the cli docs generated by this command.
    """

    def configure_parser(self, parser: argparse.ArgumentParser) -> None:
        self.parser = parser

        self.parser.add_argument("file_name", nargs=1, help="Output file name")

        self.parser.add_argument(
            "--commands",
            nargs="*",
            help="List of command names to render (optional)",
            default=None,
        )

        self.parser.add_argument(
            "--compare",
            default=False,
            action="store_true",
            help="Compare the changes and raise if output would be updated",
        )

    def execute(self, args: argparse.Namespace) -> None:
        from dlt._workspace.cli._dlt import _create_parser
        from dlt._workspace.cli._docs_command import render_argparse_markdown

        parser, _ = _create_parser()

        result = render_argparse_markdown("dlt", parser, commands=args.commands)

        if args.compare:
            with open(args.file_name[0], "r", encoding="utf-8") as f:
                if result != f.read():
                    fmt.error(
                        "Cli Docs out of date, please update, please run "
                        "update-cli-docs from the main Makefile and commit your changes. "
                    )
                    raise CliCommandException()

            fmt.note("Docs page up to date.")
        else:
            with open(args.file_name[0], "w", encoding="utf-8") as f:
                f.write(result)
            fmt.note("Docs page updated.")


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

        setup_cmd = ai_subparsers.add_parser(
            "setup",
            help="Generate IDE-specific configuration and rules files",
            description="""Get AI rules files and configuration into your local project for the selected IDE.
Files are fetched from https://github.com/dlt-hub/verified-sources by default.
""",
        )
        setup_cmd.add_argument("ide", choices=SUPPORTED_IDES, help="IDE to configure.")
        setup_cmd.add_argument(
            "--location",
            default=DEFAULT_VERIFIED_SOURCES_REPO,
            help="Advanced. Specify Git URL or local path to rules files and config.",
        )
        setup_cmd.add_argument(
            "--branch",
            default=None,
            help="Advanced. Specify Git branch to fetch rules files and config.",
        )
        # TODO support MCP-proxy configuration
        # ai_mcp_cmd = ai_subparsers.add_parser("mcp", help="Launch the dlt MCP server")

    def execute(self, args: argparse.Namespace) -> None:
        from dlt._workspace.cli._ai_command import ai_setup_command_wrapper

        ai_setup_command_wrapper(ide=args.ide, branch=args.branch, repo=args.location)


class WorkspaceCommand(SupportsCliCommand):
    command = "workspace"
    help_string = "Manage current Workspace"
    description = """
Commands to get info, cleanup local files and launch Workspace MCP. Run without command get
workspace info.
"""

    def configure_parser(self, parser: argparse.ArgumentParser) -> None:
        self.parser = parser

        parser.add_argument(
            "--verbose",
            "-v",
            action="count",
            default=0,
            help="Provides more information for certain commands.",
            dest="verbosity",
        )

        subparsers = parser.add_subparsers(
            title="Available subcommands", dest="workspace_command", required=False
        )

        # clean command
        clean_local_parser = subparsers.add_parser(
            "clean",
            help=(
                "Cleans local data for the selected profile. Locally loaded data will be deleted. "
                "Pipelines working directories are also deleted by default. Data in remote "
                "destinations is not affected."
            ),
        )
        clean_local_parser.add_argument(
            "--skip-data-dir",
            action="store_true",
            default=False,
            help="Do not delete pipelines working dir.",
        )

        subparsers.add_parser(
            "info",
            help="Displays workspace info.",
        )

        DEFAULT_DLT_MCP_PORT = 43654
        add_mcp_arg_parser(
            subparsers,
            "This MCP allows to attach to any pipeline that was previously ran in this workspace"
            " and then facilitates schema and data exploration in the pipeline's dataset.",
            "Launch dlt MCP server in current Python environment and Workspace in SSE transport"
            " mode by default.",
            DEFAULT_DLT_MCP_PORT,
        )

        show_parser = subparsers.add_parser(
            "show",
            help="Shows Workspace Dashboard for the pipelines and data in this workspace.",
        )

        show_parser.add_argument(
            "--edit",
            action="store_true",
            help="Eject Dashboard and start editable version",
            default=None,
        )

    def execute(self, args: argparse.Namespace) -> None:
        from dlt._workspace._workspace_context import active
        from dlt._workspace.cli._workspace_command import (
            print_workspace_info,
            clean_workspace,
            show_workspace,
            start_mcp,
        )

        workspace_context = active()

        if args.workspace_command == "info" or not args.workspace_command:
            print_workspace_info(workspace_context, args.verbosity)
        elif args.workspace_command == "clean":
            clean_workspace(workspace_context, args)
        elif args.workspace_command == "show":
            show_workspace(workspace_context, args.edit)
        elif args.workspace_command == "mcp":
            start_mcp(workspace_context, port=args.port, stdio=args.stdio)
        else:
            self.parser.print_usage()


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
        from dlt._workspace._workspace_context import active
        from dlt._workspace.cli._profile_command import (
            print_profile_info,
            list_profiles,
            pin_profile,
        )

        workspace_context = active()

        if args.profile_command == "info" or not args.profile_command:
            print_profile_info(workspace_context)
        elif args.profile_command == "list":
            list_profiles(workspace_context)
        elif args.profile_command == "pin":
            pin_profile(workspace_context, args.profile_name)
        else:
            self.parser.print_usage()
