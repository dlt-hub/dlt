from typing import Type

import argparse
import dlt.cli.echo as fmt


from dlt.common.configuration import plugins
from dlt.cli import SupportsCliCommand
from dlt.cli.init_command import (
    DEFAULT_VERIFIED_SOURCES_REPO,
)
from dlt.cli.exceptions import CliCommandException
from dlt.cli.command_wrappers import (
    init_command_wrapper,
    list_sources_command_wrapper,
    pipeline_command_wrapper,
    schema_command_wrapper,
    telemetry_status_command_wrapper,
    deploy_command_wrapper,
)
from dlt.cli.command_wrappers import (
    DLT_PIPELINE_COMMAND_DOCS_URL,
    DLT_INIT_DOCS_URL,
    DLT_TELEMETRY_DOCS_URL,
    DLT_DEPLOY_DOCS_URL,
)

try:
    from dlt.cli.deploy_command import (
        DeploymentMethods,
        COMMAND_DEPLOY_REPO_LOCATION,
        SecretFormats,
    )

    deploy_command_available = True
except ModuleNotFoundError:
    deploy_command_available = False


@plugins.hookspec()
def plug_cli() -> SupportsCliCommand:
    """Spec for plugin hook that returns current run context."""


class InitCommand(SupportsCliCommand):
    command = "init"
    help_string = (
        "Creates a pipeline project in the current folder by adding existing verified source or"
        " creating a new one from template."
    )
    docs_url = DLT_INIT_DOCS_URL

    def configure_parser(self, parser: argparse.ArgumentParser) -> None:
        self.parser = parser

        parser.add_argument(
            "--list-sources",
            "-l",
            default=False,
            action="store_true",
            help="List available sources",
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
            help="Advanced. Uses specific branch of the init repository to fetch the template.",
        )

        parser.add_argument(
            "--omit-core-sources",
            default=False,
            action="store_true",
            help=(
                "When present, will not create the new pipeline with a core source of the given"
                " name but will take a source of this name from the default or provided"
                " location."
            ),
        )

    def execute(self, args: argparse.Namespace) -> None:
        if args.list_sources:
            list_sources_command_wrapper(args.location, args.branch)
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
                    args.omit_core_sources,
                )


class PipelineCommand(SupportsCliCommand):
    command = "pipeline"
    help_string = "Operations on pipelines that were ran locally"
    docs_url = DLT_PIPELINE_COMMAND_DOCS_URL

    def configure_parser(self, pipe_cmd: argparse.ArgumentParser) -> None:
        self.parser = pipe_cmd

        pipe_cmd.add_argument(
            "--list-pipelines",
            "-l",
            default=False,
            action="store_true",
            help="List local pipelines",
        )
        pipe_cmd.add_argument(
            "--hot-reload",
            default=False,
            action="store_true",
            help="Reload streamlit app (for core development)",
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

        pipeline_subparsers = pipe_cmd.add_subparsers(dest="operation", required=False)

        pipe_cmd_sync_parent = argparse.ArgumentParser(add_help=False)
        pipe_cmd_sync_parent.add_argument(
            "--destination", help="Sync from this destination when local pipeline state is missing."
        )
        pipe_cmd_sync_parent.add_argument(
            "--dataset-name", help="Dataset name to sync from when local pipeline state is missing."
        )

        pipeline_subparsers.add_parser(
            "info", help="Displays state of the pipeline, use -v or -vv for more info"
        )
        pipeline_subparsers.add_parser(
            "show",
            help=(
                "Generates and launches Streamlit app with the loading status and dataset explorer"
            ),
        )
        pipeline_subparsers.add_parser(
            "failed-jobs",
            help=(
                "Displays information on all the failed loads in all completed packages, failed"
                " jobs and associated error messages"
            ),
        )
        pipeline_subparsers.add_parser(
            "drop-pending-packages",
            help=(
                "Deletes all extracted and normalized packages including those that are partially"
                " loaded."
            ),
        )
        pipeline_subparsers.add_parser(
            "sync",
            help=(
                "Drops the local state of the pipeline and resets all the schemas and restores it"
                " from destination. The destination state, data and schemas are left intact."
            ),
            parents=[pipe_cmd_sync_parent],
        )
        pipeline_subparsers.add_parser(
            "trace", help="Displays last run trace, use -v or -vv for more info"
        )
        pipe_cmd_schema = pipeline_subparsers.add_parser("schema", help="Displays default schema")
        pipe_cmd_schema.add_argument(
            "--format",
            choices=["json", "yaml"],
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
            "load-package", help="Displays information on load package, use -v or -vv for more info"
        )
        pipe_cmd_package.add_argument(
            "load_id",
            metavar="load-id",
            nargs="?",
            help="Load id of completed or normalized package. Defaults to the most recent package.",
        )

    def execute(self, args: argparse.Namespace) -> None:
        if args.list_pipelines:
            pipeline_command_wrapper("list", "-", args.pipelines_dir, args.verbosity)
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

    def configure_parser(self, parser: argparse.ArgumentParser) -> None:
        self.parser = parser

        parser.add_argument(
            "file",
            help="Schema file name, in yaml or json format, will autodetect based on extension",
        )
        parser.add_argument(
            "--format",
            choices=["json", "yaml"],
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
        schema_command_wrapper(args.file, args.format, args.remove_defaults)


class TelemetryCommand(SupportsCliCommand):
    command = "telemetry"
    help_string = "Shows telemetry status"
    docs_url = DLT_TELEMETRY_DOCS_URL

    def configure_parser(self, parser: argparse.ArgumentParser) -> None:
        self.parser = parser

    def execute(self, args: argparse.Namespace) -> None:
        telemetry_status_command_wrapper()


class DeployCommand(SupportsCliCommand):
    command = "deploy"
    help_string = "Creates a deployment package for a selected pipeline script"
    docs_url = DLT_DEPLOY_DOCS_URL

    def configure_parser(self, parser: argparse.ArgumentParser) -> None:
        self.parser = parser
        deploy_cmd = parser
        deploy_comm = argparse.ArgumentParser(
            formatter_class=argparse.ArgumentDefaultsHelpFormatter, add_help=False
        )

        deploy_cmd.add_argument(
            "pipeline_script_path", metavar="pipeline-script-path", help="Path to a pipeline script"
        )

        if not deploy_command_available:
            return

        deploy_comm.add_argument(
            "--location",
            default=COMMAND_DEPLOY_REPO_LOCATION,
            help="Advanced. Uses a specific url or local path to pipelines repository.",
        )
        deploy_comm.add_argument(
            "--branch",
            help="Advanced. Uses specific branch of the deploy repository to fetch the template.",
        )

        deploy_sub_parsers = deploy_cmd.add_subparsers(dest="deployment_method")

        # deploy github actions
        deploy_github_cmd = deploy_sub_parsers.add_parser(
            DeploymentMethods.github_actions.value,
            help="Deploys the pipeline to Github Actions",
            parents=[deploy_comm],
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
            deploy_command_wrapper(
                pipeline_script_path=deploy_args.pop("pipeline_script_path"),
                deployment_method=deploy_args.pop("deployment_method"),
                repo_location=deploy_args.pop("location"),
                branch=deploy_args.pop("branch"),
                **deploy_args,
            )


#
# Register all commands
#
@plugins.hookimpl(specname="plug_cli")
def plug_cli_init() -> Type[SupportsCliCommand]:
    return InitCommand


@plugins.hookimpl(specname="plug_cli")
def plug_cli_pipeline() -> Type[SupportsCliCommand]:
    return PipelineCommand


@plugins.hookimpl(specname="plug_cli")
def plug_cli_schema() -> Type[SupportsCliCommand]:
    return SchemaCommand


@plugins.hookimpl(specname="plug_cli")
def plug_cli_telemetry() -> Type[SupportsCliCommand]:
    return TelemetryCommand


@plugins.hookimpl(specname="plug_cli")
def plug_cli_deploy() -> Type[SupportsCliCommand]:
    return DeployCommand
