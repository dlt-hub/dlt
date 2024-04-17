from typing import Any, Sequence, Optional
import yaml
import os
import argparse
import click

from dlt.version import __version__
from dlt.common.json import json
from dlt.common.schema import Schema
from dlt.common.typing import DictStrAny
from dlt.common.runners import Venv

import dlt.cli.echo as fmt
from dlt.cli import utils
from dlt.pipeline.exceptions import CannotRestorePipelineException

from dlt.cli.init_command import (
    init_command,
    list_verified_sources_command,
    DLT_INIT_DOCS_URL,
    DEFAULT_VERIFIED_SOURCES_REPO,
)
from dlt.cli.pipeline_command import pipeline_command, DLT_PIPELINE_COMMAND_DOCS_URL
from dlt.cli.telemetry_command import (
    DLT_TELEMETRY_DOCS_URL,
    change_telemetry_status_command,
    telemetry_status_command,
)

try:
    from dlt.cli import deploy_command
    from dlt.cli.deploy_command import (
        PipelineWasNotRun,
        DLT_DEPLOY_DOCS_URL,
        DeploymentMethods,
        COMMAND_DEPLOY_REPO_LOCATION,
        SecretFormats,
    )
except ModuleNotFoundError:
    pass


DEBUG_FLAG = False


def on_exception(ex: Exception, info: str) -> None:
    click.secho(str(ex), err=True, fg="red")
    fmt.note("Please refer to %s for further assistance" % fmt.bold(info))
    if DEBUG_FLAG:
        raise ex


@utils.track_command("init", False, "source_name", "destination_type")
def init_command_wrapper(
    source_name: str,
    destination_type: str,
    use_generic_template: bool,
    repo_location: str,
    branch: str,
) -> int:
    try:
        init_command(source_name, destination_type, use_generic_template, repo_location, branch)
    except Exception as ex:
        on_exception(ex, DLT_INIT_DOCS_URL)
        return -1
    return 0


@utils.track_command("list_sources", False)
def list_verified_sources_command_wrapper(repo_location: str, branch: str) -> int:
    try:
        list_verified_sources_command(repo_location, branch)
    except Exception as ex:
        on_exception(ex, DLT_INIT_DOCS_URL)
        return -1
    return 0


@utils.track_command("deploy", False, "deployment_method")
def deploy_command_wrapper(
    pipeline_script_path: str,
    deployment_method: str,
    repo_location: str,
    branch: Optional[str] = None,
    **kwargs: Any,
) -> int:
    try:
        utils.ensure_git_command("deploy")
    except Exception as ex:
        click.secho(str(ex), err=True, fg="red")
        return -1

    from git import InvalidGitRepositoryError, NoSuchPathError

    try:
        deploy_command.deploy_command(
            pipeline_script_path=pipeline_script_path,
            deployment_method=deployment_method,
            repo_location=repo_location,
            branch=branch,
            **kwargs,
        )
    except (CannotRestorePipelineException, PipelineWasNotRun) as ex:
        fmt.note(
            "You must run the pipeline locally successfully at least once in order to deploy it."
        )
        on_exception(ex, DLT_DEPLOY_DOCS_URL)
        return -2
    except InvalidGitRepositoryError:
        click.secho(
            "No git repository found for pipeline script %s." % fmt.bold(pipeline_script_path),
            err=True,
            fg="red",
        )
        fmt.note("If you do not have a repository yet, you can do either of:")
        fmt.note(
            "- Run the following command to initialize new repository: %s" % fmt.bold("git init")
        )
        fmt.note(
            "- Add your local code to Github as described here: %s"
            % fmt.bold(
                "https://docs.github.com/en/get-started/importing-your-projects-to-github/importing-source-code-to-github/adding-locally-hosted-code-to-github"
            )
        )
        fmt.note("Please refer to %s for further assistance" % fmt.bold(DLT_DEPLOY_DOCS_URL))
        return -3
    except NoSuchPathError as path_ex:
        click.secho("The pipeline script does not exist\n%s" % str(path_ex), err=True, fg="red")
        return -4
    except Exception as ex:
        on_exception(ex, DLT_DEPLOY_DOCS_URL)
        return -5
    return 0


@utils.track_command("pipeline", True, "operation")
def pipeline_command_wrapper(
    operation: str, pipeline_name: str, pipelines_dir: str, verbosity: int, **command_kwargs: Any
) -> int:
    try:
        pipeline_command(operation, pipeline_name, pipelines_dir, verbosity, **command_kwargs)
        return 0
    except CannotRestorePipelineException as ex:
        click.secho(str(ex), err=True, fg="red")
        click.secho(
            "Try command %s to restore the pipeline state from destination"
            % fmt.bold(f"dlt pipeline {pipeline_name} sync")
        )
        return -1
    except Exception as ex:
        on_exception(ex, DLT_PIPELINE_COMMAND_DOCS_URL)
        return -2


@utils.track_command("schema", False, "operation")
def schema_command_wrapper(file_path: str, format_: str, remove_defaults: bool) -> int:
    with open(file_path, "rb") as f:
        if os.path.splitext(file_path)[1][1:] == "json":
            schema_dict: DictStrAny = json.load(f)
        else:
            schema_dict = yaml.safe_load(f)
    s = Schema.from_dict(schema_dict)
    if format_ == "json":
        schema_str = json.dumps(s.to_dict(remove_defaults=remove_defaults), pretty=True)
    else:
        schema_str = s.to_pretty_yaml(remove_defaults=remove_defaults)
    print(schema_str)
    return 0


@utils.track_command("telemetry", False)
def telemetry_status_command_wrapper() -> int:
    try:
        telemetry_status_command()
    except Exception as ex:
        on_exception(ex, DLT_TELEMETRY_DOCS_URL)
        return -1
    return 0


@utils.track_command("telemetry_switch", False, "enabled")
def telemetry_change_status_command_wrapper(enabled: bool) -> int:
    try:
        change_telemetry_status_command(enabled)
    except Exception as ex:
        on_exception(ex, DLT_TELEMETRY_DOCS_URL)
        return -1
    return 0


ACTION_EXECUTED = False


def print_help(parser: argparse.ArgumentParser) -> None:
    if not ACTION_EXECUTED:
        parser.print_help()


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
        fmt.ALWAYS_CHOOSE_DEFAULT = True


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
        global DEBUG_FLAG
        # will show stack traces (and maybe more debug things)
        DEBUG_FLAG = True


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Creates, adds, inspects and deploys dlt pipelines.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
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
        "--non-interactive",
        action=NonInteractiveAction,
        help=(
            "Non interactive mode. Default choices are automatically made for confirmations and"
            " prompts."
        ),
    )
    parser.add_argument(
        "--debug", action=DebugAction, help="Displays full stack traces on exceptions."
    )
    subparsers = parser.add_subparsers(dest="command")

    init_cmd = subparsers.add_parser(
        "init",
        help=(
            "Creates a pipeline project in the current folder by adding existing verified source or"
            " creating a new one from template."
        ),
    )
    init_cmd.add_argument(
        "--list-verified-sources",
        "-l",
        default=False,
        action="store_true",
        help="List available verified sources",
    )
    init_cmd.add_argument(
        "source",
        nargs="?",
        help=(
            "Name of data source for which to create a pipeline. Adds existing verified source or"
            " creates a new pipeline template if verified source for your data source is not yet"
            " implemented."
        ),
    )
    init_cmd.add_argument(
        "destination", nargs="?", help="Name of a destination ie. bigquery or redshift"
    )
    init_cmd.add_argument(
        "--location",
        default=DEFAULT_VERIFIED_SOURCES_REPO,
        help="Advanced. Uses a specific url or local path to verified sources repository.",
    )
    init_cmd.add_argument(
        "--branch",
        default=None,
        help="Advanced. Uses specific branch of the init repository to fetch the template.",
    )
    init_cmd.add_argument(
        "--generic",
        default=False,
        action="store_true",
        help=(
            "When present uses a generic template with all the dlt loading code present will be"
            " used. Otherwise a debug template is used that can be immediately run to get familiar"
            " with the dlt sources."
        ),
    )

    # deploy command requires additional dependencies
    try:
        # make sure the name is defined
        _ = deploy_command
        deploy_comm = argparse.ArgumentParser(
            formatter_class=argparse.ArgumentDefaultsHelpFormatter, add_help=False
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

        deploy_cmd = subparsers.add_parser(
            "deploy", help="Creates a deployment package for a selected pipeline script"
        )
        deploy_cmd.add_argument(
            "pipeline_script_path", metavar="pipeline-script-path", help="Path to a pipeline script"
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
    except NameError:
        # create placeholder command
        deploy_cmd = subparsers.add_parser(
            "deploy",
            help=(
                'Install additional dependencies with pip install "dlt[cli]" to create deployment'
                " packages"
            ),
            add_help=False,
        )
        deploy_cmd.add_argument("--help", "-h", nargs="?", const=True)
        deploy_cmd.add_argument(
            "pipeline_script_path", metavar="pipeline-script-path", nargs=argparse.REMAINDER
        )

    schema = subparsers.add_parser("schema", help="Shows, converts and upgrades schemas")
    schema.add_argument(
        "file", help="Schema file name, in yaml or json format, will autodetect based on extension"
    )
    schema.add_argument(
        "--format", choices=["json", "yaml"], default="yaml", help="Display schema in this format"
    )
    schema.add_argument(
        "--remove-defaults", action="store_true", help="Does not show default hint values"
    )

    pipe_cmd = subparsers.add_parser(
        "pipeline", help="Operations on pipelines that were ran locally"
    )
    pipe_cmd.add_argument(
        "--list-pipelines", "-l", default=False, action="store_true", help="List local pipelines"
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
        help="Generates and launches Streamlit app with the loading status and dataset explorer",
    )
    pipeline_subparsers.add_parser(
        "failed-jobs",
        help=(
            "Displays information on all the failed loads in all completed packages, failed jobs"
            " and associated error messages"
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
            "Drops the local state of the pipeline and resets all the schemas and restores it from"
            " destination. The destination state, data and schemas are left intact."
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
        "--remove-defaults", action="store_true", help="Does not show default hint values"
    )

    pipe_cmd_drop = pipeline_subparsers.add_parser(
        "drop",
        help="Selectively drop tables and reset state",
        parents=[pipe_cmd_sync_parent],
        epilog=(
            f"See {DLT_PIPELINE_COMMAND_DOCS_URL}#selectively-drop-tables-and-reset-state for more"
            " info"
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

    subparsers.add_parser("telemetry", help="Shows telemetry status")

    args = parser.parse_args()

    if Venv.is_virtual_env() and not Venv.is_venv_activated():
        fmt.warning(
            "You are running dlt installed in the global environment, however you have virtual"
            " environment activated. The dlt command will not see dependencies from virtual"
            " environment. You should uninstall the dlt from global environment and install it in"
            " the current virtual environment instead."
        )

    if args.command == "schema":
        return schema_command_wrapper(args.file, args.format, args.remove_defaults)
    elif args.command == "pipeline":
        if args.list_pipelines:
            return pipeline_command_wrapper("list", "-", args.pipelines_dir, args.verbosity)
        else:
            command_kwargs = dict(args._get_kwargs())
            if not command_kwargs.get("pipeline_name"):
                pipe_cmd.print_usage()
                return -1
            command_kwargs["operation"] = args.operation or "info"
            del command_kwargs["command"]
            del command_kwargs["list_pipelines"]
            return pipeline_command_wrapper(**command_kwargs)
    elif args.command == "init":
        if args.list_verified_sources:
            return list_verified_sources_command_wrapper(args.location, args.branch)
        else:
            if not args.source or not args.destination:
                init_cmd.print_usage()
                return -1
            else:
                return init_command_wrapper(
                    args.source, args.destination, args.generic, args.location, args.branch
                )
    elif args.command == "deploy":
        try:
            deploy_args = vars(args)
            return deploy_command_wrapper(
                pipeline_script_path=deploy_args.pop("pipeline_script_path"),
                deployment_method=deploy_args.pop("deployment_method"),
                repo_location=deploy_args.pop("location"),
                branch=deploy_args.pop("branch"),
                **deploy_args,
            )
        except (NameError, KeyError):
            fmt.warning(
                "Please install additional command line dependencies to use deploy command:"
            )
            fmt.secho('pip install "dlt[cli]"', bold=True)
            fmt.echo(
                "We ask you to install those dependencies separately to keep our core library small"
                " and make it work everywhere."
            )
            return -1
    elif args.command == "telemetry":
        return telemetry_status_command_wrapper()
    else:
        print_help(parser)
        return -1


def _main() -> None:
    """Script entry point"""
    exit(main())


if __name__ == "__main__":
    exit(main())
