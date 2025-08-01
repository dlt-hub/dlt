from typing import Any, Optional, Union
import yaml
import os
import click

from dlt.version import __version__
from dlt.common.json import json
from dlt.common.schema import Schema
from dlt.common.typing import DictStrAny

import dlt.cli.echo as fmt
from dlt.cli import utils
from dlt.pipeline.exceptions import CannotRestorePipelineException
from dlt.cli.exceptions import CliCommandException

from dlt.cli.init_command import (
    init_command,
    list_sources_command,
    list_destinations_command,
    DLT_INIT_DOCS_URL,
)
from dlt.cli.pipeline_command import pipeline_command, DLT_PIPELINE_COMMAND_DOCS_URL
from dlt.cli.telemetry_command import (
    DLT_TELEMETRY_DOCS_URL,
    change_telemetry_status_command,
    telemetry_status_command,
)
from dlt.cli.ai_command import ai_setup_command, TSupportedIde

try:
    from dlt.cli import deploy_command
    from dlt.cli.deploy_command import (
        PipelineWasNotRun,
    )
except ModuleNotFoundError:
    pass

DLT_DEPLOY_DOCS_URL = "https://dlthub.com/docs/walkthroughs/deploy-a-pipeline"


@utils.track_command("init", False, "source_name", "destination_type")
def init_command_wrapper(
    source_name: str,
    destination_type: str,
    repo_location: str,
    branch: str,
    eject_source: bool = False,
) -> None:
    init_command(
        source_name,
        destination_type,
        repo_location,
        branch,
        eject_source,
    )


@utils.track_command("list_sources", False)
def list_sources_command_wrapper(repo_location: str, branch: str) -> None:
    list_sources_command(repo_location, branch)


@utils.track_command("list_destinations", False)
def list_destinations_command_wrapper() -> None:
    list_destinations_command()


@utils.track_command("pipeline", True, "operation")
def pipeline_command_wrapper(
    operation: str, pipeline_name: str, pipelines_dir: str, verbosity: int, **command_kwargs: Any
) -> None:
    try:
        pipeline_command(operation, pipeline_name, pipelines_dir, verbosity, **command_kwargs)
    except CannotRestorePipelineException as ex:
        click.secho(str(ex), err=True, fg="red")
        click.secho(
            "Try command %s to restore the pipeline state from destination"
            % fmt.bold(f"dlt pipeline {pipeline_name} sync")
        )
        raise CliCommandException(error_code=-2)


@utils.track_command("deploy", False, "deployment_method")
def deploy_command_wrapper(
    pipeline_script_path: str,
    deployment_method: str,
    repo_location: str,
    branch: Optional[str] = None,
    **kwargs: Any,
) -> None:
    try:
        utils.ensure_git_command("deploy")
    except Exception as ex:
        click.secho(str(ex), err=True, fg="red")
        raise CliCommandException(error_code=-2)
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
        raise CliCommandException(error_code=-3, raiseable_exception=ex)
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
        raise CliCommandException(error_code=-4)
    except NoSuchPathError as path_ex:
        click.secho("The pipeline script does not exist\n%s" % str(path_ex), err=True, fg="red")
        raise CliCommandException(error_code=-5)


@utils.track_command("schema", False, "operation")
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
    else:
        schema_str = s.to_pretty_yaml(remove_defaults=remove_defaults)

    fmt.echo(schema_str)


@utils.track_command("dashboard", True)
def dashboard_command_wrapper() -> None:
    from dlt.helpers.dashboard.runner import run_dashboard

    run_dashboard()


@utils.track_command("telemetry", False)
def telemetry_status_command_wrapper() -> None:
    telemetry_status_command()


@utils.track_command("telemetry_switch", False, "enabled")
def telemetry_change_status_command_wrapper(enabled: bool) -> None:
    try:
        change_telemetry_status_command(enabled)
    except Exception as ex:
        raise CliCommandException(docs_url=DLT_TELEMETRY_DOCS_URL, raiseable_exception=ex)


@utils.track_command("ai_setup", False)
def ai_setup_command_wrapper(ide: TSupportedIde, branch: Union[str, None], repo: str) -> None:
    ai_setup_command(ide, location=repo, branch=branch)
