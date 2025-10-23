import argparse
from io import BytesIO
from pathlib import Path
from typing import Union
import time
from uuid import UUID
from typing import Optional
from dlt._workspace.deployment.file_selector import WorkspaceFileSelector
from dlt._workspace.deployment.package_builder import DeploymentPackageBuilder
from dlt._workspace.runtime_clients.api.api.runs import create_run, get_run, get_run_logs, list_runs
from dlt._workspace.runtime_clients.api.api.scripts import create_or_update_script, get_script
from dlt._workspace.runtime_clients.api.models.create_deployment_body import CreateDeploymentBody
from dlt._workspace.runtime_clients.api.models.script_type import ScriptType
from dlt._workspace.runtime_clients.api.api.deployments import list_deployments
from dlt._workspace.runtime_clients.api.types import File
from dlt.common.configuration.plugins import SupportsCliCommand
from dlt._workspace.runtime_clients.api.models.detailed_run_response import DetailedRunResponse

from dlt._workspace._workspace_context import active
from dlt._workspace.exceptions import (
    LocalWorkspaceIdNotSet,
    RuntimeNotAuthenticated,
    WorkspaceIdMismatch,
)
from dlt._workspace.runtime import RuntimeAuthService, get_api_client, get_auth_client
from dlt._workspace.runtime_clients.auth.api.github import github_oauth_complete, github_oauth_start
from dlt._workspace.runtime_clients.api.api.deployments import create_deployment
from dlt._workspace.cli import echo as fmt


def _to_uuid(value: Union[str, UUID]) -> UUID:
    if isinstance(value, UUID):
        return value
    try:
        return UUID(value)
    except ValueError:
        raise RuntimeError(f"Invalid UUID: {value}")


class RuntimeCommand(SupportsCliCommand):
    command = "runtime"
    help_string = "Connect to dltHub Runtime and run your code remotely"
    description = """
    Allows to connect to the dltHub Runtime, deploy and run local workspaces there. Requires dltHub license.
    """

    def configure_parser(self, parser: argparse.ArgumentParser) -> None:
        self.parser = parser

        subparsers = parser.add_subparsers(
            title="Available subcommands", dest="runtime_command", required=False
        )

        subparsers.add_parser(
            "login",
            help="Login to the Runtime using Github OAuth",
            description="Login to the Runtime using Github OAuth",
        )

        subparsers.add_parser(
            "logout",
            help="Logout from the Runtime",
            description="Logout from the Runtime",
        )

        # deployments
        deploy_cmd = subparsers.add_parser(
            "deploy",
            help="Deploy local workspace to the Runtime",
            description="Deploy local workspace to the Runtime",
        )
        deploy_cmd.add_argument(
            "--list",
            "-l",
            action=argparse.BooleanOptionalAction,
            help="List all deployments in workspace",
        )

        # logs
        subparsers.add_parser(
            "logs",
            help="Convenience command to get the logs of the latest run of a script",
            description="Convenience command to get the logs of the latest run of a script",
        )

        # scripts
        script_cmd = subparsers.add_parser(
            "script",
            help="Create, run and inspect scripts in runtime",
            description="Manipulate scripts in workspace",
        )
        script_cmd.add_argument(
            "script_path_or_id", nargs="?", help="Local path to the script or id of deployed script"
        )

        script_subparsers = script_cmd.add_subparsers(
            title="Available subcommands", dest="operation", required=False
        )
        script_subparsers.add_parser(
            "run",
            help="Run a script in the Runtime",
            description="Run a script in the Runtime",
        )
        script_subparsers.add_parser(
            "logs",
            help="Get the logs of a script run in the Runtime",
            description="Get the logs of a script run in the Runtime",
        )
        script_subparsers.add_parser(
            "list-runs",
            help="List all runs of a script in the Runtime",
            description="List all runs of a script in the Runtime",
        )
        script_subparsers.add_parser(
            "status",
            help="Check the status of a script run in the Runtime",
            description="Check the status of a script run in the Runtime",
        )

        runs_cmd = subparsers.add_parser(
            "runs",
            help="Manipulate runs in workspace",
            description="Manipulate runs in workspace",
        )

        runs_cmd.add_argument("run_id", nargs="?", help="The run id query")
        runs_cmd.add_argument(
            "--list", "-l", action=argparse.BooleanOptionalAction, help="List all runs in workspace"
        )
        runs_subparsers = runs_cmd.add_subparsers(
            title="Available subcommands", dest="operation", required=False
        )

        # manage runs
        runs_subparsers.add_parser(
            "status",
            help="Check the status of a run",
            description="Check the status of a run",
        )

        runs_subparsers.add_parser(
            "logs",
            help="Get the logs of a run",
            description="Get the logs of a run",
        )

        runs_subparsers.add_parser(
            "list",
            help="List all runs in workspace",
            description="List all runs in workspace",
        )

    def execute(self, args: argparse.Namespace) -> None:
        if args.runtime_command == "login":
            login()
        elif args.runtime_command == "logout":
            logout()
        elif args.runtime_command == "deploy":
            if args.list:
                get_deployments()
            else:
                deploy()
        elif args.runtime_command == "logs":
            get_logs()
        elif args.runtime_command == "runs":
            if args.list:
                get_runs()
            elif args.operation == "status":
                check_status(run_id=args.run_id)
            elif args.operation == "logs":
                get_logs(run_id=args.run_id)
        elif args.runtime_command == "script":
            if args.operation == "run":
                run(args.script_path_or_id)
            elif args.operation == "logs":
                get_logs(script_id_or_name=args.script_path_or_id)
            elif args.operation == "status":
                check_status(script_id_or_name=args.script_path_or_id)
            elif args.operation == "list-runs":
                get_runs(script_id_or_name=args.script_path_or_id)
        else:
            self.parser.print_usage()


def login() -> None:
    auth_service = RuntimeAuthService(run_context=active())
    try:
        auth_info = auth_service.authenticate()
        fmt.echo("Already logged in as %s" % fmt.bold(auth_info.email))
        authorize(auth_service=auth_service)
    except RuntimeNotAuthenticated:
        fmt.echo("Logging in with Github OAuth")
        client = get_auth_client()

        # start device flow
        login_request = github_oauth_start.sync(client=client)
        fmt.echo(
            "Please go to %s and enter the code %s"
            % (fmt.bold(login_request.verification_uri), fmt.bold(login_request.user_code))
        )
        fmt.echo("Waiting for response from Github...")

        while True:
            time.sleep(login_request.interval)
            token_response = github_oauth_complete.sync(
                client=client,
                body=github_oauth_complete.GithubDeviceFlowLoginRequest(
                    device_code=login_request.device_code
                ),
            )
            if isinstance(token_response, github_oauth_complete.LoginResponse):
                auth_info = auth_service.login(token_response.jwt)
                fmt.echo("Logged in as %s" % fmt.bold(auth_info.email))
                authorize(auth_service=auth_service)
                break
            elif isinstance(token_response, github_oauth_complete.GithubOauthCompleteResponse400):
                raise RuntimeError("Failed to complete authentication with Github")


def logout() -> None:
    auth_service = RuntimeAuthService(run_context=active())
    auth_service.logout()
    fmt.echo("Logged out")


def authorize(auth_service: Optional[RuntimeAuthService] = None) -> RuntimeAuthService:
    if auth_service is None:
        auth_service = RuntimeAuthService(run_context=active())
        auth_service.authenticate()

    try:
        auth_service.authorize()
    except LocalWorkspaceIdNotSet:
        should_overwrite = fmt.confirm(
            "No workspace id found in local config. Do you want to connect local workspace to the"
            " remote one?",
            default=True,
        )
        if should_overwrite:
            auth_service.overwrite_local_workspace_id()
            fmt.echo("Using remote workspace id")
        else:
            raise RuntimeError("Local workspace is not connected to the remote one")
    except WorkspaceIdMismatch as e:
        fmt.warning(
            "Workspace id in local config (%s) is not the same as remote workspace id (%s)"
            % (e.local_workspace_id, e.remote_workspace_id)
        )
        should_overwrite = fmt.confirm(
            "Do you want to overwrite the local workspace id with the remote one?",
            default=True,
        )
        if should_overwrite:
            auth_service.overwrite_local_workspace_id()
            fmt.echo("Local workspace id overwritten with remote workspace id")
        else:
            raise RuntimeError("Unable to synchronise remote and local workspaces")
    fmt.echo("Authorized to workspace %s" % fmt.bold(auth_service.workspace_id))
    return auth_service


def deploy() -> None:
    auth_service = authorize()
    api_client = get_api_client(auth_service)

    output_stream = BytesIO()
    package_builder = DeploymentPackageBuilder(context=active())
    package_builder.write_package_to_stream(
        file_selector=WorkspaceFileSelector(active()), output_stream=output_stream
    )

    create_deployment_result = create_deployment.sync(
        workspace_id=_to_uuid(auth_service.workspace_id),
        client=api_client,
        body=CreateDeploymentBody(
            file=File(
                payload=output_stream, file_name="workspace.tar.gz", mime_type="application/x-tar"
            )
        ),
    )
    if isinstance(create_deployment_result, create_deployment.DeploymentResponse):
        fmt.echo(f"Deployment # {create_deployment_result.version} created successfully")
        fmt.echo(f"Deployment id: {create_deployment_result.id}")
        fmt.echo(f"File count: {create_deployment_result.file_count}")
        fmt.echo(f"Content hash: {create_deployment_result.content_hash}")

    else:
        raise RuntimeError("Failed to create deployment")


def run(script_file_name: str) -> None:
    auth_service = authorize()
    api_client = get_api_client(auth_service)

    script_path = Path(active().run_dir) / script_file_name
    if not script_path.exists():
        raise RuntimeError(f"Script file {script_file_name} not found")

    # NOTE: here we can check wether anything has changed and only update if it has
    create_script_result = create_or_update_script.sync(
        client=api_client,
        workspace_id=_to_uuid(auth_service.workspace_id),
        body=create_or_update_script.CreateScriptRequest(
            name=script_file_name,
            description=f"The {script_file_name} script",
            entry_point=script_file_name,
            script_type=ScriptType.BATCH,
        ),
    )
    if not isinstance(create_script_result, create_or_update_script.ScriptResponse):
        raise RuntimeError("Failed to create script")

    create_run_result = create_run.sync(
        client=api_client,
        workspace_id=_to_uuid(auth_service.workspace_id),
        body=create_run.CreateRunRequest(
            script_id_or_name=script_file_name,
        ),
    )
    if isinstance(create_run_result, create_run.RunResponse):
        fmt.echo(
            "Script %s run for script id %s successfully created"
            % (fmt.bold(str(script_file_name)), fmt.bold(str(create_run_result.id)))
        )
    else:
        raise RuntimeError("Failed to run script")


def check_status(run_id: Union[str, UUID] = None, script_id_or_name: str = None) -> None:
    auth_service = authorize()
    api_client = get_api_client(auth_service)

    assert run_id or script_id_or_name, "Either run_id or script_id_or_name must be provided"

    if script_id_or_name:
        run = _get_latest_run(script_id_or_name)
        run_id = run.id
    else:
        run_id = _to_uuid(run_id)

    get_run_result = get_run.sync(
        client=api_client,
        workspace_id=_to_uuid(auth_service.workspace_id),
        run_id=_to_uuid(run_id),
    )
    if isinstance(get_run_result, get_run.DetailedRunResponse):
        fmt.echo("Status of run %s" % fmt.bold(str(run_id)))
        fmt.echo("Status: %s" % fmt.bold(get_run_result.status))
        fmt.echo("Started at: %s" % fmt.bold(get_run_result.time_started))  # type: ignore[arg-type]
        fmt.echo("Ended at: %s" % fmt.bold(get_run_result.time_ended))  # type: ignore[arg-type]
        fmt.echo("Duration: %s seconds" % fmt.bold(get_run_result.duration))  # type: ignore[arg-type]
        fmt.echo("Triggered by: %s" % fmt.bold(get_run_result.triggered_by))  # type: ignore[arg-type]
        fmt.echo("Deployment id: %s" % fmt.bold(get_run_result.deployment_id))  # type: ignore[arg-type]
        fmt.echo("Profile: %s" % fmt.bold(get_run_result.profile.name))

    else:
        raise RuntimeError("Failed to get run status")


def get_script_logs(script_id_or_name: str) -> None:
    """Get the logs of the most recentscript run in the Runtime for this script"""
    auth_service = authorize()
    api_client = get_api_client(auth_service)

    script = get_script.sync(
        client=api_client,
        workspace_id=_to_uuid(auth_service.workspace_id),
        script_id_or_name=script_id_or_name,
    )
    if isinstance(script, get_script.ScriptResponse):
        fmt.echo(f"Script {script.name} found on runtime.")
        runs = list_runs.sync(
            client=api_client,
            workspace_id=_to_uuid(auth_service.workspace_id),
            script_id=script.id,
        )
        if isinstance(runs, list_runs.ListRunsResponse200):
            if not runs.items:
                fmt.echo("No runs executed in this workspace")
                return
            for run in runs.items:
                fmt.echo(
                    f"Run # {run.number} of script {run.script.name}, status: {run.status},"
                    f" profile: {run.profile.name}, started at {run.time_started}, ended at"
                    f" {run.time_ended}, run id: {run.id}"
                )
                get_logs(run_id=run.id)
                break
    else:
        raise RuntimeError("Failed to get script")


def _get_latest_run(script_id_or_name: str = None) -> DetailedRunResponse:
    """Get the latest run for a script or workspace if script is not provided"""
    auth_service = authorize()
    api_client = get_api_client(auth_service)

    if script_id_or_name:
        script = get_script.sync(
            client=api_client,
            workspace_id=_to_uuid(auth_service.workspace_id),
            script_id_or_name=script_id_or_name,
        )
        if isinstance(script, get_script.ScriptResponse):
            fmt.echo(f"Script {script.name} found on runtime.")
            runs = list_runs.sync(
                client=api_client,
                workspace_id=_to_uuid(auth_service.workspace_id),
                script_id=script.id,
                limit=1,
            )
            if isinstance(runs, list_runs.ListRunsResponse200):
                if not runs.items:
                    raise RuntimeError("No runs executed in for this script")
                else:
                    return runs.items[0]
            raise RuntimeError(
                "Failed to get runs for script with name or id %s" % script_id_or_name
            )
        else:
            raise RuntimeError("Failed to get script with name or id %s" % script_id_or_name)

    else:
        runs = list_runs.sync(
            client=api_client,
            workspace_id=_to_uuid(auth_service.workspace_id),
            limit=1,
        )
        if isinstance(runs, list_runs.ListRunsResponse200):
            if not runs.items:
                raise RuntimeError("No runs executed in this workspace")
            else:
                return runs.items[0]
        raise RuntimeError("Failed to get runs for workspace")


def get_logs(run_id: Union[str, UUID] = None, script_id_or_name: str = None) -> None:
    """Get logs for a run, for the latest run of a script or workspace if script is not provided"""
    auth_service = authorize()
    api_client = get_api_client(auth_service)

    if script_id_or_name:
        run = _get_latest_run(script_id_or_name)
        run_id = run.id
    elif run_id:
        run_id = _to_uuid(run_id)
    else:
        run = _get_latest_run()
        run_id = run.id

    get_run_logs_result = get_run_logs.sync(
        client=api_client,
        workspace_id=_to_uuid(auth_service.workspace_id),
        run_id=run_id,
    )
    if isinstance(get_run_logs_result, get_run_logs.LogsResponse):
        run = get_run_logs_result.run
        run_info = (
            f"Run # {run.number} of script {run.script.name}, status: {run.status}, run id:"
            f" {run.id}"
        )
        fmt.echo(f"========== Run logs for {run_info} ==========")
        fmt.echo(get_run_logs_result.logs)
        fmt.echo(f"========== End of run logs for {run_info} ==========")
    else:
        raise RuntimeError("Failed to get run logs.")


def get_runs(script_id_or_name: str = None) -> None:
    auth_service = authorize()
    api_client = get_api_client(auth_service)
    script_id = None
    if script_id_or_name:
        script = get_script.sync(
            client=api_client,
            workspace_id=_to_uuid(auth_service.workspace_id),
            script_id_or_name=script_id_or_name,
        )
        if isinstance(script, get_script.ScriptResponse):
            script_id = script.id
        else:
            raise RuntimeError(
                "Failed to get script with name or id %s from runtime. Did you create one?"
                % script_id_or_name
            )

    list_runs_result = list_runs.sync(
        client=api_client,
        script_id=script_id,
        workspace_id=_to_uuid(auth_service.workspace_id),
    )
    if isinstance(list_runs_result, list_runs.ListRunsResponse200):
        if not list_runs_result.items:
            fmt.echo("No runs executed in this workspace")
            return
        for run in reversed(list_runs_result.items):
            fmt.echo(
                f"Run # {run.number} of script {run.script.name}, status: {run.status}, profile:"
                f" {run.profile.name}, started at {run.time_started}, ended at {run.time_ended},"
                f" run id: {run.id}"
            )
    else:
        raise RuntimeError("Failed to list workspace runs")


def get_deployments() -> None:
    auth_service = authorize()
    api_client = get_api_client(auth_service)

    list_deployments_result = list_deployments.sync(
        client=api_client,
        workspace_id=_to_uuid(auth_service.workspace_id),
    )
    if isinstance(list_deployments_result, list_deployments.ListDeploymentsResponse200):
        if not list_deployments_result.items:
            fmt.echo("No deployments found in this workspace")
            return
        for deployment in reversed(list_deployments_result.items):
            fmt.echo(
                f"Deployment # {deployment.version}, created at: {deployment.date_added}, id:"
                f" {deployment.id}, file count: {deployment.file_count}, content hash:"
                f" {deployment.content_hash}"
            )
