import argparse
from io import BytesIO
from pathlib import Path
import time
from typing import Optional
from dlt._workspace.deployment.file_selector import WorkspaceFileSelector
from dlt._workspace.deployment.package_builder import DeploymentPackageBuilder
from dlt._workspace.runtime_clients.api.api.runs import create_run, get_run, get_run_logs
from dlt._workspace.runtime_clients.api.api.scripts import create_or_update_script
from dlt._workspace.runtime_clients.api.models.create_deployment_body import CreateDeploymentBody
from dlt._workspace.runtime_clients.api.models.script_type import ScriptType
from dlt._workspace.runtime_clients.api.types import File
from dlt.common.configuration.plugins import SupportsCliCommand

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

        subparsers.add_parser(
            "deploy",
            help="Deploy local workspace to the Runtime",
            description="Deploy local workspace to the Runtime",
        )

        run_parser = subparsers.add_parser(
            "run",
            help="Run a script in the Runtime",
            description="Run a script in the Runtime",
        )
        run_parser.add_argument(
            "script_path",
            type=str,
            help="Path to the script to run in the Runtime",
        )

        status_parser = subparsers.add_parser(
            "status",
            help="Check the status of a run",
            description="Check the status of a run",
        )
        status_parser.add_argument(
            "run_id",
            type=str,
            help="The run id to query status for",
        )
        status_parser.add_argument(
            "--verbose",
            action=argparse.BooleanOptionalAction,
            help="Show detailed status output"
        )

        logs_parser = subparsers.add_parser(
            "logs",
            help="Get the logs of a run",
            description="Get the logs of a run",
        )
        logs_parser.add_argument(
            "run_id",
            type=str,
            help="The run id to fetch logs for",
        )

    def execute(self, args: argparse.Namespace) -> None:
        if args.runtime_command == "login":
            login()
        elif args.runtime_command == "logout":
            logout()
        elif args.runtime_command == "deploy":
            deploy()
        elif args.runtime_command == "run":
            run(args.script_path)
        elif args.runtime_command == "status":
            check_status(args.run_id, args.verbose)
        elif args.runtime_command == "logs":
            get_logs(args.run_id)
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
        file_selector=WorkspaceFileSelector(active()),
        output_stream=output_stream
    )

    create_deployment_result = create_deployment.sync(
        workspace_id=auth_service.workspace_id,
        client=api_client,
        body=CreateDeploymentBody(
            file=File(
                payload=output_stream,
                file_name="workspace.tar.gz",
                mime_type="application/x-tar"
            )
        )
    )
    if isinstance(create_deployment_result, create_deployment.DeploymentResponse):
        fmt.echo("Deployment created successfully")
    else:
        raise RuntimeError("Failed to create deployment")


def run(script_file_name: str) -> None:
    auth_service = authorize()
    api_client = get_api_client(auth_service)

    script_path = Path(active().run_dir) / script_file_name
    if not script_path.exists():
        raise RuntimeError(f"Script file {script_file_name} not found")

    create_script_result = create_or_update_script.sync(
        client=api_client,
        workspace_id=auth_service.workspace_id,
        body=create_or_update_script.CreateScriptRequest(
            name=script_file_name,
            description=f"The {script_file_name} script",
            entry_point="files/" + script_file_name,
            script_type=ScriptType.BATCH,
        ),
    )
    if not isinstance(create_script_result, create_or_update_script.ScriptResponse):
        raise RuntimeError("Failed to create script")

    create_run_result = create_run.sync(
        client=api_client,
        workspace_id=auth_service.workspace_id,
        body=create_run.CreateRunRequest(
            script_id_or_name=script_file_name,
        ),
    )
    if isinstance(create_run_result, create_run.RunResponse):
        fmt.echo("Script %s run successfully with run id %s" % (fmt.bold(script_file_name), fmt.bold(create_run_result.id)))
    else:
        raise RuntimeError("Failed to run script")


def check_status(run_id: str, verbose: bool = False) -> None:
    auth_service = authorize()
    api_client = get_api_client(auth_service)

    get_run_result = get_run.sync(
        client=api_client,
        workspace_id=auth_service.workspace_id,
        run_id=run_id,
    )
    if isinstance(get_run_result, get_run.DetailedRunResponse):
        if verbose:
            fmt.echo("Detailed run information")
            for key, value in get_run_result.to_dict().items():
                fmt.echo("%s: %s" % (fmt.bold(key), value))
        else:
            fmt.echo("Run status is %s" % fmt.bold(get_run_result.status))
    else:
        raise RuntimeError("Failed to get run status")


def get_logs(run_id: str) -> None:
    auth_service = authorize()
    api_client = get_api_client(auth_service)

    get_run_logs_result = get_run_logs.sync(
        client=api_client,
        workspace_id=auth_service.workspace_id,
        run_id=run_id,
    )
    if isinstance(get_run_logs_result, get_run_logs.LogsResponse):
        fmt.echo("========== Run logs: ==========")
        fmt.echo(get_run_logs_result.logs)
        fmt.echo("========== End of run logs: ==========")
    else:
        raise RuntimeError("Failed to get run logs")
