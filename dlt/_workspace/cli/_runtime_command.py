import argparse
import time
from typing import Optional
from dlt.common.configuration.plugins import SupportsCliCommand

from dlt._workspace._workspace_context import active
from dlt._workspace.exceptions import (
    LocalWorkspaceIdNotSet,
    RuntimeNotAuthenticated,
    WorkspaceIdMismatch,
)
from dlt._workspace.runtime import RuntimeAuthService, get_auth_client
from dlt._workspace.runtime_clients.auth.api.github import github_oauth_complete, github_oauth_start
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

    def execute(self, args: argparse.Namespace) -> None:
        if args.runtime_command == "login":
            login()
        elif args.runtime_command == "logout":
            logout()
        else:
            self.parser.print_usage()


def login() -> None:
    auth_service = RuntimeAuthService(run_context=active())
    try:
        auth_info = auth_service.authenticate()
        fmt.echo("Already logged in as %s" % fmt.bold(auth_info.email))
        connect(auth_service=auth_service)
    except RuntimeNotAuthenticated:
        fmt.echo("Logging in with Github OAuth")
        client = get_auth_client()

        # start device flow
        login_request = github_oauth_start.sync(client=client)
        if not isinstance(login_request, github_oauth_start.GithubDeviceFlowStartResponse):
            raise RuntimeError("Failed to log in with Github OAuth")
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
                connect(auth_service=auth_service)
                break
            elif isinstance(token_response, github_oauth_complete.ErrorResponse400):
                raise RuntimeError("Failed to complete authentication with Github")


def logout() -> None:
    auth_service = RuntimeAuthService(run_context=active())
    auth_service.logout()
    fmt.echo("Logged out")


def connect(auth_service: Optional[RuntimeAuthService] = None) -> None:
    if auth_service is None:
        auth_service = RuntimeAuthService(run_context=active())
        auth_service.authenticate()

    try:
        auth_service.connect()
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
