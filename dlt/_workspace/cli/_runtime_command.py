
import argparse
import time
from typing import Optional

from dlt._workspace._workspace_context import active
from dlt._workspace.auth import AuthService
from dlt._workspace.exceptions import RuntimeNotAuthenticated, WorkspaceIdMismatch, LocalWorkspaceIdNotSet
from dlt._workspace.runtime_clients import AUTH_BASE_URL
from dlt.cli import SupportsCliCommand, echo as fmt

from dlt._workspace.runtime_clients.auth import Client as AuthClient
from dlt._workspace.runtime_clients.auth.api.github import github_oauth_complete, github_oauth_start



class RuntimeCommand(SupportsCliCommand):
    command = "runtime"
    help_string = "Connect to Runtime and manage your remote Workspaces"
    description = """"""

    def configure_parser(self, parser: argparse.ArgumentParser) -> None:
        self.parser = parser

        subparsers = parser.add_subparsers(
            title="Available subcommands", dest="profile_command", required=False
        )

        subparsers.add_parser(
            "login",
            help="Login to the Runtime usin Github OAuth",
            description="Login to the Runtime usin Github OAuth",
        )

        subparsers.add_parser(
            "logout",
            help="Logout from the Runtime",
            description="Logout from the Runtime",
        )


    def execute(self, args: argparse.Namespace) -> None:
        if args.profile_command == "login":
            login()
        elif args.profile_command == "logout":
            logout()
        else:
            self.parser.print_usage()


def login() -> None:
    auth_service = AuthService(run_context=active())
    try:
        auth_info = auth_service.authenticate()
        fmt.echo("Already logged in as %s" % fmt.bold(auth_info.email))
        authorise(auth_service=auth_service)
    except RuntimeNotAuthenticated:
        fmt.echo("Logging in with Github OAuth")
        client = AuthClient(base_url=AUTH_BASE_URL, verify_ssl=False)

        # start device flow
        login_request = github_oauth_start.sync(client=client)

        fmt.echo(
            "Please go to %s and enter the code %s" % (
                fmt.bold(login_request.verification_uri), 
                fmt.bold(login_request.user_code)
            )
        )
        fmt.echo("Waiting for response from github...")

        while True:
            time.sleep(login_request.interval)
            token_response = github_oauth_complete.sync(
                client=client,
                body=github_oauth_complete.GithubDeviceFlowLoginRequest(
                    device_code=login_request.device_code
                ),
            )
            # TODO: handle possible errors
            if isinstance(token_response, github_oauth_complete.LoginResponse):
                auth_info = auth_service.login(token_response.jwt)
                fmt.echo("Logged in as %s" % fmt.bold(auth_info.email))
                authorise(auth_service=auth_service)
                break


def logout() -> None:
    auth_service = AuthService(run_context=active())
    auth_service.logout()
    fmt.echo("Logged out")


def authorise(auth_service: Optional[AuthService] = None) -> None:
    if auth_service is None:
        auth_service = AuthService(run_context=active())
        auth_service.authenticate()   

    try:
        auth_service.authorise()
    except LocalWorkspaceIdNotSet:
        fmt.echo(f"No workspace id found in local config, using default remote workspace")
        auth_service.overwrite_local_workspace_id()
    except WorkspaceIdMismatch as e:
        fmt.warning("Workspace id in local config (%s) is not the same as remote workspace id (%s)" % (e.local_workspace_id, e.remote_workspace_id))
        should_overwrite = fmt.prompt(
            "Do you want to overwrite the local workspace id with the remote one?",
            choices=['yes', 'no'],
            default="yes",
        )
        if should_overwrite == "yes":
            auth_service.overwrite_local_workspace_id()
            fmt.echo("Local workspace id overwritten with remote workspace id")
        else:
            fmt.warning("Unable to synchronise remote and local workspaces")
            exit()
    fmt.echo("Authorised to workspace %s" % fmt.bold(auth_service.workspace_id))
