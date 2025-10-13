
import argparse
import time

from dlt._workspace._workspace_context import active
from dlt._workspace.auth import AuthService
from dlt._workspace.exceptions import RuntimeNotAuthenticated
from dlt.cli import SupportsCliCommand, echo as fmt

from auth_client import Client
from auth_client.api.default import github_oauth_complete, github_oauth_start


AUTH_BASE_URL = "http://127.0.0.1:30001"


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
        workspace_run_context = active()
        auth_service = AuthService(workspace_run_context)

        if args.profile_command == "login":
            self.login(auth_service)
        elif args.profile_command == "logout":
            self.logout(auth_service)
        else:
            self.parser.print_usage()

    def login(self, auth_service: AuthService) -> None:
        try:
            auth_info = auth_service.authenticate()
            print(f"Already logged in as {auth_info.email}")
        except RuntimeNotAuthenticated:
            print("Logging in with Github OAuth")
            client = Client(base_url=AUTH_BASE_URL, verify_ssl=False)

            # start device flow
            login_request = github_oauth_start.sync(client=client)

            print(
                f"Please go to {login_request.verification_uri} and enter the code {login_request.user_code}"
            )
            print("Waiting for response from github...")

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
                    auth_info = auth_service.save_token(token_response)

                    print(f"Logged in as {auth_info.email}")
                    break
    
    def logout(self, auth_service: AuthService) -> None:
        auth_service.delete_token()
        print("Logged out")
