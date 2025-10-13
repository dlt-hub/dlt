import os
import argparse
import time

from dlt._workspace._workspace_context import WorkspaceRunContext, active
from dlt._workspace.profile import (
    BUILT_IN_PROFILES,
    get_profile_pin_file,
    read_profile_pin,
    save_profile_pin,
)
from dlt.cli import SupportsCliCommand, echo as fmt

from auth_client import Client
from auth_client.api.default import github_oauth_complete, github_oauth_start

from dlt.cli.config_toml_writer import WritableConfigValue, write_values
from dlt.common.configuration.providers.toml import SecretsTomlProvider
from dlt.common.configuration.specs.runtime_configuration import RuntimeConfiguration


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

        if args.profile_command == "login":
            self.login(workspace_run_context)
        elif args.profile_command == "logout":
            ...  # self.logout(workspace_context)
        else:
            self.parser.print_usage()

    def login(self, workspace_run_context: WorkspaceRunContext) -> None:
        client = Client(base_url=AUTH_BASE_URL, verify_ssl=False)

        # start device flow
        login_request = github_oauth_start.sync(client=client)

        print(
            f"Please go to {login_request.verification_uri} and enter the code {login_request.user_code}"
        )
        import runpy
        runpy.run_module("webbrowser")
        print("Waiting for response from github...")

        while True:
            time.sleep(login_request.interval)
            token_response = github_oauth_complete.sync(
                client=client,
                body=github_oauth_complete.GithubDeviceFlowLoginRequest(
                    device_code=login_request.device_code
                ),
            )
            if isinstance(token_response, github_oauth_complete.LoginResponse):
                print(f"Logged in as {token_response.email}")
                print(token_response)
                self._save_token(workspace_run_context, token_response)
                break
    
    def _save_token(self, workspace_run_context: WorkspaceRunContext, token_response: github_oauth_complete.LoginResponse) -> None:
        telemetry_value = [
            WritableConfigValue("dlthub_runtime_auth_token", str, token_response.jwt, (RuntimeConfiguration.__section__,))
        ]
        # write global config
        global_path = workspace_run_context.global_dir
        os.makedirs(global_path, exist_ok=True)
        secrets = SecretsTomlProvider(settings_dir=global_path)
        write_values(secrets._config_toml, telemetry_value, overwrite_existing=True)
        secrets.write_toml()
