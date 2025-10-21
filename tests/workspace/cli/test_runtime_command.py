import datetime
from contextlib import contextmanager
import os
import uuid
from typing import Any

import jwt as pyjwt
import pytest
from pytest_console_scripts import ScriptRunner
from unittest.mock import patch

from dlt._workspace._workspace_context import active as ws_active
from dlt._workspace.configuration import WorkspaceRuntimeConfiguration
from dlt.common.configuration.container import Container
from dlt.common.configuration.providers.toml import SecretsTomlProvider, ConfigTomlProvider
from dlt._workspace.runtime_clients.api.models.organization_response import (
    OrganizationResponse,
)
from dlt._workspace.runtime_clients.api.models.workspace_response import (
    WorkspaceResponse,
)
from dlt._workspace.runtime_clients.api.models.me_response import MeResponse

from dlt._workspace.runtime_clients.auth.api.github import github_oauth_complete
from dlt._workspace.runtime_clients.auth.models.github_device_flow_start_response import (
    GithubDeviceFlowStartResponse,
)
from dlt.common.configuration.specs.pluggable_run_context import PluggableRunContext


def reload_config_providers():
    Container()[PluggableRunContext].reload_providers()


def make_valid_jwt(email: str = "user@example.com", user_id: str | None = None) -> str:
    payload = {"email": email, "user_id": user_id or str(uuid.uuid4())}
    # signature will not be verified (verify_signature=False in runtime)
    return pyjwt.encode(payload, "secret", algorithm="HS256")


def build_me_response(default_workspace_id: uuid.UUID) -> Any:
    now = datetime.datetime.now(datetime.timezone.utc)
    org = OrganizationResponse(
        date_added=now,
        date_updated=now,
        id=uuid.uuid4(),
        name="org",
        description=None,
    )
    ws = WorkspaceResponse(
        date_added=now,
        date_updated=now,
        id=default_workspace_id,
        name="ws",
        description=None,
    )
    return MeResponse(default_organization=org, default_workspace=ws, email="user@example.com", identity_id=uuid.uuid4(), user_id=uuid.uuid4())


def get_token_from_secrets() -> str | None:
    ctx = ws_active()
    secrets = SecretsTomlProvider(settings_dir=ctx.global_dir)
    token, _ = secrets.get_value(
        "auth_token", str, None, WorkspaceRuntimeConfiguration.__section__
    )
    return token


def get_workspace_id_from_config() -> str | None:
    ctx = ws_active()
    cfg = ConfigTomlProvider(settings_dir=ctx.settings_dir)
    ws_id, _ = cfg.get_value("workspace_id", str, None, WorkspaceRuntimeConfiguration.__section__)
    return ws_id


def set_workspace_id_in_config(value: str) -> None:
    ctx = ws_active()
    # Ensure the directory and file exist
    os.makedirs(ctx.settings_dir, exist_ok=True)
    config_file = os.path.join(ctx.settings_dir, "config.toml")
    if not os.path.exists(config_file):
        with open(config_file, "w", encoding="utf-8") as f:
            f.write("")
    cfg = ConfigTomlProvider(settings_dir=ctx.settings_dir)
    cfg.set_value("workspace_id", value, None, WorkspaceRuntimeConfiguration.__section__)
    cfg.write_toml()


def set_token_in_secrets(value: str) -> None:
    ctx = ws_active()
    # Ensure the directory and file exist
    os.makedirs(ctx.global_dir, exist_ok=True)
    secrets_file = os.path.join(ctx.global_dir, "secrets.toml")
    if not os.path.exists(secrets_file):
        with open(secrets_file, "w", encoding="utf-8") as f:
            f.write("")
    secrets = SecretsTomlProvider(settings_dir=ctx.global_dir)
    secrets.set_value("auth_token", value, None, WorkspaceRuntimeConfiguration.__section__)
    secrets.write_toml()


@contextmanager
def oauth_success_ctx(token: str):
    with patch("dlt._workspace.cli._runtime_command.get_auth_client", return_value=object()), \
        patch(
            "dlt._workspace.cli._runtime_command.github_oauth_start.sync",
            return_value=GithubDeviceFlowStartResponse(
                device_code="dev_code",
                interval=0,
                user_code="USERCODE",
                verification_uri="https://verify",
            ),
        ), \
        patch(
            "dlt._workspace.cli._runtime_command.github_oauth_complete.sync",
            return_value=github_oauth_complete.LoginResponse(
                email="user@example.com", id=uuid.uuid4(), jwt=token
            ),
        ), \
        patch("dlt._workspace.cli._runtime_command.time.sleep", return_value=None):
            yield


@contextmanager
def oauth_failure_400_ctx():
    with patch("dlt._workspace.cli._runtime_command.get_auth_client", return_value=object()), \
        patch(
            "dlt._workspace.cli._runtime_command.github_oauth_start.sync",
            return_value=GithubDeviceFlowStartResponse(
                device_code="dev_code",
                interval=0,
                user_code="USERCODE",
                verification_uri="https://verify",
            ),
        ), \
        patch(
            "dlt._workspace.cli._runtime_command.github_oauth_complete.sync",
            return_value=github_oauth_complete.GithubOauthCompleteResponse400(
                detail="Bad Request", status_code=400, extra={}
            ),
        ), \
        patch("dlt._workspace.cli._runtime_command.time.sleep", return_value=None):
            yield


@contextmanager
def me_response_ctx(workspace_uuid: uuid.UUID):
    with patch("dlt._workspace.runtime.me.sync", return_value=build_me_response(workspace_uuid)):
        yield


def test_runtime_login_happy_path_inserts_token_and_workspace_id(
    script_runner: ScriptRunner
) -> None:
    token = make_valid_jwt()
    workspace_id = uuid.uuid4()

    with me_response_ctx(workspace_id), oauth_success_ctx(token):
        result = script_runner.run(["dlt", "runtime", "login"])

    assert result.returncode == 0
    assert "Logged in as" in result.stdout
    assert "Authorized to workspace" in result.stdout

    assert get_token_from_secrets() == token
    assert get_workspace_id_from_config() == str(workspace_id)


def test_runtime_login_api_exception_oauth_complete_400(
    script_runner: ScriptRunner
) -> None:
    with me_response_ctx(uuid.uuid4()), oauth_failure_400_ctx():
        result = script_runner.run(["dlt", "runtime", "login"])

    assert result.returncode == -1
    assert "Failed to complete authentication with Github" in result.stderr


def test_runtime_login_api_exception_me_failure(
    script_runner: ScriptRunner
) -> None:
    token = make_valid_jwt()
    # me returns unexpected value -> authorize raises
    with patch("dlt._workspace.runtime.me.sync", return_value=None), oauth_success_ctx(token):
        result = script_runner.run(["dlt", "runtime", "login"])

    assert result.returncode == -1
    assert "Failed to get me response" in result.stderr


def test_runtime_login_with_valid_existing_token_skips_oauth(script_runner: ScriptRunner) -> None:
    # prewrite valid token and matching workspace id
    token = make_valid_jwt(email="valid@example.com")
    workspace_id = uuid.uuid4()
    set_token_in_secrets(token)
    set_workspace_id_in_config(str(workspace_id))

    reload_config_providers()

    with me_response_ctx(workspace_id):
        with patch("dlt._workspace.cli._runtime_command.github_oauth_start.sync") as start_mock, patch(
            "dlt._workspace.cli._runtime_command.github_oauth_complete.sync"
        ) as complete_mock:
            result = script_runner.run(["dlt", "runtime", "login"])

    assert result.returncode == 0
    assert "Already logged in as" in result.stdout
    start_mock.assert_not_called()
    complete_mock.assert_not_called()


def test_runtime_login_with_invalid_existing_token_triggers_oauth(
    script_runner: ScriptRunner
) -> None:
    invalid_token = "not-a-jwt"
    set_token_in_secrets(invalid_token)
    workspace_id = uuid.uuid4()
    new_token = make_valid_jwt()

    with me_response_ctx(workspace_id), oauth_success_ctx(new_token):
        result = script_runner.run(["dlt", "runtime", "login"])

    assert result.returncode == 0
    assert get_token_from_secrets() == new_token
    assert get_workspace_id_from_config() == str(workspace_id)


def test_runtime_login_overwrites_mismatched_workspace_id(script_runner: ScriptRunner) -> None:
    # valid token exists
    token = make_valid_jwt()
    set_token_in_secrets(token)

    # local workspace_id differs -> should be overwritten after authorize
    local_ws = str(uuid.uuid4())
    remote_ws = uuid.uuid4()
    set_workspace_id_in_config(local_ws)

    reload_config_providers()

    with me_response_ctx(remote_ws):
        result = script_runner.run(["dlt", "runtime", "login"])

    assert result.returncode == 0
    assert get_workspace_id_from_config() == str(remote_ws)
    assert "Local workspace id overwritten with remote workspace id" in result.stdout


def test_runtime_login_overwrites_absent_workspace_id(script_runner: ScriptRunner) -> None:
    # valid token exists
    token = make_valid_jwt()
    set_token_in_secrets(token)

    # ensure workspace_id absent
    ctx = ws_active()
    cfg = ConfigTomlProvider(settings_dir=ctx.settings_dir)
    cfg.write_toml()  # ensure file exists but without key

    reload_config_providers()

    remote_ws = uuid.uuid4()
    with me_response_ctx(remote_ws):
        result = script_runner.run(["dlt", "runtime", "login"])

    assert result.returncode == 0
    assert get_workspace_id_from_config() == str(remote_ws)
    assert "Using remote workspace id" in result.stdout


def test_runtime_logout_deletes_token_but_keeps_workspace_id(
    script_runner: ScriptRunner
) -> None:
    # perform a full login first
    token = make_valid_jwt()
    workspace_id = uuid.uuid4()
    with me_response_ctx(workspace_id), oauth_success_ctx(token):
        login_res = script_runner.run(["dlt", "runtime", "login"])
        assert login_res.returncode == 0

    assert get_token_from_secrets() == token
    assert get_workspace_id_from_config() == str(workspace_id)

    # now logout
    result = script_runner.run(["dlt", "runtime", "logout"])
    assert result.returncode == 0
    assert get_token_from_secrets() is None
    # workspace_id remains
    assert get_workspace_id_from_config() == str(workspace_id)

