import datetime
import uuid
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from pytest_console_scripts import ScriptRunner

from dlt._workspace.cli import _runtime_command as commands
from dlt._workspace.runtime_clients.api.models.deployment_response import DeploymentResponse
from dlt._workspace.runtime_clients.api.models.list_deployments_response_200 import (
    ListDeploymentsResponse200,
)


def assert_deployment_headers(out: str) -> None:
    for h in ["Version #", "Created at", "File count", "Content hash"]:
        assert h in out


def assert_deployment_values(out: str, dep: "DeploymentResponse") -> None:
    assert str(dep.version) in out
    assert dep.date_added.isoformat() in out
    assert str(dep.file_count) in out
    assert dep.content_hash in out


def assert_out_order(out: str, first: str, second: str) -> None:
    assert out.find(first) < out.find(second)


DEPLOYMENT_1 = DeploymentResponse(
    content_hash="abc123contenthash",
    created_by=uuid.UUID("11111111-1111-1111-1111-111111111111"),
    date_added=datetime.datetime(2024, 6, 1, 12, 0, 0),
    date_updated=datetime.datetime(2024, 6, 1, 13, 0, 0),
    file_count=4,
    file_names="main.py, requirements.txt, loader.py, config.yaml",
    id=uuid.UUID("22222222-2222-2222-2222-222222222222"),
    size=1024 * 1024,
    version=1,
    workspace_id=uuid.UUID("33333333-3333-3333-3333-333333333333"),
)

DEPLOYMENT_2 = DeploymentResponse(
    content_hash="def456contenthash",
    created_by=uuid.UUID("11111111-1111-1111-1111-111111111111"),
    date_added=datetime.datetime(2024, 6, 1, 12, 0, 0),
    date_updated=datetime.datetime(2024, 6, 1, 13, 0, 0),
    file_count=4,
    file_names="main.py, requirements.txt, loader.py, config.yaml",
    id=uuid.UUID("22222222-2222-2222-2222-222222222222"),
    size=1024 * 1024,
    version=2,
    workspace_id=uuid.UUID("33333333-3333-3333-3333-333333333333"),
)

DEPLOYMENTS = [DEPLOYMENT_1, DEPLOYMENT_2]

_WORKSPACE_ID = uuid.UUID("44444444-4444-4444-4444-444444444444")


@pytest.fixture(autouse=True)
def stub_login_and_client():
    from dlt._workspace.cli import _runtime_command as commands

    class _Auth:
        workspace_id = str(_WORKSPACE_ID)

    with (
        patch.object(commands, "login", return_value=_Auth()),
        patch.object(commands, "get_api_client", return_value=object()),
    ):
        yield


def test_runtime_deployment_list_outputs_all(script_runner: ScriptRunner) -> None:
    response = ListDeploymentsResponse200(items=DEPLOYMENTS)
    with patch.object(
        commands.list_deployments, "sync_detailed", return_value=SimpleNamespace(parsed=response)
    ) as sync_detailed_mock:
        result = script_runner.run(["dlt", "runtime", "deployment", "list"])

    assert result.returncode == 0

    out = result.stdout
    assert_deployment_headers(out)
    # Values for latest (v2)
    assert_deployment_values(out, DEPLOYMENT_2)
    # Values for previous (v1)
    assert_deployment_values(out, DEPLOYMENT_1)
    # Order should be latest first (because CLI reverses the list)
    assert_out_order(out, DEPLOYMENT_2.content_hash, DEPLOYMENT_1.content_hash)

    kwargs = sync_detailed_mock.call_args.kwargs
    assert str(kwargs["workspace_id"]) == str(_WORKSPACE_ID)
    assert "client" in kwargs


def test_runtime_deployment_info_latest(script_runner: ScriptRunner) -> None:
    with patch.object(
        commands.get_latest_deployment,
        "sync_detailed",
        return_value=SimpleNamespace(parsed=DEPLOYMENT_2),
    ) as sync_detailed_mock:
        result = script_runner.run(["dlt", "runtime", "deployment", "info"])

    assert result.returncode == 0
    out = result.stdout
    assert_deployment_headers(out)
    assert_deployment_values(out, DEPLOYMENT_2)

    kwargs = sync_detailed_mock.call_args.kwargs
    assert str(kwargs["workspace_id"]) == str(_WORKSPACE_ID)
    assert "deployment_id_or_version" not in kwargs
    assert "client" in kwargs


def test_runtime_deployment_info_version_1_by_version_number(script_runner: ScriptRunner) -> None:
    with patch.object(
        commands.get_deployment, "sync_detailed", return_value=SimpleNamespace(parsed=DEPLOYMENT_1)
    ) as sync_detailed_mock:
        result = script_runner.run(
            ["dlt", "runtime", "deployment", str(DEPLOYMENT_1.version), "info"]
        )

    assert result.returncode == 0
    out = result.stdout
    assert_deployment_headers(out)
    assert_deployment_values(out, DEPLOYMENT_1)

    kwargs = sync_detailed_mock.call_args.kwargs
    assert str(kwargs["workspace_id"]) == str(_WORKSPACE_ID)
    assert kwargs["deployment_id_or_version"] == DEPLOYMENT_1.version
    assert "client" in kwargs


def test_runtime_deployment_sync_happy_path_creates_new(script_runner: ScriptRunner) -> None:
    # Ensure package hash differs from latest deployment so a new one is created
    calculated_package_hash = "different_content_hash_than_latest"
    with (
        patch.object(
            commands.PackageBuilder,
            "write_package_to_stream",
            return_value=calculated_package_hash,
        ),
        patch.object(
            commands.get_latest_deployment,
            "sync_detailed",
            # Latest exists but with a different content hash to trigger creation
            return_value=SimpleNamespace(parsed=DEPLOYMENT_1),
        ) as latest_mock,
        patch.object(
            commands.create_deployment,
            "sync_detailed",
            return_value=SimpleNamespace(parsed=DEPLOYMENT_2),
        ) as create_mock,
    ):
        result = script_runner.run(["dlt", "runtime", "deployment", "sync"])

    assert result.returncode == 0
    out = result.stdout
    # Should tabulate details from DEPLOYMENT_2
    assert_deployment_headers(out)
    assert_deployment_values(out, DEPLOYMENT_2)

    # Validate calls used workspace id and client were passed through
    latest_kwargs = latest_mock.call_args.kwargs
    assert str(latest_kwargs["workspace_id"]) == str(_WORKSPACE_ID)
    assert "client" in latest_kwargs
    create_kwargs = create_mock.call_args.kwargs
    assert str(create_kwargs["workspace_id"]) == str(_WORKSPACE_ID)
    assert "client" in create_kwargs
