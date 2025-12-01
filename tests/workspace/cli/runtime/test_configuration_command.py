import datetime
import uuid
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from pytest_console_scripts import ScriptRunner

from dlt._workspace.cli import _runtime_command as commands
from dlt._workspace.runtime_clients.api.models.configuration_response import ConfigurationResponse
from dlt._workspace.runtime_clients.api.models.list_configurations_response_200 import (
    ListConfigurationsResponse200,
)

CONFIG_1 = ConfigurationResponse(
    content_hash="cfg123contenthash",
    created_by=uuid.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
    date_added=datetime.datetime(2024, 6, 1, 12, 0, 0),
    date_updated=datetime.datetime(2024, 6, 1, 13, 0, 0),
    file_count=3,
    file_names="config.toml, secrets.toml, profiles.toml",
    id=uuid.UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
    profiles="default,prod",
    size=512 * 1024,
    version=1,
    workspace_id=uuid.UUID("cccccccc-cccc-cccc-cccc-cccccccccccc"),
)

CONFIG_2 = ConfigurationResponse(
    content_hash="cfg456contenthash",
    created_by=uuid.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
    date_added=datetime.datetime(2024, 6, 1, 14, 0, 0),
    date_updated=datetime.datetime(2024, 6, 1, 15, 0, 0),
    file_count=4,
    file_names="config.toml, secrets.toml, profiles.toml, extra.toml",
    id=uuid.UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
    profiles="default,prod,access",
    size=768 * 1024,
    version=2,
    workspace_id=uuid.UUID("cccccccc-cccc-cccc-cccc-cccccccccccc"),
)

CONFIGS = [CONFIG_1, CONFIG_2]

_WORKSPACE_ID = uuid.UUID("dddddddd-dddd-dddd-dddd-dddddddddddd")


def assert_configuration_headers(out: str) -> None:
    for h in ["Version #", "Created at", "File count", "Content hash"]:
        assert h in out


def assert_configuration_values(out: str, cfg: "ConfigurationResponse") -> None:
    assert str(cfg.version) in out
    assert cfg.date_added.isoformat() in out
    assert str(cfg.file_count) in out
    assert cfg.content_hash in out


def assert_out_order(out: str, first: str, second: str) -> None:
    assert out.find(first) < out.find(second)


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


def test_runtime_configuration_list_outputs_all(script_runner: ScriptRunner) -> None:
    response = ListConfigurationsResponse200(items=CONFIGS)
    with patch.object(
        commands.list_configurations, "sync_detailed", return_value=SimpleNamespace(parsed=response)
    ) as sync_detailed_mock:
        result = script_runner.run(["dlt", "runtime", "configuration", "list"])

    assert result.returncode == 0

    out = result.stdout
    assert_configuration_headers(out)
    # Values for latest (v2)
    assert_configuration_values(out, CONFIG_2)
    # Values for previous (v1)
    assert_configuration_values(out, CONFIG_1)
    # Order should be latest first (because CLI reverses the list)
    assert_out_order(out, CONFIG_2.content_hash, CONFIG_1.content_hash)

    kwargs = sync_detailed_mock.call_args.kwargs
    assert str(kwargs["workspace_id"]) == str(_WORKSPACE_ID)
    assert "client" in kwargs


def test_runtime_configuration_info_latest(script_runner: ScriptRunner) -> None:
    with patch.object(
        commands.get_latest_configuration,
        "sync_detailed",
        return_value=SimpleNamespace(parsed=CONFIG_2),
    ) as sync_detailed_mock:
        result = script_runner.run(["dlt", "runtime", "configuration", "info"])

    assert result.returncode == 0
    out = result.stdout
    assert_configuration_headers(out)
    assert_configuration_values(out, CONFIG_2)

    kwargs = sync_detailed_mock.call_args.kwargs
    assert str(kwargs["workspace_id"]) == str(_WORKSPACE_ID)
    assert "configuration_id_or_version" not in kwargs
    assert "client" in kwargs


def test_runtime_configuration_info_version_1_by_version_number(
    script_runner: ScriptRunner,
) -> None:
    with patch.object(
        commands.get_configuration, "sync_detailed", return_value=SimpleNamespace(parsed=CONFIG_1)
    ) as sync_detailed_mock:
        result = script_runner.run(
            ["dlt", "runtime", "configuration", str(CONFIG_1.version), "info"]
        )

    assert result.returncode == 0
    out = result.stdout
    assert_configuration_headers(out)
    assert_configuration_values(out, CONFIG_1)

    kwargs = sync_detailed_mock.call_args.kwargs
    assert str(kwargs["workspace_id"]) == str(_WORKSPACE_ID)
    # version number is passed through
    assert kwargs["configuration_id_or_version"] == CONFIG_1.version
    assert "client" in kwargs


def test_runtime_configuration_sync_happy_path_creates_new(script_runner: ScriptRunner) -> None:
    # Ensure package hash differs from latest configuration so a new one is created
    calculated_package_hash = "different_content_hash_than_latest"
    with (
        patch.object(
            commands.PackageBuilder,
            "write_package_to_stream",
            return_value=calculated_package_hash,
        ),
        patch.object(
            commands.get_latest_configuration,
            "sync_detailed",
            # Latest exists but with a different content hash to trigger creation
            return_value=SimpleNamespace(parsed=CONFIG_1),
        ) as latest_mock,
        patch.object(
            commands.create_configuration,
            "sync_detailed",
            return_value=SimpleNamespace(parsed=CONFIG_2),
        ) as create_mock,
    ):
        result = script_runner.run(["dlt", "runtime", "configuration", "sync"])

    assert result.returncode == 0
    out = result.stdout
    # Should tabulate details from CONFIG_2
    assert_configuration_headers(out)
    assert_configuration_values(out, CONFIG_2)

    # Validate calls used workspace id and client were passed through
    latest_kwargs = latest_mock.call_args.kwargs
    assert str(latest_kwargs["workspace_id"]) == str(_WORKSPACE_ID)
    assert "client" in latest_kwargs
    create_kwargs = create_mock.call_args.kwargs
    assert str(create_kwargs["workspace_id"]) == str(_WORKSPACE_ID)
    assert "client" in create_kwargs
