"""Tests for the defensive short-circuit on
`FabricCopyFileLoadJob._ensure_fabric_token_initialized`."""
from __future__ import annotations

import sys
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest


pytestmark = pytest.mark.essential


@pytest.fixture(autouse=True)
def _mock_pyodbc(monkeypatch: pytest.MonkeyPatch) -> None:
    """Install a fake pyodbc module so the mssql import chain succeeds."""
    fake_pyodbc = MagicMock(name="pyodbc_module")
    monkeypatch.setitem(sys.modules, "pyodbc", fake_pyodbc)


@pytest.fixture(autouse=True)
def _mock_azure_identity(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Install a fake azure.identity.ClientSecretCredential."""
    fake_credential_cls = MagicMock(name="ClientSecretCredential")
    fake_module = MagicMock(name="azure.identity")
    fake_module.ClientSecretCredential = fake_credential_cls
    monkeypatch.setitem(sys.modules, "azure.identity", fake_module)
    return fake_credential_cls


def _fake_load_job() -> SimpleNamespace:
    return SimpleNamespace(_token_initialized_cache={})


def test_warmup_short_circuits_when_staging_secret_is_none(
    _mock_azure_identity: MagicMock,
) -> None:
    from dlt.destinations.impl.fabric.fabric import FabricCopyFileLoadJob

    job = _fake_load_job()
    staging_credentials = SimpleNamespace(
        azure_client_id="client-id",
        azure_tenant_id="tenant-id",
        azure_client_secret=None,
    )

    FabricCopyFileLoadJob._ensure_fabric_token_initialized(job, staging_credentials)  # type: ignore[arg-type]

    _mock_azure_identity.assert_not_called()


def test_warmup_short_circuits_when_staging_secret_is_empty_string(
    _mock_azure_identity: MagicMock,
) -> None:
    from dlt.destinations.impl.fabric.fabric import FabricCopyFileLoadJob

    job = _fake_load_job()
    staging_credentials = SimpleNamespace(
        azure_client_id="client-id",
        azure_tenant_id="tenant-id",
        azure_client_secret="",
    )

    FabricCopyFileLoadJob._ensure_fabric_token_initialized(job, staging_credentials)  # type: ignore[arg-type]

    _mock_azure_identity.assert_not_called()


def test_warmup_still_runs_when_staging_secret_is_real_value(
    _mock_azure_identity: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The short-circuit must not break the SP happy path."""
    fake_requests = MagicMock(name="requests_module")
    fake_requests.get.return_value = SimpleNamespace(status_code=200, text="ok")
    monkeypatch.setitem(sys.modules, "requests", fake_requests)

    from dlt.destinations.impl.fabric.fabric import FabricCopyFileLoadJob

    job = _fake_load_job()
    staging_credentials = SimpleNamespace(
        azure_client_id="client-id",
        azure_tenant_id="tenant-id",
        azure_client_secret="real-secret",
    )

    FabricCopyFileLoadJob._ensure_fabric_token_initialized(job, staging_credentials)  # type: ignore[arg-type]

    _mock_azure_identity.assert_called_once_with(
        tenant_id="tenant-id",
        client_id="client-id",
        client_secret="real-secret",
    )
    assert job._token_initialized_cache["client-id"] is True
