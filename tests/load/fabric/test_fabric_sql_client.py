"""Tests for `FabricSqlClient.open_connection` under mocked pyodbc."""
from __future__ import annotations

import struct
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest


pytestmark = pytest.mark.essential


@pytest.fixture(autouse=True)
def _mock_pyodbc(monkeypatch: pytest.MonkeyPatch) -> None:  # type: ignore[misc]
    """Install a fake pyodbc module in sys.modules."""
    fake_pyodbc = MagicMock(name="pyodbc_module")
    monkeypatch.setitem(sys.modules, "pyodbc", fake_pyodbc)
    yield


def _fake_sql_client() -> SimpleNamespace:
    """Stand-in FabricSqlClient with attributes open_connection touches."""
    creds = SimpleNamespace(
        host="test.datawarehouse.fabric.microsoft.com",
        port=1433,
        database="testdb",
        connect_timeout=15,
        access_token=None,
        azure_credential=None,
        azure_client_id=None,
        azure_tenant_id=None,
        azure_client_secret=None,
    )
    creds.get_access_token = lambda: (
        str(creds.access_token) if creds.access_token is not None else None
    )
    creds.to_odbc_dsn = MagicMock(
        return_value=(
            "DRIVER={ODBC Driver 18 for SQL Server};"
            "SERVER=test.datawarehouse.fabric.microsoft.com,1433;"
            "DATABASE=testdb;"
            "LongAsMax=yes;Encrypt=yes;TrustServerCertificate=no;"
        )
    )
    return SimpleNamespace(credentials=creds, _conn=None)


def test_open_connection_passes_token_via_attrs_before_1256() -> None:
    from dlt.destinations.impl.fabric.sql_client import FabricSqlClient

    client = _fake_sql_client()
    client.credentials.access_token = "FAKE_TOKEN"

    FabricSqlClient.open_connection(client)  # type: ignore[arg-type]

    pyodbc = sys.modules["pyodbc"]
    assert pyodbc.connect.called
    attrs_before = pyodbc.connect.call_args.kwargs["attrs_before"]
    assert 1256 in attrs_before
    raw = "FAKE_TOKEN".encode("utf-16-le")
    expected = struct.pack(f"<I{len(raw)}s", len(raw), raw)
    assert attrs_before[1256] == expected


def test_open_connection_passes_connect_timeout_in_token_mode() -> None:
    from dlt.destinations.impl.fabric.sql_client import FabricSqlClient

    client = _fake_sql_client()
    client.credentials.access_token = "FAKE_TOKEN"
    client.credentials.connect_timeout = 42

    FabricSqlClient.open_connection(client)  # type: ignore[arg-type]

    pyodbc = sys.modules["pyodbc"]
    assert pyodbc.connect.call_args.kwargs["timeout"] == 42


def test_open_connection_uses_sp_path_when_no_access_token() -> None:
    from unittest.mock import patch

    from dlt.destinations.impl.fabric.sql_client import FabricSqlClient
    from dlt.destinations.impl.mssql.sql_client import PyOdbcMsSqlClient

    # super() requires a real FabricSqlClient instance, so build one
    # with __init__ bypassed
    client = FabricSqlClient.__new__(FabricSqlClient)
    base = _fake_sql_client()
    client.credentials = base.credentials
    client._conn = None
    client.credentials.access_token = None

    sentinel = object()
    with patch.object(PyOdbcMsSqlClient, "open_connection", return_value=sentinel) as mock_super:
        result = FabricSqlClient.open_connection(client)

    mock_super.assert_called_once()
    assert result is sentinel


def test_open_connection_sets_autocommit_true_in_token_mode() -> None:
    from dlt.destinations.impl.fabric.sql_client import FabricSqlClient

    client = _fake_sql_client()
    client.credentials.access_token = "FAKE_TOKEN"

    FabricSqlClient.open_connection(client)  # type: ignore[arg-type]

    pyodbc = sys.modules["pyodbc"]
    returned_conn = pyodbc.connect.return_value
    assert returned_conn.autocommit is True


def test_open_connection_installs_datetimeoffset_converter_in_token_mode() -> None:
    from dlt.destinations.impl.fabric.sql_client import FabricSqlClient

    client = _fake_sql_client()
    client.credentials.access_token = "FAKE_TOKEN"

    FabricSqlClient.open_connection(client)  # type: ignore[arg-type]

    pyodbc = sys.modules["pyodbc"]
    returned_conn = pyodbc.connect.return_value
    returned_conn.add_output_converter.assert_called()
    converter_args = returned_conn.add_output_converter.call_args.args
    assert converter_args[0] == -155


def test_open_connection_caches_conn_on_self() -> None:
    from dlt.destinations.impl.fabric.sql_client import FabricSqlClient

    client = _fake_sql_client()
    client.credentials.access_token = "FAKE_TOKEN"

    returned = FabricSqlClient.open_connection(client)  # type: ignore[arg-type]

    assert client._conn is returned
