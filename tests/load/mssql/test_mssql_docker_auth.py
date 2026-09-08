"""Integration tests for the mssql destination against a local SQL Server.

Start the server with `docker compose up` in this directory (see docker-compose.yml).
Tests skip automatically when the server is unreachable, so they are safe to run anywhere.

These exercise the real `PyOdbcMsSqlClient.open_connection` code path. SQL Server in Docker
only supports SQL login (no Entra ID), so token/Azure AD authentication is validated against a
real Azure SQL / Fabric instance instead, via the parametrized `tests/load` suite.
"""

import pytest

from dlt.common.configuration import resolve_configuration
from dlt.common.schema import Schema
from dlt.destinations.impl.mssql.configuration import MsSqlClientConfiguration, MsSqlCredentials

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential

DOCKER_HOST = "localhost"
DOCKER_DATABASE = "master"
DOCKER_USERNAME = "sa"
DOCKER_PASSWORD = "Strong!Passw0rd"


def _docker_credentials() -> MsSqlCredentials:
    creds = MsSqlCredentials()
    creds.host = DOCKER_HOST
    creds.database = DOCKER_DATABASE
    creds.username = DOCKER_USERNAME
    creds.password = DOCKER_PASSWORD
    creds.connect_timeout = 5
    # local container uses a self-signed certificate
    creds.query = {"trustservercertificate": "yes", "encrypt": "no"}
    return resolve_configuration(creds)


def _sql_client(creds: MsSqlCredentials):
    from dlt.destinations import mssql

    config = MsSqlClientConfiguration(credentials=creds)._bind_dataset_name("dataset")
    client = mssql().client(Schema("schema"), config)
    return client.sql_client


def test_mssql_docker_sql_login_opens_connection() -> None:
    """SQL login (no Azure AD) opens a real connection - attrs_before is skipped."""
    creds = _docker_credentials()
    assert creds.to_odbc_attrs_before() is None

    sql_client = _sql_client(creds)
    try:
        sql_client.open_connection()
    except Exception as ex:  # noqa: BLE001
        pytest.skip(f"Local SQL Server not reachable (run `docker compose up`): {ex}")

    try:
        rows = sql_client.execute_sql("SELECT 1")
        assert rows[0][0] == 1
    finally:
        sql_client.close_connection()
