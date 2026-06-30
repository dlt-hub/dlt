"""Integration tests for the mssql destination against a local SQL Server.

Start the server with `docker compose up` in this directory (see docker-compose.yml).
Tests skip automatically when the server is unreachable, so they are safe to run anywhere.

These exercise the real mssql-python connection and load path. SQL Server in Docker only
supports SQL login (no Entra ID), so token/Azure AD authentication is validated against a real
Azure SQL / Fabric instance instead, via the parametrized `tests/load` suite.
"""

from typing import Any

import pytest

import dlt
from dlt.common.configuration import resolve_configuration
from dlt.common.schema import Schema
from dlt.destinations import mssql
from dlt.destinations.impl.mssql.configuration import MsSqlClientConfiguration, MsSqlCredentials

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential

DOCKER_HOST = "localhost"
DOCKER_USERNAME = "sa"
DOCKER_PASSWORD = "Strong!Passw0rd"
TEST_DATABASE = "dlt_ci_test"


def _docker_credentials(database: str) -> MsSqlCredentials:
    creds = MsSqlCredentials()
    creds.host = DOCKER_HOST
    creds.database = database
    creds.username = DOCKER_USERNAME
    creds.password = DOCKER_PASSWORD
    creds.connect_timeout = 5
    # local container uses a self-signed certificate
    creds.query = {"trustservercertificate": "yes", "encrypt": "no"}
    return resolve_configuration(creds)


def _sql_client(creds: MsSqlCredentials) -> Any:
    config = MsSqlClientConfiguration(credentials=creds)._bind_dataset_name("dataset")
    return mssql().client(Schema("schema"), config).sql_client


def _ensure_server_and_database() -> None:
    """Open a real connection (skips if unreachable) and create the test database."""
    sql_client = _sql_client(_docker_credentials("master"))
    try:
        sql_client.open_connection()
    except Exception as ex:  # noqa: BLE001
        pytest.skip(f"Local SQL Server not reachable (run `docker compose up`): {ex}")
    try:
        sql_client.execute_sql(
            f"IF DB_ID('{TEST_DATABASE}') IS NULL CREATE DATABASE {TEST_DATABASE}"
        )
    finally:
        sql_client.close_connection()


def test_mssql_docker_sql_login_opens_connection() -> None:
    """SQL login (no Azure AD) opens a real connection - attrs_before is skipped."""
    creds = _docker_credentials("master")
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


def test_mssql_docker_pipeline_load() -> None:
    """A full pipeline load round-trips data through the mssql-python driver."""
    _ensure_server_and_database()

    pipeline = dlt.pipeline(
        pipeline_name="mssql_ci",
        destination=mssql(credentials=_docker_credentials(TEST_DATABASE)),
        dataset_name="mssql_ci_dataset",
        dev_mode=True,
    )
    pipeline.run(
        [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}, {"id": 3, "name": "c"}],
        table_name="items",
    )

    with pipeline.sql_client() as client:
        qualified = client.make_qualified_table_name("items")
        rows = client.execute_sql(f"SELECT COUNT(*) FROM {qualified}")
        assert rows[0][0] == 3
