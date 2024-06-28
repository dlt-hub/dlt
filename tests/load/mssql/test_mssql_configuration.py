import os
import pyodbc
import pytest

from dlt.common.configuration import resolve_configuration, ConfigFieldMissingException
from dlt.common.exceptions import SystemConfigurationException
from dlt.common.schema import Schema

from dlt.destinations import mssql
from dlt.destinations.impl.mssql.configuration import MsSqlCredentials, MsSqlClientConfiguration

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


def test_mssql_factory() -> None:
    schema = Schema("schema")
    dest = mssql()
    client = dest.client(schema, MsSqlClientConfiguration()._bind_dataset_name("dataset"))
    assert client.config.create_indexes is False
    assert client.config.has_case_sensitive_identifiers is False
    assert client.capabilities.has_case_sensitive_identifiers is False
    assert client.capabilities.casefold_identifier is str

    # set args explicitly
    dest = mssql(has_case_sensitive_identifiers=True, create_indexes=True)
    client = dest.client(schema, MsSqlClientConfiguration()._bind_dataset_name("dataset"))
    assert client.config.create_indexes is True
    assert client.config.has_case_sensitive_identifiers is True
    assert client.capabilities.has_case_sensitive_identifiers is True
    assert client.capabilities.casefold_identifier is str

    # set args via config
    os.environ["DESTINATION__CREATE_INDEXES"] = "True"
    os.environ["DESTINATION__HAS_CASE_SENSITIVE_IDENTIFIERS"] = "True"
    dest = mssql()
    client = dest.client(schema, MsSqlClientConfiguration()._bind_dataset_name("dataset"))
    assert client.config.create_indexes is True
    assert client.config.has_case_sensitive_identifiers is True
    assert client.capabilities.has_case_sensitive_identifiers is True
    assert client.capabilities.casefold_identifier is str


def test_mssql_credentials_defaults() -> None:
    creds = MsSqlCredentials()
    assert creds.port == 1433
    assert creds.connect_timeout == 15
    assert MsSqlCredentials.__config_gen_annotations__ == ["port", "connect_timeout"]
    # port should be optional
    resolve_configuration(creds, explicit_value="mssql://loader:loader@localhost/dlt_data")
    assert creds.port == 1433


def test_parse_native_representation() -> None:
    # Case: unsupported driver specified.
    with pytest.raises(SystemConfigurationException):
        resolve_configuration(
            MsSqlCredentials(
                "mssql://test_user:test_pwd@sql.example.com/test_db?DRIVER=ODBC+Driver+13+for+SQL+Server"
            )
        )
    # Case: password not specified.
    with pytest.raises(ConfigFieldMissingException):
        resolve_configuration(
            MsSqlCredentials(
                "mssql://test_user@sql.example.com/test_db?DRIVER=ODBC+Driver+18+for+SQL+Server"
            )
        )


def test_to_odbc_dsn_supported_driver_specified() -> None:
    # Case: supported driver specified — ODBC Driver 18 for SQL Server.
    creds = resolve_configuration(
        MsSqlCredentials(
            "mssql://test_user:test_pwd@sql.example.com/test_db?DRIVER=ODBC+Driver+18+for+SQL+Server"
        )
    )
    dsn = creds.to_odbc_dsn()
    result = {k: v for k, v in (param.split("=") for param in dsn.split(";"))}
    assert result == {
        "DRIVER": "ODBC Driver 18 for SQL Server",
        "SERVER": "sql.example.com,1433",
        "DATABASE": "test_db",
        "UID": "test_user",
        "PWD": "test_pwd",
    }

    # Case: supported driver specified — ODBC Driver 17 for SQL Server.
    creds = resolve_configuration(
        MsSqlCredentials(
            "mssql://test_user:test_pwd@sql.example.com/test_db?DRIVER=ODBC+Driver+17+for+SQL+Server"
        )
    )
    dsn = creds.to_odbc_dsn()
    result = {k: v for k, v in (param.split("=") for param in dsn.split(";"))}
    assert result == {
        "DRIVER": "ODBC Driver 17 for SQL Server",
        "SERVER": "sql.example.com,1433",
        "DATABASE": "test_db",
        "UID": "test_user",
        "PWD": "test_pwd",
    }

    # Case: port and supported driver specified.
    creds = resolve_configuration(
        MsSqlCredentials(
            "mssql://test_user:test_pwd@sql.example.com:12345/test_db?DRIVER=ODBC+Driver+18+for+SQL+Server"
        )
    )
    dsn = creds.to_odbc_dsn()
    result = {k: v for k, v in (param.split("=") for param in dsn.split(";"))}
    assert result == {
        "DRIVER": "ODBC Driver 18 for SQL Server",
        "SERVER": "sql.example.com,12345",
        "DATABASE": "test_db",
        "UID": "test_user",
        "PWD": "test_pwd",
    }


def test_to_odbc_dsn_arbitrary_keys_specified() -> None:
    # Case: arbitrary query keys (and supported driver) specified.
    creds = resolve_configuration(
        MsSqlCredentials(
            "mssql://test_user:test_pwd@sql.example.com:12345/test_db?FOO=a&BAR=b&DRIVER=ODBC+Driver+18+for+SQL+Server"
        )
    )
    dsn = creds.to_odbc_dsn()
    result = {k: v for k, v in (param.split("=") for param in dsn.split(";"))}
    assert result == {
        "DRIVER": "ODBC Driver 18 for SQL Server",
        "SERVER": "sql.example.com,12345",
        "DATABASE": "test_db",
        "UID": "test_user",
        "PWD": "test_pwd",
        "FOO": "a",
        "BAR": "b",
    }

    # Case: arbitrary capitalization.
    creds = resolve_configuration(
        MsSqlCredentials(
            "mssql://test_user:test_pwd@sql.example.com:12345/test_db?FOO=a&bar=b&Driver=ODBC+Driver+18+for+SQL+Server"
        )
    )
    dsn = creds.to_odbc_dsn()
    result = {k: v for k, v in (param.split("=") for param in dsn.split(";"))}
    assert result == {
        "DRIVER": "ODBC Driver 18 for SQL Server",
        "SERVER": "sql.example.com,12345",
        "DATABASE": "test_db",
        "UID": "test_user",
        "PWD": "test_pwd",
        "FOO": "a",
        "BAR": "b",
    }


available_drivers = [d for d in pyodbc.drivers() if d in MsSqlCredentials.SUPPORTED_DRIVERS]


@pytest.mark.skipif(not available_drivers, reason="no supported driver available")
def test_to_odbc_dsn_driver_not_specified() -> None:
    # Case: driver not specified, but supported driver is available.
    creds = resolve_configuration(
        MsSqlCredentials("mssql://test_user:test_pwd@sql.example.com/test_db")
    )
    dsn = creds.to_odbc_dsn()
    result = {k: v for k, v in (param.split("=") for param in dsn.split(";"))}
    assert result in [
        {
            "DRIVER": d,
            "SERVER": "sql.example.com,1433",
            "DATABASE": "test_db",
            "UID": "test_user",
            "PWD": "test_pwd",
        }
        for d in MsSqlCredentials.SUPPORTED_DRIVERS
    ]
