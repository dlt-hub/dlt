import pyodbc
import pytest

from dlt.common.configuration import resolve_configuration
from dlt.common.exceptions import SystemConfigurationException

from dlt.destinations.impl.mssql.configuration import MsSqlCredentials, SUPPORTED_DRIVERS


def test_parse_native_representation_unsupported_driver_specified() -> None:
    # Case: unsupported driver specified.
    with pytest.raises(SystemConfigurationException):
        resolve_configuration(
            MsSqlCredentials(
                "mssql://test_user:test_password@sql.example.com:12345/test_db?DRIVER=foo"
            )
        )


def test_to_odbc_dsn_supported_driver_specified() -> None:
    # Case: supported driver specified — ODBC Driver 18 for SQL Server.
    creds = resolve_configuration(
        MsSqlCredentials(
            "mssql://test_user:test_password@sql.example.com:12345/test_db?DRIVER=ODBC+Driver+18+for+SQL+Server"
        )
    )
    dsn = creds.to_odbc_dsn()
    result = {k: v for k, v in (param.split("=") for param in dsn.split(";"))}
    assert result == {
        "DRIVER": "ODBC Driver 18 for SQL Server",
        "SERVER": "sql.example.com,12345",
        "DATABASE": "test_db",
        "UID": "test_user",
        "PWD": "test_password",
    }

    # Case: supported driver specified — ODBC Driver 17 for SQL Server.
    creds = resolve_configuration(
        MsSqlCredentials(
            "mssql://test_user:test_password@sql.example.com:12345/test_db?DRIVER=ODBC+Driver+17+for+SQL+Server"
        )
    )
    dsn = creds.to_odbc_dsn()
    result = {k: v for k, v in (param.split("=") for param in dsn.split(";"))}
    assert result == {
        "DRIVER": "ODBC Driver 17 for SQL Server",
        "SERVER": "sql.example.com,12345",
        "DATABASE": "test_db",
        "UID": "test_user",
        "PWD": "test_password",
    }


def test_to_odbc_dsn_arbitrary_keys_specified() -> None:
    # Case: arbitrary query keys (and supported driver) specified.
    creds = resolve_configuration(
        MsSqlCredentials(
            "mssql://test_user:test_password@sql.example.com:12345/test_db?FOO=a&BAR=b&DRIVER=ODBC+Driver+18+for+SQL+Server"
        )
    )
    dsn = creds.to_odbc_dsn()
    result = {k: v for k, v in (param.split("=") for param in dsn.split(";"))}
    assert result == {
        "DRIVER": "ODBC Driver 18 for SQL Server",
        "SERVER": "sql.example.com,12345",
        "DATABASE": "test_db",
        "UID": "test_user",
        "PWD": "test_password",
        "FOO": "a",
        "BAR": "b",
    }

    # Case: arbitrary capitalization.
    creds = resolve_configuration(
        MsSqlCredentials(
            "mssql://test_user:test_password@sql.example.com:12345/test_db?FOO=a&bar=b&Driver=ODBC+Driver+18+for+SQL+Server"
        )
    )
    dsn = creds.to_odbc_dsn()
    result = {k: v for k, v in (param.split("=") for param in dsn.split(";"))}
    assert result == {
        "DRIVER": "ODBC Driver 18 for SQL Server",
        "SERVER": "sql.example.com,12345",
        "DATABASE": "test_db",
        "UID": "test_user",
        "PWD": "test_password",
        "FOO": "a",
        "BAR": "b",
    }


available_drivers = [d for d in pyodbc.drivers() if d in SUPPORTED_DRIVERS]


@pytest.mark.skipif(not available_drivers, reason="no supported driver available")
def test_to_odbc_dsn_driver_not_specified() -> None:
    # Case: driver not specified, but supported driver is available.
    creds = resolve_configuration(
        MsSqlCredentials("mssql://test_user:test_password@sql.example.com:12345/test_db")
    )
    dsn = creds.to_odbc_dsn()
    result = {k: v for k, v in (param.split("=") for param in dsn.split(";"))}
    assert result in [
        {
            "DRIVER": d,
            "SERVER": "sql.example.com,12345",
            "DATABASE": "test_db",
            "UID": "test_user",
            "PWD": "test_password",
        }
        for d in SUPPORTED_DRIVERS
    ]
