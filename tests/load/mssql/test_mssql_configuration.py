import os
import struct

import pyodbc
import pytest

from dlt.common.configuration import ConfigFieldMissingException, resolve_configuration
from dlt.common.configuration.exceptions import ConfigurationException
from dlt.common.exceptions import SystemConfigurationException
from dlt.common.schema import Schema
from dlt.common.utils import digest128
from dlt.destinations import mssql
from dlt.destinations.impl.mssql.configuration import (
    MsSqlClientConfiguration,
    MsSqlCredentials,
    uses_token_authentication,
    validate_authentication,
)

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

    # MSSQL uses ADBC for parquet loading which doesn't support dictionary-encoded arrays
    assert client.capabilities.parquet_format is not None
    assert client.capabilities.parquet_format.supports_dictionary_encoding is False

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
    assert creds.connect_timeout == 30
    assert MsSqlCredentials.__config_gen_annotations__ == ["port", "connect_timeout"]
    # port should be optional
    resolve_configuration(creds, explicit_value="mssql://loader:loader@localhost/dlt_data")
    assert creds.port == 1433


@pytest.mark.parametrize(
    "connection_string,expected_fingerprint",
    [
        pytest.param("", "", id="empty"),
        pytest.param(
            "mssql://user1:pass1@host1:1433/db1",
            digest128("host1"),
            id="legacy_host_only_default_port",
        ),
        pytest.param(
            "mssql://user1:pass1@host1:1434/db1",
            digest128("host1"),
            id="legacy_host_only_custom_port",
        ),
    ],
)
def test_mssql_fingerprint(connection_string: str, expected_fingerprint: str) -> None:
    if connection_string:
        credentials = MsSqlCredentials(connection_string)
        config = MsSqlClientConfiguration(credentials=credentials)
    else:
        config = MsSqlClientConfiguration()

    assert config.fingerprint() == expected_fingerprint


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


# ---------------------------------------------------------------------------
# Authentication methods
# ---------------------------------------------------------------------------


class _FakeAccessToken:
    token = "fake-access-token"


class _FakeTokenCredential:
    """Minimal azure-identity-like credential, avoids hitting Azure in unit tests."""

    def get_token(self, *scopes: str, **kwargs: object) -> _FakeAccessToken:
        return _FakeAccessToken()


def _mssql_credentials(authentication: object = None, **kwargs: object) -> MsSqlCredentials:
    creds = MsSqlCredentials()
    creds.host = "sql.example.com"
    creds.database = "test_db"
    creds.driver = "ODBC Driver 18 for SQL Server"  # avoid probing for an installed driver
    if authentication is not None:
        creds.authentication = authentication  # type: ignore[assignment]
    for key, value in kwargs.items():
        setattr(creds, key, value)
    return creds


def test_mssql_authentication_defaults_to_sql_login() -> None:
    assert MsSqlCredentials().authentication is None


def test_mssql_sql_login_dsn_uses_uid_pwd() -> None:
    creds = _mssql_credentials(username="loader", password="secret")
    creds.on_partial()

    dsn = creds.get_odbc_dsn_dict()
    assert "AUTHENTICATION" not in dsn
    assert dsn["UID"] == "loader"
    assert dsn["PWD"] == "secret"
    assert creds.to_odbc_attrs_before() is None


@pytest.mark.parametrize(
    "authentication,expected",
    [
        ("default", "DefaultAzureCredential"),
        ("ActiveDirectoryDefault", "DefaultAzureCredential"),
        ("ActiveDirectoryDeviceCode", "DeviceCodeCredential"),
    ],
)
def test_mssql_azure_identity_credential_mapping(authentication: str, expected: str) -> None:
    creds = _mssql_credentials(authentication)
    creds.on_partial()

    assert type(creds.default_credentials()).__name__ == expected

    dsn = creds.get_odbc_dsn_dict()
    assert "AUTHENTICATION" not in dsn
    assert "UID" not in dsn
    assert "PWD" not in dsn


@pytest.mark.parametrize(
    "authentication",
    ["auto", "cli", "environment", "interactive", "devicecode", "msi", "managedidentity"],
)
def test_mssql_removed_dlt_custom_alias_raises(authentication: str) -> None:
    """The old dlt-custom lowercase aliases were replaced by native ODBC/azure-identity names."""
    creds = _mssql_credentials(authentication)
    with pytest.raises(ConfigurationException):
        validate_authentication(creds)


def test_mssql_service_principal_driver_native() -> None:
    creds = _mssql_credentials(
        "ActiveDirectoryServicePrincipal",
        azure_tenant_id="t",
        azure_client_id="c",
        azure_client_secret="s",
    )
    creds.on_partial()

    assert uses_token_authentication(creds) is False
    dsn = creds.get_odbc_dsn_dict()
    assert dsn["AUTHENTICATION"] == "ActiveDirectoryServicePrincipal"
    assert dsn["UID"] == "c@t"
    assert dsn["PWD"] == "s"
    assert creds.to_odbc_attrs_before() is None


def test_mssql_service_principal_without_secret_falls_back_to_token() -> None:
    creds = _mssql_credentials("ActiveDirectoryServicePrincipal")
    creds.on_partial()

    assert uses_token_authentication(creds) is True
    assert type(creds.default_credentials()).__name__ == "DefaultAzureCredential"
    assert "AUTHENTICATION" not in creds.get_odbc_dsn_dict()


@pytest.mark.parametrize(
    "authentication",
    ["ActiveDirectoryIntegrated", "ActiveDirectoryInteractive", "ActiveDirectoryMsi"],
)
def test_mssql_driver_native_passthrough(authentication: str) -> None:
    creds = _mssql_credentials(authentication)
    creds.on_partial()

    dsn = creds.get_odbc_dsn_dict()
    assert dsn["AUTHENTICATION"] == authentication
    assert "UID" not in dsn
    assert "PWD" not in dsn
    assert creds.to_odbc_attrs_before() is None


def test_mssql_active_directory_password() -> None:
    creds = _mssql_credentials(
        "ActiveDirectoryPassword", username="user@contoso.com", password="pwd"
    )
    creds.on_partial()

    dsn = creds.get_odbc_dsn_dict()
    assert dsn["AUTHENTICATION"] == "ActiveDirectoryPassword"
    assert dsn["UID"] == "user@contoso.com"
    assert dsn["PWD"] == "pwd"


def test_mssql_active_directory_password_requires_username_password() -> None:
    creds = _mssql_credentials("ActiveDirectoryPassword")
    with pytest.raises(ConfigurationException):
        validate_authentication(creds)


def test_mssql_unsupported_authentication_raises() -> None:
    creds = _mssql_credentials("SqlPassword", username="u", password="p")
    with pytest.raises(ConfigurationException):
        creds.on_partial()  # resolves (all present) -> on_resolved -> validate raises


def test_mssql_to_odbc_attrs_before_token_struct() -> None:
    creds = _mssql_credentials("default")
    creds._set_default_credentials(_FakeTokenCredential())

    attrs = creds.to_odbc_attrs_before()
    assert attrs is not None
    token_struct = attrs[1256]  # SQL_COPT_SS_ACCESS_TOKEN
    length = struct.unpack("<I", token_struct[:4])[0]
    assert length == len(token_struct) - 4
    assert token_struct[4:].decode("utf-16-le") == "fake-access-token"


def test_mssql_resolve_configuration_token_authentication() -> None:
    creds = MsSqlCredentials()
    creds.host = "sql.example.com"
    creds.database = "test_db"
    creds.driver = "ODBC Driver 18 for SQL Server"
    creds.authentication = "ActiveDirectoryDeviceCode"

    resolved = resolve_configuration(creds)

    assert resolved.is_resolved()
    assert uses_token_authentication(resolved) is True
    assert type(resolved.default_credentials()).__name__ == "DeviceCodeCredential"
    assert "AUTHENTICATION" not in resolved.get_odbc_dsn_dict()
