import os

import pytest

import mssql_python

from dlt.common.configuration import ConfigFieldMissingException, resolve_configuration
from dlt.common.configuration.exceptions import ConfigurationException
from dlt.common.schema import Schema
from dlt.common.utils import digest128
from dlt.destinations import mssql
from dlt.destinations.impl.mssql.configuration import (
    MsSqlClientConfiguration,
    MsSqlCredentials,
    get_access_token,
    uses_token_authentication,
    validate_authentication,
)
from dlt.destinations.exceptions import (
    DatabaseTerminalException,
    DatabaseTransientException,
    DatabaseUndefinedRelation,
)
from dlt.destinations.impl.mssql.sql_client import MsSqlClient

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
    # Case: password not specified.
    with pytest.raises(ConfigFieldMissingException):
        resolve_configuration(MsSqlCredentials("mssql://test_user@sql.example.com/test_db"))


def test_to_odbc_dsn() -> None:
    # mssql-python bundles its own driver, so the DSN carries no DRIVER and any `driver`
    # query parameter (legacy pyodbc config) is ignored.
    creds = resolve_configuration(
        MsSqlCredentials(
            "mssql://test_user:test_pwd@sql.example.com/test_db?DRIVER=ODBC+Driver+18+for+SQL+Server"
        )
    )
    dsn = creds.to_odbc_dsn()
    result = {k: v for k, v in (param.split("=") for param in dsn.split(";"))}
    assert result == {
        "SERVER": "sql.example.com,1433",
        "DATABASE": "test_db",
        "UID": "test_user",
        "PWD": "test_pwd",
    }

    # Case: custom port.
    creds = resolve_configuration(
        MsSqlCredentials("mssql://test_user:test_pwd@sql.example.com:12345/test_db")
    )
    dsn = creds.to_odbc_dsn()
    result = {k: v for k, v in (param.split("=") for param in dsn.split(";"))}
    assert result == {
        "SERVER": "sql.example.com,12345",
        "DATABASE": "test_db",
        "UID": "test_user",
        "PWD": "test_pwd",
    }


def test_to_odbc_dsn_arbitrary_keys_specified() -> None:
    # Arbitrary query keys are passed through (the `driver` key is dropped).
    creds = resolve_configuration(
        MsSqlCredentials(
            "mssql://test_user:test_pwd@sql.example.com:12345/test_db?FOO=a&BAR=b&Driver=ODBC+Driver+18+for+SQL+Server"
        )
    )
    dsn = creds.to_odbc_dsn()
    result = {k: v for k, v in (param.split("=") for param in dsn.split(";"))}
    assert result == {
        "SERVER": "sql.example.com,12345",
        "DATABASE": "test_db",
        "UID": "test_user",
        "PWD": "test_pwd",
        "FOO": "a",
        "BAR": "b",
    }


def test_to_odbc_dsn_connect_timeout_and_longasmax_dropped() -> None:
    # mssql-python's connection-string parser rejects unknown keywords, so `connect_timeout`
    # (passed via the connect() `timeout=` parameter instead) and `LongAsMax` (the driver
    # handles long/max types natively) must never end up in the DSN.
    creds = resolve_configuration(
        MsSqlCredentials(
            "mssql://test_user:test_pwd@sql.example.com/test_db?connect_timeout=15&LongAsMax=yes&Encrypt=yes"
        )
    )
    dsn = creds.to_odbc_dsn()
    assert "connect_timeout" not in dsn.lower()
    assert "longasmax" not in dsn.lower()
    result = {k: v for k, v in (param.split("=") for param in dsn.split(";"))}
    assert result == {
        "SERVER": "sql.example.com,1433",
        "DATABASE": "test_db",
        "UID": "test_user",
        "PWD": "test_pwd",
        "ENCRYPT": "yes",
    }


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


def test_mssql_default_alias_normalizes_in_dsn() -> None:
    """The `default` alias resolves to the canonical name mssql-python recognizes.

    mssql-python only understands `ActiveDirectoryDefault` in the `Authentication=` DSN keyword,
    not the thin dlt-side alias, so this must be written to the DSN in its normalized form.
    """
    creds = _mssql_credentials("default")
    creds.on_partial()

    dsn = creds.get_odbc_dsn_dict()
    assert dsn["AUTHENTICATION"] == "ActiveDirectoryDefault"
    assert "UID" not in dsn
    assert "PWD" not in dsn
    assert creds.to_odbc_attrs_before() is None
    assert creds.has_default_credentials() is False


@pytest.mark.parametrize(
    "authentication",
    ["auto", "cli", "environment", "interactive", "devicecode", "msi", "managedidentity"],
)
def test_mssql_unsupported_alias_raises(authentication: str) -> None:
    """Only the canonical `ActiveDirectory*` names (and the `default` alias) are supported."""
    creds = _mssql_credentials(authentication)
    with pytest.raises(ConfigurationException):
        validate_authentication(creds)


def test_mssql_service_principal_with_secret() -> None:
    creds = _mssql_credentials(
        "ActiveDirectoryServicePrincipal",
        azure_tenant_id="t",
        azure_client_id="c",
        azure_client_secret="s",
    )
    creds.on_partial()

    dsn = creds.get_odbc_dsn_dict()
    assert dsn["AUTHENTICATION"] == "ActiveDirectoryServicePrincipal"
    assert dsn["UID"] == "c@t"
    assert dsn["PWD"] == "s"
    assert creds.to_odbc_attrs_before() is None


def test_mssql_service_principal_without_secret_passes_through() -> None:
    """No secret configured: dlt does not fall back to anything else, same as any other method."""
    creds = _mssql_credentials("ActiveDirectoryServicePrincipal")
    creds.on_partial()

    dsn = creds.get_odbc_dsn_dict()
    assert dsn["AUTHENTICATION"] == "ActiveDirectoryServicePrincipal"
    assert "UID" not in dsn
    assert "PWD" not in dsn
    assert creds.to_odbc_attrs_before() is None
    assert creds.has_default_credentials() is False


@pytest.mark.parametrize(
    "authentication",
    [
        "ActiveDirectoryIntegrated",
        "ActiveDirectoryInteractive",
        "ActiveDirectoryMsi",
        "ActiveDirectoryDefault",
        "ActiveDirectoryDeviceCode",
    ],
)
def test_mssql_authentication_method_passthrough(authentication: str) -> None:
    """Written straight to `Authentication=`; dlt builds no credential or attrs_before.

    mssql-python performs the sign-in for every supported method itself.
    """
    creds = _mssql_credentials(authentication)
    creds.on_partial()

    dsn = creds.get_odbc_dsn_dict()
    assert dsn["AUTHENTICATION"] == authentication
    assert "UID" not in dsn
    assert "PWD" not in dsn
    assert creds.to_odbc_attrs_before() is None
    assert creds.has_default_credentials() is False


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


def test_mssql_to_odbc_attrs_before_always_none() -> None:
    """mssql-python signs in for every supported authentication method itself: dlt injects
    nothing, regardless of what's configured."""
    creds = _mssql_credentials("ActiveDirectoryDefault")
    assert creds.to_odbc_attrs_before() is None

    creds = _mssql_credentials(username="loader", password="secret")
    assert creds.to_odbc_attrs_before() is None


def test_mssql_resolve_configuration_service_principal_without_secret() -> None:
    """Resolution succeeds without a Service Principal secret; dlt does not fall back to
    anything — the DSN just carries the method with no credentials attached."""
    creds = MsSqlCredentials()
    creds.host = "sql.example.com"
    creds.database = "test_db"
    creds.authentication = "ActiveDirectoryServicePrincipal"

    resolved = resolve_configuration(creds)

    assert resolved.is_resolved()
    assert resolved.to_odbc_attrs_before() is None
    dsn = resolved.get_odbc_dsn_dict()
    assert dsn["AUTHENTICATION"] == "ActiveDirectoryServicePrincipal"
    assert "UID" not in dsn
    assert "PWD" not in dsn


def test_mssql_resolve_configuration_authentication_passthrough() -> None:
    """A full `resolve_configuration()` round-trip writes the method straight to the DSN."""
    creds = MsSqlCredentials()
    creds.host = "sql.example.com"
    creds.database = "test_db"
    creds.authentication = "ActiveDirectoryDeviceCode"

    resolved = resolve_configuration(creds)

    assert resolved.is_resolved()
    assert uses_token_authentication(resolved) is True
    assert type(resolved.default_credentials()).__name__ == "DeviceCodeCredential"
    assert "AUTHENTICATION" not in resolved.get_odbc_dsn_dict()


class _RaisingTokenCredential:
    """A TokenCredential whose `get_token` must never be called (used to prove precedence)."""

    def get_token(self, *scopes: str, **kwargs: object) -> _FakeAccessToken:
        raise AssertionError("azure_credential.get_token() should not have been called")


def test_mssql_access_token_and_azure_credential_default_to_none() -> None:
    creds = MsSqlCredentials()
    assert creds.access_token is None
    assert creds.azure_credential is None


def test_mssql_get_access_token_returns_none_without_token_or_credential() -> None:
    creds = _mssql_credentials("ActiveDirectoryDeviceCode")
    assert get_access_token(creds) is None


def test_mssql_get_access_token_uses_azure_credential() -> None:
    creds = _mssql_credentials(azure_credential=_FakeTokenCredential())
    assert get_access_token(creds) == "fake-access-token"


def test_mssql_get_access_token_prefers_access_token_over_azure_credential() -> None:
    creds = _mssql_credentials(
        access_token="explicit-token", azure_credential=_RaisingTokenCredential()
    )
    assert get_access_token(creds) == "explicit-token"


def test_mssql_access_token_takes_precedence_over_authentication() -> None:
    creds = _mssql_credentials(
        "ActiveDirectoryServicePrincipal",
        azure_tenant_id="t",
        azure_client_id="c",
        azure_client_secret="s",
        access_token="explicit-token",
    )
    creds.on_partial()

    assert uses_token_authentication(creds) is True
    dsn = creds.get_odbc_dsn_dict()
    assert "AUTHENTICATION" not in dsn
    assert "UID" not in dsn
    assert "PWD" not in dsn

    attrs = creds.to_odbc_attrs_before()
    assert attrs is not None
    assert attrs[1256][4:].decode("utf-16-le") == "explicit-token"


def test_mssql_azure_credential_takes_precedence_over_authentication() -> None:
    creds = _mssql_credentials("ActiveDirectoryDeviceCode", azure_credential=_FakeTokenCredential())
    creds.on_partial()

    # setup_token_credential must skip the azure-identity DefaultAzureCredential machinery
    assert creds.has_default_credentials() is False
    assert uses_token_authentication(creds) is True

    dsn = creds.get_odbc_dsn_dict()
    assert "AUTHENTICATION" not in dsn

    attrs = creds.to_odbc_attrs_before()
    assert attrs is not None
    assert attrs[1256][4:].decode("utf-16-le") == "fake-access-token"


def test_mssql_resolve_configuration_access_token_without_username_password() -> None:
    creds = MsSqlCredentials()
    creds.host = "sql.example.com"
    creds.database = "test_db"
    creds.driver = "ODBC Driver 18 for SQL Server"
    creds.access_token = "explicit-token"

    resolved = resolve_configuration(creds)

    assert resolved.is_resolved()
    assert uses_token_authentication(resolved) is True
    assert "AUTHENTICATION" not in resolved.get_odbc_dsn_dict()
    assert resolved.to_odbc_attrs_before()[1256][4:].decode("utf-16-le") == "explicit-token"


def test_mssql_resolve_configuration_azure_credential_without_username_password() -> None:
    creds = MsSqlCredentials()
    creds.host = "sql.example.com"
    creds.database = "test_db"
    creds.driver = "ODBC Driver 18 for SQL Server"
    creds.azure_credential = _FakeTokenCredential()

    resolved = resolve_configuration(creds)

    assert resolved.is_resolved()
    assert uses_token_authentication(resolved) is True
    assert "AUTHENTICATION" not in resolved.get_odbc_dsn_dict()
