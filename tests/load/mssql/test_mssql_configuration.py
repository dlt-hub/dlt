import os
from typing import Any, Dict, List, Optional

import pytest

import mssql_python
from mssql_python import TokenProvider
from azure.identity import DefaultAzureCredential

from dlt.common.configuration import ConfigFieldMissingException, resolve_configuration
from dlt.common.configuration.exceptions import ConfigurationException
from dlt.common.schema import Schema
from dlt.common.utils import digest128
from dlt.common.warnings import DltDeprecationWarning
from dlt.destinations import mssql
from dlt.destinations.impl.mssql.configuration import (
    MsSqlClientConfiguration,
    MsSqlCredentials,
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
    # mssql-python installs and manages its own driver dependency, so the DSN carries no DRIVER
    # and any `driver` query parameter (legacy pyodbc config) is ignored.
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


def test_driver_is_accepted_but_deprecated() -> None:
    # A pyodbc-era `driver` still resolves, so existing configs keep working, but warns.
    with pytest.warns(DltDeprecationWarning, match="`driver` is deprecated"):
        creds = resolve_configuration(
            MsSqlCredentials(
                "mssql://test_user:test_pwd@sql.example.com/test_db?DRIVER=ODBC+Driver+17+for+SQL+Server"
            )
        )
    assert creds.driver == "ODBC Driver 17 for SQL Server"
    assert "DRIVER" not in creds.to_odbc_dsn()


def test_no_driver_does_not_warn(recwarn: pytest.WarningsRecorder) -> None:
    resolve_configuration(MsSqlCredentials("mssql://test_user:test_pwd@sql.example.com/test_db"))
    assert not [w for w in recwarn if issubclass(w.category, DltDeprecationWarning)]


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
    assert resolved.to_odbc_attrs_before() is None
    assert resolved.get_odbc_dsn_dict()["AUTHENTICATION"] == "ActiveDirectoryDeviceCode"


class _RaisingTokenCredential:
    """A TokenCredential whose `get_token` must never be called.

    dlt hands `azure_credential` to mssql-python untouched, so nothing on the dlt side may call
    it — neither the losing side of a precedence rule nor the winning one.
    """

    def get_token(self, *scopes: str, **kwargs: object) -> _FakeAccessToken:
        raise AssertionError("azure_credential.get_token() should not have been called")


class _MinimalTokenProvider:
    """The narrowest shape mssql-python documents: a single positional scope, no kwargs."""

    def __init__(self) -> None:
        self.scopes: List[str] = []

    def get_token(self, scope: str) -> _FakeAccessToken:
        self.scopes.append(scope)
        return _FakeAccessToken()


class _FalsyTokenCredential(_RaisingTokenCredential):
    """A credential that is falsy but present, e.g. a wrapper delegating `__len__` to a cache."""

    def __len__(self) -> int:
        return 0


def test_mssql_access_token_and_azure_credential_default_to_none() -> None:
    creds = MsSqlCredentials()
    assert creds.access_token is None
    assert creds.azure_credential is None


def test_mssql_no_attrs_without_token_or_credential() -> None:
    creds = _mssql_credentials("ActiveDirectoryDeviceCode")
    assert creds.to_odbc_attrs_before() is None


def test_mssql_azure_credential_is_handed_to_the_driver() -> None:
    """dlt passes the credential object through and never acquires a token itself."""
    credential = _RaisingTokenCredential()
    creds = _mssql_credentials(azure_credential=credential)

    assert creds.to_odbc_token_provider() is credential
    assert creds.to_odbc_attrs_before() is None


@pytest.mark.parametrize(
    "credential",
    [
        pytest.param(_FakeTokenCredential(), id="azure_identity_shape"),
        pytest.param(_MinimalTokenProvider(), id="minimal_get_token_scope"),
        # identity only: driving DefaultAzureCredential.get_token() would reach the network
        pytest.param(DefaultAzureCredential, id="default_azure_credential"),
    ],
)
def test_mssql_token_provider_shapes_pass_through_unchanged(credential: object) -> None:
    """Every provider shape mssql-python accepts reaches it as the same object dlt was given."""
    if credential is DefaultAzureCredential:
        # constructing it performs no I/O; a token is only requested on get_token()
        credential = DefaultAzureCredential()
    creds = _mssql_credentials(azure_credential=credential)

    assert creds.to_odbc_token_provider() is credential
    assert creds.to_odbc_attrs_before() is None


def test_mssql_minimal_token_provider_satisfies_the_driver_calling_convention() -> None:
    """The narrowest `get_token(scope)` shape must survive the driver's own acquisition path.

    A provider with no `*args`/`**kwargs` is the one most likely to break on a signature change,
    and dlt now hands such objects straight to mssql-python without ever calling them itself.
    """
    # private API (not in __all__/.pyi); scoped so a rename only breaks this test
    from mssql_python.auth import acquire_token_from_credential

    provider = _MinimalTokenProvider()
    # mypy gates the structural conformance to the protocol the driver declares for token_provider=
    checked: TokenProvider = provider

    token_struct, _ = acquire_token_from_credential(checked)  # type: ignore[arg-type]

    assert token_struct[4:].decode("utf-16-le") == "fake-access-token"
    assert provider.scopes == ["https://database.windows.net/.default"]


def test_mssql_access_token_wins_over_azure_credential() -> None:
    creds = _mssql_credentials(
        access_token="explicit-token", azure_credential=_RaisingTokenCredential()
    )
    attrs = creds.to_odbc_attrs_before()
    assert attrs is not None
    assert attrs[1256][4:].decode("utf-16-le") == "explicit-token"
    # the credential must not also reach the driver: it rejects two token sources at once
    assert creds.to_odbc_token_provider() is None


def test_mssql_access_token_takes_precedence_over_authentication() -> None:
    creds = _mssql_credentials(
        "ActiveDirectoryServicePrincipal",
        azure_tenant_id="t",
        azure_client_id="c",
        azure_client_secret="s",
        access_token="explicit-token",
    )
    creds.on_partial()

    dsn = creds.get_odbc_dsn_dict()
    assert "AUTHENTICATION" not in dsn
    assert "UID" not in dsn
    assert "PWD" not in dsn

    attrs = creds.to_odbc_attrs_before()
    assert attrs is not None
    assert attrs[1256][4:].decode("utf-16-le") == "explicit-token"


def test_mssql_azure_credential_takes_precedence_over_authentication() -> None:
    credential = _RaisingTokenCredential()
    creds = _mssql_credentials("ActiveDirectoryDeviceCode", azure_credential=credential)
    creds.on_partial()

    assert creds.has_default_credentials() is False

    dsn = creds.get_odbc_dsn_dict()
    assert "AUTHENTICATION" not in dsn

    assert creds.to_odbc_token_provider() is credential
    assert creds.to_odbc_attrs_before() is None


def _dsn_dict(creds: MsSqlCredentials) -> Dict[str, str]:
    """Parse `to_odbc_dsn()` back into a dict — what actually reaches mssql-python."""
    return dict(param.split("=", 1) for param in creds.to_odbc_dsn().split(";"))


_AUTH_QUERY_DSN = (
    "mssql://sql.example.com/test_db"
    "?authentication=ActiveDirectoryDefault&uid=u&pwd=p&trusted_connection=yes&Encrypt=yes"
)


@pytest.mark.parametrize("token_key", ["access_token", "azure_credential"])
def test_mssql_query_authentication_keys_dropped_for_explicit_token(token_key: str) -> None:
    """Auth query keys bypass `apply_authentication_to_dsn` and would win over the token.

    mssql-python rejects the `token_provider` combination outright, and on the `access_token` path
    its own `Authentication=` sign-in overwrites the injected token instead. The set mirrors the
    driver's `_SENSITIVE_KEYS`, so nothing an Entra token supersedes is left in the string.
    """
    token = "explicit-token" if token_key == "access_token" else _RaisingTokenCredential()
    creds = MsSqlCredentials(_AUTH_QUERY_DSN)
    setattr(creds, token_key, token)

    dsn = _dsn_dict(creds)
    assert "AUTHENTICATION" not in dsn
    assert "UID" not in dsn
    assert "PWD" not in dsn
    assert "TRUSTED_CONNECTION" not in dsn
    # only auth keys go; unrelated ODBC settings still pass through
    assert dsn["ENCRYPT"] == "yes"


def test_mssql_query_authentication_keys_kept_without_explicit_token() -> None:
    """Without a token the passthrough is untouched — the drop is scoped to the token paths."""
    dsn = _dsn_dict(MsSqlCredentials(_AUTH_QUERY_DSN))

    assert dsn["AUTHENTICATION"] == "ActiveDirectoryDefault"
    assert dsn["UID"] == "u"
    assert dsn["TRUSTED_CONNECTION"] == "yes"


def test_mssql_adbc_dsn_dict_keeps_query_credentials() -> None:
    """`get_odbc_dsn_dict()` also feeds `MssqlParquetCopyJob`, whose go-mssqldb driver cannot use
    an Entra token — dropping query credentials there would leave it with no way to authenticate."""
    creds = MsSqlCredentials(_AUTH_QUERY_DSN)
    creds.access_token = "explicit-token"

    assert creds.get_odbc_dsn_dict()["UID"] == "u"
    assert creds.get_odbc_dsn_dict()["PWD"] == "p"


def test_mssql_resolve_configuration_access_token_without_username_password() -> None:
    creds = MsSqlCredentials()
    creds.host = "sql.example.com"
    creds.database = "test_db"
    creds.driver = "ODBC Driver 18 for SQL Server"
    creds.access_token = "explicit-token"

    resolved = resolve_configuration(creds)

    assert resolved.is_resolved()
    assert "AUTHENTICATION" not in resolved.get_odbc_dsn_dict()
    assert resolved.to_odbc_attrs_before()[1256][4:].decode("utf-16-le") == "explicit-token"


def test_mssql_resolve_configuration_falsy_azure_credential() -> None:
    """`on_partial` must test the credential by identity, not truthiness.

    A credential delegating `__len__` to an empty cache is falsy but perfectly usable. Treated as
    absent it drops through to the SQL-login branch and demands `username`/`password` that the
    token path never uses. Only `resolve_configuration` exercises `on_partial`, so the
    `to_odbc_*` matrix cannot catch this.
    """
    creds = MsSqlCredentials()
    creds.host = "sql.example.com"
    creds.database = "test_db"
    credential = _FalsyTokenCredential()
    creds.azure_credential = credential

    resolved = resolve_configuration(creds)

    assert resolved.is_resolved()
    assert resolved.to_odbc_token_provider() is credential
    assert "AUTHENTICATION" not in _dsn_dict(resolved)


def test_mssql_resolve_configuration_azure_credential_without_username_password() -> None:
    creds = MsSqlCredentials()
    creds.host = "sql.example.com"
    creds.database = "test_db"
    creds.driver = "ODBC Driver 18 for SQL Server"
    credential = _RaisingTokenCredential()
    creds.azure_credential = credential

    resolved = resolve_configuration(creds)

    assert resolved.is_resolved()
    assert "AUTHENTICATION" not in resolved.get_odbc_dsn_dict()
    assert resolved.to_odbc_token_provider() is credential


@pytest.mark.parametrize(
    "kwargs,expected",
    [
        pytest.param({"username": "loader", "password": "s"}, None, id="sql_login"),
        pytest.param({"authentication": "ActiveDirectoryDefault"}, "dsn", id="authentication"),
        pytest.param(
            {"authentication": "ActiveDirectoryServicePrincipal", "azure_client_id": "c"},
            "dsn",
            id="service_principal",
        ),
        pytest.param({"access_token": "t"}, "attrs_before", id="access_token"),
        pytest.param(
            {"azure_credential": _RaisingTokenCredential()}, "token_provider", id="azure_credential"
        ),
        pytest.param(
            {"access_token": "t", "azure_credential": _RaisingTokenCredential()},
            "attrs_before",
            id="access_token_over_credential",
        ),
        pytest.param(
            {
                "authentication": "ActiveDirectoryDefault",
                "azure_credential": _RaisingTokenCredential(),
            },
            "token_provider",
            id="credential_over_authentication",
        ),
        pytest.param(
            {
                "authentication": "ActiveDirectoryDefault",
                "access_token": "t",
                "azure_credential": _RaisingTokenCredential(),
            },
            "attrs_before",
            id="access_token_over_both",
        ),
        pytest.param(
            {
                "authentication": "ActiveDirectoryDefault",
                "azure_credential": _FalsyTokenCredential(),
            },
            "token_provider",
            id="falsy_credential_over_authentication",
        ),
    ],
)
def test_mssql_exactly_one_authentication_mechanism_reaches_the_driver(
    kwargs: Dict[str, Any], expected: Optional[str]
) -> None:
    """mssql-python raises `InterfaceError` when two token sources arrive together.

    Whatever is configured, at most one of `Authentication=`, `attrs_before` and `token_provider`
    may be populated, and it must be the one precedence selects.
    """
    creds = _mssql_credentials(**kwargs)

    mechanisms = {
        "dsn": "AUTHENTICATION" in _dsn_dict(creds),
        "attrs_before": creds.to_odbc_attrs_before() is not None,
        "token_provider": creds.to_odbc_token_provider() is not None,
    }

    assert [name for name, used in mechanisms.items() if used] == ([expected] if expected else [])


def test_mssql_distinct_raw_tokens_are_not_pooled_by_connection_string_alone() -> None:
    """Two `access_token` values produce an identical DSN — nothing in the connection-string text
    distinguishes them. `compute_token_identity` is the driver's own v1.13 pool-identity function
    for raw `attrs_before` tokens (only exists from that baseline on, so this test cannot even
    collect against an older driver); it must hash the two tokens to different pool identities, so
    they cannot be described as sharing a connection-string-only pool."""
    # private API (not in __all__/.pyi); scoped so a rename only breaks this test
    from mssql_python.auth import compute_token_identity

    creds_a = _mssql_credentials(access_token="token-a")
    creds_b = _mssql_credentials(access_token="token-b")

    assert creds_a.to_odbc_dsn() == creds_b.to_odbc_dsn()

    token_struct_a = creds_a.to_odbc_attrs_before()[1256]
    token_struct_b = creds_b.to_odbc_attrs_before()[1256]
    assert compute_token_identity(token_struct_a) != compute_token_identity(token_struct_b)


class _FailingTokenCredential:
    """Stands in for a credential that cannot sign in (expired CLI login, unreachable IMDS, ...)."""

    def get_token(self, scope: str) -> _FakeAccessToken:
        raise ValueError("credential could not acquire a token")


def _mssql_client(creds: MsSqlCredentials) -> MsSqlClient:
    return MsSqlClient("dataset", "staging_dataset", creds, mssql().capabilities())


def test_mssql_token_provider_failure_keeps_credential_error_as_cause() -> None:
    """The driver acquires the token inside `connect()`, before any network I/O.

    A failing credential now surfaces as a driver exception dlt's classifier recognises, with the
    original credential error still reachable as its cause. `open_connection` carries no
    `@raise_open_connection_error`, so the classifier is applied at the call sites that wrap driver
    calls, not here — asserting on the raw exception is what actually reaches a caller today.
    """
    creds = _mssql_credentials(azure_credential=_FailingTokenCredential())

    with pytest.raises(mssql_python.OperationalError) as exc_info:
        _mssql_client(creds).open_connection()

    cause = exc_info.value.__cause__
    assert isinstance(cause, ValueError)
    assert str(cause) == "credential could not acquire a token"

    assert MsSqlClient.is_dbapi_exception(exc_info.value)
    db_ex = MsSqlClient._make_database_exception(exc_info.value)
    assert isinstance(db_ex, DatabaseTransientException)


def test_mssql_token_provider_rejected_alongside_other_token_sources() -> None:
    """The exclusivity dlt upholds is enforced by the driver, not merely assumed.

    Both combinations are rejected before any token acquisition or network I/O, so a regression in
    dlt's precedence surfaces as a hard `InterfaceError` rather than a silently ignored token.
    """
    provider = _MinimalTokenProvider()

    with pytest.raises(mssql_python.InterfaceError, match="Authentication"):
        mssql_python.connect(
            "SERVER=sql.example.com,1433;DATABASE=test_db;AUTHENTICATION=ActiveDirectoryDefault",
            token_provider=provider,
        )

    with pytest.raises(mssql_python.InterfaceError, match="SQL_COPT_SS_ACCESS_TOKEN"):
        mssql_python.connect(
            "SERVER=sql.example.com,1433;DATABASE=test_db",
            attrs_before=_mssql_credentials(access_token="t").to_odbc_attrs_before(),  # type: ignore[arg-type]
            token_provider=provider,
        )

    assert provider.scopes == []
