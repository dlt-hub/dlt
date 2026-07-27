"""Tests for Microsoft Fabric Warehouse destination configuration"""

import base64
import json
import os
import struct
import sys
import time
from typing import Optional, cast
from unittest.mock import MagicMock

import pytest

from dlt.common.configuration import resolve_configuration
from dlt.common.configuration.exceptions import ConfigurationException
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.typing import PreparedTableSchema
from dlt.common.schema import Schema
from dlt.common.schema.typing import TColumnSchema, TDataType
from dlt.common.utils import digest128
from dlt.destinations.impl.fabric.factory import fabric, FabricTypeMapper
from dlt.destinations.impl.fabric.configuration import (
    FabricCredentials,
    FabricClientConfiguration,
)
from dlt.common.runtime.fab_notebookutils import (
    FabNotebookUtilsCredential,
    is_fab_notebookutils_available,
    _decode_jwt_expiry,
)

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


def test_fabric_factory() -> None:
    """Test Fabric destination factory with default settings"""
    dest = fabric()

    # Test destination properties without requiring credentials
    assert dest.destination_name == "fabric"
    assert dest.capabilities().has_case_sensitive_identifiers is True
    # Without staging configured, Fabric uses insert_values (inherited from Synapse)
    assert dest.capabilities().preferred_loader_file_format == "insert_values"
    assert dest.capabilities().sqlglot_dialect == "fabric"


def test_fabric_credentials_service_principal() -> None:
    """Test Fabric credentials with Service Principal configuration"""
    creds = FabricCredentials()
    creds.host = "abc12345-6789-def0-1234-56789abcdef0.datawarehouse.fabric.microsoft.com"
    creds.database = "mydb"
    creds.azure_tenant_id = "test-tenant-id"
    creds.azure_client_id = "test-client-id"
    creds.azure_client_secret = "test-client-secret"
    # Set driver to skip ODBC check

    # Call on_partial manually to trigger credential conversion
    creds.on_partial()

    # Check that username/password were auto-generated from Service Principal
    assert creds.azure_client_id
    assert creds.azure_client_secret


def test_fabric_credentials_odbc_dsn() -> None:
    """Test that Fabric credentials generate correct ODBC DSN with Fabric-specific parameters"""
    creds = FabricCredentials()
    creds.host = "abc12345-6789-def0-1234-56789abcdef0.datawarehouse.fabric.microsoft.com"
    creds.database = "mydb"
    creds.azure_tenant_id = "test-tenant-id"
    creds.azure_client_id = "test-client-id"
    creds.azure_client_secret = "test-client-secret"

    # Resolve to trigger on_partial and on_resolved
    # Get ODBC DSN parameters
    dsn_dict = creds.get_odbc_dsn_dict()

    # Verify Fabric-specific parameters are added
    assert dsn_dict["AUTHENTICATION"] == "ActiveDirectoryServicePrincipal"
    assert "LongAsMax" not in dsn_dict
    assert dsn_dict["UID"] == "test-client-id@test-tenant-id"
    assert dsn_dict["PWD"] == "test-client-secret"
    # mssql-python bundles its own driver, so the DSN carries no DRIVER key
    assert "DRIVER" not in dsn_dict
    assert (
        dsn_dict["SERVER"]
        == "abc12345-6789-def0-1234-56789abcdef0.datawarehouse.fabric.microsoft.com,1433"
    )
    assert dsn_dict["DATABASE"] == "mydb"


def test_fabric_configuration_defaults() -> None:
    """Test Fabric configuration with default collation"""
    config = FabricClientConfiguration()

    # Fabric should default to UTF-8 collation
    assert config.collation == "Latin1_General_100_BIN2_UTF8"
    assert config.destination_type == "fabric"


@pytest.mark.parametrize(
    "host,port,expected_fingerprint",
    [
        pytest.param(None, None, "", id="empty"),
        pytest.param("host1", 1433, digest128("host1:1433"), id="host_default_port"),
        pytest.param("host1", 1444, digest128("host1:1444"), id="host_custom_port"),
    ],
)
def test_fabric_fingerprint(
    host: Optional[str], port: Optional[int], expected_fingerprint: str
) -> None:
    if host:
        credentials = FabricCredentials()
        credentials.host = host
        if port is not None:
            credentials.port = port
    else:
        credentials = None

    config = FabricClientConfiguration(credentials=credentials)

    assert config.fingerprint() == expected_fingerprint


def test_fabric_configuration_custom_collation() -> None:
    """Test Fabric configuration with custom collation"""
    config = FabricClientConfiguration()
    config.collation = "Latin1_General_100_CI_AS_KS_WS_SC_UTF8"

    assert config.collation == "Latin1_General_100_CI_AS_KS_WS_SC_UTF8"


def test_fabric_type_mapper() -> None:
    """Test Fabric type mapper converts nvarchar to varchar and datetimeoffset to datetime2"""
    # Create a mock table for testing
    table = cast(PreparedTableSchema, {"name": "test_table", "columns": {}})

    caps = DestinationCapabilitiesContext.generic_capabilities("parquet")
    mapper = FabricTypeMapper(caps)

    # Test that text type gets converted to varchar (not nvarchar)
    text_col = cast(TColumnSchema, {"name": "test", "data_type": "text", "nullable": True})
    result = mapper.to_destination_type(text_col, table)
    assert "varchar" in result.lower()
    assert "nvarchar" not in result.lower()

    # Test that timestamp uses datetime2 with precision 6 (not datetimeoffset)
    timestamp_col = cast(
        TColumnSchema, {"name": "test", "data_type": "timestamp", "nullable": True}
    )
    result = mapper.to_destination_type(timestamp_col, table)
    assert "datetime2" in result.lower()
    assert "datetimeoffset" not in result.lower()


def test_fabric_credentials_missing_service_principal() -> None:
    """Test that credentials can be built without Service Principal fields set"""
    creds = FabricCredentials()
    creds.host = "test.datawarehouse.fabric.microsoft.com"
    creds.database = "testdb"

    assert creds.host == "test.datawarehouse.fabric.microsoft.com"
    assert creds.database == "testdb"


def test_fabric_credentials_service_principal_auto_conversion() -> None:
    """Test that Service Principal credentials are automatically converted to username/password"""
    creds = FabricCredentials()
    creds.host = "test.datawarehouse.fabric.microsoft.com"
    creds.database = "testdb"

    creds.azure_tenant_id = "test-tenant"
    creds.azure_client_id = "test-client"
    creds.azure_client_secret = "test-secret"

    creds = resolve_configuration(creds)
    # Verify automatic conversion happened
    assert creds.azure_client_id
    assert creds.azure_client_secret


def test_fabric_credentials_no_driver_validation() -> None:
    """Test that Fabric credentials don't enforce ODBC driver restrictions at config time"""
    # Fabric requires ODBC Driver 18, but we allow configuration with driver parameter
    # The actual driver validation happens at connection time, not during config parsing
    creds = FabricCredentials()
    creds.host = "test.datawarehouse.fabric.microsoft.com"
    creds.database = "test_db"
    creds.azure_tenant_id = "test-tenant-id"
    creds.azure_client_id = "test-client-id"
    creds.azure_client_secret = "test-client-secret"

    # Verify credentials can be created (driver validation is not enforced at this stage)
    assert creds.host == "test.datawarehouse.fabric.microsoft.com"
    assert creds.database == "test_db"


def test_fabric_credentials_longasmax_absent() -> None:
    """Test that LongAsMax is never emitted: mssql-python handles long/max types natively
    and rejects unknown DSN keywords."""
    creds = FabricCredentials()
    creds.host = "test.datawarehouse.fabric.microsoft.com"
    creds.database = "testdb"
    creds.azure_tenant_id = "test-tenant"
    creds.azure_client_id = "test-client"
    creds.azure_client_secret = "test-secret"

    # Get ODBC DSN and verify LongAsMax is absent
    dsn_dict = creds.get_odbc_dsn_dict()
    assert "LongAsMax" not in dsn_dict


def test_fabric_credentials_authentication_method() -> None:
    """Test that Service Principal authentication method is correctly set"""
    creds = FabricCredentials()
    creds.host = "test.datawarehouse.fabric.microsoft.com"
    creds.database = "testdb"
    creds.azure_tenant_id = "test-tenant"
    creds.azure_client_id = "test-client"
    creds.azure_client_secret = "test-secret"

    # Verify ActiveDirectoryServicePrincipal is set
    dsn_dict = creds.get_odbc_dsn_dict()
    assert dsn_dict["AUTHENTICATION"] == "ActiveDirectoryServicePrincipal"


@pytest.mark.parametrize("data_type", ["text", "json"])
def test_fabric_type_mapper_scales_varchar_precision(data_type: TDataType) -> None:
    """Fabric varchar uses UTF-8 byte semantics; character precision must be scaled."""
    table = cast(PreparedTableSchema, {"name": "test_table", "columns": {}})
    caps = DestinationCapabilitiesContext.generic_capabilities("parquet")
    mapper = FabricTypeMapper(caps)

    col = cast(
        TColumnSchema, {"name": "c", "data_type": data_type, "precision": 100, "nullable": True}
    )
    assert mapper.to_destination_type(col, table) == "varchar(400)"

    # 2000*4=8000 is the longest length Fabric accepts
    col = cast(
        TColumnSchema,
        {"name": "c", "data_type": data_type, "precision": 2000, "nullable": True},
    )
    assert mapper.to_destination_type(col, table) == "varchar(8000)"

    # 2001*4=8004 > 8000 → varchar(max)
    col = cast(
        TColumnSchema,
        {"name": "c", "data_type": data_type, "precision": 2001, "nullable": True},
    )
    assert mapper.to_destination_type(col, table) == "varchar(max)"

    col = cast(TColumnSchema, {"name": "c", "data_type": data_type, "nullable": True})
    assert mapper.to_destination_type(col, table) == "varchar(max)"

    col = cast(
        TColumnSchema, {"name": "c", "data_type": data_type, "precision": 10, "nullable": True}
    )
    assert mapper.to_destination_type(col, table) == "varchar(40)"


class _FakeAccessToken:
    token = "fake-access-token"


class _FakeTokenCredential:
    """Minimal azure-identity-like credential, avoids hitting Azure in unit tests."""

    def get_token(self, *scopes: str, **kwargs: object) -> _FakeAccessToken:
        return _FakeAccessToken()


def _warehouse_credentials(
    authentication: str | None = None, **kwargs: object
) -> FabricCredentials:
    creds = FabricCredentials()
    creds.host = "test.datawarehouse.fabric.microsoft.com"
    creds.database = "testdb"
    if authentication is not None:
        creds.authentication = authentication
    for key, value in kwargs.items():
        setattr(creds, key, value)
    return creds


def test_fabric_authentication_default_is_service_principal() -> None:
    assert FabricCredentials().authentication == "ActiveDirectoryServicePrincipal"


def test_fabric_default_alias_normalizes_in_dsn() -> None:
    """The `default` alias resolves to the canonical name mssql-python recognizes."""
    creds = _warehouse_credentials("default")
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
def test_fabric_unsupported_alias_raises(authentication: str) -> None:
    """Only the canonical `ActiveDirectory*` names (and the `default` alias) are supported."""
    creds = _warehouse_credentials(authentication)
    with pytest.raises(ConfigurationException):
        creds.on_partial()  # resolves (host+database present) -> on_resolved -> validate raises


def test_fabric_service_principal_without_secret_passes_through() -> None:
    """No secret configured: dlt does not fall back to anything else, same as any other method."""
    creds = _warehouse_credentials()
    creds.on_partial()

    dsn = creds.get_odbc_dsn_dict()
    assert dsn["AUTHENTICATION"] == "ActiveDirectoryServicePrincipal"
    assert "UID" not in dsn
    assert "PWD" not in dsn
    assert creds.to_odbc_attrs_before() is None
    assert creds.has_default_credentials() is False


def test_fabric_service_principal_with_secret() -> None:
    creds = _warehouse_credentials(
        azure_tenant_id="t", azure_client_id="c", azure_client_secret="s"
    )
    creds.on_partial()

    dsn = creds.get_odbc_dsn_dict()
    assert dsn["AUTHENTICATION"] == "ActiveDirectoryServicePrincipal"
    assert dsn["UID"] == "c@t"
    assert dsn["PWD"] == "s"
    assert creds.to_odbc_attrs_before() is None


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
def test_fabric_authentication_method_passthrough(authentication: str) -> None:
    """Written straight to `Authentication=`; dlt builds no credential or attrs_before.

    mssql-python performs the sign-in for every supported method itself.
    """
    creds = _warehouse_credentials(authentication)
    creds.on_partial()

    dsn = creds.get_odbc_dsn_dict()
    assert dsn["AUTHENTICATION"] == authentication
    assert "UID" not in dsn
    assert "PWD" not in dsn
    assert creds.to_odbc_attrs_before() is None
    assert creds.has_default_credentials() is False


def test_fabric_active_directory_password() -> None:
    creds = _warehouse_credentials(
        "ActiveDirectoryPassword", username="user@contoso.com", password="pwd"
    )
    creds.on_partial()

    dsn = creds.get_odbc_dsn_dict()
    assert dsn["AUTHENTICATION"] == "ActiveDirectoryPassword"
    assert dsn["UID"] == "user@contoso.com"
    assert dsn["PWD"] == "pwd"
    assert creds.to_odbc_attrs_before() is None


def test_fabric_active_directory_password_requires_username_password() -> None:
    creds = _warehouse_credentials("ActiveDirectoryPassword")
    with pytest.raises(ConfigurationException):
        creds.on_partial()  # on_partial -> resolve() -> on_resolved validates


def test_fabric_unsupported_authentication_raises() -> None:
    creds = _warehouse_credentials("SqlPassword")
    with pytest.raises(ConfigurationException):
        creds.on_partial()


def test_fabric_to_odbc_attrs_before_always_none() -> None:
    """mssql-python signs in for every supported authentication method itself: dlt injects
    nothing, regardless of what's configured."""
    creds = _warehouse_credentials("ActiveDirectoryDefault")
    assert creds.to_odbc_attrs_before() is None

    creds = _warehouse_credentials("ActiveDirectoryServicePrincipal")  # no secret set
    assert creds.to_odbc_attrs_before() is None


def test_fabric_resolve_configuration_service_principal_without_secret() -> None:
    """Resolution succeeds without a Service Principal secret; dlt does not fall back to
    anything — the DSN just carries the method with no credentials attached."""
    creds = FabricCredentials()
    creds.host = "abc.datawarehouse.fabric.microsoft.com"
    creds.database = "mydb"

    resolved = resolve_configuration(creds)

    assert resolved.is_resolved()
    assert resolved.to_odbc_attrs_before() is None
    dsn = resolved.get_odbc_dsn_dict()
    assert dsn["AUTHENTICATION"] == "ActiveDirectoryServicePrincipal"
    assert "UID" not in dsn
    assert "PWD" not in dsn


def test_fabric_resolve_configuration_authentication_passthrough() -> None:
    """A full `resolve_configuration()` round-trip writes the method straight to the DSN."""
    creds = FabricCredentials()
    creds.host = "abc.datawarehouse.fabric.microsoft.com"
    creds.database = "mydb"
    creds.authentication = "ActiveDirectoryDeviceCode"

    resolved = resolve_configuration(creds)

    assert resolved.is_resolved()
    assert resolved.to_odbc_attrs_before() is None
    assert resolved.get_odbc_dsn_dict()["AUTHENTICATION"] == "ActiveDirectoryDeviceCode"


class _RaisingTokenCredential:
    """A TokenCredential whose `get_token` must never be called (used to prove precedence)."""

    def get_token(self, *scopes: str, **kwargs: object) -> _FakeAccessToken:
        raise AssertionError("azure_credential.get_token() should not have been called")


def test_fabric_access_token_and_azure_credential_default_to_none() -> None:
    creds = FabricCredentials()
    assert creds.access_token is None
    assert creds.azure_credential is None


def test_fabric_azure_credential_is_injected() -> None:
    creds = _warehouse_credentials(azure_credential=_FakeTokenCredential())
    attrs = creds.to_odbc_attrs_before()
    assert attrs is not None
    assert attrs[1256][4:].decode("utf-16-le") == "fake-access-token"


def test_fabric_access_token_wins_over_azure_credential() -> None:
    creds = _warehouse_credentials(
        access_token="explicit-token", azure_credential=_RaisingTokenCredential()
    )
    attrs = creds.to_odbc_attrs_before()
    assert attrs is not None
    assert attrs[1256][4:].decode("utf-16-le") == "explicit-token"


def test_fabric_access_token_takes_precedence_over_default_service_principal() -> None:
    """access_token bypasses the default ActiveDirectoryServicePrincipal authentication entirely,
    even though that method is on by default for Fabric and no Service Principal secret is set."""
    creds = _warehouse_credentials(access_token="explicit-token")
    creds.on_partial()

    dsn = creds.get_odbc_dsn_dict()
    assert "AUTHENTICATION" not in dsn
    assert "UID" not in dsn
    assert "PWD" not in dsn

    attrs = creds.to_odbc_attrs_before()
    assert attrs is not None
    assert attrs[1256][4:].decode("utf-16-le") == "explicit-token"


def test_fabric_azure_credential_takes_precedence_over_authentication() -> None:
    creds = _warehouse_credentials(
        "ActiveDirectoryDeviceCode", azure_credential=_FakeTokenCredential()
    )
    creds.on_partial()

    assert creds.has_default_credentials() is False

    dsn = creds.get_odbc_dsn_dict()
    assert "AUTHENTICATION" not in dsn

    attrs = creds.to_odbc_attrs_before()
    assert attrs is not None
    assert attrs[1256][4:].decode("utf-16-le") == "fake-access-token"


def test_fabric_resolve_configuration_access_token_without_service_principal() -> None:
    creds = FabricCredentials()
    creds.host = "abc.datawarehouse.fabric.microsoft.com"
    creds.database = "mydb"
    creds.access_token = "explicit-token"

    resolved = resolve_configuration(creds)

    assert resolved.is_resolved()
    assert "AUTHENTICATION" not in resolved.get_odbc_dsn_dict()


def _make_jwt(exp: int) -> str:
    header = base64.urlsafe_b64encode(b'{"alg":"none"}').rstrip(b"=").decode()
    payload = base64.urlsafe_b64encode(json.dumps({"exp": exp}).encode()).rstrip(b"=").decode()
    return f"{header}.{payload}.sig"


@pytest.fixture()
def mock_notebookutils():
    mod = MagicMock()
    mod.credentials.getToken = MagicMock(return_value=_make_jwt(int(time.time()) + 3600))
    sys.modules["notebookutils"] = mod
    yield mod
    sys.modules.pop("notebookutils", None)


@pytest.fixture()
def no_notebookutils():
    saved = sys.modules.pop("notebookutils", None)
    yield
    if saved is not None:
        sys.modules["notebookutils"] = saved


def test_decode_jwt_expiry_valid() -> None:
    exp = int(time.time()) + 7200
    assert _decode_jwt_expiry(_make_jwt(exp)) == exp


def test_decode_jwt_expiry_garbage() -> None:
    assert _decode_jwt_expiry("not-a-jwt") is None


def test_notebookutils_get_token(mock_notebookutils: MagicMock) -> None:
    cred = FabNotebookUtilsCredential("https://database.windows.net/")
    token = cred.get_token()
    assert token.token == mock_notebookutils.credentials.getToken.return_value
    mock_notebookutils.credentials.getToken.assert_called_once_with("https://database.windows.net/")


def test_notebookutils_sql_alias(mock_notebookutils: MagicMock) -> None:
    cred = FabNotebookUtilsCredential("sql")
    cred.get_token()
    mock_notebookutils.credentials.getToken.assert_called_once_with("https://database.windows.net/")


def test_notebookutils_caches_token(mock_notebookutils: MagicMock) -> None:
    cred = FabNotebookUtilsCredential("storage")
    t1 = cred.get_token()
    t2 = cred.get_token()
    assert t1 is t2
    assert mock_notebookutils.credentials.getToken.call_count == 1


def test_notebookutils_refreshes_near_expiry(mock_notebookutils: MagicMock) -> None:
    near_expiry_jwt = _make_jwt(int(time.time()) + 100)
    fresh_jwt = _make_jwt(int(time.time()) + 3600)
    mock_notebookutils.credentials.getToken.side_effect = [near_expiry_jwt, fresh_jwt]

    cred = FabNotebookUtilsCredential("storage")
    t1 = cred.get_token()
    assert t1.token == near_expiry_jwt
    t2 = cred.get_token()
    assert t2.token == fresh_jwt
    assert mock_notebookutils.credentials.getToken.call_count == 2


def test_notebookutils_mssparkutils_fallback(mock_notebookutils: MagicMock) -> None:
    del mock_notebookutils.credentials.getToken
    mock_notebookutils.mssparkutils.credentials.getToken = MagicMock(
        return_value=_make_jwt(int(time.time()) + 3600)
    )

    cred = FabNotebookUtilsCredential("storage")
    cred.get_token()
    mock_notebookutils.mssparkutils.credentials.getToken.assert_called_once()


def test_notebookutils_unavailable_raises(no_notebookutils: None) -> None:
    cred = FabNotebookUtilsCredential("storage")
    with pytest.raises(ConfigurationException, match="NotebookUtils"):
        cred.get_token()


def test_notebookutils_available_in_fabric(mock_notebookutils: MagicMock) -> None:
    assert is_fab_notebookutils_available() is True


def test_notebookutils_available_outside_fabric(no_notebookutils: None) -> None:
    assert is_fab_notebookutils_available() is False


def test_notebookutils_available_without_credential_api(mock_notebookutils: MagicMock) -> None:
    del mock_notebookutils.credentials
    del mock_notebookutils.mssparkutils
    assert is_fab_notebookutils_available() is False


def test_notebookutils_closeable(mock_notebookutils: MagicMock) -> None:
    """adlfs and the Azure SDK clients close the credential they were handed."""
    with FabNotebookUtilsCredential("storage") as cred:
        assert cred.get_token().token
    cred.close()
    assert cred.get_token().token


def test_notebookutils_importable_from_runtime() -> None:
    from dlt.common.runtime.fab_notebookutils import FabNotebookUtilsCredential as Cls

    assert Cls is FabNotebookUtilsCredential


def test_fabric_notebookutils_creates_credential(mock_notebookutils: MagicMock) -> None:
    creds = _warehouse_credentials("fab_notebookutils")
    creds.on_partial()
    assert isinstance(creds.azure_credential, FabNotebookUtilsCredential)


def test_fabric_notebookutils_dsn_has_no_authentication_key(
    mock_notebookutils: MagicMock,
) -> None:
    creds = _warehouse_credentials("fab_notebookutils")
    creds.on_partial()
    dsn = creds.get_odbc_dsn_dict()
    assert "AUTHENTICATION" not in dsn
    assert "UID" not in dsn
    assert "PWD" not in dsn


def test_fabric_notebookutils_attrs_before(mock_notebookutils: MagicMock) -> None:
    creds = _warehouse_credentials("fab_notebookutils")
    creds.on_partial()
    attrs = creds.to_odbc_attrs_before()
    assert attrs is not None
    token_struct = attrs[1256]
    length = struct.unpack("<I", token_struct[:4])[0]
    assert length == len(token_struct) - 4


def test_fabric_notebookutils_does_not_replace_explicit_azure_credential(
    mock_notebookutils: MagicMock,
) -> None:
    explicit = _FakeTokenCredential()
    creds = _warehouse_credentials("fab_notebookutils", azure_credential=explicit)
    creds.on_partial()
    assert creds.azure_credential is explicit


def test_fabric_access_token_precedence_over_notebookutils(
    mock_notebookutils: MagicMock,
) -> None:
    creds = _warehouse_credentials("fab_notebookutils", access_token="explicit-token")
    creds.on_partial()
    attrs = creds.to_odbc_attrs_before()
    assert attrs is not None
    assert attrs[1256][4:].decode("utf-16-le") == "explicit-token"
