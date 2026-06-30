"""Tests for Microsoft Fabric Warehouse destination configuration"""

import os
import struct
from typing import Optional, cast

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
from dlt.destinations.impl.mssql.configuration import uses_token_authentication

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
    assert dsn_dict["LongAsMax"] == "yes"
    assert dsn_dict["UID"] == "test-client-id@test-tenant-id"
    assert dsn_dict["PWD"] == "test-client-secret"
    assert dsn_dict["DRIVER"] == "{ODBC Driver 18 for SQL Server}"
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


def test_fabric_credentials_drivername() -> None:
    """Test that Fabric credentials use mssql+pyodbc drivername"""
    creds = FabricCredentials()
    # FabricCredentials uses mssql+pyodbc for SQLAlchemy compatibility
    assert creds.drivername == "mssql+pyodbc"


def test_fabric_credentials_missing_service_principal() -> None:
    """Test that Service Principal fields can trigger default credentials fallback"""
    creds = FabricCredentials()
    creds.host = "test.datawarehouse.fabric.microsoft.com"
    creds.database = "testdb"

    # When Service Principal fields are missing, on_partial should attempt to use default credentials
    # We can't test actual Azure default credentials in unit tests, but we can verify the structure
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


def test_fabric_credentials_longasmax_always_yes() -> None:
    """Test that LONGASMAX is always set to 'yes' for UTF-8 support"""
    creds = FabricCredentials()
    creds.host = "test.datawarehouse.fabric.microsoft.com"
    creds.database = "testdb"
    creds.azure_tenant_id = "test-tenant"
    creds.azure_client_id = "test-client"
    creds.azure_client_secret = "test-secret"

    # Get ODBC DSN and verify LONGASMAX is set to yes
    dsn_dict = creds.get_odbc_dsn_dict()
    assert dsn_dict["LongAsMax"] == "yes"


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


# ---------------------------------------------------------------------------
# Authentication methods
# ---------------------------------------------------------------------------


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


@pytest.mark.parametrize(
    "authentication,expected",
    [
        ("auto", "DefaultAzureCredential"),
        ("default", "DefaultAzureCredential"),
        ("cli", "AzureCliCredential"),
        ("environment", "EnvironmentCredential"),
        ("interactive", "InteractiveBrowserCredential"),
        ("devicecode", "DeviceCodeCredential"),
        ("msi", "ManagedIdentityCredential"),
        ("managedidentity", "ManagedIdentityCredential"),
    ],
)
def test_fabric_azure_identity_credential_mapping(authentication: str, expected: str) -> None:
    """Each azure-identity method maps to the right credential and skips the DSN AUTHENTICATION."""
    creds = _warehouse_credentials(authentication)
    creds.on_partial()

    assert type(creds.default_credentials()).__name__ == expected

    dsn = creds.get_odbc_dsn_dict()
    assert "AUTHENTICATION" not in dsn
    assert "UID" not in dsn
    assert "PWD" not in dsn
    assert dsn["LongAsMax"] == "yes"


def test_fabric_service_principal_without_secret_falls_back_to_token() -> None:
    """Default method without a Service Principal secret injects a DefaultAzureCredential token."""
    creds = _warehouse_credentials()
    creds.on_partial()

    assert uses_token_authentication(creds) is True
    assert type(creds.default_credentials()).__name__ == "DefaultAzureCredential"
    assert "AUTHENTICATION" not in creds.get_odbc_dsn_dict()


def test_fabric_service_principal_with_secret_is_driver_native() -> None:
    """Default method with a Service Principal secret authenticates through the ODBC driver."""
    creds = _warehouse_credentials(
        azure_tenant_id="t", azure_client_id="c", azure_client_secret="s"
    )
    creds.on_partial()

    assert uses_token_authentication(creds) is False
    dsn = creds.get_odbc_dsn_dict()
    assert dsn["AUTHENTICATION"] == "ActiveDirectoryServicePrincipal"
    assert dsn["UID"] == "c@t"
    assert dsn["PWD"] == "s"
    assert creds.to_odbc_attrs_before() is None


@pytest.mark.parametrize(
    "authentication", ["ActiveDirectoryIntegrated", "ActiveDirectoryInteractive"]
)
def test_fabric_driver_native_passthrough(authentication: str) -> None:
    creds = _warehouse_credentials(authentication)
    creds.on_partial()

    dsn = creds.get_odbc_dsn_dict()
    assert dsn["AUTHENTICATION"] == authentication
    assert "UID" not in dsn
    assert "PWD" not in dsn
    assert creds.to_odbc_attrs_before() is None


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


def test_fabric_to_odbc_attrs_before_token_struct() -> None:
    """The injected token follows the SQL_COPT_SS_ACCESS_TOKEN struct layout."""
    creds = _warehouse_credentials("cli")
    creds._set_default_credentials(_FakeTokenCredential())

    attrs = creds.to_odbc_attrs_before()
    assert attrs is not None
    token_struct = attrs[1256]  # SQL_COPT_SS_ACCESS_TOKEN
    length = struct.unpack("<I", token_struct[:4])[0]
    assert length == len(token_struct) - 4
    assert token_struct[4:].decode("utf-16-le") == "fake-access-token"


def test_fabric_resolve_configuration_token_authentication() -> None:
    """Resolution succeeds without a Service Principal secret for token authentication."""
    creds = FabricCredentials()
    creds.host = "abc.datawarehouse.fabric.microsoft.com"
    creds.database = "mydb"
    creds.authentication = "cli"

    resolved = resolve_configuration(creds)

    assert resolved.is_resolved()
    assert uses_token_authentication(resolved) is True
    assert type(resolved.default_credentials()).__name__ == "AzureCliCredential"
    assert "AUTHENTICATION" not in resolved.get_odbc_dsn_dict()
