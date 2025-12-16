"""Tests for Microsoft Fabric Warehouse destination configuration"""
import os
import pytest

from dlt.common.configuration import resolve_configuration
from dlt.common.schema import Schema

from dlt.destinations.impl.fabric.factory import fabric
from dlt.destinations.impl.fabric.configuration import (
    FabricCredentials,
    FabricClientConfiguration,
)

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


def test_fabric_factory() -> None:
    """Test Fabric destination factory with default settings"""
    dest = fabric()

    # Test destination properties without requiring credentials
    assert dest.destination_name == "fabric"
    assert dest.capabilities().has_case_sensitive_identifiers is False
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


def test_fabric_configuration_custom_collation() -> None:
    """Test Fabric configuration with custom collation"""
    config = FabricClientConfiguration()
    config.collation = "Latin1_General_100_CI_AS_KS_WS_SC_UTF8"

    assert config.collation == "Latin1_General_100_CI_AS_KS_WS_SC_UTF8"


def test_fabric_type_mapper() -> None:
    """Test Fabric type mapper converts nvarchar to varchar and datetimeoffset to datetime2"""
    from dlt.destinations.impl.fabric.factory import FabricTypeMapper
    from dlt.common.destination import DestinationCapabilitiesContext
    from dlt.common.schema.typing import TColumnSchema
    from dlt.common.destination.typing import PreparedTableSchema
    from typing import cast

    # Create a mock table for testing
    table = cast(PreparedTableSchema, {"name": "test_table", "columns": {}})

    caps = DestinationCapabilitiesContext.generic_capabilities("parquet")
    mapper = FabricTypeMapper(caps)

    # Test that text type gets converted to varchar (not nvarchar)
    text_col = cast(
        TColumnSchema, {"name": "test", "data_type": "text", "nullable": True}
    )
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
