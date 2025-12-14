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
    creds.driver = "ODBC Driver 18 for SQL Server"  # Set driver to skip ODBC check

    # Call on_partial manually to trigger credential conversion
    creds.on_partial()

    # Check that username/password were auto-generated from Service Principal
    assert creds.username == "test-client-id@test-tenant-id"
    assert creds.password == "test-client-secret"


def test_fabric_credentials_odbc_dsn() -> None:
    """Test that Fabric credentials generate correct ODBC DSN with Fabric-specific parameters"""
    creds = FabricCredentials()
    creds.host = "abc12345-6789-def0-1234-56789abcdef0.datawarehouse.fabric.microsoft.com"
    creds.database = "mydb"
    creds.azure_tenant_id = "test-tenant-id"
    creds.azure_client_id = "test-client-id"
    creds.azure_client_secret = "test-client-secret"
    creds.driver = "ODBC Driver 18 for SQL Server"

    # Resolve to trigger on_partial and on_resolved
    creds = resolve_configuration(creds)

    # Get ODBC DSN parameters
    dsn_dict = creds.get_odbc_dsn_dict()

    # Verify Fabric-specific parameters are added
    assert dsn_dict["AUTHENTICATION"] == "ActiveDirectoryServicePrincipal"
    assert dsn_dict["LONGASMAX"] == "yes"
    assert dsn_dict["UID"] == "test-client-id@test-tenant-id"
    assert dsn_dict["PWD"] == "test-client-secret"
    assert dsn_dict["DRIVER"] == "ODBC Driver 18 for SQL Server"
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
    """Test Fabric type mapper uses varchar instead of nvarchar"""
    from dlt.destinations.impl.fabric.factory import FabricTypeMapper

    mapper = FabricTypeMapper(capabilities=None)

    # Fabric should use varchar, not nvarchar
    assert mapper.sct_to_unbound_dbt["text"] == "varchar(max)"
    assert mapper.sct_to_dbt["text"] == "varchar(%i)"

    # Fabric should use datetime2, not datetimeoffset
    assert mapper.sct_to_unbound_dbt["timestamp"] == "datetime2(6)"
    assert mapper.sct_to_dbt["timestamp"] == "datetime2(%i)"

    # Verify reverse mapping
    assert mapper.dbt_to_sct["varchar"] == "text"
    assert mapper.dbt_to_sct["datetime2"] == "timestamp"


def test_fabric_credentials_drivername() -> None:
    """Test that Fabric credentials have correct drivername"""
    creds = FabricCredentials()
    assert creds.drivername == "fabric"
