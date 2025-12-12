"""Tests for Fabric Warehouse table builder and SQL generation"""
import pytest
import sqlfluff

from dlt.common.utils import uniq_id
from dlt.common.schema import Schema

pytest.importorskip("dlt.destinations.impl.fabric.fabric", reason="MSSQL ODBC driver not installed")

from dlt.destinations.impl.fabric.factory import fabric
from dlt.destinations.impl.fabric.fabric import FabricJobClient
from dlt.destinations.impl.fabric.configuration import FabricClientConfiguration, FabricCredentials

from tests.load.utils import TABLE_UPDATE, empty_schema

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


@pytest.fixture
def client(empty_schema: Schema) -> FabricJobClient:
    """Return Fabric client without opening connection"""
    # Create credentials with driver set to avoid ODBC check
    creds = FabricCredentials()
    creds.host = "test.datawarehouse.fabric.microsoft.com"
    creds.database = "testdb"
    creds.tenant_id = "test-tenant"
    creds.client_id = "test-client"
    creds.client_secret = "test-secret"
    creds.driver = "ODBC Driver 18 for SQL Server"  # Set driver to skip check
    creds.username = "test-client@test-tenant"
    creds.password = "test-secret"
    
    config = FabricClientConfiguration(credentials=creds)._bind_dataset_name(
        dataset_name="test_" + uniq_id()
    )
    return fabric().client(empty_schema, config)


def test_create_table_uses_varchar(client: FabricJobClient) -> None:
    """Test that Fabric uses varchar instead of nvarchar"""
    sql = client._get_table_update_sql("event_test_table", TABLE_UPDATE, False)[0]
    
    # Parse with fabric dialect (falls back to tsql if not available)
    try:
        sqlfluff.parse(sql, dialect="fabric")
    except:
        sqlfluff.parse(sql, dialect="tsql")
    
    assert "event_test_table" in sql
    assert '"col1" bigint  NOT NULL' in sql
    assert '"col2" float  NOT NULL' in sql
    assert '"col3" bit  NOT NULL' in sql
    
    # Fabric should use datetime2 instead of datetimeoffset
    assert '"col4" datetime2' in sql
    
    # Fabric should use varchar instead of nvarchar
    assert '"col5" varchar' in sql
    assert 'nvarchar' not in sql.lower()  # No nvarchar should appear
    
    assert '"col6" decimal(38,9)  NOT NULL' in sql
    assert '"col7" varbinary' in sql
    assert '"col8" decimal(38,0)' in sql
    
    # Fabric uses varchar(max) for JSON
    assert '"col9" varchar(max)  NOT NULL' in sql
    
    assert '"col10" date  NOT NULL' in sql
    assert '"col11" time' in sql
    assert '"col1_precision" smallint  NOT NULL' in sql
    
    # Fabric uses datetime2 with precision 0-6
    assert '"col4_precision" datetime2(3)  NOT NULL' in sql
    
    # varchar instead of nvarchar
    assert '"col5_precision" varchar(25)' in sql
    
    assert '"col6_precision" decimal(6,2)  NOT NULL' in sql
    assert '"col7_precision" varbinary(19)' in sql
    assert '"col11_precision" time(3)  NOT NULL' in sql


def test_alter_table_uses_varchar(client: FabricJobClient) -> None:
    """Test that ALTER TABLE statements use varchar instead of nvarchar"""
    sql = client._get_table_update_sql("event_test_table", TABLE_UPDATE, True)[0]
    
    try:
        sqlfluff.parse(sql, dialect="fabric")
    except:
        sqlfluff.parse(sql, dialect="tsql")
    
    qualified_name = client.sql_client.make_qualified_table_name("event_test_table")
    assert sql.count(f"ALTER TABLE {qualified_name}\nADD") == 1
    
    # Verify varchar is used, not nvarchar
    assert '"col5" varchar' in sql
    assert 'nvarchar' not in sql.lower()
    
    # Verify datetime2 is used, not datetimeoffset
    assert '"col4" datetime2' in sql
    assert 'datetimeoffset' not in sql.lower()


def test_create_dlt_version_table_uses_varchar(client: FabricJobClient) -> None:
    """Test that _dlt_version table uses varchar to avoid text/ntext binding issues"""
    sql = client._get_table_update_sql("_dlt_version", TABLE_UPDATE, False)[0]
    
    try:
        sqlfluff.parse(sql, dialect="fabric")
    except:
        sqlfluff.parse(sql, dialect="tsql")
    
    qualified_name = client.sql_client.make_qualified_table_name("_dlt_version")
    assert f"CREATE TABLE {qualified_name}" in sql
    
    # Should use varchar, not nvarchar or text
    assert 'nvarchar' not in sql.lower()


def test_unique_column_uses_varchar(client: FabricJobClient) -> None:
    """Test that unique text columns use varchar with proper length"""
    # Create a table schema with a unique text column
    prepared_table = client.prepare_load_table("event_test_table")
    
    # Add a unique text column to test
    unique_column = {
        "name": "unique_text_col",
        "data_type": "text",
        "nullable": False,
        "unique": True,
    }
    
    # Get column definition SQL
    col_sql = client._get_column_def_sql(unique_column, prepared_table)
    
    # Should use varchar (not nvarchar) with 900 byte limit for unique columns
    assert "varchar(900)" in col_sql
    assert "nvarchar" not in col_sql


def test_fabric_capabilities() -> None:
    """Test Fabric destination capabilities"""
    dest = fabric()
    caps = dest.capabilities()
    
    # Verify Fabric uses its own dialect
    assert caps.sqlglot_dialect == "fabric"
    
    # Verify type mapper is FabricTypeMapper
    from dlt.destinations.impl.fabric.factory import FabricTypeMapper
    assert isinstance(caps.type_mapper, type) and issubclass(caps.type_mapper, FabricTypeMapper)
