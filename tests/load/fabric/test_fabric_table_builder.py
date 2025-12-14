"""Tests for Fabric Warehouse table builder and SQL generation"""
import pytest
import sqlfluff

from dlt.common.utils import uniq_id
from dlt.common.schema import Schema

pytest.importorskip("dlt.destinations.impl.fabric.fabric", reason="MSSQL ODBC driver not installed")

from dlt.destinations.impl.fabric.factory import fabric
from dlt.destinations.impl.fabric.fabric import FabricClient
from dlt.destinations.impl.fabric.configuration import FabricClientConfiguration, FabricCredentials

from tests.load.utils import TABLE_UPDATE, empty_schema

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


@pytest.fixture
def client(empty_schema: Schema) -> FabricClient:
    """Return Fabric client without opening connection"""
    # Create credentials with driver set to avoid ODBC check
    creds = FabricCredentials()
    creds.host = "test.datawarehouse.fabric.microsoft.com"
    creds.database = "testdb"
    creds.azure_tenant_id = "test-tenant"
    creds.azure_client_id = "test-client"
    creds.azure_client_secret = "test-secret"
    creds.driver = "ODBC Driver 18 for SQL Server"  # Set driver to skip check
    creds.username = "test-client@test-tenant"
    creds.password = "test-secret"

    config = FabricClientConfiguration(credentials=creds)._bind_dataset_name(
        dataset_name="test_" + uniq_id()
    )
    client_instance = fabric().client(empty_schema, config)  # type: ignore[arg-type]
    assert isinstance(client_instance, FabricClient)
    return client_instance


def test_create_table_uses_varchar(client: FabricClient) -> None:
    """Test that Fabric uses varchar instead of nvarchar"""
    sql = client._get_table_update_sql("event_test_table", TABLE_UPDATE, False)[0]

    # Parse with fabric dialect (falls back to tsql if not available)
    try:
        sqlfluff.parse(sql, dialect="fabric")
    except Exception:
        sqlfluff.parse(sql, dialect="tsql")

    assert "event_test_table" in sql
    assert '"col1" bigint  NOT NULL' in sql
    assert '"col2" float  NOT NULL' in sql
    assert '"col3" bit  NOT NULL' in sql

    # Fabric should use datetime2 instead of datetimeoffset
    assert '"col4" datetime2' in sql

    # Fabric should use varchar instead of nvarchar
    assert '"col5" varchar' in sql
    assert "nvarchar" not in sql.lower()  # No nvarchar should appear

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


def test_alter_table_uses_varchar(client: FabricClient) -> None:
    """Test that ALTER TABLE statements use varchar instead of nvarchar"""
    sql = client._get_table_update_sql("event_test_table", TABLE_UPDATE, True)[0]

    try:
        sqlfluff.parse(sql, dialect="fabric")
    except Exception:
        sqlfluff.parse(sql, dialect="tsql")

    qualified_name = client.sql_client.make_qualified_table_name("event_test_table")
    assert sql.count(f"ALTER TABLE {qualified_name}\nADD") == 1

    # Verify varchar is used, not nvarchar
    assert '"col5" varchar' in sql
    assert "nvarchar" not in sql.lower()

    # Verify datetime2 is used, not datetimeoffset
    assert '"col4" datetime2' in sql
    assert "datetimeoffset" not in sql.lower()


def test_create_dlt_version_table_uses_varchar(client: FabricClient) -> None:
    """Test that _dlt_version table uses varchar to avoid text/ntext binding issues"""
    sql = client._get_table_update_sql("_dlt_version", TABLE_UPDATE, False)[0]

    try:
        sqlfluff.parse(sql, dialect="fabric")
    except Exception:
        sqlfluff.parse(sql, dialect="tsql")

    qualified_name = client.sql_client.make_qualified_table_name("_dlt_version")
    assert f"CREATE TABLE {qualified_name}" in sql

    # Should use varchar, not nvarchar or text
    assert "nvarchar" not in sql.lower()


def test_unique_column_uses_varchar(client: FabricClient) -> None:
    """Test that unique text columns use varchar with proper length"""
    from dlt.common.schema.typing import TColumnSchema

    # Create a table schema with a unique text column
    prepared_table = client.prepare_load_table("event_test_table")

    # Add a unique text column to test
    unique_column: TColumnSchema = {
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


@pytest.fixture
def client_with_indexes_enabled(empty_schema: Schema) -> FabricClient:
    """Return Fabric client with indexes enabled"""
    creds = FabricCredentials()
    creds.host = "test.datawarehouse.fabric.microsoft.com"
    creds.database = "testdb"
    creds.azure_tenant_id = "test-tenant"
    creds.azure_client_id = "test-client"
    creds.azure_client_secret = "test-secret"
    creds.driver = "ODBC Driver 18 for SQL Server"
    creds.username = "test-client@test-tenant"
    creds.password = "test-secret"

    config = FabricClientConfiguration(credentials=creds, create_indexes=True)._bind_dataset_name(
        dataset_name="test_" + uniq_id()
    )
    client_instance = fabric().client(empty_schema, config)  # type: ignore[arg-type]
    assert isinstance(client_instance, FabricClient)
    assert client_instance.config.create_indexes is True
    return client_instance


def test_create_table_with_primary_key_hint(
    client: FabricClient, client_with_indexes_enabled: FabricClient
) -> None:
    """Test that primary_key hint creates proper constraint when indexes are enabled"""
    from dlt.common.schema.typing import TColumnSchema
    from copy import deepcopy

    # Case: table without hint
    sql = client._get_table_update_sql("event_test_table", TABLE_UPDATE, False)[0]
    assert "PRIMARY KEY NONCLUSTERED NOT ENFORCED" not in sql

    # Case: table with hint, but client does not have indexes enabled
    mod_update = deepcopy(TABLE_UPDATE)
    mod_update[0]["primary_key"] = True
    sql = client._get_table_update_sql("event_test_table", mod_update, False)[0]
    assert "PRIMARY KEY NONCLUSTERED NOT ENFORCED" not in sql

    # Case: table with hint, client has indexes enabled
    sql = client_with_indexes_enabled._get_table_update_sql("event_test_table", mod_update, False)[
        0
    ]
    assert '"col1" bigint PRIMARY KEY NONCLUSTERED NOT ENFORCED NOT NULL' in sql


def test_create_table_with_unique_hint(
    client: FabricClient, client_with_indexes_enabled: FabricClient
) -> None:
    """Test that unique hint creates proper constraint when indexes are enabled"""
    from copy import deepcopy

    # Case: table without hint
    sql = client._get_table_update_sql("event_test_table", TABLE_UPDATE, False)[0]
    assert "UNIQUE NOT ENFORCED" not in sql

    # Case: table with hint, but client does not have indexes enabled
    mod_update = deepcopy(TABLE_UPDATE)
    mod_update[0]["unique"] = True
    sql = client._get_table_update_sql("event_test_table", mod_update, False)[0]
    assert "UNIQUE NOT ENFORCED" not in sql

    # Case: table with hint, client has indexes enabled
    sql = client_with_indexes_enabled._get_table_update_sql("event_test_table", mod_update, False)[
        0
    ]
    assert '"col1" bigint UNIQUE NOT ENFORCED NOT NULL' in sql


def test_hints_disabled_by_default(client: FabricClient) -> None:
    """Test that indexes/hints are disabled by default"""
    assert client.config.create_indexes is False

    # Even with hints in columns, they should not appear in SQL
    from copy import deepcopy

    mod_update = deepcopy(TABLE_UPDATE)
    mod_update[0]["primary_key"] = True
    mod_update[1]["unique"] = True

    sql = client._get_table_update_sql("event_test_table", mod_update, False)[0]
    assert "PRIMARY KEY" not in sql
    assert "UNIQUE NOT ENFORCED" not in sql


def test_fabric_no_with_clause_in_create_table(client: FabricClient) -> None:
    """Test that Fabric does not add WITH clause for HEAP or CLUSTERED COLUMNSTORE INDEX"""
    sql = client._get_table_update_sql("event_test_table", TABLE_UPDATE, False)[0]

    # Fabric should not have any WITH clause
    assert "WITH (" not in sql
    assert "HEAP" not in sql
    assert "CLUSTERED COLUMNSTORE INDEX" not in sql
