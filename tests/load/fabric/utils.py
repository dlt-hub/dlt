from typing import cast
from textwrap import dedent

from dlt.destinations.impl.fabric.sql_client import FabricSqlClient
from dlt.destinations.impl.synapse.synapse_adapter import TTableIndexType


def get_storage_table_index_type(sql_client: FabricSqlClient, table_name: str) -> TTableIndexType:
    """Returns table index type of table in storage destination."""
    with sql_client:
        schema_name = sql_client.fully_qualified_dataset_name(quote=False)
        sql = dedent(f"""
            SELECT
                CASE i.type_desc
                    WHEN 'HEAP' THEN 'heap'
                    WHEN 'CLUSTERED COLUMNSTORE' THEN 'clustered_columnstore_index'
                END AS table_index_type
            FROM sys.indexes i
            INNER JOIN sys.tables t ON t.object_id = i.object_id
            INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
            WHERE s.name = '{schema_name}' AND t.name = '{table_name}'
        """)
        table_index_type = sql_client.execute_sql(sql)[0][0]
        return cast(TTableIndexType, table_index_type)


def verify_varchar_usage(sql_client: FabricSqlClient, table_name: str, column_name: str) -> bool:
    """Verify that a specific column uses varchar (not nvarchar)."""
    with sql_client:
        schema_name = sql_client.fully_qualified_dataset_name(quote=False)
        sql = dedent(f"""
            SELECT
                TYPE_NAME(c.system_type_id) AS data_type
            FROM sys.columns c
            INNER JOIN sys.tables t ON t.object_id = c.object_id
            INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
            WHERE s.name = '{schema_name}'
                AND t.name = '{table_name}'
                AND c.name = '{column_name}'
        """)
        result = sql_client.execute_sql(sql)
        if result:
            data_type = result[0][0]
            return data_type == "varchar"
        return False


def get_table_collation(sql_client: FabricSqlClient, table_name: str, column_name: str) -> str:
    """Get the collation for a specific column."""
    with sql_client:
        schema_name = sql_client.fully_qualified_dataset_name(quote=False)
        sql = dedent(f"""
            SELECT
                c.collation_name
            FROM sys.columns c
            INNER JOIN sys.tables t ON t.object_id = c.object_id
            INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
            WHERE s.name = '{schema_name}'
                AND t.name = '{table_name}'
                AND c.name = '{column_name}'
        """)
        result = sql_client.execute_sql(sql)
        if result:
            return result[0][0] or ""
        return ""
