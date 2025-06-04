from typing import cast
from textwrap import dedent

from dlt.destinations.impl.synapse.sql_client import SynapseSqlClient
from dlt.destinations.impl.synapse.synapse_adapter import TTableIndexType


def get_storage_table_index_type(sql_client: SynapseSqlClient, table_name: str) -> TTableIndexType:
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
