from typing import ClassVar, Sequence, List, Dict, Any, Optional, cast
from copy import deepcopy
from textwrap import dedent

from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.reference import SupportsStagingDestination, NewLoadJob

from dlt.common.schema import TTableSchema, TColumnSchema, Schema, TColumnHint
from dlt.common.schema.typing import TTableSchemaColumns, TTableIndexType

from dlt.destinations.sql_jobs import SqlStagingCopyJob, SqlJobParams
from dlt.destinations.sql_client import SqlClientBase
from dlt.destinations.insert_job_client import InsertValuesJobClient
from dlt.destinations.job_client_impl import SqlJobClientBase

from dlt.destinations.impl.mssql.mssql import (
    MsSqlTypeMapper,
    MsSqlClient,
    VARCHAR_MAX_N,
    VARBINARY_MAX_N,
)

from dlt.destinations.impl.synapse import capabilities
from dlt.destinations.impl.synapse.sql_client import SynapseSqlClient
from dlt.destinations.impl.synapse.configuration import SynapseClientConfiguration


HINT_TO_SYNAPSE_ATTR: Dict[TColumnHint, str] = {
    "primary_key": "PRIMARY KEY NONCLUSTERED NOT ENFORCED",
    "unique": "UNIQUE NOT ENFORCED",
}
TABLE_INDEX_TYPE_TO_SYNAPSE_ATTR: Dict[TTableIndexType, str] = {
    "heap": "HEAP",
    "clustered_columnstore_index": "CLUSTERED COLUMNSTORE INDEX",
}


class SynapseClient(MsSqlClient):
    capabilities: ClassVar[DestinationCapabilitiesContext] = capabilities()

    def __init__(self, schema: Schema, config: SynapseClientConfiguration) -> None:
        super().__init__(schema, config)
        self.config: SynapseClientConfiguration = config
        self.sql_client = SynapseSqlClient(
            config.normalize_dataset_name(schema), config.credentials
        )

        self.active_hints = deepcopy(HINT_TO_SYNAPSE_ATTR)
        if not self.config.create_indexes:
            self.active_hints.pop("primary_key", None)
            self.active_hints.pop("unique", None)

    def _get_table_update_sql(
        self, table_name: str, new_columns: Sequence[TColumnSchema], generate_alter: bool
    ) -> List[str]:
        table = self.get_load_table(table_name)
        if table is None:
            table_index_type = self.config.default_table_index_type
        else:
            table_index_type = table.get("table_index_type")
            if table_index_type == "clustered_columnstore_index":
                new_columns = self._get_columstore_valid_columns(new_columns)

        _sql_result = SqlJobClientBase._get_table_update_sql(
            self, table_name, new_columns, generate_alter
        )
        if not generate_alter:
            table_index_type_attr = TABLE_INDEX_TYPE_TO_SYNAPSE_ATTR[table_index_type]
            sql_result = [_sql_result[0] + f"\n WITH ( {table_index_type_attr} );"]
        else:
            sql_result = _sql_result
        return sql_result

    def _get_columstore_valid_columns(
        self, columns: Sequence[TColumnSchema]
    ) -> Sequence[TColumnSchema]:
        return [self._get_columstore_valid_column(c) for c in columns]

    def _get_columstore_valid_column(self, c: TColumnSchema) -> TColumnSchema:
        """
        Returns TColumnSchema that maps to a Synapse data type that can participate in a columnstore index.

        varchar(max), nvarchar(max), and varbinary(max) are replaced with
        varchar(n), nvarchar(n), and varbinary(n), respectively, where
        n equals the user-specified precision, or the maximum allowed
        value if the user did not specify a precision.
        """
        varchar_source_types = [
            sct
            for sct, dbt in MsSqlTypeMapper.sct_to_unbound_dbt.items()
            if dbt in ("varchar(max)", "nvarchar(max)")
        ]
        varbinary_source_types = [
            sct
            for sct, dbt in MsSqlTypeMapper.sct_to_unbound_dbt.items()
            if dbt == "varbinary(max)"
        ]
        if c["data_type"] in varchar_source_types and "precision" not in c:
            return {**c, **{"precision": VARCHAR_MAX_N}}
        elif c["data_type"] in varbinary_source_types and "precision" not in c:
            return {**c, **{"precision": VARBINARY_MAX_N}}
        return c

    def _create_replace_followup_jobs(
        self, table_chain: Sequence[TTableSchema]
    ) -> List[NewLoadJob]:
        if self.config.replace_strategy == "staging-optimized":
            return [SynapseStagingCopyJob.from_table_chain(table_chain, self.sql_client)]
        return super()._create_replace_followup_jobs(table_chain)

    def get_load_table(self, table_name: str, staging: bool = False) -> TTableSchema:
        table = super().get_load_table(table_name, staging)
        if table is None:
            return None
        if table_name in self.schema.dlt_table_names():
            # dlt tables should always be heap tables, regardless of the user
            # configuration. Why? "For small lookup tables, less than 60 million rows,
            # consider using HEAP or clustered index for faster query performance."
            # https://learn.microsoft.com/en-us/azure/synapse-analytics/sql-data-warehouse/sql-data-warehouse-tables-index#heap-tables
            table["table_index_type"] = "heap"
        if table["table_index_type"] is None:
            table["table_index_type"] = self.config.default_table_index_type
        return table

    def get_storage_table_index_type(self, table_name: str) -> TTableIndexType:
        """Returns table index type of table in storage destination."""
        with self.sql_client as sql_client:
            schema_name = sql_client.fully_qualified_dataset_name(escape=False)
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


class SynapseStagingCopyJob(SqlStagingCopyJob):
    @classmethod
    def generate_sql(
        cls,
        table_chain: Sequence[TTableSchema],
        sql_client: SqlClientBase[Any],
        params: Optional[SqlJobParams] = None,
    ) -> List[str]:
        sql: List[str] = []
        for table in table_chain:
            with sql_client.with_staging_dataset(staging=True):
                staging_table_name = sql_client.make_qualified_table_name(table["name"])
            table_name = sql_client.make_qualified_table_name(table["name"])
            # drop destination table
            sql.append(f"DROP TABLE {table_name};")
            # moving staging table to destination schema
            sql.append(
                f"ALTER SCHEMA {sql_client.fully_qualified_dataset_name()} TRANSFER"
                f" {staging_table_name};"
            )
            # recreate staging table
            # In some cases, when multiple instances of this CTAS query are
            # executed concurrently, Synapse suspends the queries and hangs.
            # This can be prevented by setting the env var LOAD__WORKERS = "1".
            sql.append(
                f"CREATE TABLE {staging_table_name}"
                " WITH ( DISTRIBUTION = ROUND_ROBIN, HEAP )"  # distribution must be explicitly specified with CTAS
                f" AS SELECT * FROM {table_name}"
                " WHERE 1 = 0;"  # no data, table structure only
            )

        return sql
