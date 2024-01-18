from typing import ClassVar, Sequence, List, Dict, Any, Optional
from copy import deepcopy

from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.reference import SupportsStagingDestination, NewLoadJob

from dlt.common.schema import TTableSchema, TColumnSchema, Schema, TColumnHint
from dlt.common.schema.typing import TTableSchemaColumns

from dlt.destinations.sql_jobs import SqlStagingCopyJob, SqlJobParams
from dlt.destinations.sql_client import SqlClientBase
from dlt.destinations.insert_job_client import InsertValuesJobClient
from dlt.destinations.job_client_impl import SqlJobClientBase

from dlt.destinations.impl.mssql.mssql import MsSqlTypeMapper, MsSqlClient, HINT_TO_MSSQL_ATTR

from dlt.destinations.impl.synapse import capabilities
from dlt.destinations.impl.synapse.sql_client import SynapseSqlClient
from dlt.destinations.impl.synapse.configuration import SynapseClientConfiguration


HINT_TO_SYNAPSE_ATTR: Dict[TColumnHint, str] = {
    "primary_key": "PRIMARY KEY NONCLUSTERED NOT ENFORCED",
    "unique": "UNIQUE NOT ENFORCED",
}


class SynapseClient(MsSqlClient, SupportsStagingDestination):
    capabilities: ClassVar[DestinationCapabilitiesContext] = capabilities()

    def __init__(self, schema: Schema, config: SynapseClientConfiguration) -> None:
        sql_client = SynapseSqlClient(config.normalize_dataset_name(schema), config.credentials)
        InsertValuesJobClient.__init__(self, schema, config, sql_client)
        self.config: SynapseClientConfiguration = config
        self.sql_client = sql_client
        self.type_mapper = MsSqlTypeMapper(self.capabilities)

        self.active_hints = deepcopy(HINT_TO_SYNAPSE_ATTR)
        if not self.config.create_indexes:
            self.active_hints.pop("primary_key", None)
            self.active_hints.pop("unique", None)

    def _get_table_update_sql(
        self, table_name: str, new_columns: Sequence[TColumnSchema], generate_alter: bool
    ) -> List[str]:
        _sql_result = SqlJobClientBase._get_table_update_sql(
            self, table_name, new_columns, generate_alter
        )
        if not generate_alter:
            # Append WITH clause to create heap table instead of default
            # columnstore table. Heap tables are a more robust choice, because
            # columnstore tables do not support varchar(max), nvarchar(max),
            # and varbinary(max).
            # https://learn.microsoft.com/en-us/azure/synapse-analytics/sql-data-warehouse/sql-data-warehouse-tables-index
            sql_result = [_sql_result[0] + "\n WITH ( HEAP );"]
        else:
            sql_result = _sql_result
        return sql_result

    def _create_replace_followup_jobs(
        self, table_chain: Sequence[TTableSchema]
    ) -> List[NewLoadJob]:
        if self.config.replace_strategy == "staging-optimized":
            return [SynapseStagingCopyJob.from_table_chain(table_chain, self.sql_client)]
        return super()._create_replace_followup_jobs(table_chain)


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
