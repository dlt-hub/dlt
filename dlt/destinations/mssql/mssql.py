from typing import ClassVar, Dict, Optional, Sequence, List, Any

from dlt.common.wei import EVM_DECIMAL_PRECISION
from dlt.common.destination.reference import NewLoadJob
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.data_types import TDataType
from dlt.common.schema import TColumnSchema, TColumnHint, Schema
from dlt.common.schema.typing import TTableSchema

from dlt.destinations.sql_jobs import SqlStagingCopyJob

from dlt.destinations.insert_job_client import InsertValuesJobClient

from dlt.destinations.mssql import capabilities
from dlt.destinations.mssql.sql_client import PyOdbcMsSqlClient
from dlt.destinations.mssql.configuration import MsSqlClientConfiguration
from dlt.destinations.sql_client import SqlClientBase


SCT_TO_PGT: Dict[TDataType, str] = {
    "complex": "nvarchar(max)",
    "text": "nvarchar(max)",
    "double": "float",
    "bool": "bit",
    "timestamp": "datetimeoffset",
    "date": "date",
    "bigint": "bigint",
    "binary": "varbinary(max)",
    "decimal": "decimal(%i,%i)"
}

PGT_TO_SCT: Dict[str, TDataType] = {
    "nvarchar": "text",
    "float": "double",
    "bit": "bool",
    "datetimeoffset": "timestamp",
    "date": "date",
    "bigint": "bigint",
    "varbinary": "binary",
    "decimal": "decimal"
}

HINT_TO_MSSQL_ATTR: Dict[TColumnHint, str] = {
    "unique": "UNIQUE"
}

class MsSqlStagingCopyJob(SqlStagingCopyJob):

    @classmethod
    def generate_sql(cls, table_chain: Sequence[TTableSchema], sql_client: SqlClientBase[Any]) -> List[str]:
        sql: List[str] = []
        for table in table_chain:
            with sql_client.with_staging_dataset(staging=True):
                staging_table_name = sql_client.make_qualified_table_name(table["name"])
            table_name = sql_client.make_qualified_table_name(table["name"])
            # drop destination table
            sql.append(f"DROP TABLE IF EXISTS {table_name};")
            # moving staging table to destination schema
            sql.append(f"ALTER TABLE {staging_table_name} SET SCHEMA {sql_client.fully_qualified_dataset_name()};")
            # recreate staging table
            sql.append(f"CREATE TABLE {staging_table_name} (like {table_name} including all);")
        return sql

class MsSqlClient(InsertValuesJobClient):

    capabilities: ClassVar[DestinationCapabilitiesContext] = capabilities()

    def __init__(self, schema: Schema, config: MsSqlClientConfiguration) -> None:
        sql_client = PyOdbcMsSqlClient(
            config.normalize_dataset_name(schema),
            config.credentials
        )
        super().__init__(schema, config, sql_client)
        self.config: MsSqlClientConfiguration = config
        self.sql_client = sql_client
        self.active_hints = HINT_TO_MSSQL_ATTR if self.config.create_indexes else {}

    def _make_add_column_sql(self, new_columns: Sequence[TColumnSchema]) -> List[str]:
        # Override because mssql requires multiple columns in a single ADD COLUMN clause
        return ["ADD \n" + ",\n".join(self._get_column_def_sql(c) for c in new_columns)]

    def _get_column_def_sql(self, c: TColumnSchema) -> str:
        sc_type = c["data_type"]
        if sc_type == "text" and c.get("unique"):
            # MSSQL does not allow index on large TEXT columns
            db_type = "nvarchar(900)"
        else:
            db_type = self._to_db_type(sc_type)

        hints_str = " ".join(self.active_hints.get(h, "") for h in self.active_hints.keys() if c.get(h, False) is True)
        column_name = self.capabilities.escape_identifier(c["name"])
        return f"{column_name} {db_type} {hints_str} {self._gen_not_null(c['nullable'])}"

    def _create_optimized_replace_job(self, table_chain: Sequence[TTableSchema]) -> NewLoadJob:
        return MsSqlStagingCopyJob.from_table_chain(table_chain, self.sql_client)

    @classmethod
    def _to_db_type(cls, sc_t: TDataType) -> str:
        if sc_t == "wei":
            return SCT_TO_PGT["decimal"] % cls.capabilities.wei_precision
        if sc_t == "decimal":
            return SCT_TO_PGT["decimal"] % cls.capabilities.decimal_precision

        if sc_t == "wei":
            return f"numeric({2*EVM_DECIMAL_PRECISION},{EVM_DECIMAL_PRECISION})"
        return SCT_TO_PGT[sc_t]

    @classmethod
    def _from_db_type(cls, pq_t: str, precision: Optional[int], scale: Optional[int]) -> TDataType:
        if pq_t == "numeric":
            if (precision, scale) == cls.capabilities.wei_precision:
                return "wei"
        return PGT_TO_SCT[pq_t]

