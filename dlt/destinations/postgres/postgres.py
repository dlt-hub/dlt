from typing import ClassVar, Dict, Optional, Sequence, List, Any

from dlt.common.wei import EVM_DECIMAL_PRECISION
from dlt.common.destination.reference import NewLoadJob
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.data_types import TDataType
from dlt.common.schema import TColumnSchema, TColumnHint, Schema
from dlt.common.schema.typing import TTableSchema

from dlt.destinations.sql_jobs import SqlStagingCopyJob

from dlt.destinations.insert_job_client import InsertValuesJobClient

from dlt.destinations.postgres import capabilities
from dlt.destinations.postgres.sql_client import Psycopg2SqlClient
from dlt.destinations.postgres.configuration import PostgresClientConfiguration
from dlt.destinations.sql_client import SqlClientBase
from dlt.destinations.type_mapping import TypeMapper


PGT_TO_SCT: Dict[str, TDataType] = {
    "varchar": "text",
    "jsonb": "complex",
    "double precision": "double",
    "boolean": "bool",
    "timestamp with time zone": "timestamp",
    "date": "date",
    "bigint": "bigint",
    "bytea": "binary",
    "numeric": "decimal",
    "time without time zone": "time"
}

HINT_TO_POSTGRES_ATTR: Dict[TColumnHint, str] = {
    "unique": "UNIQUE"
}

class PostgresTypeMapper(TypeMapper):
    sct_to_unbound_dbt = {
        "complex": "jsonb",
        "text": "varchar",
        "double": "double precision",
        "bool": "boolean",
        "date": "date",
        "bigint": "bigint",
        "binary": "bytea",
    }

    sct_to_dbt = {
        "text": "varchar(%i)",
        "timestamp": "timestamp (%i) with time zone",
        "binary": "bytea(%i)",
        "decimal": "numeric(%i,%i)",
        "time": "time (%i) without time zone",
        "wei": "numeric(%i,%i)"
    }


class PostgresStagingCopyJob(SqlStagingCopyJob):

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


class PostgresClient(InsertValuesJobClient):

    capabilities: ClassVar[DestinationCapabilitiesContext] = capabilities()

    def __init__(self, schema: Schema, config: PostgresClientConfiguration) -> None:
        sql_client = Psycopg2SqlClient(
            config.normalize_dataset_name(schema),
            config.credentials
        )
        super().__init__(schema, config, sql_client)
        self.config: PostgresClientConfiguration = config
        self.sql_client = sql_client
        self.active_hints = HINT_TO_POSTGRES_ATTR if self.config.create_indexes else {}
        self.type_mapper = PostgresTypeMapper(self.capabilities)

    def _get_column_def_sql(self, c: TColumnSchema) -> str:
        hints_str = " ".join(self.active_hints.get(h, "") for h in self.active_hints.keys() if c.get(h, False) is True)
        column_name = self.capabilities.escape_identifier(c["name"])
        return f"{column_name} {self.type_mapper.to_db_type(c)} {hints_str} {self._gen_not_null(c.get('nullable', True))}"

    def _create_optimized_replace_job(self, table_chain: Sequence[TTableSchema]) -> NewLoadJob:
        return PostgresStagingCopyJob.from_table_chain(table_chain, self.sql_client)

    # @classmethod
    # def _to_db_type(cls, column: TColumnSchema) -> str:
    #     sc_t = column["data_type"]
    #     precision, scale = column.get("precision"), column.get("scale")
    #     if sc_t in ("timestamp", "time"):
    #         return SCT_TO_PGT[sc_t] % (precision or cls.capabilities.timestamp_precision)
    #     if sc_t == "wei":
    #         default_precision, default_scale = cls.capabilities.wei_precision
    #         precision = precision if precision is not None else default_precision
    #         scale = scale if scale is not None else default_scale
    #         return SCT_TO_PGT["decimal"] % (precision, scale)
    #     if sc_t == "decimal":
    #         default_precision, default_scale = cls.capabilities.decimal_precision
    #         precision = precision if precision is not None else default_precision
    #         scale = scale if scale is not None else default_scale
    #         return SCT_TO_PGT["decimal"] % (precision, scale)
    #     if sc_t == "text":
    #         if precision is not None:
    #             return "varchar (%i)" % precision
    #         return "varchar"
    #     return SCT_TO_PGT[sc_t]

    @classmethod
    def _from_db_type(cls, pq_t: str, precision: Optional[int], scale: Optional[int]) -> TDataType:
        if pq_t == "numeric":
            if (precision, scale) == cls.capabilities.wei_precision:
                return "wei"
        return PGT_TO_SCT.get(pq_t, "text")
