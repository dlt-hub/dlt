from typing import ClassVar, Dict, Optional, Sequence, List, Any

from dlt.common.wei import EVM_DECIMAL_PRECISION
from dlt.common.destination.reference import NewLoadJob
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.data_types import TDataType
from dlt.common.schema import TColumnSchema, TColumnHint, Schema
from dlt.common.schema.typing import TTableSchema, TColumnType, TTableFormat

from dlt.destinations.sql_jobs import SqlStagingCopyJob, SqlJobParams

from dlt.destinations.insert_job_client import InsertValuesJobClient

from dlt.destinations.impl.postgres import capabilities
from dlt.destinations.impl.postgres.sql_client import Psycopg2SqlClient
from dlt.destinations.impl.postgres.configuration import PostgresClientConfiguration
from dlt.destinations.sql_client import SqlClientBase
from dlt.destinations.type_mapping import TypeMapper


HINT_TO_POSTGRES_ATTR: Dict[TColumnHint, str] = {"unique": "UNIQUE"}


class PostgresTypeMapper(TypeMapper):
    sct_to_unbound_dbt = {
        "complex": "jsonb",
        "text": "varchar",
        "double": "double precision",
        "bool": "boolean",
        "date": "date",
        "bigint": "bigint",
        "binary": "bytea",
        "timestamp": "timestamp with time zone",
        "time": "time without time zone",
    }

    sct_to_dbt = {
        "text": "varchar(%i)",
        "timestamp": "timestamp (%i) with time zone",
        "decimal": "numeric(%i,%i)",
        "time": "time (%i) without time zone",
        "wei": "numeric(%i,%i)",
    }

    dbt_to_sct = {
        "varchar": "text",
        "jsonb": "complex",
        "double precision": "double",
        "boolean": "bool",
        "timestamp with time zone": "timestamp",
        "date": "date",
        "bigint": "bigint",
        "bytea": "binary",
        "numeric": "decimal",
        "time without time zone": "time",
        "character varying": "text",
        "smallint": "bigint",
        "integer": "bigint",
    }

    def to_db_integer_type(
        self, precision: Optional[int], table_format: TTableFormat = None
    ) -> str:
        if precision is None:
            return "bigint"
        # Precision is number of bits
        if precision <= 16:
            return "smallint"
        elif precision <= 32:
            return "integer"
        return "bigint"

    def from_db_type(
        self, db_type: str, precision: Optional[int] = None, scale: Optional[int] = None
    ) -> TColumnType:
        if db_type == "numeric":
            if (precision, scale) == self.capabilities.wei_precision:
                return dict(data_type="wei")
        return super().from_db_type(db_type, precision, scale)


class PostgresStagingCopyJob(SqlStagingCopyJob):
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
            sql.append(f"DROP TABLE IF EXISTS {table_name};")
            # moving staging table to destination schema
            sql.append(
                f"ALTER TABLE {staging_table_name} SET SCHEMA"
                f" {sql_client.fully_qualified_dataset_name()};"
            )
            # recreate staging table
            sql.append(f"CREATE TABLE {staging_table_name} (like {table_name} including all);")
        return sql


class PostgresClient(InsertValuesJobClient):
    capabilities: ClassVar[DestinationCapabilitiesContext] = capabilities()

    def __init__(self, schema: Schema, config: PostgresClientConfiguration) -> None:
        sql_client = Psycopg2SqlClient(config.normalize_dataset_name(schema), config.credentials)
        super().__init__(schema, config, sql_client)
        self.config: PostgresClientConfiguration = config
        self.sql_client = sql_client
        self.active_hints = HINT_TO_POSTGRES_ATTR if self.config.create_indexes else {}
        self.type_mapper = PostgresTypeMapper(self.capabilities)

    def _get_column_def_sql(self, c: TColumnSchema, table_format: TTableFormat = None) -> str:
        hints_str = " ".join(
            self.active_hints.get(h, "")
            for h in self.active_hints.keys()
            if c.get(h, False) is True
        )
        column_name = self.capabilities.escape_identifier(c["name"])
        return (
            f"{column_name} {self.type_mapper.to_db_type(c)} {hints_str} {self._gen_not_null(c.get('nullable', True))}"
        )

    def _create_replace_followup_jobs(
        self, table_chain: Sequence[TTableSchema]
    ) -> List[NewLoadJob]:
        if self.config.replace_strategy == "staging-optimized":
            return [PostgresStagingCopyJob.from_table_chain(table_chain, self.sql_client)]
        return super()._create_replace_followup_jobs(table_chain)

    def _from_db_type(
        self, pq_t: str, precision: Optional[int], scale: Optional[int]
    ) -> TColumnType:
        return self.type_mapper.from_db_type(pq_t, precision, scale)
