from typing import ClassVar, Dict, Optional, Sequence, List, Any, Tuple

from dlt.common.wei import EVM_DECIMAL_PRECISION
from dlt.common.destination.reference import NewLoadJob
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.data_types import TDataType
from dlt.common.schema import TColumnSchema, TColumnHint, Schema
from dlt.common.schema.typing import TTableSchema, TColumnType, TTableFormat
from dlt.common.utils import uniq_id

from dlt.destinations.sql_jobs import SqlStagingCopyJob, SqlMergeJob, SqlJobParams

from dlt.destinations.insert_job_client import InsertValuesJobClient

from dlt.destinations.impl.mssql import capabilities
from dlt.destinations.impl.mssql.sql_client import PyOdbcMsSqlClient
from dlt.destinations.impl.mssql.configuration import MsSqlClientConfiguration
from dlt.destinations.sql_client import SqlClientBase
from dlt.destinations.type_mapping import TypeMapper


HINT_TO_MSSQL_ATTR: Dict[TColumnHint, str] = {"unique": "UNIQUE"}


class MsSqlTypeMapper(TypeMapper):
    sct_to_unbound_dbt = {
        "complex": "nvarchar(max)",
        "text": "nvarchar(max)",
        "double": "float",
        "bool": "bit",
        "bigint": "bigint",
        "binary": "varbinary(max)",
        "date": "date",
        "timestamp": "datetimeoffset",
        "time": "time",
    }

    sct_to_dbt = {
        "complex": "nvarchar(%i)",
        "text": "nvarchar(%i)",
        "timestamp": "datetimeoffset(%i)",
        "binary": "varbinary(%i)",
        "decimal": "decimal(%i,%i)",
        "time": "time(%i)",
        "wei": "decimal(%i,%i)",
    }

    dbt_to_sct = {
        "nvarchar": "text",
        "float": "double",
        "bit": "bool",
        "datetimeoffset": "timestamp",
        "date": "date",
        "bigint": "bigint",
        "varbinary": "binary",
        "decimal": "decimal",
        "time": "time",
        "tinyint": "bigint",
        "smallint": "bigint",
        "int": "bigint",
    }

    def to_db_integer_type(
        self, precision: Optional[int], table_format: TTableFormat = None
    ) -> str:
        if precision is None:
            return "bigint"
        if precision <= 8:
            return "tinyint"
        if precision <= 16:
            return "smallint"
        if precision <= 32:
            return "int"
        return "bigint"

    def from_db_type(
        self, db_type: str, precision: Optional[int], scale: Optional[int]
    ) -> TColumnType:
        if db_type == "decimal":
            if (precision, scale) == self.capabilities.wei_precision:
                return dict(data_type="wei")
        return super().from_db_type(db_type, precision, scale)


class MsSqlStagingCopyJob(SqlStagingCopyJob):
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
                f"ALTER SCHEMA {sql_client.fully_qualified_dataset_name()} TRANSFER"
                f" {staging_table_name};"
            )
            # recreate staging table
            sql.append(f"SELECT * INTO {staging_table_name} FROM {table_name} WHERE 1 = 0;")
        return sql


class MsSqlMergeJob(SqlMergeJob):
    @classmethod
    def gen_key_table_clauses(
        cls,
        root_table_name: str,
        staging_root_table_name: str,
        key_clauses: Sequence[str],
        for_delete: bool,
    ) -> List[str]:
        """Generate sql clauses that may be used to select or delete rows in root table of destination dataset"""
        if for_delete:
            # MS SQL doesn't support alias in DELETE FROM
            return [
                f"FROM {root_table_name} WHERE EXISTS (SELECT 1 FROM"
                f" {staging_root_table_name} WHERE"
                f" {' OR '.join([c.format(d=root_table_name,s=staging_root_table_name) for c in key_clauses])})"
            ]
        return SqlMergeJob.gen_key_table_clauses(
            root_table_name, staging_root_table_name, key_clauses, for_delete
        )

    @classmethod
    def _to_temp_table(cls, select_sql: str, temp_table_name: str) -> str:
        return f"SELECT * INTO {temp_table_name} FROM ({select_sql}) as t;"

    @classmethod
    def _new_temp_table_name(cls, name_prefix: str) -> str:
        name = SqlMergeJob._new_temp_table_name(name_prefix)
        return "#" + name


class MsSqlClient(InsertValuesJobClient):
    capabilities: ClassVar[DestinationCapabilitiesContext] = capabilities()

    def __init__(self, schema: Schema, config: MsSqlClientConfiguration) -> None:
        sql_client = PyOdbcMsSqlClient(config.normalize_dataset_name(schema), config.credentials)
        super().__init__(schema, config, sql_client)
        self.config: MsSqlClientConfiguration = config
        self.sql_client = sql_client
        self.active_hints = HINT_TO_MSSQL_ATTR if self.config.create_indexes else {}
        self.type_mapper = MsSqlTypeMapper(self.capabilities)

    def _create_merge_followup_jobs(self, table_chain: Sequence[TTableSchema]) -> List[NewLoadJob]:
        return [MsSqlMergeJob.from_table_chain(table_chain, self.sql_client)]

    def _make_add_column_sql(
        self, new_columns: Sequence[TColumnSchema], table_format: TTableFormat = None
    ) -> List[str]:
        # Override because mssql requires multiple columns in a single ADD COLUMN clause
        return [
            "ADD \n" + ",\n".join(self._get_column_def_sql(c, table_format) for c in new_columns)
        ]

    def _get_column_def_sql(self, c: TColumnSchema, table_format: TTableFormat = None) -> str:
        sc_type = c["data_type"]
        if sc_type == "text" and c.get("unique"):
            # MSSQL does not allow index on large TEXT columns
            db_type = "nvarchar(%i)" % (c.get("precision") or 900)
        else:
            db_type = self.type_mapper.to_db_type(c)

        hints_str = " ".join(
            self.active_hints.get(h, "")
            for h in self.active_hints.keys()
            if c.get(h, False) is True
        )
        column_name = self.capabilities.escape_identifier(c["name"])
        return f"{column_name} {db_type} {hints_str} {self._gen_not_null(c['nullable'])}"

    def _create_replace_followup_jobs(
        self, table_chain: Sequence[TTableSchema]
    ) -> List[NewLoadJob]:
        if self.config.replace_strategy == "staging-optimized":
            return [MsSqlStagingCopyJob.from_table_chain(table_chain, self.sql_client)]
        return super()._create_replace_followup_jobs(table_chain)

    def _from_db_type(
        self, pq_t: str, precision: Optional[int], scale: Optional[int]
    ) -> TColumnType:
        return self.type_mapper.from_db_type(pq_t, precision, scale)
