from typing import ClassVar, Dict, Optional, Sequence, List, Any, Tuple

from dlt.common.wei import EVM_DECIMAL_PRECISION
from dlt.common.destination.reference import NewLoadJob
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.data_types import TDataType
from dlt.common.schema import TColumnSchema, TColumnHint, Schema
from dlt.common.schema.typing import TTableSchema, TColumnType, TTableFormat
from dlt.common.utils import uniq_id

from dlt.destinations.sql_jobs import SqlStagingCopyJob, SqlMergeJob

from dlt.destinations.insert_job_client import InsertValuesJobClient

from dlt.destinations.synapse import capabilities
from dlt.destinations.synapse.sql_client import PyOdbcSynapseClient
from dlt.destinations.synapse.configuration import SynapseClientConfiguration
from dlt.destinations.sql_client import SqlClientBase
from dlt.common.schema.typing import COLUMN_HINTS
from dlt.destinations.type_mapping import TypeMapper

import logging  # Import the logging module if it's not already imported

logger = logging.getLogger(__name__)


class SynapseTypeMapper(TypeMapper):
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
        "wei": "decimal(%i,%i)"
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

HINT_TO_SYNAPSE_ATTR: Dict[TColumnHint, str] = {
    "unique": "UNIQUE"
}

class SynapseStagingCopyJob(SqlStagingCopyJob):

    @classmethod
    def generate_sql(cls, table_chain: Sequence[TTableSchema], sql_client: SqlClientBase[Any]) -> List[str]:
        sql: List[str] = []
        for table in table_chain:
            with sql_client.with_staging_dataset(staging=True):
                staging_table_name = sql_client.make_qualified_table_name(table["name"])
            table_name = sql_client.make_qualified_table_name(table["name"])
            # drop destination table
            sql.append(f"DROP TABLE {table_name};")
            # moving staging table to destination schema
            sql.append(f"ALTER SCHEMA {sql_client.fully_qualified_dataset_name()} TRANSFER {staging_table_name};")
        return sql


class SynapseMergeJob(SqlMergeJob):
    @classmethod
    def gen_key_table_clauses(cls, root_table_name: str, staging_root_table_name: str, key_clauses: Sequence[str], for_delete: bool) -> List[str]:
        """Generate sql clauses that may be used to select or delete rows in root table of destination dataset
        """
        if for_delete:
            # MS SQL doesn't support alias in DELETE FROM
            return [f"FROM {root_table_name} WHERE EXISTS (SELECT 1 FROM {staging_root_table_name} WHERE {' OR '.join([c.format(d=root_table_name,s=staging_root_table_name) for c in key_clauses])})"]
        return SqlMergeJob.gen_key_table_clauses(root_table_name, staging_root_table_name, key_clauses, for_delete)

    @classmethod
    def _to_temp_table(cls, select_sql: str, temp_table_name: str) -> str:
        return f"SELECT * INTO {temp_table_name} FROM ({select_sql}) as t;"

    @classmethod
    def _new_temp_table_name(cls, name_prefix: str) -> str:
        name = SqlMergeJob._new_temp_table_name(name_prefix)
        return '#' + name

class SynapseClient(InsertValuesJobClient):
    #Synapse does not support multi-row inserts using a single INSERT INTO statement

    capabilities: ClassVar[DestinationCapabilitiesContext] = capabilities()

    def __init__(self, schema: Schema, config: SynapseClientConfiguration) -> None:
        sql_client = PyOdbcSynapseClient(
            config.normalize_dataset_name(schema),
            config.credentials
        )
        super().__init__(schema, config, sql_client)
        self.config: SynapseClientConfiguration = config
        self.sql_client = sql_client
        self.active_hints = HINT_TO_SYNAPSE_ATTR if self.config.create_indexes else {}
        self.type_mapper = SynapseTypeMapper(self.capabilities)

    def _create_merge_job(self, table_chain: Sequence[TTableSchema]) -> NewLoadJob:
        return SynapseMergeJob.from_table_chain(table_chain, self.sql_client)

    def _make_add_column_sql(self, new_columns: Sequence[TColumnSchema]) -> List[str]:
        # Override because mssql requires multiple columns in a single ADD COLUMN clause
        return ["ADD \n" + ",\n".join(self._get_column_def_sql(c) for c in new_columns)]

    def _get_table_update_sql(self, table_name: str, new_columns: Sequence[TColumnSchema], generate_alter: bool) -> List[str]:
        # build sql
        canonical_name = self.sql_client.make_qualified_table_name(table_name)
        sql_result: List[str] = []
        if not generate_alter:
            # build CREATE
            sql = f"CREATE TABLE {canonical_name} (\n"
            sql += ",\n".join([self._get_column_def_sql(c) for c in new_columns])
            sql += ") WITH (HEAP)"
            sql_result.append(sql)
        else:
            sql_base = f"ALTER TABLE {canonical_name}\n"
            add_column_statements = self._make_add_column_sql(new_columns)
            if self.capabilities.alter_add_multi_column:
                column_sql = ",\n"
                sql_result.append(sql_base + column_sql.join(add_column_statements))
            else:
                # build ALTER as separate statement for each column (redshift limitation)
                sql_result.extend([sql_base + col_statement for col_statement in add_column_statements])

        # scan columns to get hints
        if generate_alter:
            # no hints may be specified on added columns
            for hint in COLUMN_HINTS:
                if any(c.get(hint, False) is True for c in new_columns):
                    hint_columns = [self.capabilities.escape_identifier(c["name"]) for c in new_columns if c.get(hint, False)]
                    if hint == "not_null":
                        logger.warning(f"Column(s) {hint_columns} with NOT NULL are being added to existing table {canonical_name}."
                                       " If there's data in the table the operation will fail.")
                    else:
                        logger.warning(f"Column(s) {hint_columns} with hint {hint} are being added to existing table {canonical_name}."
                                       " Several hint types may not be added to existing tables.")
        return sql_result

    def _create_merge_followup_jobs(self, table_chain: Sequence[TTableSchema]) -> List[NewLoadJob]:
        return [SynapseMergeJob.from_table_chain(table_chain, self.sql_client)]

    def _make_add_column_sql(self, new_columns: Sequence[TColumnSchema], table_format: TTableFormat = None) -> List[str]:
        # Override because mssql requires multiple columns in a single ADD COLUMN clause
        return ["ADD \n" + ",\n".join(self._get_column_def_sql(c, table_format) for c in new_columns)]

    def _get_column_def_sql(self, c: TColumnSchema, table_format: TTableFormat = None) -> str:
        sc_type = c["data_type"]
        if sc_type == "text" and c.get("unique"):
            # MSSQL does not allow index on large TEXT columns
            db_type = "nvarchar(%i)" % (c.get("precision") or 900)
        else:
            db_type = self.type_mapper.to_db_type(c)

        hints_str = " ".join(self.active_hints.get(h, "") for h in self.active_hints.keys() if c.get(h, False) is True)
        column_name = self.capabilities.escape_identifier(c["name"])
        return f"{column_name} {db_type} {hints_str} {self._gen_not_null(c['nullable'])}"

    def _create_replace_followup_jobs(self, table_chain: Sequence[TTableSchema]) -> List[NewLoadJob]:
        if self.config.replace_strategy == "staging-optimized":
            return [SynapseStagingCopyJob.from_table_chain(table_chain, self.sql_client)]
        return super()._create_replace_followup_jobs(table_chain)

    def _from_db_type(self, pq_t: str, precision: Optional[int], scale: Optional[int]) -> TColumnType:
        return self.type_mapper.from_db_type(pq_t, precision, scale)
