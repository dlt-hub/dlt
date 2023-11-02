from typing import ClassVar, Dict, Optional, Sequence, List, Any, Tuple, Iterator

from dlt.common.wei import EVM_DECIMAL_PRECISION
from dlt.common.destination.reference import NewLoadJob
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.data_types import TDataType
from dlt.common.schema import TColumnSchema, TColumnHint, Schema
from dlt.common.schema.typing import TTableSchema, TColumnType, TTableFormat
from dlt.common.storages import FileStorage
from dlt.common.utils import uniq_id

from dlt.destinations.sql_jobs import SqlStagingCopyJob, SqlMergeJob

from dlt.destinations.insert_job_client import InsertValuesJobClient, InsertValuesLoadJob

from dlt.destinations.synapse import capabilities
from dlt.destinations.synapse.sql_client import PyOdbcSynapseClient
from dlt.destinations.synapse.configuration import SynapseClientConfiguration
from dlt.destinations.sql_client import SqlClientBase
from dlt.common.schema.typing import COLUMN_HINTS
from dlt.destinations.type_mapping import TypeMapper

from dlt.common.data_writers.escape import escape_synapse_identifier, escape_synapse_literal


import re

# TODO remove logging
import logging  # Import the logging module if it's not already imported
logger = logging.getLogger(__name__)


class SynapseTypeMapper(TypeMapper):
    # setting to nvarchar(4000) instead of nvarchar(max); cannot use nvarchar(max) for columnstore index
    # https://learn.microsoft.com/en-us/answers/questions/470492/capacity-limits-for-dedicated-sql-pool-in-azure-sy
    sct_to_unbound_dbt = {
        "complex": "nvarchar(4000)",
        "text": "nvarchar(4000)",
        "double": "float",
        "bool": "bit",
        "bigint": "bigint",
        "binary": "varbinary(8000)",
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

class SynapseInsertValuesLoadJob(InsertValuesLoadJob):

    def __init__(self, table_name: str, file_path: str, sql_client: SqlClientBase[Any]) -> None:
        # First, set any attributes specific to this subclass
        self._sql_client = sql_client
        self._file_name = FileStorage.get_file_name_from_file_path(file_path)

        # Then, call the parent class's __init__ method with the required arguments
        super().__init__(table_name, file_path, sql_client)

    def _insert(self, qualified_table_name: str, file_path: str) -> Iterator[List[str]]:
        # Get the rows of data directly
        rows = self.get_values_rows(file_path)  # This will need to be implemented

        # Begin constructing the insert SQL with UNION ALL syntax for Azure Synapse
        insert_sql = []
        for row in rows:
            # Construct the SELECT part of the UNION ALL statement for each row
            select_statement = 'SELECT ' + ', '.join(f"'{value}'" for value in row)
            insert_sql.append(select_statement)

        # Combine all SELECT statements with UNION ALL
        final_insert_sql = ' UNION ALL '.join(insert_sql)
        # The full INSERT statement includes the insert into the table part
        full_insert_sql = f'INSERT INTO {qualified_table_name} ({self.get_column_names()}) ' + final_insert_sql

        # Yield the final insert SQL statement
        yield [full_insert_sql]


class SynapseClient(InsertValuesJobClient):

    capabilities: ClassVar[DestinationCapabilitiesContext] = capabilities()

    def __init__(self, schema: Schema, config: SynapseClientConfiguration) -> None:
        if hasattr(config.credentials, 'on_resolved'):
            config.credentials.on_resolved()

        sql_client = PyOdbcSynapseClient(
            config.normalize_dataset_name(schema),
            config.credentials
        )
        super().__init__(schema, config, sql_client)
        self.config: SynapseClientConfiguration = config
        self.sql_client = sql_client
        self.active_hints = HINT_TO_SYNAPSE_ATTR if self.config.create_indexes else {}
        self.type_mapper = SynapseTypeMapper(self.capabilities)

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
