from typing import ClassVar, Dict, Optional, Sequence, List, Any, Tuple, Iterator


from dlt.common.wei import EVM_DECIMAL_PRECISION
from dlt.common.destination.reference import NewLoadJob
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.data_types import TDataType
from dlt.common.schema import TColumnSchema, TColumnHint, Schema
from dlt.common.schema.typing import TTableSchema, TColumnType, TTableFormat
from dlt.common.storages import FileStorage
from dlt.common.utils import uniq_id

from dlt.common.destination.reference import LoadJob, FollowupJob, TLoadJobState


from dlt.destinations.sql_jobs import SqlStagingCopyJob, SqlMergeJob

from dlt.destinations.insert_job_client import InsertValuesJobClient, InsertValuesLoadJob

from dlt.destinations.synapse import capabilities
from dlt.destinations.synapse.sql_client import PyOdbcSynapseClient
from dlt.destinations.synapse.configuration import SynapseClientConfiguration
from dlt.destinations.sql_client import SqlClientBase
from dlt.common.schema.typing import COLUMN_HINTS
from dlt.destinations.type_mapping import TypeMapper

from dlt.common.data_writers.escape import escape_synapse_identifier, escape_synapse_literal
from dlt.destinations.job_impl import EmptyLoadJob


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
        with FileStorage.open_zipsafe_ro(file_path, "r", encoding="utf-8") as f:
            header = f.readline().strip()  # Read the header which contains the INSERT INTO statement template
            #print("Here are the columns: " + str(header))
            f.readline()  # Skip the "VALUES" marker line

            # Now read the file line by line and construct the SQL INSERT statements
            insert_sql_parts = []
            for line in f:
                #print("current line: " + str(line))
                line = line.strip()

                # Remove outer parentheses and trailing comma or semicolon using a regular expression
                line = re.sub(r'^\(|\)[,;]?$', '', line)

                #print("post-cleanup line: " + str(line))
                if not line:
                    continue  # Skip empty lines

                # Construct the SELECT part of the SQL statement for each row of values
                values_str = ', '.join(value for value in line.split(','))
                # Ensure no comma is added at the end of the SELECT statement
                insert_sql_parts.append(f"SELECT {values_str}")

            if insert_sql_parts:
                # Combine the SELECT statements with UNION ALL and format the final INSERT INTO statement
                insert_sql = header.format(qualified_table_name) + "\n" + "\nUNION ALL\n".join(insert_sql_parts) + ";"
                yield [insert_sql]


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

    def restore_file_load(self, file_path: str) -> LoadJob:
        """Returns a completed SqlLoadJob or InsertValuesJob

        Returns completed jobs as SqlLoadJob and InsertValuesJob executed atomically in start_file_load so any jobs that should be recreated are already completed.
        Obviously the case of asking for jobs that were never created will not be handled. With correctly implemented loader that cannot happen.

        Args:
            file_path (str): a path to a job file

        Returns:
            LoadJob: Always a restored job completed
        """
        job = super().restore_file_load(file_path)
        if not job:
            job = EmptyLoadJob.from_file_path(file_path, "completed")
        return job

    def start_file_load(self, table: TTableSchema, file_path: str, load_id: str) -> LoadJob:
        job = SynapseInsertValuesLoadJob(table["name"], file_path, self.sql_client)
        return job

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
