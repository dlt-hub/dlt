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


class SynapseInsertValuesLoadJob(InsertValuesLoadJob):

    def __init__(self, table_name: str, file_path: str, sql_client: SqlClientBase[Any]) -> None:
        # First, set any attributes specific to this subclass
        self._sql_client = sql_client
        self._file_name = FileStorage.get_file_name_from_file_path(file_path)

        # Then, call the parent class's __init__ method with the required arguments
        super().__init__(table_name, file_path, sql_client)

    def _insert(self, qualified_table_name: str, file_path: str) -> Iterator[List[str]]:
        # First, get the original SQL fragments
        original_sql_fragments = super()._insert(qualified_table_name, file_path)

        # Now, adapt each SQL fragment for Synapse using the generate_insert_query method
        for original_sql in original_sql_fragments:
            # Parse the original SQL to extract table name, columns, and rows
            # This is a simplified example, you'll need a more robust way to parse the SQL
            original_sql_joined = ''.join(original_sql)
            table_name_match = re.search(r'INSERT INTO (.*?)\(', original_sql_joined)
            columns_match = re.search(r'\((.*?)\)', original_sql_joined)
            values_match = re.search(r'VALUES(.*?);', original_sql_joined, re.DOTALL)

            if table_name_match and columns_match and values_match:
                table_name = table_name_match.group(1)
                columns_str = columns_match.group(1)
                values_str = values_match.group(1)

                # Split columns and values strings into lists
                columns = [col.strip() for col in columns_str.split(',')]
                values = [[val.strip() for val in value_group.split(',')] for value_group in values_str.split('),(')]

                # Call generate_insert_query with the extracted values
                adapted_sql, param_values = self.generate_insert_query(table_name, columns, values)
                yield adapted_sql, param_values
            else:
                logger.error(f"Failed to parse original SQL: {original_sql_joined}")
                raise ValueError("Failed to parse original SQL")

    def generate_insert_query(self, table_name: str, columns: List[str], rows: List[List[Any]]) -> Tuple[str, List[Any]]:
        print("HERE are INCOMING ROWS for INSERT SQL: " + str(rows))  # This will print the generated SQL
        print("HERE is table_name for INSERT SQL: " + str(table_name))  # This will print the generated SQL
        print("HERE are columns for INSERT SQL: " + str(columns))  # This will print the generated SQL
        try:
            # Escaping table name and column names
            # escaped_table_name = escape_synapse_identifier(table_name)
            # escaped_column_names = ', '.join(escape_synapse_identifier(col) for col in columns)

            print(F"\n\nHERE are ESCAPED table_name for INSERT SQL: " + str(table_name))  # This will print the generated SQL
            print("HERE are ESCAPED columns for INSERT SQL: " + str(columns))  # This will print the generated SQL

            # Building SELECT statements with parameter markers
            select_statements = [f"SELECT {', '.join(['?' for _ in row])}" for row in rows]
            print("HERE Generated SELECT statements for INSERT SQL: " + str(select_statements))  # This will print the generated SQL

            # Combining SELECT statements with UNION ALL
            all_select_statements = " UNION ALL ".join(select_statements)

            # Building the final SQL query
            new_sql = f'INSERT INTO {table_name}({columns}) {all_select_statements}'

            # Extracting parameter values
            param_values = [item for row in rows for item in row]
            #
            # # Ensure each item in param_values is properly formatted
            # formatted_param_values = [escape_synapse_literal(item) for item in param_values]

            print("New Generated INSERT SQL: " + new_sql)  # This will print the generated SQL
            print("Param values: " + str(param_values))  # This will print the parameters

            return new_sql, param_values

        except Exception as e:
            logger.error(f"Failed to generate insert query: {e}")
            raise

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
