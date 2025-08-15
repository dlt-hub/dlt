import os
from abc import abstractmethod
import base64
import contextlib
from copy import copy
from types import TracebackType
from typing import (
    Any,
    ClassVar,
    Dict,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
    Iterable,
    Iterator,
)
import zlib
import re

import sqlglot.expressions

from dlt.common.libs.sqlglot import TSqlGlotDialect
from dlt.common import pendulum, logger
from dlt.common.destination.capabilities import DataTypeMapper
from dlt.common.destination.utils import resolve_replace_strategy
from dlt.common.json import json
from dlt.common.schema.typing import (
    C_DLT_LOAD_ID,
    C_DLT_LOADS_TABLE_LOAD_ID,
    COLUMN_HINTS,
    TColumnType,
    TColumnSchemaBase,
    TPartialTableSchema,
)
from dlt.common.schema.utils import (
    get_inherited_table_hint,
    has_default_column_prop_value,
    loads_table,
    normalize_table_identifiers,
    version_table,
)
from dlt.common.utils import read_dialect_and_sql
from dlt.common.storages import FileStorage
from dlt.common.storages.load_package import LoadJobInfo, ParsedLoadJobFileName
from dlt.common.schema import TColumnSchema, Schema, TTableSchemaColumns, TSchemaTables
from dlt.common.schema import TColumnHint
from dlt.common.destination.client import (
    PreparedTableSchema,
    StateInfo,
    StorageSchemaInfo,
    WithStateSync,
    DestinationClientConfiguration,
    DestinationClientDwhConfiguration,
    FollowupJobRequest,
    WithStagingDataset,
    RunnableLoadJob,
    LoadJob,
    JobClientBase,
    HasFollowupJobs,
    CredentialsConfiguration,
)

from dlt.destinations.exceptions import DatabaseUndefinedRelation
from dlt.destinations.job_impl import (
    ReferenceFollowupJobRequest,
)
from dlt.destinations.sql_jobs import SqlMergeFollowupJob, SqlStagingReplaceFollowupJob
from dlt.destinations.typing import TNativeConn
from dlt.destinations.sql_client import SqlClientBase, WithSqlClient
from dlt.destinations.utils import (
    get_pipeline_state_query_columns,
    info_schema_null_to_bool,
    verify_schema_merge_disposition,
    verify_schema_replace_disposition,
)

import sqlglot

# this should suffice for now
DDL_COMMANDS = ["ALTER", "CREATE", "DROP"]
# TODO: move to respective sql clients
UNLOGGED_COMMANDS = ["ALTER SCHEMA"]


class SqlLoadJob(RunnableLoadJob):
    """A job executing sql statement, without followup trait"""

    def __init__(self, file_path: str) -> None:
        super().__init__(file_path)
        self._job_client: "SqlJobClientBase" = None

    def run(self) -> None:
        self._sql_client = self._job_client.sql_client
        # execute immediately if client present
        with FileStorage.open_zipsafe_ro(self._file_path, "r", encoding="utf-8") as f:
            sql = f.read()

        with self.maybe_transaction(sql):
            # Some clients (e.g. databricks) do not support multiple statements in one execute call
            self._sql_client.execute_many(self._split_fragments(sql))

    @contextlib.contextmanager
    def maybe_transaction(self, sql: str) -> Iterator[None]:
        """Begins a transaction if sql client supports it, otherwise works in auto commit."""
        if (
            self._job_client.capabilities.supports_ddl_transactions
            or not self._string_contains_ddl_queries(sql)
        ) and not self._has_out_of_transaction_commands(sql):
            with self._sql_client.begin_transaction():
                yield
        else:
            yield

    def _string_contains_ddl_queries(self, sql: str) -> bool:
        for cmd in DDL_COMMANDS:
            if re.search(cmd, sql, re.IGNORECASE):
                return True
        return False

    def _has_out_of_transaction_commands(self, sql: str) -> bool:
        for cmd in UNLOGGED_COMMANDS:
            if re.search(cmd, sql, re.IGNORECASE):
                return True
        # disable transaction on synapse when doing sql jobs, to enable:
        # TODO: 1. disable transaction if temp table is created via select into
        # 2. swap TRUNCATE for delete in replace jobs
        if "SynapseClient" in self._job_client.__class__.__name__:
            return True
        return False

    def _split_fragments(self, sql: str) -> List[str]:
        return [s.strip() for line in sql.split("\n") for s in line.split(";") if s.strip()]

    @staticmethod
    def is_sql_job(file_path: str) -> bool:
        return os.path.splitext(file_path)[1][1:] == "sql"


class ModelLoadJob(RunnableLoadJob, HasFollowupJobs):
    """
    A job to insert rows into a table from a model file which contains a single select statement
    """

    def __init__(self, file_path: str) -> None:
        super().__init__(file_path)
        self._job_client: "SqlJobClientBase" = None

    def run(self) -> None:
        with FileStorage.open_zipsafe_ro(self._file_path, "r", encoding="utf-8") as f:
            select_dialect, select_statement = read_dialect_and_sql(
                file_obj=f,
                fallback_dialect=self._job_client.capabilities.sqlglot_dialect,  # caps are available at this point
            )

        sql_client = self._job_client.sql_client
        insert_statement = self._insert_statement_from_select_statement(
            select_dialect, select_statement
        )
        sql_client.execute_sql(insert_statement)

    def _insert_statement_from_select_statement(
        self, select_dialect: TSqlGlotDialect, select_statement: str
    ) -> str:
        """
        Generates an INSERT statement from a SELECT statement using sqlglot.
        Transpiles the SELECT if needed and adjusts identifiers for the destination dialect.
        """
        sql_client = self._job_client.sql_client
        target_table = sql_client.make_qualified_table_name(self._load_table["name"])
        target_catalog = sql_client.catalog_name(quote=False)
        destination_dialect = self._job_client.capabilities.sqlglot_dialect

        # Parse SELECT
        parsed_select = sqlglot.parse_one(select_statement, read=select_dialect)

        # Adjust table parts (catalog/db/this) based on dialect and catalog presence
        if select_dialect != destination_dialect:
            # TODO: We might need this
            for table in parsed_select.find_all(sqlglot.exp.Table):
                parts = list(table.parts)
                if target_catalog:
                    if len(parts) == 3:
                        table.set("catalog", sqlglot.to_identifier(target_catalog))
                        table.set("db", sqlglot.to_identifier(parts[1].name))
                        table.set("this", sqlglot.to_identifier(parts[2].name))
                    elif len(parts) == 2:
                        table.set("catalog", sqlglot.to_identifier(target_catalog))
                        table.set("db", sqlglot.to_identifier(parts[0].name))
                        table.set("this", sqlglot.to_identifier(parts[1].name))
                else:
                    if len(parts) == 3:
                        table.set("catalog", None)
                        table.set("db", sqlglot.to_identifier(parts[1].name))
                        table.set("this", sqlglot.to_identifier(parts[2].name))

        # Ensure there's a top-level SELECT, otherwise it doesn't make sense
        top_level_select = parsed_select.find(sqlglot.exp.Select)
        if top_level_select is None:
            raise ValueError(
                "No top-level SELECT statement found in the model query. Models must be defined"
                " using a SELECT statement."
            )

        # Get identifiers for the columns in the INSERT statement
        # Normalizer aliased all selections
        columns = []
        for _, expr in enumerate(top_level_select.expressions):
            alias_name = expr.alias
            columns.append(sqlglot.to_identifier(alias_name, quoted=True))

        # Build final INSERT
        query = sqlglot.expressions.insert(
            expression=parsed_select,
            into=target_table,
            columns=columns,
            dialect=destination_dialect,
        ).sql(destination_dialect)

        return query

    @staticmethod
    def is_model_job(file_path: str) -> bool:
        return os.path.splitext(file_path)[1][1:] == "model"


class CopyRemoteFileLoadJob(RunnableLoadJob, HasFollowupJobs):
    def __init__(
        self,
        file_path: str,
        staging_credentials: Optional[CredentialsConfiguration] = None,
    ) -> None:
        super().__init__(file_path)
        self._job_client: "SqlJobClientBase" = None
        self._staging_credentials = staging_credentials
        self._bucket_path = ReferenceFollowupJobRequest.resolve_reference(file_path)


class SqlJobClientBase(WithSqlClient, JobClientBase, WithStateSync):
    def __init__(
        self,
        schema: Schema,
        config: DestinationClientConfiguration,
        sql_client: SqlClientBase[TNativeConn],
    ) -> None:
        # get definitions of the dlt tables, normalize column names and keep for later use
        version_table_ = normalize_table_identifiers(version_table(), schema.naming)
        self.version_table_schema_columns = ", ".join(
            sql_client.escape_column_name(col) for col in version_table_["columns"]
        )
        loads_table_ = normalize_table_identifiers(loads_table(), schema.naming)
        self.loads_table_schema_columns = ", ".join(
            sql_client.escape_column_name(col) for col in loads_table_["columns"]
        )
        state_table_ = normalize_table_identifiers(
            get_pipeline_state_query_columns(), schema.naming
        )
        self.state_table_columns = ", ".join(
            sql_client.escape_column_name(col) for col in state_table_["columns"]
        )
        self.active_hints: Dict[TColumnHint, str] = {}
        self.type_mapper: DataTypeMapper = None
        super().__init__(schema, config, sql_client.capabilities)
        self.sql_client = sql_client
        assert isinstance(config, DestinationClientDwhConfiguration)
        self.config: DestinationClientDwhConfiguration = config

    @property
    def sql_client_class(self) -> Type[SqlClientBase[Any]]:
        return self._sql_client.__class__

    @property
    def sql_client(self) -> SqlClientBase[TNativeConn]:
        return self._sql_client

    @sql_client.setter
    def sql_client(self, client: SqlClientBase[TNativeConn]) -> None:
        self._sql_client = client

    def drop_storage(self) -> None:
        self.sql_client.drop_dataset()
        with contextlib.suppress(DatabaseUndefinedRelation):
            with self.sql_client.with_staging_dataset():
                self.sql_client.drop_dataset()

    def initialize_storage(self, truncate_tables: Iterable[str] = None) -> None:
        if not self.is_storage_initialized():
            self.sql_client.create_dataset()
        elif truncate_tables:
            self.sql_client.truncate_tables(*truncate_tables)

    def is_storage_initialized(self) -> bool:
        return self.sql_client.has_dataset()

    def update_stored_schema(
        self,
        only_tables: Iterable[str] = None,
        expected_update: TSchemaTables = None,
    ) -> Optional[TSchemaTables]:
        super().update_stored_schema(only_tables, expected_update)
        applied_update: TSchemaTables = {}
        schema_info = self.get_stored_schema_by_hash(self.schema.stored_version_hash)
        if schema_info is None:
            logger.info(
                f"Schema with hash {self.schema.stored_version_hash} not found in the storage."
                " upgrading"
            )

            with self.maybe_ddl_transaction():
                applied_update = self._execute_schema_update_sql(only_tables)
        else:
            logger.info(
                f"Schema with hash {self.schema.stored_version_hash} inserted at"
                f" {schema_info.inserted_at} found in storage, no upgrade required"
            )
        return applied_update

    def drop_tables(self, *tables: str, delete_schema: bool = True) -> None:
        """Drop tables in destination database and optionally delete the stored schema as well.
        Clients that support ddl transactions will have both operations performed in a single transaction.

        Args:
            tables: Names of tables to drop.
            delete_schema: If True, also delete all versions of the current schema from storage
        """
        with self.maybe_ddl_transaction():
            self.sql_client.drop_tables(*tables)
            if delete_schema:
                self._delete_schema_in_storage(self.schema)

    @contextlib.contextmanager
    def maybe_ddl_transaction(self) -> Iterator[None]:
        """Begins a transaction if sql client supports it, otherwise works in auto commit."""
        if self.capabilities.supports_ddl_transactions:
            with self.sql_client.begin_transaction():
                yield
        else:
            yield

    def prepare_load_table(self, table_name: str) -> PreparedTableSchema:
        load_table = super().prepare_load_table(table_name)
        # apply x-replace-strategy (still unofficial)
        if load_table["write_disposition"] == "replace":
            replace_strategy = resolve_replace_strategy(
                load_table, self.config.replace_strategy, self.capabilities
            )
            assert replace_strategy, f"Must be able to get replace strategy for {table_name}"
            load_table["x-replace-strategy"] = replace_strategy  # type: ignore[typeddict-unknown-key]
        return load_table

    def should_truncate_table_before_load(self, table_name: str) -> bool:
        table = self.prepare_load_table(table_name)
        return (
            table["write_disposition"] == "replace"
            and table["x-replace-strategy"] == "truncate-and-insert"  # type: ignore[typeddict-item]
        )

    def _create_append_followup_jobs(
        self, table_chain: Sequence[PreparedTableSchema]
    ) -> List[FollowupJobRequest]:
        return []

    def _create_merge_followup_jobs(
        self, table_chain: Sequence[PreparedTableSchema]
    ) -> List[FollowupJobRequest]:
        return [SqlMergeFollowupJob.from_table_chain(table_chain, self.sql_client)]

    def _create_replace_followup_jobs(
        self, table_chain: Sequence[PreparedTableSchema]
    ) -> List[FollowupJobRequest]:
        jobs: List[FollowupJobRequest] = []
        root_table = table_chain[0]
        if root_table["x-replace-strategy"] in ["insert-from-staging", "staging-optimized"]:  # type: ignore[typeddict-item]
            jobs.append(SqlStagingReplaceFollowupJob.from_table_chain(table_chain, self.sql_client))
        return jobs

    def create_table_chain_completed_followup_jobs(
        self,
        table_chain: Sequence[PreparedTableSchema],
        completed_table_chain_jobs: Optional[Sequence[LoadJobInfo]] = None,
    ) -> List[FollowupJobRequest]:
        """Creates a list of followup jobs for merge write disposition and staging replace strategies"""
        jobs = super().create_table_chain_completed_followup_jobs(
            table_chain, completed_table_chain_jobs
        )
        write_disposition = table_chain[0]["write_disposition"]
        if write_disposition == "append":
            jobs.extend(self._create_append_followup_jobs(table_chain))
        elif write_disposition == "merge":
            jobs.extend(self._create_merge_followup_jobs(table_chain))
        elif write_disposition == "replace":
            jobs.extend(self._create_replace_followup_jobs(table_chain))
        return jobs

    def create_load_job(
        self, table: PreparedTableSchema, file_path: str, load_id: str, restore: bool = False
    ) -> LoadJob:
        """Starts SqlLoadJob for files ending with .sql or returns None to let derived classes to handle their specific jobs"""
        if SqlLoadJob.is_sql_job(file_path):
            # create sql load job
            return SqlLoadJob(file_path)
        if ModelLoadJob.is_model_job(file_path):
            # create model load job
            return ModelLoadJob(file_path)
        return None

    def complete_load(self, load_id: str) -> None:
        name = self.sql_client.make_qualified_table_name(self.schema.loads_table_name)
        now_ts = pendulum.now()
        self.sql_client.execute_sql(
            f"INSERT INTO {name}({self.loads_table_schema_columns}) VALUES(%s, %s, %s, %s, %s)",
            load_id,
            self.schema.name,
            0,
            now_ts,
            self.schema.version_hash,
        )

    def __enter__(self) -> "SqlJobClientBase":
        self.sql_client.open_connection()
        return self

    def __exit__(
        self, exc_type: Type[BaseException], exc_val: BaseException, exc_tb: TracebackType
    ) -> None:
        self.sql_client.close_connection()

    def get_storage_tables(
        self, table_names: Iterable[str]
    ) -> Iterable[Tuple[str, TTableSchemaColumns]]:
        """Uses INFORMATION_SCHEMA to retrieve table and column information for tables in `table_names` iterator.
        Table names should be normalized according to naming convention and will be further converted to desired casing
        in order to (in most cases) create case-insensitive name suitable for search in information schema.

        The column names are returned as in information schema. To match those with columns in existing table, you'll need to use
        `schema.get_new_table_columns` method and pass the correct casing. Most of the casing function are irreversible so it is not
        possible to convert identifiers into INFORMATION SCHEMA back into case sensitive dlt schema.
        """
        table_names = list(table_names)
        if len(table_names) == 0:
            # empty generator
            return
        # get schema search components
        catalog_name, schema_name, folded_table_names = (
            self.sql_client._get_information_schema_components(*table_names)
        )
        # create table name conversion lookup table
        name_lookup = {
            folded_name: name for folded_name, name in zip(folded_table_names, table_names)
        }
        # this should never happen: we verify schema for name collisions before loading
        assert len(name_lookup) == len(table_names), (
            f"One or more of tables in {table_names} after applying"
            f" {self.capabilities.casefold_identifier} produced a name collision."
        )
        # if we have more tables to lookup than a threshold, we prefer to filter them in code
        if (
            len(name_lookup) > self.config.info_tables_query_threshold
            or len(",".join(folded_table_names)) > self.capabilities.max_query_length / 2
        ):
            logger.info(
                "Fallback to query all columns from INFORMATION_SCHEMA due to limited query length"
                " or table threshold"
            )
            folded_table_names = []

        query, db_params = self._get_info_schema_columns_query(
            catalog_name, schema_name, folded_table_names
        )
        rows = self.sql_client.execute_sql(query, *db_params)
        prev_table: str = None
        storage_columns: TTableSchemaColumns = None
        for c in rows:
            # if we are selecting all tables this is expected
            if not folded_table_names and c[0] not in name_lookup:
                continue
            # make sure that new table is known
            assert (
                c[0] in name_lookup
            ), f"Table name {c[0]} not in expected tables {name_lookup.keys()}"
            table_name = name_lookup[c[0]]
            if prev_table != table_name:
                # yield what we have
                if storage_columns:
                    yield (prev_table, storage_columns)
                # we have new table
                storage_columns = {}
                prev_table = table_name
                # remove from table_names
                table_names.remove(prev_table)
            # add columns
            col_name = c[1]
            numeric_precision = (
                c[4] if self.capabilities.schema_supports_numeric_precision else None
            )
            numeric_scale = c[5] if self.capabilities.schema_supports_numeric_precision else None

            schema_c: TColumnSchemaBase = {
                "name": col_name,
                "nullable": info_schema_null_to_bool(c[3]),
                **self._from_db_type(c[2], numeric_precision, numeric_scale),
            }
            storage_columns[col_name] = schema_c  # type: ignore
        # yield last table, it must have at least one column or we had no rows
        if storage_columns:
            yield (prev_table, storage_columns)
        # if no columns we assume that table does not exist
        for table_name in table_names:
            yield (table_name, {})

    def get_storage_table(self, table_name: str) -> Tuple[bool, TTableSchemaColumns]:
        """Uses get_storage_tables to get single `table_name` schema.

        Returns (True, ...) if table exists and (False, {}) when not
        """
        storage_table = list(self.get_storage_tables([table_name]))[0]
        return len(storage_table[1]) > 0, storage_table[1]

    @abstractmethod
    def _from_db_type(
        self, db_type: str, precision: Optional[int], scale: Optional[int]
    ) -> TColumnType:
        pass

    def get_stored_schema(self, schema_name: str = None) -> StorageSchemaInfo:
        name = self.sql_client.make_qualified_table_name(self.schema.version_table_name)
        c_schema_name, c_inserted_at = self._norm_and_escape_columns("schema_name", "inserted_at")
        if not schema_name:
            query = (
                f"SELECT {self.version_table_schema_columns} FROM {name}"
                f" ORDER BY {c_inserted_at} DESC"
            )
            return self._row_to_schema_info(query)
        else:
            query = (
                f"SELECT {self.version_table_schema_columns} FROM {name} WHERE {c_schema_name} = %s"
                f" ORDER BY {c_inserted_at} DESC"
            )
            return self._row_to_schema_info(query, schema_name)

    def get_stored_state(self, pipeline_name: str) -> StateInfo:
        state_table = self.sql_client.make_qualified_table_name(self.schema.state_table_name)
        loads_table = self.sql_client.make_qualified_table_name(self.schema.loads_table_name)
        c_load_id, c_dlt_load_id, c_pipeline_name, c_status = self._norm_and_escape_columns(
            C_DLT_LOADS_TABLE_LOAD_ID, C_DLT_LOAD_ID, "pipeline_name", "status"
        )

        maybe_limit_clause_1, maybe_limit_clause_2 = self.sql_client._limit_clause_sql(1)

        query = (
            f"SELECT {maybe_limit_clause_1} {self.state_table_columns} FROM {state_table} AS s JOIN"
            f" {loads_table} AS l ON l.{c_load_id} = s.{c_dlt_load_id} WHERE {c_pipeline_name} = %s"
            f" AND l.{c_status} = 0 ORDER BY {c_load_id} DESC {maybe_limit_clause_2}"
        )
        with self.sql_client.execute_query(query, pipeline_name) as cur:
            row = cur.fetchone()
        if not row:
            return None
        # NOTE: we request order of columns in SELECT statement which corresponds to StateInfo
        return StateInfo(
            version=row[0],
            engine_version=row[1],
            pipeline_name=row[2],
            state=row[3],
            created_at=pendulum.instance(row[4]),
            _dlt_load_id=row[5],
        )

    def _norm_and_escape_columns(self, *columns: str) -> Iterator[str]:
        return map(
            self.sql_client.escape_column_name, map(self.schema.naming.normalize_path, columns)
        )

    def get_stored_schema_by_hash(self, version_hash: str) -> StorageSchemaInfo:
        table_name = self.sql_client.make_qualified_table_name(self.schema.version_table_name)
        (c_version_hash,) = self._norm_and_escape_columns("version_hash")

        maybe_limit_clause_1, maybe_limit_clause_2 = self.sql_client._limit_clause_sql(1)

        query = (
            f"SELECT {maybe_limit_clause_1} {self.version_table_schema_columns} FROM"
            f" {table_name} WHERE {c_version_hash} = %s {maybe_limit_clause_2}"
        )
        return self._row_to_schema_info(query, version_hash)

    def _get_info_schema_columns_query(
        self, catalog_name: Optional[str], schema_name: str, folded_table_names: List[str]
    ) -> Tuple[str, List[Any]]:
        """Generates SQL to query INFORMATION_SCHEMA.COLUMNS for a set of tables in `folded_table_names`. Input identifiers must be already
        in a form that can be passed to a query via db_params. `catalogue_name` and `folded_tableS_name` is optional and when None, the part of query selecting it
        is skipped.

        Returns: query and list of db_params tuple
        """
        query = f"""
SELECT {",".join(self._get_storage_table_query_columns())}
    FROM INFORMATION_SCHEMA.COLUMNS
WHERE """

        db_params = []
        if catalog_name:
            db_params.append(catalog_name)
            query += "table_catalog = %s AND "
        db_params.append(schema_name)
        select_tables_clause = ""
        # look for particular tables only when requested, otherwise return the full schema
        if folded_table_names:
            db_params = db_params + folded_table_names
            # placeholder for each table
            table_placeholders = ",".join(["%s"] * len(folded_table_names))
            select_tables_clause = f"AND table_name IN ({table_placeholders})"
        query += f"table_schema = %s {select_tables_clause} ORDER BY table_name, ordinal_position"

        return query, db_params

    def _get_storage_table_query_columns(self) -> List[str]:
        """Column names used when querying table from information schema.
        Override for databases that use different namings.
        """
        fields = ["table_name", "column_name", "data_type", "is_nullable"]
        if self.capabilities.schema_supports_numeric_precision:
            fields += ["numeric_precision", "numeric_scale"]
        return fields

    def _execute_schema_update_sql(self, only_tables: Iterable[str]) -> TSchemaTables:
        # Only `only_tables` are included, or all if None.
        sql_scripts, schema_update = self._build_schema_update_sql(
            list(self.get_storage_tables(only_tables or self.schema.tables.keys()))
        )
        # Stay within max query size when doing DDL.
        # Some DB backends use bytes not characters, so decrease the limit by half,
        # assuming most of the characters in DDL encoded into single bytes.
        self.sql_client.execute_many(sql_scripts)
        self._update_schema_in_storage(self.schema)
        return schema_update

    def _build_schema_update_sql(
        self, storage_tables: Iterable[Tuple[str, TTableSchemaColumns]]
    ) -> Tuple[List[str], TSchemaTables]:
        """Generates CREATE/ALTER sql for tables that differ between the destination and in the client's Schema.

        This method compares schema tables to the respective tables in the destination passed in `storage_tables`
        It detects only new tables and new columns. Any other changes like data types, hints, etc. are ignored.

        Args:
            storage_tables (Iterable[Tuple[str, TTableSchemaColumns]]): list of storage tables (tuples (name, column schema))

        Returns:
            Tuple[List[str], TSchemaTables]: Tuple with a list of CREATE/ALTER scripts, and a list of all tables with columns that will be added.
        """
        sql_updates = []
        post_sql_updates = []
        schema_update: TSchemaTables = {}
        for table_name, storage_columns in storage_tables:
            # this will skip incomplete columns
            new_columns = self._create_table_update(table_name, storage_columns)
            generate_alter = len(storage_columns) > 0
            if len(new_columns) > 0:
                # build and add sql to execute
                self._check_table_update_hints(table_name, new_columns, generate_alter)
                sql_statements = self._get_table_update_sql(table_name, new_columns, generate_alter)
                for sql in sql_statements:
                    sql_updates.append(sql)
                # create a schema update for particular table
                partial_table = copy(self.prepare_load_table(table_name))
                # keep only new columns
                partial_table["columns"] = {c["name"]: c for c in new_columns}
                schema_update[table_name] = partial_table
                post_sql_statements = self._get_table_post_update_sql(partial_table)
                for sql in post_sql_statements:
                    post_sql_updates.append(sql)

        # add post sql updates at the end
        sql_updates.extend(post_sql_updates)

        return sql_updates, schema_update

    def _make_add_column_sql(
        self, new_columns: Sequence[TColumnSchema], table: PreparedTableSchema = None
    ) -> List[str]:
        """Make one or more ADD COLUMN sql clauses to be joined in ALTER TABLE statement(s)"""
        return [f"ADD COLUMN {self._get_column_def_sql(c, table)}" for c in new_columns]

    def _make_create_table(self, qualified_name: str, table: PreparedTableSchema) -> str:
        """Begins CREATE TABLE statement"""
        not_exists_clause = " "
        if (
            table["name"] in self.schema.dlt_table_names()
            and self.capabilities.supports_create_table_if_not_exists
        ):
            not_exists_clause = " IF NOT EXISTS "
        return f"CREATE TABLE{not_exists_clause}{qualified_name}"

    def _get_table_update_sql(
        self, table_name: str, new_columns: Sequence[TColumnSchema], generate_alter: bool
    ) -> List[str]:
        """Generates a list of SQL statements that updates table `table_name` to include `new_columns`
        columns. `generate_alter` is set to True if table already exists in destination.
        """
        qualified_name = self.sql_client.make_qualified_table_name(table_name)
        table = self.prepare_load_table(table_name)
        sql_result: List[str] = []
        if not generate_alter:
            # build CREATE
            sql = self._make_create_table(qualified_name, table) + " (\n"
            sql += ",\n".join([self._get_column_def_sql(c, table) for c in new_columns])
            sql += self._get_constraints_sql(table_name, new_columns, generate_alter)
            sql += ")"
            sql_result.append(sql)
        else:
            sql_base = f"ALTER TABLE {qualified_name}\n"
            add_column_statements = self._make_add_column_sql(new_columns, table)
            if self.capabilities.alter_add_multi_column:
                column_sql = ",\n"
                sql_result.append(sql_base + column_sql.join(add_column_statements))
            else:
                # build ALTER as a separate statement for each column (redshift limitation)
                sql_result.extend(
                    [sql_base + col_statement for col_statement in add_column_statements]
                )
            constraints_sql = self._get_constraints_sql(table_name, new_columns, generate_alter)
            if constraints_sql:
                sql_result.append(constraints_sql)
        return sql_result

    def _get_constraints_sql(
        self, table_name: str, new_columns: Sequence[TColumnSchema], generate_alter: bool
    ) -> str:
        """Creates or alters additional constraints that are not inlined into CREATE/ALTER TABLE statements"""
        return ""

    def _get_table_post_update_sql(self, partial_table: TPartialTableSchema) -> List[str]:
        """Generates SQL statements executed after all tables are migrated i.e. containing foreign reference.
        `partial_table` contains all table hints and new columns with their hints.
        """
        return []

    def _check_table_update_hints(
        self, table_name: str, new_columns: Sequence[TColumnSchema], generate_alter: bool
    ) -> None:
        # scan columns to get hints
        if generate_alter:
            # no hints may be specified on added columns
            for hint in COLUMN_HINTS:
                if any(not has_default_column_prop_value(hint, c.get(hint)) for c in new_columns):
                    hint_columns = [
                        self.sql_client.escape_column_name(c["name"])
                        for c in new_columns
                        if not has_default_column_prop_value(hint, c.get(hint))
                    ]
                    # some hints are not materialized in destination or may be materialized in alter
                    # TODO: add this information to TColumnPropInfo
                    if hint in [
                        "hard_delete",
                        "dedup_sort",
                        "merge_key",
                        "variant",
                        "row_key",
                        "parent_key",
                        "root_key",
                    ]:
                        # you may add those
                        pass
                    elif hint == "nullable":
                        logger.warning(
                            f"Column(s) {hint_columns} with NOT NULL are being added to existing"
                            f" table {table_name}. If there's data in the table the operation"
                            " will fail."
                        )
                    else:
                        logger.warning(
                            f"Column(s) {hint_columns} with hint {hint} are being added to existing"
                            f" table {table_name}. Several hint types may not be added to"
                            " existing tables."
                        )

    def _get_column_def_sql(self, c: TColumnSchema, table: PreparedTableSchema = None) -> str:
        hints_ = self._get_column_hints_sql(c)
        column_name = self.sql_client.escape_column_name(c["name"])
        nullability = self._gen_not_null(c.get("nullable", True))
        column_type = self.type_mapper.to_destination_type(c, table)

        return f"{column_name} {column_type} {hints_} {nullability}"

    def _get_column_hints_sql(self, c: TColumnSchema) -> str:
        return " ".join(
            self.active_hints.get(h, "")
            for h in self.active_hints.keys()
            if c.get(h, False) is True  # use ColumnPropInfos to get default value
        )

    def _gen_not_null(self, nullable: bool) -> str:
        return "NOT NULL" if not nullable else ""

    def _create_table_update(
        self, table_name: str, storage_columns: TTableSchemaColumns
    ) -> Sequence[TColumnSchema]:
        """Compares storage columns with schema table and produce delta columns difference"""
        updates = self.schema.get_new_table_columns(
            table_name,
            storage_columns,
            case_sensitive=self.capabilities.generates_case_sensitive_identifiers(),
        )
        logger.info(f"Found {len(updates)} updates for {table_name} in {self.schema.name}")
        return updates

    def _row_to_schema_info(self, query: str, *args: Any) -> StorageSchemaInfo:
        row: Tuple[Any, ...] = None
        # if there's no dataset/schema return none info
        with contextlib.suppress(DatabaseUndefinedRelation):
            with self.sql_client.execute_query(query, *args) as cur:
                row = cur.fetchone()
        if not row:
            return None

        # get schema as string
        # TODO: Re-use decompress/compress_state() implementation from dlt.pipeline.state_sync
        schema_str: str = row[5]
        try:
            schema_bytes = base64.b64decode(schema_str, validate=True)
            schema_str = zlib.decompress(schema_bytes).decode("utf-8")
        except ValueError:
            # not a base64 string
            pass

        # make utc datetime
        inserted_at = pendulum.instance(row[2])

        return StorageSchemaInfo(row[4], row[3], row[0], row[1], inserted_at, schema_str)

    def _delete_schema_in_storage(self, schema: Schema) -> None:
        """
        Delete all stored versions with the same name as given schema.
        Fails silently if versions table does not exist
        """
        name = self.sql_client.make_qualified_table_name(self.schema.version_table_name)
        (c_schema_name,) = self._norm_and_escape_columns("schema_name")
        self.sql_client.execute_sql(f"DELETE FROM {name} WHERE {c_schema_name} = %s", schema.name)

    def _update_schema_in_storage(self, schema: Schema) -> None:
        # get schema string or zip
        schema_str = json.dumps(schema.to_dict())
        # TODO: not all databases store data as utf-8 but this exception is mostly for redshift
        schema_bytes = schema_str.encode("utf-8")
        if len(schema_bytes) > self.capabilities.max_text_data_type_length:
            # compress and to base64
            schema_str = base64.b64encode(zlib.compress(schema_bytes, level=9)).decode("ascii")
        self._commit_schema_update(schema, schema_str)

    def _commit_schema_update(self, schema: Schema, schema_str: str) -> None:
        now_ts = pendulum.now()
        name = self.sql_client.make_qualified_table_name(self.schema.version_table_name)
        # values =  schema.version_hash, schema.name, schema.version, schema.ENGINE_VERSION, str(now_ts), schema_str
        self.sql_client.execute_sql(
            f"INSERT INTO {name}({self.version_table_schema_columns}) VALUES (%s, %s, %s, %s, %s,"
            " %s)",
            schema.version,
            schema.ENGINE_VERSION,
            now_ts,
            schema.name,
            schema.stored_version_hash,
            schema_str,
        )

    def verify_schema(
        self, only_tables: Iterable[str] = None, new_jobs: Iterable[ParsedLoadJobFileName] = None
    ) -> List[PreparedTableSchema]:
        loaded_tables = super().verify_schema(only_tables, new_jobs)
        if exceptions := verify_schema_merge_disposition(
            self.schema, loaded_tables, self.capabilities, warnings=True
        ):
            for exception in exceptions:
                logger.error(str(exception))
            raise exceptions[0]
        if exceptions := verify_schema_replace_disposition(
            self.schema,
            loaded_tables,
            self.capabilities,
            self.config.replace_strategy,
            warnings=True,
        ):
            for exception in exceptions:
                logger.error(str(exception))
            raise exceptions[0]
        return loaded_tables

    def prepare_load_job_execution(self, job: RunnableLoadJob) -> None:
        self._set_query_tags_for_job(load_id=job._load_id, table=job._load_table)

    def _set_query_tags_for_job(self, load_id: str, table: PreparedTableSchema) -> None:
        """Sets query tags in sql_client for a job in package `load_id`, starting for a particular `table`"""
        from dlt.common.pipeline import current_pipeline

        pipeline = current_pipeline()
        pipeline_name = pipeline.pipeline_name if pipeline else ""
        self.sql_client.set_query_tags(
            {
                "source": self.schema.name,
                "resource": (
                    get_inherited_table_hint(
                        self.schema.tables, table["name"], "resource", allow_none=True
                    )
                    or ""
                ),
                "table": table["name"],
                "load_id": load_id,
                "pipeline_name": pipeline_name,
            }
        )


class SqlJobClientWithStagingDataset(SqlJobClientBase, WithStagingDataset):
    in_staging_dataset_mode: bool = False

    @contextlib.contextmanager
    def with_staging_dataset(self) -> Iterator["SqlJobClientBase"]:
        try:
            with self.sql_client.with_staging_dataset():
                self.in_staging_dataset_mode = True
                yield self
        finally:
            self.in_staging_dataset_mode = False

    def should_load_data_to_staging_dataset(self, table_name: str) -> bool:
        table = self.prepare_load_table(table_name)
        if table["write_disposition"] == "merge":
            return True
        elif table["write_disposition"] == "replace" and (
            table["x-replace-strategy"] in ["insert-from-staging", "staging-optimized"]  # type: ignore[typeddict-item]
        ):
            return True
        return False
