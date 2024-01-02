import os
from abc import abstractmethod
import base64
import binascii
import contextlib
from copy import copy
import datetime  # noqa: 251
from types import TracebackType
from typing import (
    Any,
    ClassVar,
    List,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
    Type,
    Iterable,
    Iterator,
    ContextManager,
    cast,
)
import zlib
import re

from dlt.common import json, pendulum, logger
from dlt.common.data_types import TDataType
from dlt.common.schema.typing import (
    COLUMN_HINTS,
    TColumnType,
    TColumnSchemaBase,
    TTableSchema,
    TWriteDisposition,
    TTableFormat,
)
from dlt.common.storages import FileStorage
from dlt.common.schema import TColumnSchema, Schema, TTableSchemaColumns, TSchemaTables
from dlt.common.destination.reference import (
    StateInfo,
    StorageSchemaInfo,
    WithStateSync,
    DestinationClientConfiguration,
    DestinationClientDwhConfiguration,
    DestinationClientDwhWithStagingConfiguration,
    NewLoadJob,
    WithStagingDataset,
    TLoadJobState,
    LoadJob,
    JobClientBase,
    FollowupJob,
    CredentialsConfiguration,
)
from dlt.common.utils import concat_strings_with_limit
from dlt.destinations.exceptions import (
    DatabaseUndefinedRelation,
    DestinationSchemaTampered,
    DestinationSchemaWillNotUpdate,
)
from dlt.destinations.job_impl import EmptyLoadJobWithoutFollowup, NewReferenceJob
from dlt.destinations.sql_jobs import SqlMergeJob, SqlStagingCopyJob
from dlt.common.schema.typing import LOADS_TABLE_NAME, VERSION_TABLE_NAME

from dlt.destinations.typing import TNativeConn
from dlt.destinations.sql_client import SqlClientBase

# this should suffice for now
DDL_COMMANDS = ["ALTER", "CREATE", "DROP"]


class SqlLoadJob(LoadJob):
    """A job executing sql statement, without followup trait"""

    def __init__(self, file_path: str, sql_client: SqlClientBase[Any]) -> None:
        super().__init__(FileStorage.get_file_name_from_file_path(file_path))
        # execute immediately if client present
        with FileStorage.open_zipsafe_ro(file_path, "r", encoding="utf-8") as f:
            sql = f.read()

        # if we detect ddl transactions, only execute transaction if supported by client
        if (
            not self._string_containts_ddl_queries(sql)
            or sql_client.capabilities.supports_ddl_transactions
        ):
            # with sql_client.begin_transaction():
            sql_client.execute_sql(sql)
        else:
            sql_client.execute_sql(sql)

    def state(self) -> TLoadJobState:
        # this job is always done
        return "completed"

    def exception(self) -> str:
        # this part of code should be never reached
        raise NotImplementedError()

    def _string_containts_ddl_queries(self, sql: str) -> bool:
        for cmd in DDL_COMMANDS:
            if re.search(cmd, sql, re.IGNORECASE):
                return True
        return False

    @staticmethod
    def is_sql_job(file_path: str) -> bool:
        return os.path.splitext(file_path)[1][1:] == "sql"


class CopyRemoteFileLoadJob(LoadJob, FollowupJob):
    def __init__(
        self,
        table: TTableSchema,
        file_path: str,
        sql_client: SqlClientBase[Any],
        staging_credentials: Optional[CredentialsConfiguration] = None,
    ) -> None:
        super().__init__(FileStorage.get_file_name_from_file_path(file_path))
        self._sql_client = sql_client
        self._staging_credentials = staging_credentials

        self.execute(table, NewReferenceJob.resolve_reference(file_path))

    def execute(self, table: TTableSchema, bucket_path: str) -> None:
        # implement in child implementations
        raise NotImplementedError()

    def state(self) -> TLoadJobState:
        # this job is always done
        return "completed"


class SqlJobClientBase(JobClientBase, WithStateSync):
    _VERSION_TABLE_SCHEMA_COLUMNS: ClassVar[Tuple[str, ...]] = (
        "version_hash",
        "schema_name",
        "version",
        "engine_version",
        "inserted_at",
        "schema",
    )
    _STATE_TABLE_COLUMNS: ClassVar[Tuple[str, ...]] = (
        "version",
        "engine_version",
        "pipeline_name",
        "state",
        "created_at",
        "_dlt_load_id",
    )

    def __init__(
        self,
        schema: Schema,
        config: DestinationClientConfiguration,
        sql_client: SqlClientBase[TNativeConn],
    ) -> None:
        self.version_table_schema_columns = ", ".join(
            sql_client.escape_column_name(col) for col in self._VERSION_TABLE_SCHEMA_COLUMNS
        )
        self.state_table_columns = ", ".join(
            sql_client.escape_column_name(col) for col in self._STATE_TABLE_COLUMNS
        )

        super().__init__(schema, config)
        self.sql_client = sql_client
        assert isinstance(config, DestinationClientDwhConfiguration)
        self.config: DestinationClientDwhConfiguration = config

    def drop_storage(self) -> None:
        self.sql_client.drop_dataset()

    def initialize_storage(self, truncate_tables: Iterable[str] = None) -> None:
        if not self.is_storage_initialized():
            self.sql_client.create_dataset()
        else:
            # truncate requested tables
            if truncate_tables:
                self.sql_client.truncate_tables(*truncate_tables)

    def is_storage_initialized(self) -> bool:
        return self.sql_client.has_dataset()

    def update_stored_schema(
        self, only_tables: Iterable[str] = None, expected_update: TSchemaTables = None
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

    def drop_tables(self, *tables: str, replace_schema: bool = True) -> None:
        with self.maybe_ddl_transaction():
            self.sql_client.drop_tables(*tables)
            if replace_schema:
                self._replace_schema_in_storage(self.schema)

    @contextlib.contextmanager
    def maybe_ddl_transaction(self) -> Iterator[None]:
        """Begins a transaction if sql client supports it, otherwise works in auto commit"""
        if self.capabilities.supports_ddl_transactions:
            with self.sql_client.begin_transaction():
                yield
        else:
            yield

    def should_truncate_table_before_load(self, table: TTableSchema) -> bool:
        return (
            table["write_disposition"] == "replace"
            and self.config.replace_strategy == "truncate-and-insert"
        )

    def _create_append_followup_jobs(self, table_chain: Sequence[TTableSchema]) -> List[NewLoadJob]:
        return []

    def _create_merge_followup_jobs(self, table_chain: Sequence[TTableSchema]) -> List[NewLoadJob]:
        return [SqlMergeJob.from_table_chain(table_chain, self.sql_client)]

    def _create_replace_followup_jobs(
        self, table_chain: Sequence[TTableSchema]
    ) -> List[NewLoadJob]:
        jobs: List[NewLoadJob] = []
        if self.config.replace_strategy in ["insert-from-staging", "staging-optimized"]:
            jobs.append(
                SqlStagingCopyJob.from_table_chain(table_chain, self.sql_client, {"replace": True})
            )
        return jobs

    def create_table_chain_completed_followup_jobs(
        self, table_chain: Sequence[TTableSchema]
    ) -> List[NewLoadJob]:
        """Creates a list of followup jobs for merge write disposition and staging replace strategies"""
        jobs = super().create_table_chain_completed_followup_jobs(table_chain)
        write_disposition = table_chain[0]["write_disposition"]
        if write_disposition == "append":
            jobs.extend(self._create_append_followup_jobs(table_chain))
        elif write_disposition == "merge":
            jobs.extend(self._create_merge_followup_jobs(table_chain))
        elif write_disposition == "replace":
            jobs.extend(self._create_replace_followup_jobs(table_chain))
        return jobs

    def start_file_load(self, table: TTableSchema, file_path: str, load_id: str) -> LoadJob:
        """Starts SqlLoadJob for files ending with .sql or returns None to let derived classes to handle their specific jobs"""
        if SqlLoadJob.is_sql_job(file_path):
            # execute sql load job
            return SqlLoadJob(file_path, self.sql_client)
        return None

    def restore_file_load(self, file_path: str) -> LoadJob:
        """Returns a completed SqlLoadJob or None to let derived classes to handle their specific jobs

        Returns completed jobs as SqlLoadJob is executed atomically in start_file_load so any jobs that should be recreated are already completed.
        Obviously the case of asking for jobs that were never created will not be handled. With correctly implemented loader that cannot happen.

        Args:
            file_path (str): a path to a job file

        Returns:
            LoadJob: A restored job or none
        """
        if SqlLoadJob.is_sql_job(file_path):
            return EmptyLoadJobWithoutFollowup.from_file_path(file_path, "completed")
        return None

    def complete_load(self, load_id: str) -> None:
        name = self.sql_client.make_qualified_table_name(self.schema.loads_table_name)
        now_ts = pendulum.now()
        self.sql_client.execute_sql(
            f"INSERT INTO {name}(load_id, schema_name, status, inserted_at, schema_version_hash)"
            " VALUES(%s, %s, %s, %s, %s);",
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

    def get_storage_table(self, table_name: str) -> Tuple[bool, TTableSchemaColumns]:
        def _null_to_bool(v: str) -> bool:
            if v == "NO":
                return False
            elif v == "YES":
                return True
            raise ValueError(v)

        fields = ["column_name", "data_type", "is_nullable"]
        if self.capabilities.schema_supports_numeric_precision:
            fields += ["numeric_precision", "numeric_scale"]
        db_params = self.sql_client.make_qualified_table_name(table_name, escape=False).split(
            ".", 3
        )
        query = f"""
SELECT {",".join(fields)}
    FROM INFORMATION_SCHEMA.COLUMNS
WHERE """
        if len(db_params) == 3:
            query += "table_catalog = %s AND "
        query += "table_schema = %s AND table_name = %s ORDER BY ordinal_position;"
        rows = self.sql_client.execute_sql(query, *db_params)

        # if no rows we assume that table does not exist
        schema_table: TTableSchemaColumns = {}
        if len(rows) == 0:
            # TODO: additionally check if table exists
            return False, schema_table
        # TODO: pull more data to infer indexes, PK and uniques attributes/constraints
        for c in rows:
            numeric_precision = (
                c[3] if self.capabilities.schema_supports_numeric_precision else None
            )
            numeric_scale = c[4] if self.capabilities.schema_supports_numeric_precision else None
            schema_c: TColumnSchemaBase = {
                "name": c[0],
                "nullable": _null_to_bool(c[2]),
                **self._from_db_type(c[1], numeric_precision, numeric_scale),
            }
            schema_table[c[0]] = schema_c  # type: ignore
        return True, schema_table

    @abstractmethod
    def _from_db_type(
        self, db_type: str, precision: Optional[int], scale: Optional[int]
    ) -> TColumnType:
        pass

    def get_stored_schema(self) -> StorageSchemaInfo:
        name = self.sql_client.make_qualified_table_name(self.schema.version_table_name)
        query = (
            f"SELECT {self.version_table_schema_columns} FROM {name} WHERE schema_name = %s ORDER"
            " BY inserted_at DESC;"
        )
        return self._row_to_schema_info(query, self.schema.name)

    def get_stored_state(self, pipeline_name: str) -> StateInfo:
        state_table = self.sql_client.make_qualified_table_name(self.schema.state_table_name)
        loads_table = self.sql_client.make_qualified_table_name(self.schema.loads_table_name)
        query = (
            f"SELECT {self.state_table_columns} FROM {state_table} AS s JOIN {loads_table} AS l ON"
            " l.load_id = s._dlt_load_id WHERE pipeline_name = %s AND l.status = 0 ORDER BY"
            " created_at DESC"
        )
        with self.sql_client.execute_query(query, pipeline_name) as cur:
            row = cur.fetchone()
        if not row:
            return None
        return StateInfo(row[0], row[1], row[2], row[3], pendulum.instance(row[4]))

    # def get_stored_states(self, state_table: str) -> List[StateInfo]:
    #     """Loads list of compressed states from destination storage, optionally filtered by pipeline name"""
    #     query = f"SELECT {self.STATE_TABLE_COLUMNS} FROM {state_table} AS s ORDER BY created_at DESC"
    #     result: List[StateInfo] = []
    #     with self.sql_client.execute_query(query) as cur:
    #         for row in cur.fetchall():
    #             result.append(StateInfo(row[0], row[1], row[2], row[3], pendulum.instance(row[4])))
    #     return result

    def get_stored_schema_by_hash(self, version_hash: str) -> StorageSchemaInfo:
        name = self.sql_client.make_qualified_table_name(self.schema.version_table_name)
        query = f"SELECT {self.version_table_schema_columns} FROM {name} WHERE version_hash = %s;"
        return self._row_to_schema_info(query, version_hash)

    def _execute_schema_update_sql(self, only_tables: Iterable[str]) -> TSchemaTables:
        sql_scripts, schema_update = self._build_schema_update_sql(only_tables)
        # stay within max query size when doing DDL. some db backends use bytes not characters so decrease limit by half
        # assuming that most of the characters in DDL encode into single bytes
        for sql_fragment in concat_strings_with_limit(
            sql_scripts, "\n", self.capabilities.max_query_length // 2
        ):
            self.sql_client.execute_sql(sql_fragment)
        self._update_schema_in_storage(self.schema)
        return schema_update

    def _build_schema_update_sql(
        self, only_tables: Iterable[str]
    ) -> Tuple[List[str], TSchemaTables]:
        """Generates CREATE/ALTER sql for tables that differ between the destination and in client's Schema.

        This method compares all or `only_tables` defined in self.schema to the respective tables in the destination. It detects only new tables and new columns.
        Any other changes like data types, hints etc. are ignored.

        Args:
            only_tables (Iterable[str]): Only `only_tables` are included, or all if None.

        Returns:
            Tuple[List[str], TSchemaTables]: Tuple with a list of CREATE/ALTER scripts and a list of all tables with columns that will be added.
        """
        sql_updates = []
        schema_update: TSchemaTables = {}
        for table_name in only_tables or self.schema.tables:
            exists, storage_table = self.get_storage_table(table_name)
            new_columns = self._create_table_update(table_name, storage_table)
            if len(new_columns) > 0:
                # build and add sql to execute
                sql_statements = self._get_table_update_sql(table_name, new_columns, exists)
                for sql in sql_statements:
                    if not sql.endswith(";"):
                        sql += ";"
                    sql_updates.append(sql)
                # create a schema update for particular table
                partial_table = copy(self.get_load_table(table_name))
                # keep only new columns
                partial_table["columns"] = {c["name"]: c for c in new_columns}
                schema_update[table_name] = partial_table

        return sql_updates, schema_update

    def _make_add_column_sql(
        self, new_columns: Sequence[TColumnSchema], table_format: TTableFormat = None
    ) -> List[str]:
        """Make one or more  ADD COLUMN sql clauses to be joined in ALTER TABLE statement(s)"""
        return [f"ADD COLUMN {self._get_column_def_sql(c, table_format)}" for c in new_columns]

    def _get_table_update_sql(
        self, table_name: str, new_columns: Sequence[TColumnSchema], generate_alter: bool
    ) -> List[str]:
        # build sql
        canonical_name = self.sql_client.make_qualified_table_name(table_name)
        table = self.get_load_table(table_name)
        table_format = table.get("table_format") if table else None
        sql_result: List[str] = []
        if not generate_alter:
            # build CREATE
            sql = f"CREATE TABLE {canonical_name} (\n"
            sql += ",\n".join([self._get_column_def_sql(c, table_format) for c in new_columns])
            sql += ")"
            sql_result.append(sql)
        else:
            sql_base = f"ALTER TABLE {canonical_name}\n"
            add_column_statements = self._make_add_column_sql(new_columns, table_format)
            if self.capabilities.alter_add_multi_column:
                column_sql = ",\n"
                sql_result.append(sql_base + column_sql.join(add_column_statements))
            else:
                # build ALTER as separate statement for each column (redshift limitation)
                sql_result.extend(
                    [sql_base + col_statement for col_statement in add_column_statements]
                )

        # scan columns to get hints
        if generate_alter:
            # no hints may be specified on added columns
            for hint in COLUMN_HINTS:
                if any(c.get(hint, False) is True for c in new_columns):
                    hint_columns = [
                        self.capabilities.escape_identifier(c["name"])
                        for c in new_columns
                        if c.get(hint, False)
                    ]
                    if hint == "not_null":
                        logger.warning(
                            f"Column(s) {hint_columns} with NOT NULL are being added to existing"
                            f" table {canonical_name}. If there's data in the table the operation"
                            " will fail."
                        )
                    else:
                        logger.warning(
                            f"Column(s) {hint_columns} with hint {hint} are being added to existing"
                            f" table {canonical_name}. Several hint types may not be added to"
                            " existing tables."
                        )
        return sql_result

    @abstractmethod
    def _get_column_def_sql(self, c: TColumnSchema, table_format: TTableFormat = None) -> str:
        pass

    @staticmethod
    def _gen_not_null(v: bool) -> str:
        return "NOT NULL" if not v else ""

    def _create_table_update(
        self, table_name: str, storage_columns: TTableSchemaColumns
    ) -> Sequence[TColumnSchema]:
        # compare table with stored schema and produce delta
        updates = self.schema.get_new_table_columns(table_name, storage_columns)
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
        inserted_at = pendulum.instance(row[4])

        return StorageSchemaInfo(row[0], row[1], row[2], row[3], inserted_at, schema_str)

    def _replace_schema_in_storage(self, schema: Schema) -> None:
        """
        Save the given schema in storage and remove all previous versions with the same name
        """
        name = self.sql_client.make_qualified_table_name(self.schema.version_table_name)
        self.sql_client.execute_sql(f"DELETE FROM {name} WHERE schema_name = %s;", schema.name)
        self._update_schema_in_storage(schema)

    def _update_schema_in_storage(self, schema: Schema) -> None:
        # make sure that schema being saved was not modified from the moment it was loaded from storage
        version_hash = schema.version_hash
        if version_hash != schema.stored_version_hash:
            raise DestinationSchemaTampered(schema.name, version_hash, schema.stored_version_hash)
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
            " %s);",
            schema.stored_version_hash,
            schema.name,
            schema.version,
            schema.ENGINE_VERSION,
            now_ts,
            schema_str,
        )


class SqlJobClientWithStaging(SqlJobClientBase, WithStagingDataset):
    in_staging_mode: bool = False

    @contextlib.contextmanager
    def with_staging_dataset(self) -> Iterator["SqlJobClientBase"]:
        try:
            with self.sql_client.with_staging_dataset(True):
                self.in_staging_mode = True
                yield self
        finally:
            self.in_staging_mode = False

    def should_load_data_to_staging_dataset(self, table: TTableSchema) -> bool:
        if table["write_disposition"] == "merge":
            return True
        elif table["write_disposition"] == "replace" and (
            self.config.replace_strategy in ["insert-from-staging", "staging-optimized"]
        ):
            return True
        return False
