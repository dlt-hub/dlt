import os
from abc import abstractmethod
import base64
import contextlib
from copy import copy
from types import TracebackType
from typing import (
    Any,
    ClassVar,
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

from dlt.common import pendulum, logger
from dlt.common.json import json
from dlt.common.schema.typing import (
    COLUMN_HINTS,
    TColumnType,
    TColumnSchemaBase,
    TTableSchema,
    TTableFormat,
)
from dlt.common.schema.utils import (
    get_inherited_table_hint,
    loads_table,
    normalize_table_identifiers,
    version_table,
)
from dlt.common.storages import FileStorage
from dlt.common.storages.load_package import LoadJobInfo
from dlt.common.schema import TColumnSchema, Schema, TTableSchemaColumns, TSchemaTables
from dlt.common.destination.reference import (
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
from dlt.destinations.sql_jobs import SqlMergeFollowupJob, SqlStagingCopyFollowupJob
from dlt.destinations.typing import TNativeConn
from dlt.destinations.sql_client import SqlClientBase
from dlt.destinations.utils import (
    get_pipeline_state_query_columns,
    info_schema_null_to_bool,
    verify_sql_job_client_schema,
)

# this should suffice for now
DDL_COMMANDS = ["ALTER", "CREATE", "DROP"]


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

        # Some clients (e.g. databricks) do not support multiple statements in one execute call
        if not self._sql_client.capabilities.supports_multiple_statements:
            self._sql_client.execute_many(self._split_fragments(sql))
        # if we detect ddl transactions, only execute transaction if supported by client
        elif (
            not self._string_contains_ddl_queries(sql)
            or self._sql_client.capabilities.supports_ddl_transactions
        ):
            # with sql_client.begin_transaction():
            self._sql_client.execute_sql(sql)
        else:
            # sql_client.execute_sql(sql)
            self._sql_client.execute_many(self._split_fragments(sql))

    def _string_contains_ddl_queries(self, sql: str) -> bool:
        for cmd in DDL_COMMANDS:
            if re.search(cmd, sql, re.IGNORECASE):
                return True
        return False

    def _split_fragments(self, sql: str) -> List[str]:
        return [s + (";" if not s.endswith(";") else "") for s in sql.split(";") if s.strip()]

    @staticmethod
    def is_sql_job(file_path: str) -> bool:
        return os.path.splitext(file_path)[1][1:] == "sql"


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


class SqlJobClientBase(JobClientBase, WithStateSync):
    INFO_TABLES_QUERY_THRESHOLD: ClassVar[int] = 1000
    """Fallback to querying all tables in the information schema if checking more than threshold"""

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
        super().__init__(schema, config, sql_client.capabilities)
        self.sql_client = sql_client
        assert isinstance(config, DestinationClientDwhConfiguration)
        self.config: DestinationClientDwhConfiguration = config

    def drop_storage(self) -> None:
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

    def should_truncate_table_before_load(self, table: TTableSchema) -> bool:
        return (
            table["write_disposition"] == "replace"
            and self.config.replace_strategy == "truncate-and-insert"
        )

    def _create_append_followup_jobs(
        self, table_chain: Sequence[TTableSchema]
    ) -> List[FollowupJobRequest]:
        return []

    def _create_merge_followup_jobs(
        self, table_chain: Sequence[TTableSchema]
    ) -> List[FollowupJobRequest]:
        return [SqlMergeFollowupJob.from_table_chain(table_chain, self.sql_client)]

    def _create_replace_followup_jobs(
        self, table_chain: Sequence[TTableSchema]
    ) -> List[FollowupJobRequest]:
        jobs: List[FollowupJobRequest] = []
        if self.config.replace_strategy in ["insert-from-staging", "staging-optimized"]:
            jobs.append(
                SqlStagingCopyFollowupJob.from_table_chain(
                    table_chain, self.sql_client, {"replace": True}
                )
            )
        return jobs

    def create_table_chain_completed_followup_jobs(
        self,
        table_chain: Sequence[TTableSchema],
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
        self, table: TTableSchema, file_path: str, load_id: str, restore: bool = False
    ) -> LoadJob:
        """Starts SqlLoadJob for files ending with .sql or returns None to let derived classes to handle their specific jobs"""
        if SqlLoadJob.is_sql_job(file_path):
            # create sql load job
            return SqlLoadJob(file_path)
        return None

    def complete_load(self, load_id: str) -> None:
        name = self.sql_client.make_qualified_table_name(self.schema.loads_table_name)
        now_ts = pendulum.now()
        self.sql_client.execute_sql(
            f"INSERT INTO {name}({self.loads_table_schema_columns}) VALUES(%s, %s, %s, %s, %s);",
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
            len(name_lookup) > self.INFO_TABLES_QUERY_THRESHOLD
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

    def get_stored_schema(self) -> StorageSchemaInfo:
        name = self.sql_client.make_qualified_table_name(self.schema.version_table_name)
        c_schema_name, c_inserted_at = self._norm_and_escape_columns("schema_name", "inserted_at")
        query = (
            f"SELECT {self.version_table_schema_columns} FROM {name} WHERE {c_schema_name} = %s"
            f" ORDER BY {c_inserted_at} DESC;"
        )
        return self._row_to_schema_info(query, self.schema.name)

    def get_stored_state(self, pipeline_name: str) -> StateInfo:
        state_table = self.sql_client.make_qualified_table_name(self.schema.state_table_name)
        loads_table = self.sql_client.make_qualified_table_name(self.schema.loads_table_name)
        c_load_id, c_dlt_load_id, c_pipeline_name, c_status = self._norm_and_escape_columns(
            "load_id", "_dlt_load_id", "pipeline_name", "status"
        )
        query = (
            f"SELECT {self.state_table_columns} FROM {state_table} AS s JOIN {loads_table} AS l ON"
            f" l.{c_load_id} = s.{c_dlt_load_id} WHERE {c_pipeline_name} = %s AND l.{c_status} = 0"
            f" ORDER BY {c_load_id} DESC"
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
        query = (
            f"SELECT {self.version_table_schema_columns} FROM {table_name} WHERE"
            f" {c_version_hash} = %s;"
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
        query += f"table_schema = %s {select_tables_clause} ORDER BY table_name, ordinal_position;"

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
        sql_scripts, schema_update = self._build_schema_update_sql(only_tables)
        # Stay within max query size when doing DDL.
        # Some DB backends use bytes not characters, so decrease the limit by half,
        # assuming most of the characters in DDL encoded into single bytes.
        self.sql_client.execute_many(sql_scripts)
        self._update_schema_in_storage(self.schema)
        return schema_update

    def _build_schema_update_sql(
        self, only_tables: Iterable[str]
    ) -> Tuple[List[str], TSchemaTables]:
        """Generates CREATE/ALTER sql for tables that differ between the destination and in the client's Schema.

        This method compares all or `only_tables` defined in `self.schema` to the respective tables in the destination.
        It detects only new tables and new columns.
        Any other changes like data types, hints, etc. are ignored.

        Args:
            only_tables (Iterable[str]): Only `only_tables` are included, or all if None.

        Returns:
            Tuple[List[str], TSchemaTables]: Tuple with a list of CREATE/ALTER scripts, and a list of all tables with columns that will be added.
        """
        sql_updates = []
        schema_update: TSchemaTables = {}
        for table_name, storage_columns in self.get_storage_tables(
            only_tables or self.schema.tables.keys()
        ):
            # this will skip incomplete columns
            new_columns = self._create_table_update(table_name, storage_columns)
            if len(new_columns) > 0:
                # build and add sql to execute
                sql_statements = self._get_table_update_sql(
                    table_name, new_columns, len(storage_columns) > 0
                )
                for sql in sql_statements:
                    if not sql.endswith(";"):
                        sql += ";"
                    sql_updates.append(sql)
                # create a schema update for particular table
                partial_table = copy(self.prepare_load_table(table_name))
                # keep only new columns
                partial_table["columns"] = {c["name"]: c for c in new_columns}
                schema_update[table_name] = partial_table

        return sql_updates, schema_update

    def _make_add_column_sql(
        self, new_columns: Sequence[TColumnSchema], table_format: TTableFormat = None
    ) -> List[str]:
        """Make one or more ADD COLUMN sql clauses to be joined in ALTER TABLE statement(s)"""
        return [f"ADD COLUMN {self._get_column_def_sql(c, table_format)}" for c in new_columns]

    def _make_create_table(self, qualified_name: str, table: TTableSchema) -> str:
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
        # build sql
        qualified_name = self.sql_client.make_qualified_table_name(table_name)
        table = self.prepare_load_table(table_name)
        table_format = table.get("table_format")
        sql_result: List[str] = []
        if not generate_alter:
            # build CREATE
            sql = self._make_create_table(qualified_name, table) + " (\n"
            sql += ",\n".join([self._get_column_def_sql(c, table_format) for c in new_columns])
            sql += ")"
            sql_result.append(sql)
        else:
            sql_base = f"ALTER TABLE {qualified_name}\n"
            add_column_statements = self._make_add_column_sql(new_columns, table_format)
            if self.capabilities.alter_add_multi_column:
                column_sql = ",\n"
                sql_result.append(sql_base + column_sql.join(add_column_statements))
            else:
                # build ALTER as a separate statement for each column (redshift limitation)
                sql_result.extend(
                    [sql_base + col_statement for col_statement in add_column_statements]
                )

        # scan columns to get hints
        if generate_alter:
            # no hints may be specified on added columns
            for hint in COLUMN_HINTS:
                if any(c.get(hint, False) is True for c in new_columns):
                    hint_columns = [
                        self.sql_client.escape_column_name(c["name"])
                        for c in new_columns
                        if c.get(hint, False)
                    ]
                    if hint == "not_null":
                        logger.warning(
                            f"Column(s) {hint_columns} with NOT NULL are being added to existing"
                            f" table {qualified_name}. If there's data in the table the operation"
                            " will fail."
                        )
                    else:
                        logger.warning(
                            f"Column(s) {hint_columns} with hint {hint} are being added to existing"
                            f" table {qualified_name}. Several hint types may not be added to"
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
        Delete all stored versions with the same name as given schema
        """
        name = self.sql_client.make_qualified_table_name(self.schema.version_table_name)
        (c_schema_name,) = self._norm_and_escape_columns("schema_name")
        self.sql_client.execute_sql(f"DELETE FROM {name} WHERE {c_schema_name} = %s;", schema.name)

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
            " %s);",
            schema.version,
            schema.ENGINE_VERSION,
            now_ts,
            schema.name,
            schema.stored_version_hash,
            schema_str,
        )

    def _verify_schema(self) -> None:
        super()._verify_schema()
        if exceptions := verify_sql_job_client_schema(self.schema, warnings=True):
            for exception in exceptions:
                logger.error(str(exception))
            raise exceptions[0]

    def prepare_load_job_execution(self, job: RunnableLoadJob) -> None:
        self._set_query_tags_for_job(load_id=job._load_id, table=job._load_table)

    def _set_query_tags_for_job(self, load_id: str, table: TTableSchema) -> None:
        """Sets query tags in sql_client for a job in package `load_id`, starting for a particular `table`"""
        from dlt.common.pipeline import current_pipeline

        pipeline = current_pipeline()
        pipeline_name = pipeline.pipeline_name if pipeline else ""
        self.sql_client.set_query_tags(
            {
                "source": self.schema.name,
                "resource": (
                    get_inherited_table_hint(
                        self.schema._schema_tables, table["name"], "resource", allow_none=True
                    )
                    or ""
                ),
                "table": table["name"],
                "load_id": load_id,
                "pipeline_name": pipeline_name,
            }
        )


class SqlJobClientWithStaging(SqlJobClientBase, WithStagingDataset):
    in_staging_mode: bool = False

    @contextlib.contextmanager
    def with_staging_dataset(self) -> Iterator["SqlJobClientBase"]:
        try:
            with self.sql_client.with_staging_dataset():
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
