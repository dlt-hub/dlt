import os
from abc import abstractmethod
import base64
import binascii
import contextlib
from copy import copy
import datetime  # noqa: 251
from types import TracebackType
from typing import Any, ClassVar, List, NamedTuple, Optional, Sequence, Tuple, Type, Iterable, Iterator
import zlib

from dlt.common import json, pendulum, logger
from dlt.common.data_types import TDataType
from dlt.common.schema.typing import COLUMN_HINTS, LOADS_TABLE_NAME, VERSION_TABLE_NAME, TColumnSchemaBase, TTableSchema
from dlt.common.schema.utils import add_missing_hints
from dlt.common.storages import FileStorage
from dlt.common.schema import TColumnSchema, Schema, TTableSchemaColumns, TSchemaTables
from dlt.common.destination.reference import DestinationClientConfiguration, DestinationClientDwhConfiguration, NewLoadJob, TLoadJobState, LoadJob, JobClientBase
from dlt.destinations.exceptions import DatabaseUndefinedRelation, DestinationSchemaWillNotUpdate
from dlt.destinations.job_impl import EmptyLoadJobWithoutFollowup
from dlt.destinations.sql_merge_job import SqlMergeJob

from dlt.destinations.typing import TNativeConn
from dlt.destinations.sql_client import SqlClientBase


class StorageSchemaInfo(NamedTuple):
    version_hash: str
    schema_name: str
    version: int
    engine_version: str
    inserted_at: datetime.datetime
    schema: str


class SqlLoadJob(LoadJob):
    """A job executing sql statement, without followup trait"""

    def __init__(self, file_path: str, sql_client: SqlClientBase[Any]) -> None:
        super().__init__(FileStorage.get_file_name_from_file_path(file_path))
        # execute immediately if client present
        with open(file_path, "r", encoding="utf-8") as f:
            sql = f.read()
        with sql_client.begin_transaction():
            sql_client.execute_sql(sql)

    def state(self) -> TLoadJobState:
        # this job is always done
        return "completed"

    def exception(self) -> str:
        # this part of code should be never reached
        raise NotImplementedError()

    @staticmethod
    def is_sql_job(file_path: str) -> bool:
        return os.path.splitext(file_path)[1][1:] == "sql"


class SqlJobClientBase(JobClientBase):

    VERSION_TABLE_SCHEMA_COLUMNS: ClassVar[str] = "version_hash, schema_name, version, engine_version, inserted_at, schema"

    def __init__(self, schema: Schema, config: DestinationClientConfiguration,  sql_client: SqlClientBase[TNativeConn]) -> None:
        super().__init__(schema, config)
        self.sql_client = sql_client
        assert isinstance(config, DestinationClientDwhConfiguration)
        self.config: DestinationClientDwhConfiguration = config

    def initialize_storage(self, staging: bool = False, truncate_tables: Iterable[str] = None) -> None:
        # use regular or staging dataset name
        with self.sql_client.with_staging_dataset(staging):
            if not self.is_storage_initialized():
                self.sql_client.create_dataset()
            else:
                # truncate requested tables
                if truncate_tables:
                    self.sql_client.truncate_tables(*truncate_tables)


    def is_storage_initialized(self, staging: bool = False) -> bool:
        with self.sql_client.with_staging_dataset(staging):
            return self.sql_client.has_dataset()

    def update_storage_schema(self, staging: bool = False, only_tables: Iterable[str] = None, expected_update: TSchemaTables = None) -> Optional[TSchemaTables]:
        with self.sql_client.with_staging_dataset(staging):
            super().update_storage_schema(staging, only_tables, expected_update)
            applied_update: TSchemaTables = {}
            schema_info = self.get_schema_by_hash(self.schema.stored_version_hash)
            if schema_info is None:
                logger.info(f"Schema with hash {self.schema.stored_version_hash} not found in the storage. upgrading")

                with self.maybe_ddl_transaction():
                    applied_update = self._execute_schema_update_sql(only_tables)
            else:
                logger.info(f"Schema with hash {self.schema.stored_version_hash} inserted at {schema_info.inserted_at} found in storage, no upgrade required")
            return applied_update

    def drop_tables(self, *tables: str, staging: bool = False, replace_schema: bool = True) -> None:
        with self.maybe_ddl_transaction():
            with self.sql_client.with_staging_dataset(staging):
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

    def create_merge_job(self, table_chain: Sequence[TTableSchema]) -> NewLoadJob:
        return SqlMergeJob.from_table_chain(table_chain, self.sql_client)

    def start_file_load(self, table: TTableSchema, file_path: str) -> LoadJob:
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
        name = self.sql_client.make_qualified_table_name(LOADS_TABLE_NAME)
        now_ts = pendulum.now()
        self.sql_client.execute_sql(
            f"INSERT INTO {name}(load_id, schema_name, status, inserted_at) VALUES(%s, %s, %s, %s);", load_id, self.schema.name, 0, now_ts)

    def __enter__(self) -> "SqlJobClientBase":
        self.sql_client.open_connection()
        return self

    def __exit__(self, exc_type: Type[BaseException], exc_val: BaseException, exc_tb: TracebackType) -> None:
        self.sql_client.close_connection()

    def get_storage_table(self, table_name: str) -> Tuple[bool, TTableSchemaColumns]:

        def _null_to_bool(v: str) -> bool:
            if v == "NO":
                return False
            elif v == "YES":
                return True
            raise ValueError(v)

        schema_table: TTableSchemaColumns = {}
        query = """
                SELECT column_name, data_type, is_nullable, numeric_precision, numeric_scale
                    FROM INFORMATION_SCHEMA.COLUMNS
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position;
                """
        rows = self.sql_client.execute_sql(query, self.sql_client.fully_qualified_dataset_name(escape=False), table_name)
        # if no rows we assume that table does not exist
        if len(rows) == 0:
            # TODO: additionally check if table exists
            return False, schema_table
        # TODO: pull more data to infer indexes, PK and uniques attributes/constraints
        for c in rows:
            schema_c: TColumnSchemaBase = {
                "name": c[0],
                "nullable": _null_to_bool(c[2]),
                "data_type": self._from_db_type(c[1], c[3], c[4]),
            }
            schema_table[c[0]] = add_missing_hints(schema_c)
        return True, schema_table

    @staticmethod
    @abstractmethod
    def _to_db_type(schema_type: TDataType) -> str:
        pass

    @staticmethod
    @abstractmethod
    def _from_db_type(db_type: str, precision: Optional[int], scale: Optional[int]) -> TDataType:
        pass

    def get_newest_schema_from_storage(self) -> StorageSchemaInfo:
        name = self.sql_client.make_qualified_table_name(VERSION_TABLE_NAME)
        query = f"SELECT {self.VERSION_TABLE_SCHEMA_COLUMNS} FROM {name} WHERE schema_name = %s ORDER BY inserted_at DESC;"
        return self._row_to_schema_info(query, self.schema.name)

    def get_schema_by_hash(self, version_hash: str) -> StorageSchemaInfo:
        name = self.sql_client.make_qualified_table_name(VERSION_TABLE_NAME)
        query = f"SELECT {self.VERSION_TABLE_SCHEMA_COLUMNS} FROM {name} WHERE version_hash = %s;"
        return self._row_to_schema_info(query, version_hash)

    def _execute_schema_update_sql(self, only_tables: Iterable[str]) -> TSchemaTables:
        sql_scripts, schema_update = self._build_schema_update_sql(only_tables)
        if len(schema_update) > 0:
            # execute updates in a single batch
            sql = "\n".join(sql_scripts)
            self.sql_client.execute_sql(sql)
        self._update_schema_in_storage(self.schema)
        return schema_update

    def _build_schema_update_sql(self, only_tables: Iterable[str]) -> Tuple[List[str], TSchemaTables]:
        """Generates CREATE/ALTER sql for tables that differ int the destination and in Schema.

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
                sql = self._get_table_update_sql(table_name, new_columns, exists)
                if not sql.endswith(";"):
                    sql += ";"
                sql_updates.append(sql)
                # create a schema update for particular table
                partial_table = copy(self.schema.get_table(table_name))
                # keep only new columns
                partial_table["columns"] = {c["name"]: c for c in new_columns}
                schema_update[table_name] = partial_table

        return sql_updates, schema_update

    def _get_table_update_sql(self, table_name: str, new_columns: Sequence[TColumnSchema], generate_alter: bool) -> str:
        # build sql
        canonical_name = self.sql_client.make_qualified_table_name(table_name)
        if not generate_alter:
            # build CREATE
            sql = f"CREATE TABLE {canonical_name} (\n"
            sql += ",\n".join([self._get_column_def_sql(c) for c in new_columns])
            sql += ")"
        else:
            sql = f"ALTER TABLE {canonical_name}\n"
            if self.capabilities.alter_add_multi_column:
                column_sql = ",\n"
            else:
                # build ALTER as separate statement for each column (redshift limitation)
                column_sql = ";" + sql
            sql += column_sql.join([f"ADD COLUMN {self._get_column_def_sql(c)}" for c in new_columns])
        # scan columns to get hints
        if generate_alter:
            # no hints may be specified on added columns
            for hint in COLUMN_HINTS:
                if any(c.get(hint, False) is True for c in new_columns):
                    hint_columns = [self.capabilities.escape_identifier(c["name"]) for c in new_columns if c.get(hint, False)]
                    raise DestinationSchemaWillNotUpdate(canonical_name, hint_columns, f"{hint} requested after table was created")
        return sql

    @abstractmethod
    def _get_column_def_sql(self, c: TColumnSchema) -> str:
        pass

    @staticmethod
    def _gen_not_null(v: bool) -> str:
        return "NOT NULL" if not v else ""

    def _create_table_update(self, table_name: str, storage_table: TTableSchemaColumns) -> Sequence[TColumnSchema]:
        # compare table with stored schema and produce delta
        updates = self.schema.get_new_complete_columns(table_name, storage_table)
        logger.info(f"Found {len(updates)} updates for {table_name} in {self.schema.name}")
        return updates

    def _row_to_schema_info(self, query: str, *args: Any) -> StorageSchemaInfo:
        row: Tuple[Any,...] = None
        # if there's no dataset/schema return none info
        with contextlib.suppress(DatabaseUndefinedRelation):
            with self.sql_client.execute_query(query, *args) as cur:
                row = cur.fetchone()
        if not row:
            return None

        # get schema as string
        # TODO: Re-use decompress/compress_state() implementation from dlt.pipeline.state_sync
        schema_str = row[5]
        try:
            schema_bytes = base64.b64decode(schema_str, validate=True)
            schema_str = zlib.decompress(schema_bytes).decode("utf-8")
        except binascii.Error:
            pass

        # make utc datetime
        inserted_at = pendulum.instance(row[4])

        return StorageSchemaInfo(row[0], row[1], row[2], row[3], inserted_at, schema_str)

    def _replace_schema_in_storage(self, schema: Schema) -> None:
        """
        Save the given schema in storage and remove all previous versions with the same name
        """
        name = self.sql_client.make_qualified_table_name(VERSION_TABLE_NAME)
        self.sql_client.execute_sql(
            f"DELETE FROM {name} WHERE schema_name = %s;", schema.name
        )
        self._update_schema_in_storage(schema)

    def _update_schema_in_storage(self, schema: Schema) -> None:
        now_ts = str(pendulum.now())
        # get schema string or zip
        schema_str = json.dumps(schema.to_dict())
        # TODO: not all databases store data as utf-8 but this exception is mostly for redshift
        schema_bytes = schema_str.encode("utf-8")
        if len(schema_bytes) > self.capabilities.max_text_data_type_length:
            # compress and to base64
            schema_str = base64.b64encode(zlib.compress(schema_bytes, level=9)).decode("ascii")
        # insert
        name = self.sql_client.make_qualified_table_name(VERSION_TABLE_NAME)
        # values =  schema.version_hash, schema.name, schema.version, schema.ENGINE_VERSION, str(now_ts), schema_str
        self.sql_client.execute_sql(
            f"INSERT INTO {name}({self.VERSION_TABLE_SCHEMA_COLUMNS}) VALUES (%s, %s, %s, %s, %s, %s);", schema.stored_version_hash, schema.name, schema.version, schema.ENGINE_VERSION, now_ts, schema_str
        )
