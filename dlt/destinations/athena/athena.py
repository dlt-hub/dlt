from typing import Optional, ClassVar, Iterator, Any, AnyStr, Sequence, Tuple, List, Dict, Callable, Iterable
from copy import deepcopy
import re

from contextlib import contextmanager
from pendulum.datetime import DateTime

import pyathena
from pyathena import connect
from pyathena.connection import Connection
from pyathena.error import OperationalError, DatabaseError, ProgrammingError, IntegrityError, Error
from pyathena.formatter import DefaultParameterFormatter, _DEFAULT_FORMATTERS, Formatter

from dlt.destinations.typing import DBApi
from dlt.destinations.exceptions import DatabaseTerminalException, DatabaseTransientException, DatabaseUndefinedRelation
from dlt.common import pendulum
from dlt.common.schema.typing import TTableSchemaColumns
from dlt.common.data_types import TDataType
from dlt.common.schema import TColumnSchema, Schema
from dlt.common.schema.typing import TTableSchema, TColumnSchemaBase
from dlt.destinations.athena import capabilities
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.destinations.sql_client import SqlClientBase, DBApiCursorImpl, raise_database_error, raise_open_connection_error
from dlt.destinations.typing import DBApiCursor
from dlt.common.destination.reference import LoadJob
from dlt.common.destination.reference import TLoadJobState
from dlt.common.storages import FileStorage
from dlt.common.schema.typing import VERSION_TABLE_NAME
from dlt.common.schema.utils import add_missing_hints

from dlt.destinations.job_client_impl import SqlJobClientBase, StorageSchemaInfo
from dlt.destinations.athena.configuration import AthenaClientConfiguration
from dlt.destinations.job_impl import NewReferenceJob
from dlt.destinations import path_utils

SCT_TO_HIVET: Dict[TDataType, str] = {
    "complex": "string",
    "text": "string",
    "double": "double",
    "bool": "boolean",
    "date": "date",
    "timestamp": "timestamp",
    "bigint": "bigint",
    "binary": "binary",
    "decimal": "decimal(%i,%i)"
}

HIVET_TO_SCT: Dict[str, TDataType] = {
    "varchar": "text",
    "double": "double",
    "boolean": "bool",
    "date": "date",
    "timestamp": "timestamp",
    "bigint": "bigint",
    "binary": "binary",
    "varbinary": "binary",
    "decimal": "decimal"
}

# add a formatter for pendulum to be used by pyathen dbapi
def _format_pendulum_datetime(formatter: Formatter, escaper: Callable[[str], str], val: Any) -> Any:
    # copied from https://github.com/laughingman7743/PyAthena/blob/f4b21a0b0f501f5c3504698e25081f491a541d4e/pyathena/formatter.py#L114
    val_string = val.strftime("%Y-%m-%d %H:%M:%S.%f")
    return f"""TIMESTAMP '{val_string}'"""

class DLTAthenaFormatter(DefaultParameterFormatter):
    def __init__(self) -> None:
        formatters = deepcopy(_DEFAULT_FORMATTERS)
        formatters[DateTime] = _format_pendulum_datetime
        super(DefaultParameterFormatter, self).__init__(
            mappings=formatters, default=None
        )

class DoNothingJob(LoadJob):
    """The most lazy class of dlt"""

    def __init__(self, file_path: str) -> None:
        super().__init__(FileStorage.get_file_name_from_file_path(file_path))

    def state(self) -> TLoadJobState:
        # this job is always done
        return "completed"

    def exception(self) -> str:
        # this part of code should be never reached
        raise NotImplementedError()

class AthenaSQLClient(SqlClientBase[Connection]):

    capabilities: ClassVar[DestinationCapabilitiesContext] = capabilities()
    dbapi: ClassVar[DBApi] = pyathena

    def __init__(self, dataset_name: str, config: AthenaClientConfiguration) -> None:
        super().__init__(None, dataset_name)
        self._conn: Connection = None
        self.config = config

    @raise_open_connection_error
    def open_connection(self) -> Connection:
        native_credentials = self.config.credentials.to_native_representation()
        self._conn = connect(
            schema_name=self.dataset_name,
            s3_staging_dir=self.config.query_result_bucket,
            work_group=self.config.workgroup,
            **native_credentials)
        return self._conn

    def close_connection(self) -> None:
        self._conn.close()
        self._conn = None

    @property
    def native_connection(self) -> Connection:
        return self._conn

    def create_dataset(self) -> None:
        self.execute_sql(f"CREATE DATABASE {self.fully_qualified_dataset_name(escape=False)};")

    def drop_dataset(self) -> None:
        self.execute_sql(f"DROP DATABASE {self.fully_qualified_dataset_name(escape=False)} CASCADE;")

    def fully_qualified_dataset_name(self, escape: bool = True) -> str:
        # for some reason dataset need to be esacped with " and not `
        return f"\"{self.dataset_name}\"" if escape else self.dataset_name

    def make_qualified_table_name(self, table_name: str, escape: bool = True) -> str:
        if escape:
            table_name = f"\"{table_name}\""
        return f"{self.fully_qualified_dataset_name(escape)}.{table_name}"

    def drop_tables(self, *tables: str) -> None:
        if not tables:
            return
        statements = [f"DROP TABLE IF EXISTS {self.capabilities.escape_identifier(table)};" for table in tables]
        self.execute_fragments(statements)

    @contextmanager
    @raise_database_error
    def begin_transaction(self) -> Iterator[Any]:
        yield self

    @staticmethod
    def _make_database_exception(ex: Exception) -> Exception:
        print(str(ex))
        if isinstance(ex, OperationalError):
            if "TABLE_NOT_FOUND" in str(ex):
                return DatabaseUndefinedRelation(ex)
            elif "SCHEMA_NOT_FOUND" in str(ex):
                return DatabaseUndefinedRelation(ex)
            elif "Table not found" in str(ex):
                return DatabaseUndefinedRelation(ex)
            elif "Database does not exist" in str(ex):
                return DatabaseUndefinedRelation(ex)
            return DatabaseTerminalException(ex)
        elif isinstance(ex, (ProgrammingError, IntegrityError)):
            return DatabaseTerminalException(ex)
        if isinstance(ex, DatabaseError):
            return DatabaseTransientException(ex)
        return ex

    def execute_sql(self, sql: AnyStr, *args: Any, **kwargs: Any) -> Optional[Sequence[Sequence[Any]]]:
        with self.execute_query(sql, *args, **kwargs) as curr:
            if curr.description is None:
                return None
            else:
                f = curr.fetchall()
                return f

    @staticmethod
    def _convert_to_old_pyformat(new_style_string: str, args: Tuple[Any, ...]) -> Tuple[str, Dict[str, Any]]:
        # create a list of keys
        keys = ["arg"+str(i) for i, _ in enumerate(args)]
        # create an old style string and replace placeholders
        old_style_string, count = re.subn(r"%s", lambda _: "%(" + keys.pop(0) + ")s", new_style_string)
        # create a dictionary mapping keys to args
        mapping = dict(zip(["arg"+str(i) for i, _ in enumerate(args)], args))
        # raise if there is a mismatch between args and string
        if count != len(args):
            raise DatabaseTransientException(OperationalError())
        return old_style_string, mapping

    @contextmanager
    @raise_database_error
    def execute_query(self, query: AnyStr, *args: Any, **kwargs: Any) -> Iterator[DBApiCursor]:
        assert isinstance(query, str)

        db_args = kwargs
        # convert sql and params to PyFormat, as athena does not support anything else
        if args:
            query, db_args = self._convert_to_old_pyformat(query, args)
            if kwargs:
                db_args.update(kwargs)
        cursor = self._conn.cursor(formatter=DLTAthenaFormatter())
        for query_line in query.split(";"):
            if query_line.strip():
                try:
                    cursor.execute(query_line, db_args)
                # catch key error only here, this will show up if we have a missing parameter
                except KeyError:
                    raise DatabaseTransientException(OperationalError())

        yield DBApiCursorImpl(cursor)  # type: ignore

    def has_dataset(self) -> bool:
        query = f"""SHOW DATABASES LIKE {self.fully_qualified_dataset_name()};"""
        rows = self.execute_sql(query)
        return len(rows) > 0

class AthenaClient(SqlJobClientBase):

    capabilities: ClassVar[DestinationCapabilitiesContext] = capabilities()

    def __init__(self, schema: Schema, config: AthenaClientConfiguration) -> None:
        sql_client = AthenaSQLClient(
            self.make_dataset_name(schema, config.dataset_name, config.default_schema_name),
            config
        )
        super().__init__(schema, config, sql_client)

    def initialize_storage(self, truncate_tables: Iterable[str] = None) -> None:
        # never truncate tables in athena
        super().initialize_storage([])

    def _get_column_def_sql(self, c: TColumnSchema) -> str:
        return f"{self.capabilities.escape_identifier(c['name'])} {self._to_db_type(c['data_type'])}"

    @classmethod
    def _to_db_type(cls, sc_t: TDataType) -> str:
        if sc_t == "wei":
            return SCT_TO_HIVET["decimal"] % cls.capabilities.wei_precision
        if sc_t == "decimal":
            return SCT_TO_HIVET["decimal"] % cls.capabilities.decimal_precision
        return SCT_TO_HIVET[sc_t]

    @classmethod
    def _from_db_type(cls, hive_t: str, precision: Optional[int], scale: Optional[int]) -> TDataType:
        for key, val in HIVET_TO_SCT.items():
            if hive_t.startswith(key):
                return val
        return None

    def get_schema_by_hash(self, version_hash: str) -> StorageSchemaInfo:
        query = f"""SELECT {self.VERSION_TABLE_SCHEMA_COLUMNS} FROM "{VERSION_TABLE_NAME}" WHERE version_hash = '{version_hash}';"""
        try:
            return self._row_to_schema_info(query)
        except OperationalError:
            return None

    def _get_table_update_sql(self, table_name: str, new_columns: Sequence[TColumnSchema], generate_alter: bool) -> List[str]:

        bucket = self.config.staging_config.bucket_url
        dataset = self.sql_client.dataset_name
        schema_name = self.schema.name
        sql: List[str] = []

        # for the system tables we need to create empty iceberg tables to be able to run, DELETE and UPDATE queries
        is_iceberg = self.schema.tables[table_name].get("write_disposition", None) == "skip"
        columns = ", ".join([self._get_column_def_sql(c) for c in new_columns])

        # this will fail if the table prefix is not properly defined
        table_prefix = path_utils.get_table_prefix(self.config.staging_config.layout, schema_name=schema_name, table_name=table_name)
        location = f"{bucket}/{dataset}/{table_prefix}"
        table_name = self.capabilities.escape_identifier(table_name)
        if is_iceberg and not generate_alter:
            sql.append(f"""CREATE TABLE {table_name}
                    ({columns})
                    LOCATION '{location}'
                    TBLPROPERTIES ('table_type'='ICEBERG', 'format'='parquet');""")
        elif not generate_alter:
            sql.append(f"""CREATE EXTERNAL TABLE {table_name}
                    ({columns})
                    STORED AS PARQUET
                    LOCATION '{location}';""")
        # alter table to add new columns at the end
        else:
            sql.append(f"""ALTER TABLE {table_name} ADD COLUMNS ({columns});""")

        return sql

    def start_file_load(self, table: TTableSchema, file_path: str, load_id: str) -> LoadJob:
        """Starts SqlLoadJob for files ending with .sql or returns None to let derived classes to handle their specific jobs"""
        job = super().start_file_load(table, file_path, load_id)
        if not job:
            job = DoNothingJob(file_path)
        return job

    @staticmethod
    def is_dbapi_exception(ex: Exception) -> bool:
        return isinstance(ex, Error)