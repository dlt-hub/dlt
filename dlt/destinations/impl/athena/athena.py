from typing import (
    Optional,
    ClassVar,
    Iterator,
    Any,
    AnyStr,
    Sequence,
    Tuple,
    List,
    Dict,
    Callable,
    Iterable,
    Type,
)
from copy import deepcopy
import re

from contextlib import contextmanager
from pendulum.datetime import DateTime, Date
from datetime import datetime  # noqa: I251

import pyathena
from pyathena import connect
from pyathena.connection import Connection
from pyathena.error import OperationalError, DatabaseError, ProgrammingError, IntegrityError, Error
from pyathena.formatter import (
    DefaultParameterFormatter,
    _DEFAULT_FORMATTERS,
    Formatter,
    _format_date,
)

from dlt.common import logger
from dlt.common.utils import without_none
from dlt.common.data_types import TDataType
from dlt.common.schema import TColumnSchema, Schema, TSchemaTables, TTableSchema
from dlt.common.schema.typing import TTableSchema, TColumnType, TWriteDisposition, TTableFormat
from dlt.common.schema.utils import table_schema_has_type, get_table_format
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.reference import LoadJob, FollowupJob
from dlt.common.destination.reference import TLoadJobState, NewLoadJob, SupportsStagingDestination
from dlt.common.storages import FileStorage
from dlt.common.data_writers.escape import escape_bigquery_identifier
from dlt.destinations.sql_jobs import SqlStagingCopyJob

from dlt.destinations.typing import DBApi, DBTransaction
from dlt.destinations.exceptions import (
    DatabaseTerminalException,
    DatabaseTransientException,
    DatabaseUndefinedRelation,
    LoadJobTerminalException,
)
from dlt.destinations.impl.athena import capabilities
from dlt.destinations.sql_client import (
    SqlClientBase,
    DBApiCursorImpl,
    raise_database_error,
    raise_open_connection_error,
)
from dlt.destinations.typing import DBApiCursor
from dlt.destinations.job_client_impl import SqlJobClientWithStaging
from dlt.destinations.impl.athena.configuration import AthenaClientConfiguration
from dlt.destinations.type_mapping import TypeMapper
from dlt.destinations import path_utils


class AthenaTypeMapper(TypeMapper):
    sct_to_unbound_dbt = {
        "complex": "string",
        "text": "string",
        "double": "double",
        "bool": "boolean",
        "date": "date",
        "timestamp": "timestamp",
        "bigint": "bigint",
        "binary": "binary",
        "time": "string",
    }

    sct_to_dbt = {"decimal": "decimal(%i,%i)", "wei": "decimal(%i,%i)"}

    dbt_to_sct = {
        "varchar": "text",
        "double": "double",
        "boolean": "bool",
        "date": "date",
        "timestamp": "timestamp",
        "bigint": "bigint",
        "binary": "binary",
        "varbinary": "binary",
        "decimal": "decimal",
        "tinyint": "bigint",
        "smallint": "bigint",
        "int": "bigint",
    }

    def __init__(self, capabilities: DestinationCapabilitiesContext):
        super().__init__(capabilities)

    def to_db_integer_type(
        self, precision: Optional[int], table_format: TTableFormat = None
    ) -> str:
        if precision is None:
            return "bigint"
        if precision <= 8:
            return "int" if table_format == "iceberg" else "tinyint"
        elif precision <= 16:
            return "int" if table_format == "iceberg" else "smallint"
        elif precision <= 32:
            return "int"
        return "bigint"

    def from_db_type(
        self, db_type: str, precision: Optional[int], scale: Optional[int]
    ) -> TColumnType:
        for key, val in self.dbt_to_sct.items():
            if db_type.startswith(key):
                return without_none(dict(data_type=val, precision=precision, scale=scale))  # type: ignore[return-value]
        return dict(data_type=None)


# add a formatter for pendulum to be used by pyathen dbapi
def _format_pendulum_datetime(formatter: Formatter, escaper: Callable[[str], str], val: Any) -> Any:
    # copied from https://github.com/laughingman7743/PyAthena/blob/f4b21a0b0f501f5c3504698e25081f491a541d4e/pyathena/formatter.py#L114
    # https://docs.aws.amazon.com/athena/latest/ug/engine-versions-reference-0003.html#engine-versions-reference-0003-timestamp-changes
    # ICEBERG tables have TIMESTAMP(6), other tables have TIMESTAMP(3), we always generate TIMESTAMP(6)
    # it is up to the user to cut the microsecond part
    val_string = val.strftime("%Y-%m-%d %H:%M:%S.%f")
    return f"""TIMESTAMP '{val_string}'"""


class DLTAthenaFormatter(DefaultParameterFormatter):
    _INSTANCE: ClassVar["DLTAthenaFormatter"] = None

    def __new__(cls: Type["DLTAthenaFormatter"]) -> "DLTAthenaFormatter":
        if cls._INSTANCE:
            return cls._INSTANCE
        return super().__new__(cls)

    def __init__(self) -> None:
        if DLTAthenaFormatter._INSTANCE:
            return
        formatters = deepcopy(_DEFAULT_FORMATTERS)
        formatters[DateTime] = _format_pendulum_datetime
        formatters[datetime] = _format_pendulum_datetime
        formatters[Date] = _format_date

        super(DefaultParameterFormatter, self).__init__(mappings=formatters, default=None)
        DLTAthenaFormatter._INSTANCE = self


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


class DoNothingFollowupJob(DoNothingJob, FollowupJob):
    """The second most lazy class of dlt"""

    pass


class AthenaSQLClient(SqlClientBase[Connection]):
    capabilities: ClassVar[DestinationCapabilitiesContext] = capabilities()
    dbapi: ClassVar[DBApi] = pyathena

    def __init__(self, dataset_name: str, config: AthenaClientConfiguration) -> None:
        super().__init__(None, dataset_name)
        self._conn: Connection = None
        self.config = config
        self.credentials = config.credentials

    @raise_open_connection_error
    def open_connection(self) -> Connection:
        native_credentials = self.config.credentials.to_native_representation()
        self._conn = connect(
            schema_name=self.dataset_name,
            s3_staging_dir=self.config.query_result_bucket,
            work_group=self.config.athena_work_group,
            **native_credentials,
        )
        return self._conn

    def close_connection(self) -> None:
        self._conn.close()
        self._conn = None

    @property
    def native_connection(self) -> Connection:
        return self._conn

    def escape_ddl_identifier(self, v: str) -> str:
        # https://docs.aws.amazon.com/athena/latest/ug/tables-databases-columns-names.html
        # Athena uses HIVE to create tables but for querying it uses PRESTO (so normal escaping)
        if not v:
            return v
        # bigquery uses hive escaping
        return escape_bigquery_identifier(v)

    def fully_qualified_ddl_dataset_name(self) -> str:
        return self.escape_ddl_identifier(self.dataset_name)

    def make_qualified_ddl_table_name(self, table_name: str) -> str:
        table_name = self.escape_ddl_identifier(table_name)
        return f"{self.fully_qualified_ddl_dataset_name()}.{table_name}"

    def create_dataset(self) -> None:
        # HIVE escaping for DDL
        self.execute_sql(f"CREATE DATABASE {self.fully_qualified_ddl_dataset_name()};")

    def drop_dataset(self) -> None:
        self.execute_sql(f"DROP DATABASE {self.fully_qualified_ddl_dataset_name()} CASCADE;")

    def fully_qualified_dataset_name(self, escape: bool = True) -> str:
        return (
            self.capabilities.escape_identifier(self.dataset_name) if escape else self.dataset_name
        )

    def drop_tables(self, *tables: str) -> None:
        if not tables:
            return
        statements = [
            f"DROP TABLE IF EXISTS {self.make_qualified_ddl_table_name(table)};" for table in tables
        ]
        self.execute_many(statements)

    @contextmanager
    @raise_database_error
    def begin_transaction(self) -> Iterator[DBTransaction]:
        logger.warning(
            "Athena does not support transactions! Each SQL statement is auto-committed separately."
        )
        yield self

    @raise_database_error
    def commit_transaction(self) -> None:
        pass

    @raise_database_error
    def rollback_transaction(self) -> None:
        raise NotImplementedError("You cannot rollback Athena SQL statements.")

    @staticmethod
    def _make_database_exception(ex: Exception) -> Exception:
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

    def execute_sql(
        self, sql: AnyStr, *args: Any, **kwargs: Any
    ) -> Optional[Sequence[Sequence[Any]]]:
        with self.execute_query(sql, *args, **kwargs) as curr:
            if curr.description is None:
                return None
            else:
                f = curr.fetchall()
                return f

    @staticmethod
    def _convert_to_old_pyformat(
        new_style_string: str, args: Tuple[Any, ...]
    ) -> Tuple[str, Dict[str, Any]]:
        # create a list of keys
        keys = ["arg" + str(i) for i, _ in enumerate(args)]
        # create an old style string and replace placeholders
        old_style_string, count = re.subn(
            r"%s", lambda _: "%(" + keys.pop(0) + ")s", new_style_string
        )
        # create a dictionary mapping keys to args
        mapping = dict(zip(["arg" + str(i) for i, _ in enumerate(args)], args))
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
        # PRESTO escaping for queries
        query = f"""SHOW DATABASES LIKE {self.fully_qualified_dataset_name()};"""
        rows = self.execute_sql(query)
        return len(rows) > 0


class AthenaClient(SqlJobClientWithStaging, SupportsStagingDestination):
    capabilities: ClassVar[DestinationCapabilitiesContext] = capabilities()

    def __init__(self, schema: Schema, config: AthenaClientConfiguration) -> None:
        # verify if staging layout is valid for Athena
        # this will raise if the table prefix is not properly defined
        # we actually that {table_name} is first, no {schema_name} is allowed
        self.table_prefix_layout = path_utils.get_table_prefix_layout(
            config.staging_config.layout, []
        )

        sql_client = AthenaSQLClient(config.normalize_dataset_name(schema), config)
        super().__init__(schema, config, sql_client)
        self.sql_client: AthenaSQLClient = sql_client  # type: ignore
        self.config: AthenaClientConfiguration = config
        self.type_mapper = AthenaTypeMapper(self.capabilities)

    def initialize_storage(self, truncate_tables: Iterable[str] = None) -> None:
        # only truncate tables in iceberg mode
        truncate_tables = []
        super().initialize_storage(truncate_tables)

    def _from_db_type(
        self, hive_t: str, precision: Optional[int], scale: Optional[int]
    ) -> TColumnType:
        return self.type_mapper.from_db_type(hive_t, precision, scale)

    def _get_column_def_sql(self, c: TColumnSchema, table_format: TTableFormat = None) -> str:
        return (
            f"{self.sql_client.escape_ddl_identifier(c['name'])} {self.type_mapper.to_db_type(c, table_format)}"
        )

    def _get_table_update_sql(
        self, table_name: str, new_columns: Sequence[TColumnSchema], generate_alter: bool
    ) -> List[str]:
        bucket = self.config.staging_config.bucket_url
        dataset = self.sql_client.dataset_name

        sql: List[str] = []

        # for the system tables we need to create empty iceberg tables to be able to run, DELETE and UPDATE queries
        # or if we are in iceberg mode, we create iceberg tables for all tables
        table = self.prepare_load_table(table_name, self.in_staging_mode)
        table_format = table.get("table_format")
        is_iceberg = self._is_iceberg_table(table) or table.get("write_disposition", None) == "skip"
        columns = ", ".join([self._get_column_def_sql(c, table_format) for c in new_columns])

        # this will fail if the table prefix is not properly defined
        table_prefix = self.table_prefix_layout.format(table_name=table_name)
        location = f"{bucket}/{dataset}/{table_prefix}"

        # use qualified table names
        qualified_table_name = self.sql_client.make_qualified_ddl_table_name(table_name)
        if generate_alter:
            # alter table to add new columns at the end
            sql.append(f"""ALTER TABLE {qualified_table_name} ADD COLUMNS ({columns});""")
        else:
            if is_iceberg:
                sql.append(f"""CREATE TABLE {qualified_table_name}
                        ({columns})
                        LOCATION '{location}'
                        TBLPROPERTIES ('table_type'='ICEBERG', 'format'='parquet');""")
            elif table_format == "jsonl":
                sql.append(f"""CREATE EXTERNAL TABLE {qualified_table_name}
                        ({columns})
                        ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
                        LOCATION '{location}';""")
            else:
                sql.append(f"""CREATE EXTERNAL TABLE {qualified_table_name}
                        ({columns})
                        STORED AS PARQUET
                        LOCATION '{location}';""")
        return sql

    def start_file_load(self, table: TTableSchema, file_path: str, load_id: str) -> LoadJob:
        """Starts SqlLoadJob for files ending with .sql or returns None to let derived classes to handle their specific jobs"""
        if table_schema_has_type(table, "time"):
            raise LoadJobTerminalException(
                file_path,
                "Athena cannot load TIME columns from parquet tables. Please convert"
                " `datetime.time` objects in your data to `str` or `datetime.datetime`.",
            )
        job = super().start_file_load(table, file_path, load_id)
        if not job:
            job = (
                DoNothingFollowupJob(file_path)
                if self._is_iceberg_table(self.prepare_load_table(table["name"]))
                else DoNothingJob(file_path)
            )
        return job

    def _create_append_followup_jobs(self, table_chain: Sequence[TTableSchema]) -> List[NewLoadJob]:
        if self._is_iceberg_table(self.prepare_load_table(table_chain[0]["name"])):
            return [
                SqlStagingCopyJob.from_table_chain(table_chain, self.sql_client, {"replace": False})
            ]
        return super()._create_append_followup_jobs(table_chain)

    def _create_replace_followup_jobs(
        self, table_chain: Sequence[TTableSchema]
    ) -> List[NewLoadJob]:
        if self._is_iceberg_table(self.prepare_load_table(table_chain[0]["name"])):
            return [
                SqlStagingCopyJob.from_table_chain(table_chain, self.sql_client, {"replace": True})
            ]
        return super()._create_replace_followup_jobs(table_chain)

    def _create_merge_followup_jobs(self, table_chain: Sequence[TTableSchema]) -> List[NewLoadJob]:
        # fall back to append jobs for merge
        return self._create_append_followup_jobs(table_chain)

    def _is_iceberg_table(self, table: TTableSchema) -> bool:
        table_format = table.get("table_format")
        return table_format == "iceberg"

    def should_load_data_to_staging_dataset(self, table: TTableSchema) -> bool:
        # all iceberg tables need staging
        if self._is_iceberg_table(self.prepare_load_table(table["name"])):
            return True
        return super().should_load_data_to_staging_dataset(table)

    def should_truncate_table_before_load_on_staging_destination(self, table: TTableSchema) -> bool:
        # on athena we only truncate replace tables that are not iceberg
        table = self.prepare_load_table(table["name"])
        if table["write_disposition"] == "replace" and not self._is_iceberg_table(
            self.prepare_load_table(table["name"])
        ):
            return True
        return False

    def should_load_data_to_staging_dataset_on_staging_destination(
        self, table: TTableSchema
    ) -> bool:
        """iceberg table data goes into staging on staging destination"""
        if self._is_iceberg_table(self.prepare_load_table(table["name"])):
            return True
        return super().should_load_data_to_staging_dataset_on_staging_destination(table)

    def prepare_load_table(
        self, table_name: str, prepare_for_staging: bool = False
    ) -> TTableSchema:
        table = super().prepare_load_table(table_name, prepare_for_staging)
        if self.config.force_iceberg:
            table["table_format"] = "iceberg"
        if prepare_for_staging and table.get("table_format", None) == "iceberg":
            table.pop("table_format")
        return table

    @staticmethod
    def is_dbapi_exception(ex: Exception) -> bool:
        return isinstance(ex, Error)
