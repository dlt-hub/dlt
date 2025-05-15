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
    cast,
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
from dlt.common.utils import uniq_id
from dlt.common.schema import TColumnSchema, Schema
from dlt.common.schema.typing import (
    TColumnType,
    TTableFormat,
    TSortOrder,
)
from dlt.common.destination import DestinationCapabilitiesContext, PreparedTableSchema
from dlt.common.destination.client import FollowupJobRequest, SupportsStagingDestination, LoadJob
from dlt.common.destination.dataset import DBApiCursor
from dlt.common.data_writers.escape import escape_hive_identifier
from dlt.destinations.sql_jobs import (
    SqlStagingCopyFollowupJob,
    SqlStagingReplaceFollowupJob,
    SqlMergeFollowupJob,
)

from dlt.destinations.typing import DBApi, DBTransaction
from dlt.destinations.exceptions import (
    DatabaseTerminalException,
    DatabaseTransientException,
    DatabaseUndefinedRelation,
)
from dlt.destinations.sql_client import (
    SqlClientBase,
    DBApiCursorImpl,
    raise_database_error,
    raise_open_connection_error,
)
from dlt.destinations.job_client_impl import SqlJobClientWithStagingDataset
from dlt.destinations.job_impl import FinalizedLoadJobWithFollowupJobs, FinalizedLoadJob
from dlt.destinations.impl.athena.configuration import AthenaClientConfiguration
from dlt.destinations import path_utils
from dlt.destinations.impl.athena.athena_adapter import PARTITION_HINT
from dlt.destinations.utils import get_deterministic_temp_table_name


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


class AthenaMergeJob(SqlMergeFollowupJob):
    @classmethod
    def _new_temp_table_name(cls, table_name: str, op: str, sql_client: SqlClientBase[Any]) -> str:
        # reproducible name so we know which table to drop
        with sql_client.with_staging_dataset():
            return sql_client.make_qualified_table_name(
                cls._shorten_table_name(
                    get_deterministic_temp_table_name(table_name, op), sql_client
                )
            )

    @classmethod
    def _to_temp_table(cls, select_sql: str, temp_table_name: str) -> str:
        # regular table because Athena does not support temporary tables
        return f"CREATE TABLE {temp_table_name} AS {select_sql};"

    @classmethod
    def gen_insert_temp_table_sql(
        cls,
        table_name: str,
        staging_root_table_name: str,
        sql_client: SqlClientBase[Any],
        primary_keys: Sequence[str],
        unique_column: str,
        dedup_sort: Tuple[str, TSortOrder] = None,
        condition: str = None,
        condition_columns: Sequence[str] = None,
    ) -> Tuple[List[str], str]:
        sql, temp_table_name = super().gen_insert_temp_table_sql(
            table_name,
            staging_root_table_name,
            sql_client,
            primary_keys,
            unique_column,
            dedup_sort,
            condition,
            condition_columns,
        )
        # DROP needs backtick as escape identifier
        sql.insert(0, f"""DROP TABLE IF EXISTS {temp_table_name.replace('"', '`')};""")
        return sql, temp_table_name

    @classmethod
    def gen_delete_temp_table_sql(
        cls,
        table_name: str,
        unique_column: str,
        key_table_clauses: Sequence[str],
        sql_client: SqlClientBase[Any],
    ) -> Tuple[List[str], str]:
        sql, temp_table_name = super().gen_delete_temp_table_sql(
            table_name, unique_column, key_table_clauses, sql_client
        )
        # DROP needs backtick as escape identifier
        sql.insert(0, f"""DROP TABLE IF EXISTS {temp_table_name.replace('"', '`')};""")
        return sql, temp_table_name

    @classmethod
    def gen_concat_sql(cls, columns: Sequence[str]) -> str:
        # Athena requires explicit casting
        columns = [f"CAST({c} AS VARCHAR)" for c in columns]
        return f"CONCAT({', '.join(columns)})"

    @classmethod
    def requires_temp_table_for_delete(cls) -> bool:
        return True


class AthenaSQLClient(SqlClientBase[Connection]):
    dbapi: ClassVar[DBApi] = pyathena

    def __init__(
        self,
        dataset_name: str,
        staging_dataset_name: str,
        config: AthenaClientConfiguration,
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        super().__init__(None, dataset_name, staging_dataset_name, capabilities)
        self._conn: Connection = None
        self.config = config
        self.credentials = config.credentials

    @raise_open_connection_error
    def open_connection(self) -> Connection:
        self._conn = connect(schema_name=self.dataset_name, **self.config.to_connector_params())
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
        v = self.capabilities.casefold_identifier(v)
        # bigquery uses hive escaping
        return escape_hive_identifier(v)

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
            elif "Table" in str(ex) and " not found" in str(ex):
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

        with self._conn.cursor(formatter=DLTAthenaFormatter()) as cursor:
            for query_line in query.split(";"):
                if query_line.strip():
                    try:
                        cursor.execute(query_line, db_args)
                    # catch key error only here, this will show up if we have a missing parameter
                    except KeyError:
                        raise DatabaseTransientException(OperationalError())

            # TODO: (important) allow to use PandasCursor and ArrowCursor to get fast data access
            #    the problem: you need to set the cursor type upfront. so if user uses wrong cursor
            #    we won't be able to dynamically change it.
            yield DBApiCursorImpl(cursor)  # type: ignore


class AthenaClient(SqlJobClientWithStagingDataset, SupportsStagingDestination):
    def __init__(
        self,
        schema: Schema,
        config: AthenaClientConfiguration,
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        # verify if staging layout is valid for Athena
        # this will raise if the table prefix is not properly defined
        # we actually make sure that {table_name} is first, no {schema_name} is allowed
        if config.staging_config:
            self.table_prefix_layout = path_utils.get_table_prefix_layout(
                config.staging_config.layout,
                supported_prefix_placeholders=[],
                table_needs_own_folder=True,
            )

        sql_client = AthenaSQLClient(
            config.normalize_dataset_name(schema),
            config.normalize_staging_dataset_name(schema),
            config,
            capabilities,
        )
        super().__init__(schema, config, sql_client)
        self.sql_client: AthenaSQLClient = sql_client  # type: ignore
        self.config: AthenaClientConfiguration = config
        self.type_mapper = self.capabilities.get_type_mapper()

    def initialize_storage(self, truncate_tables: Iterable[str] = None) -> None:
        # only truncate tables in iceberg mode
        truncate_tables = []
        super().initialize_storage(truncate_tables)

    def _from_db_type(
        self, hive_t: str, precision: Optional[int], scale: Optional[int]
    ) -> TColumnType:
        return self.type_mapper.from_destination_type(hive_t, precision, scale)

    def _get_column_def_sql(self, c: TColumnSchema, table: PreparedTableSchema = None) -> str:
        return (
            f"{self.sql_client.escape_ddl_identifier(c['name'])} {self.type_mapper.to_destination_type(c, table)}"
        )

    def _iceberg_partition_clause(self, partition_hints: Optional[Dict[str, str]]) -> str:
        if not partition_hints:
            return ""
        formatted_strings = []
        for column_name, template in partition_hints.items():
            formatted_strings.append(
                template.format(column_name=self.sql_client.escape_ddl_identifier(column_name))
            )
        return f"PARTITIONED BY ({', '.join(formatted_strings)})"

    def _iceberg_table_properties(self) -> str:
        table_properties = (
            self.config.table_properties.copy() if self.config.table_properties else {}
        )
        mandatory_properties = {"table_type": "ICEBERG", "format": "parquet"}
        table_properties.update(mandatory_properties)
        return ", ".join([f"'{k}'='{v}'" for k, v in table_properties.items()])

    def _get_table_update_sql(
        self, table_name: str, new_columns: Sequence[TColumnSchema], generate_alter: bool
    ) -> List[str]:
        bucket = self.config.staging_config.bucket_url
        dataset = self.sql_client.dataset_name

        sql: List[str] = []

        # for the system tables we need to create empty iceberg tables to be able to run, DELETE and UPDATE queries
        # or if we are in iceberg mode, we create iceberg tables for all tables
        table = self.prepare_load_table(table_name)
        # do not create iceberg tables on staging dataset
        create_iceberg = self._is_iceberg_table(table, self.in_staging_dataset_mode)
        columns = ", ".join([self._get_column_def_sql(c, table) for c in new_columns])

        # use qualified table names
        qualified_table_name = self.sql_client.make_qualified_ddl_table_name(table_name)
        if generate_alter:
            # alter table to add new columns at the end
            sql.append(f"""ALTER TABLE {qualified_table_name} ADD COLUMNS ({columns});""")
        else:
            table_prefix = self.table_prefix_layout.format(table_name=table_name)
            if create_iceberg:
                partition_clause = self._iceberg_partition_clause(
                    cast(Optional[Dict[str, str]], table.get(PARTITION_HINT))
                )
                # create unique tag for iceberg table so it is never recreated in the same folder
                # athena requires some kind of special cleaning (or that is a bug) so we cannot refresh
                # iceberg tables without it, by default tag is not added
                location = f"{bucket}/" + self.config.table_location_layout.format(
                    dataset_name=dataset,
                    table_name=table_prefix.rstrip("/"),
                    location_tag=uniq_id(6),
                )
                table_properties = self._iceberg_table_properties()
                logger.info(f"Will create ICEBERG table {table_name} in {location}")
                # this will fail if the table prefix is not properly defined
                sql.append(f"""{self._make_create_table(qualified_table_name, table)}
                        ({columns})
                        {partition_clause}
                        LOCATION '{location.rstrip('/')}'
                        TBLPROPERTIES ({table_properties});""")
            # elif table_format == "jsonl":
            #     sql.append(f"""CREATE EXTERNAL TABLE {qualified_table_name}
            #             ({columns})
            #             ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
            #             LOCATION '{location}';""")
            else:
                # external tables always here
                location = f"{bucket}/{dataset}/{table_prefix}"
                logger.info(f"Will create EXTERNAL table {table_name} from {location}")
                sql.append(f"""CREATE EXTERNAL TABLE {qualified_table_name}
                        ({columns})
                        STORED AS PARQUET
                        LOCATION '{location}';""")
        return sql

    def create_load_job(
        self, table: PreparedTableSchema, file_path: str, load_id: str, restore: bool = False
    ) -> LoadJob:
        """Starts SqlLoadJob for files ending with .sql or returns None to let derived classes to handle their specific jobs"""
        job = super().create_load_job(table, file_path, load_id, restore)
        if not job:
            job = (
                FinalizedLoadJobWithFollowupJobs(file_path)
                if self._is_iceberg_table(table)
                else FinalizedLoadJob(file_path)
            )
        return job

    def _create_append_followup_jobs(
        self, table_chain: Sequence[PreparedTableSchema]
    ) -> List[FollowupJobRequest]:
        if self._is_iceberg_table(table_chain[0]):
            return [SqlStagingCopyFollowupJob.from_table_chain(table_chain, self.sql_client)]
        return super()._create_append_followup_jobs(table_chain)

    def _create_replace_followup_jobs(
        self, table_chain: Sequence[PreparedTableSchema]
    ) -> List[FollowupJobRequest]:
        if self._is_iceberg_table(table_chain[0]):
            return [SqlStagingReplaceFollowupJob.from_table_chain(table_chain, self.sql_client)]
        return super()._create_replace_followup_jobs(table_chain)

    def _create_merge_followup_jobs(
        self, table_chain: Sequence[PreparedTableSchema]
    ) -> List[FollowupJobRequest]:
        return [AthenaMergeJob.from_table_chain(table_chain, self.sql_client)]

    def _is_iceberg_table(
        self, table: PreparedTableSchema, is_staging_dataset: bool = False
    ) -> bool:
        table_format = table.get("table_format")
        # all dlt tables that are not loaded via files are iceberg tables, no matter if they are on staging or regular dataset
        # all other iceberg tables are HIVE (external) tables on staging dataset
        table_format_iceberg = table_format == "iceberg"
        return (table_format_iceberg and not is_staging_dataset) or table[
            "write_disposition"
        ] == "skip"

    def should_load_data_to_staging_dataset(self, table_name: str) -> bool:
        # all iceberg tables need staging
        table = self.prepare_load_table(table_name)
        if self._is_iceberg_table(table):
            return True
        return super().should_load_data_to_staging_dataset(table_name)

    def should_truncate_table_before_load_on_staging_destination(self, table_name: str) -> bool:
        # on athena we only truncate replace tables that are not iceberg
        table = self.prepare_load_table(table_name)
        if table["write_disposition"] == "replace" and not self._is_iceberg_table(table):
            return True
        return False

    def should_load_data_to_staging_dataset_on_staging_destination(self, table_name: str) -> bool:
        """iceberg table data goes into staging on staging destination"""
        table = self.prepare_load_table(table_name)
        if self._is_iceberg_table(table):
            return True
        return super().should_load_data_to_staging_dataset_on_staging_destination(table_name)

    @staticmethod
    def is_dbapi_exception(ex: Exception) -> bool:
        return isinstance(ex, Error)
