from typing import (
    Optional,
    ClassVar,
    Iterator,
    Any,
    AnyStr,
    Sequence,
    Tuple,
    Dict,
    Callable,
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
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.dataset import DBApiCursor
from dlt.common.data_writers.escape import escape_hive_identifier

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
from dlt.destinations.impl.athena.configuration import AthenaClientConfiguration


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
        # athena uses hive escaping
        return escape_hive_identifier(v)

    def fully_qualified_ddl_dataset_name(self) -> str:
        return self.escape_ddl_identifier(self.dataset_name)

    def make_qualified_ddl_table_name(self, table_name: str) -> str:
        table_name = self.escape_ddl_identifier(table_name)
        return f"{self.fully_qualified_ddl_dataset_name()}.{table_name}"

    def create_dataset(self) -> None:
        db_location_clause = (
            f" LOCATION '{self.config.db_location}'" if self.config.db_location else ""
        )
        # HIVE escaping for DDL
        self.execute_sql(
            f"CREATE DATABASE {self.fully_qualified_ddl_dataset_name()}{db_location_clause}"
        )

    def drop_dataset(self) -> None:
        self.execute_sql(f"DROP DATABASE {self.fully_qualified_ddl_dataset_name()} CASCADE")

    def drop_tables(self, *tables: str) -> None:
        if not tables:
            return
        statements = [
            f"DROP TABLE IF EXISTS {self.make_qualified_ddl_table_name(table)}" for table in tables
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
            yield DBApiCursorImpl(cursor)
