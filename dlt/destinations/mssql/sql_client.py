import platform
import struct
from datetime import datetime, timedelta, timezone  # noqa: I251

from dlt.common.destination import DestinationCapabilitiesContext

import pyodbc

from contextlib import contextmanager
from typing import Any, AnyStr, ClassVar, Iterator, Optional, Sequence

from dlt.destinations.exceptions import DatabaseTerminalException, DatabaseTransientException, DatabaseUndefinedRelation
from dlt.destinations.typing import DBApi, DBApiCursor, DBTransaction
from dlt.destinations.sql_client import DBApiCursorImpl, SqlClientBase, raise_database_error, raise_open_connection_error

from dlt.destinations.mssql.configuration import MsSqlCredentials
from dlt.destinations.mssql import capabilities


def handle_datetimeoffset(dto_value: bytes) -> datetime:
    # ref: https://github.com/mkleehammer/pyodbc/issues/134#issuecomment-281739794
    tup = struct.unpack("<6hI2h", dto_value)  # e.g., (2017, 3, 16, 10, 35, 18, 500000000, -6, 0)
    return datetime(
        tup[0], tup[1], tup[2], tup[3], tup[4], tup[5], tup[6] // 1000, timezone(timedelta(hours=tup[7], minutes=tup[8]))
    )


class PymssqlClient(SqlClientBase[pyodbc.Connection], DBTransaction):

    dbapi: ClassVar[DBApi] = pyodbc
    capabilities: ClassVar[DestinationCapabilitiesContext] = capabilities()

    def __init__(self, dataset_name: str, credentials: MsSqlCredentials) -> None:
        super().__init__(credentials.database, dataset_name)
        self._conn: pyodbc.Connection = None
        self.credentials = credentials

    def open_connection(self) -> pyodbc.Connection:
        self._conn = pyodbc.connect(
            driver="{ODBC Driver 17 for SQL Server}",
            server=self.credentials.host,
            uid=self.credentials.username,
            pwd=self.credentials.password,
            database=self.credentials.database,
            port=self.credentials.port,
        )
        # https://github.com/mkleehammer/pyodbc/wiki/Using-an-Output-Converter-function
        self._conn.add_output_converter(-155, handle_datetimeoffset)
        return self._conn

    @raise_open_connection_error
    def close_connection(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    @contextmanager
    def begin_transaction(self) -> Iterator[DBTransaction]:
        try:
            yield self
            self.commit_transaction()
        except Exception:
            self.rollback_transaction()
            raise

    @raise_database_error
    def commit_transaction(self) -> None:
        self._conn.commit()

    @raise_database_error
    def rollback_transaction(self) -> None:
        self._conn.rollback()

    @property
    def native_connection(self) -> pyodbc.Connection:
        return self._conn

    def execute_sql(self, sql: AnyStr, *args: Any, **kwargs: Any) -> Optional[Sequence[Sequence[Any]]]:
        with self.execute_query(sql, *args, **kwargs) as curr:
            if curr.description is None:
                return None
            else:
                f = curr.fetchall()
                return f

    @contextmanager
    @raise_database_error
    def execute_query(self, query: AnyStr, *args: Any, **kwargs: Any) -> Iterator[DBApiCursor]:
        assert isinstance(query, str)
        curr: DBApiCursor = None
        if kwargs:
            raise NotImplementedError("pyodbc does not support named parameters in queries")
        if args:
            # TODO: this is bad. See duckdb & athena also
            query = query.replace("%s", "?")
        with self._conn.cursor() as curr:
            try:
                # unpack because empty tuple gets interpreted as a single argument
                # https://github.com/mkleehammer/pyodbc/wiki/Features-beyond-the-DB-API#passing-parameters
                curr.execute(query, *args)
                yield DBApiCursorImpl(curr)  # type: ignore
            except pyodbc.Error as outer:
                raise outer

    def fully_qualified_dataset_name(self, escape: bool = True) -> str:
        return self.capabilities.escape_identifier(self.dataset_name) if escape else self.dataset_name

    @classmethod
    def _make_database_exception(cls, ex: Exception) -> Exception:
        # TODO: pyodb errors
        return DatabaseTerminalException(ex)
        if isinstance(ex, pyodbc.ProgrammingError):
            if ex.args[0] == 208:
                return DatabaseUndefinedRelation(ex)
        elif isinstance(ex, pyodbc.OperationalError):
            return DatabaseTransientException(ex)
        return DatabaseTerminalException(ex)

    @staticmethod
    def is_dbapi_exception(ex: Exception) -> bool:
        return isinstance(ex, pyodbc.Error)
