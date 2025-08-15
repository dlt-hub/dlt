import struct
from datetime import datetime, timedelta, timezone  # noqa: I251

from dlt.common.destination import DestinationCapabilitiesContext

import pyodbc

from contextlib import contextmanager
from typing import Any, AnyStr, ClassVar, Iterator, Optional, Sequence, Tuple

from dlt.destinations.exceptions import (
    DatabaseTerminalException,
    DatabaseTransientException,
    DatabaseUndefinedRelation,
)
from dlt.destinations.typing import DBApi, DBTransaction
from dlt.destinations.sql_client import (
    DBApiCursorImpl,
    SqlClientBase,
    raise_database_error,
    raise_open_connection_error,
)

from dlt.destinations.impl.mssql.configuration import MsSqlCredentials
from dlt.common.destination.dataset import DBApiCursor


def handle_datetimeoffset(dto_value: bytes) -> datetime:
    # ref: https://github.com/mkleehammer/pyodbc/issues/134#issuecomment-281739794
    tup = struct.unpack("<6hI2h", dto_value)  # e.g., (2017, 3, 16, 10, 35, 18, 500000000, -6, 0)
    return datetime(
        tup[0],
        tup[1],
        tup[2],
        tup[3],
        tup[4],
        tup[5],
        tup[6] // 1000,
        timezone(timedelta(hours=tup[7], minutes=tup[8])),
    )


class PyOdbcMsSqlClient(SqlClientBase[pyodbc.Connection], DBTransaction):
    dbapi: ClassVar[DBApi] = pyodbc

    def __init__(
        self,
        dataset_name: str,
        staging_dataset_name: str,
        credentials: MsSqlCredentials,
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        super().__init__(credentials.database, dataset_name, staging_dataset_name, capabilities)
        self._conn: pyodbc.Connection = None
        self.credentials = credentials

    def open_connection(self) -> pyodbc.Connection:
        self._conn = pyodbc.connect(
            self.credentials.to_odbc_dsn(),
            timeout=self.credentials.connect_timeout,
        )
        # https://github.com/mkleehammer/pyodbc/wiki/Using-an-Output-Converter-function
        self._conn.add_output_converter(-155, handle_datetimeoffset)
        self._conn.autocommit = True
        return self._conn

    @raise_open_connection_error
    def close_connection(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    @contextmanager
    def begin_transaction(self) -> Iterator[DBTransaction]:
        try:
            self._conn.autocommit = False
            yield self
            self.commit_transaction()
        except Exception:
            self.rollback_transaction()
            raise

    @raise_database_error
    def commit_transaction(self) -> None:
        self._conn.commit()
        self._conn.autocommit = True

    @raise_database_error
    def rollback_transaction(self) -> None:
        try:
            self._conn.rollback()
        except pyodbc.ProgrammingError as ex:
            if (
                ex.args[0] == "42000" and "(111214)" in ex.args[1]
            ):  # "no corresponding transaction found"
                pass  # there was nothing to rollback, we silently ignore the error
            else:
                raise
        finally:
            self._conn.autocommit = True

    @property
    def native_connection(self) -> pyodbc.Connection:
        return self._conn

    def drop_dataset(self) -> None:
        # MS Sql doesn't support DROP ... CASCADE, drop tables in the schema first
        # Drop all views
        rows = self.execute_sql(
            "SELECT table_name FROM information_schema.views WHERE table_schema = %s",
            self.capabilities.casefold_identifier(self.dataset_name),
        )
        view_names = [row[0] for row in rows]
        self._drop_views(*view_names)
        # Drop all tables
        rows = self.execute_sql(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = %s",
            self.capabilities.casefold_identifier(self.dataset_name),
        )
        table_names = [row[0] for row in rows]
        self.drop_tables(*table_names)
        # Drop schema
        self._drop_schema()

    def _drop_views(self, *tables: str) -> None:
        if not tables:
            return
        statements = [
            f"DROP VIEW IF EXISTS {self.make_qualified_table_name(table)}" for table in tables
        ]
        self.execute_many(statements)

    def _drop_schema(self) -> None:
        self.execute_sql("DROP SCHEMA %s" % self.fully_qualified_dataset_name())

    def execute_sql(
        self, sql: AnyStr, *args: Any, **kwargs: Any
    ) -> Optional[Sequence[Sequence[Any]]]:
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
        if kwargs:
            raise NotImplementedError("pyodbc does not support named parameters in queries")
        if args:
            # TODO: this is bad. See duckdb & athena also
            query = query.replace("%s", "?")
        # NOTE: do not convert it into context manager. it does not close the cursor!
        curr = self._conn.cursor()
        try:
            # unpack because empty tuple gets interpreted as a single argument
            # https://github.com/mkleehammer/pyodbc/wiki/Features-beyond-the-DB-API#passing-parameters
            curr.execute(query, *args)
            # # NOTE: firsts recordset is wrapped in a cursor
            yield DBApiCursorImpl(curr)
            # clear all pending result sets
            try:
                while curr.nextset():
                    pass
            except pyodbc.Error:
                pass
        except pyodbc.Error as outer:
            # clear all pending result sets
            try:
                while curr.nextset():
                    pass
            except pyodbc.Error:
                pass
            # immediately rollback transaction
            try:
                self._conn.rollback()
            except pyodbc.Error:
                pass
            raise outer
        finally:
            # always close cursor
            curr.close()

    @classmethod
    def _make_database_exception(cls, ex: Exception) -> Exception:
        if isinstance(ex, pyodbc.ProgrammingError):
            if ex.args[0] == "42S02":
                return DatabaseUndefinedRelation(ex)
            # certain pyodbc exceptions do not have second argument
            if len(ex.args) > 1:
                if ex.args[1] == "HY000":
                    return DatabaseTransientException(ex)
                elif ex.args[0] == "42000":
                    if "(15151)" in ex.args[1]:
                        return DatabaseUndefinedRelation(ex)
                    return DatabaseTransientException(ex)
        elif isinstance(ex, pyodbc.OperationalError):
            return DatabaseTransientException(ex)
        elif isinstance(ex, pyodbc.Error):
            if ex.args[0] == "07002":  # incorrect number of arguments supplied
                return DatabaseTransientException(ex)
        return DatabaseTerminalException(ex)

    @staticmethod
    def is_dbapi_exception(ex: Exception) -> bool:
        return isinstance(ex, pyodbc.Error)

    def _limit_clause_sql(self, limit: int) -> Tuple[str, str]:
        return f"TOP ({limit})", ""
