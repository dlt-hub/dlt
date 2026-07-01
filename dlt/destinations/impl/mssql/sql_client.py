from dlt.common.destination import DestinationCapabilitiesContext

import mssql_python

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


class MsSqlClient(SqlClientBase[mssql_python.Connection], DBTransaction):
    dbapi: ClassVar[DBApi] = mssql_python

    def __init__(
        self,
        dataset_name: str,
        staging_dataset_name: str,
        credentials: MsSqlCredentials,
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        super().__init__(credentials.database, dataset_name, staging_dataset_name, capabilities)
        self._conn: mssql_python.Connection = None
        self.credentials = credentials

    def open_connection(self) -> mssql_python.Connection:
        # mssql-python bundles its own driver, so the connection string carries no DRIVER, and it
        # signs in for every supported Entra ID authentication method itself from the
        # `Authentication=` DSN keyword — dlt injects no `attrs_before` for those.
        #
        # mssql-python auto-enables connection pooling (default: 100 connections, 600s idle
        # timeout) on the first connection any process opens, unless the application calls
        # `mssql_python.pooling()` first — which dlt does not do, since these defaults are
        # already sane for our workload. The pool matches purely on connection-string text, so
        # our credentials building a stable DSN is what makes reuse actually happen.
        self._conn = mssql_python.connect(
            self.credentials.to_odbc_dsn(),
            autocommit=True,
            attrs_before=self.credentials.to_odbc_attrs_before(),  # type: ignore[arg-type]
            timeout=self.credentials.connect_timeout,
        )
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
        # mssql-python treats a rollback without an active transaction as a no-op.
        try:
            self._conn.rollback()
        finally:
            self._conn.autocommit = True

    @property
    def native_connection(self) -> mssql_python.Connection:
        return self._conn

    def drop_dataset(self) -> None:
        # MS Sql doesn't support DROP ... CASCADE, drop tables in the schema first
        # Drop all views
        rows = self.execute_sql(
            "SELECT table_name FROM INFORMATION_SCHEMA.VIEWS WHERE table_schema = %s",
            self.capabilities.casefold_identifier(self.dataset_name),
        )
        view_names = [row[0] for row in rows]
        self._drop_views(*view_names)
        # Drop all tables
        rows = self.execute_sql(
            "SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = %s",
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
        if args:
            # dlt emits %s positional placeholders; mssql-python expects qmark (?)
            # TODO: this is bad. See duckdb & athena also
            query = query.replace("%s", "?")
        # NOTE: do not convert it into context manager. it does not close the cursor!
        curr = self._conn.cursor()
        try:
            if kwargs:
                # mssql-python's paramstyle is pyformat: pass named parameters (%(name)s) through
                curr.execute(query, kwargs)
            else:
                # unpack because empty tuple gets interpreted as a single argument
                curr.execute(query, *args)
            # NOTE: firsts recordset is wrapped in a cursor
            yield DBApiCursorImpl(curr)  # type: ignore[arg-type]
            # clear all pending result sets
            try:
                while curr.nextset():
                    pass
            except mssql_python.Error:
                pass
        except mssql_python.Error:
            # clear all pending result sets
            try:
                while curr.nextset():
                    pass
            except mssql_python.Error:
                pass
            # immediately rollback transaction
            try:
                self._conn.rollback()
            except mssql_python.Error:
                pass
            raise
        finally:
            # clear all pending result sets
            while curr.nextset():
                pass
            # always close cursor
            curr.close()

    @classmethod
    def _make_database_exception(cls, ex: Exception) -> Exception:
        # mssql-python maps the SQLSTATE to a stable `driver_error` label, which we classify on
        # (the ddbc_error message is server/locale dependent, the label is not).
        driver_error = getattr(ex, "driver_error", "")
        if isinstance(ex, mssql_python.ProgrammingError):
            if driver_error == "Base table or view not found":  # SQLSTATE 42S02
                return DatabaseUndefinedRelation(ex)
            if driver_error == "Syntax error or access violation":  # SQLSTATE 42000
                # error 15151 ("Cannot drop the ... because it does not exist") shares this
                # SQLSTATE with real syntax errors; mssql-python drops the error number from the
                # message, so match on the text as well.
                msg = str(ex)
                if "(15151)" in msg or "does not exist" in msg:
                    return DatabaseUndefinedRelation(ex)
                return DatabaseTransientException(ex)
            if driver_error == "COUNT field incorrect":  # SQLSTATE 07002, wrong parameter count
                return DatabaseTransientException(ex)
            return DatabaseTerminalException(ex)
        if isinstance(ex, mssql_python.OperationalError):
            return DatabaseTransientException(ex)
        return DatabaseTerminalException(ex)

    @staticmethod
    def is_dbapi_exception(ex: Exception) -> bool:
        return isinstance(ex, mssql_python.Error)

    def _limit_clause_sql(self, limit: int) -> Tuple[str, str]:
        return f"TOP ({limit})", ""


# Backwards-compatible alias: this client now uses the mssql-python driver instead of pyodbc.
PyOdbcMsSqlClient = MsSqlClient
