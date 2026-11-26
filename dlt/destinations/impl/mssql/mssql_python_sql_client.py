from contextlib import contextmanager, suppress
from typing import Any, AnyStr, ClassVar, Iterator, Optional, Protocol, Sequence, Tuple

import mssql_python

from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.dataset import DBApiCursor

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

# mssql-python exceptions carry no SQLSTATE, `driver_error` is the fixed text it maps each one to
# feature request at https://github.com/microsoft/mssql-python/issues/581
TABLE_NOT_FOUND = "Base table or view not found"  # 42S02
SYNTAX_ERROR_OR_ACCESS_VIOLATION = "Syntax error or access violation"  # 42000
COUNT_FIELD_INCORRECT = "COUNT field incorrect"  # 07002
# exceptions have no native error number either, so server messages are matched
MSG_OBJECT_DOES_NOT_EXIST = "because it does not exist or you do not have permission"  # 15151
MSG_NO_TRANSACTION = "No corresponding transaction found"  # 111214


class SupportsOdbcDsn(Protocol):
    database: str
    connect_timeout: int

    def to_odbc_dsn(self) -> str: ...


class MssqlPythonSqlClient(SqlClientBase[mssql_python.Connection], DBTransaction):
    dbapi: ClassVar[DBApi] = mssql_python

    def __init__(
        self,
        dataset_name: str,
        staging_dataset_name: str,
        credentials: SupportsOdbcDsn,
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        super().__init__(credentials.database, dataset_name, staging_dataset_name, capabilities)
        self._conn: mssql_python.Connection = None
        self.credentials = credentials

    def open_connection(self) -> mssql_python.Connection:
        self._conn = mssql_python.connect(
            self.credentials.to_odbc_dsn(),
            autocommit=True,
            timeout=self.credentials.connect_timeout,
            # `uniqueidentifier` as str, like pyodbc
            native_uuid=False,
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
        try:
            self._conn.rollback()
        except mssql_python.ProgrammingError as ex:
            # there was nothing to rollback
            if MSG_NO_TRANSACTION not in str(ex.ddbc_error):
                raise
        finally:
            self._conn.autocommit = True

    @property
    def native_connection(self) -> mssql_python.Connection:
        return self._conn

    def drop_dataset(self) -> None:
        # MS Sql doesn't support DROP ... CASCADE, drop views and tables in the schema first
        rows = self.execute_sql(
            "SELECT table_name FROM INFORMATION_SCHEMA.VIEWS WHERE table_schema = %s",
            self.capabilities.casefold_identifier(self.dataset_name),
        )
        self._drop_views(*[row[0] for row in rows])
        rows = self.execute_sql(
            "SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = %s",
            self.capabilities.casefold_identifier(self.dataset_name),
        )
        self.drop_tables(*[row[0] for row in rows])
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
            return curr.fetchall()

    @contextmanager
    @raise_database_error
    def execute_query(self, query: AnyStr, *args: Any, **kwargs: Any) -> Iterator[DBApiCursor]:
        assert isinstance(query, str)
        if args:
            query = query.replace("%s", "?")
        curr = self._conn.cursor()
        try:
            if args:
                curr.execute(query, *args)
            elif kwargs:
                curr.execute(query, kwargs)
            else:
                curr.execute(query)
            yield DBApiCursorImpl(curr)  # type: ignore[arg-type]
            # errors of the remaining statements in a batch are raised here
            while curr.nextset():
                pass
        except mssql_python.Error:
            # let the server finish the batch, then rollback immediately
            with suppress(mssql_python.Error):
                while curr.nextset():
                    pass
            with suppress(mssql_python.Error):
                self._conn.rollback()
            raise
        finally:
            curr.close()

    @classmethod
    def _make_database_exception(cls, ex: Exception) -> Exception:
        if isinstance(ex, mssql_python.ProgrammingError):
            if ex.driver_error == TABLE_NOT_FOUND:
                return DatabaseUndefinedRelation(ex)
            if ex.driver_error == SYNTAX_ERROR_OR_ACCESS_VIOLATION:
                if MSG_OBJECT_DOES_NOT_EXIST in str(ex.ddbc_error):
                    return DatabaseUndefinedRelation(ex)
                return DatabaseTransientException(ex)
            if ex.driver_error == COUNT_FIELD_INCORRECT:
                return DatabaseTransientException(ex)
        elif isinstance(ex, mssql_python.OperationalError):
            return DatabaseTransientException(ex)
        return DatabaseTerminalException(ex)

    @staticmethod
    def is_dbapi_exception(ex: Exception) -> bool:
        return isinstance(ex, mssql_python.Error)

    def _limit_clause_sql(self, limit: int) -> Tuple[str, str]:
        return f"TOP ({limit})", ""
