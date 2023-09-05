import platform

from dlt.common.destination import DestinationCapabilitiesContext

import pymssql

from contextlib import contextmanager
from typing import Any, AnyStr, ClassVar, Iterator, Optional, Sequence

from dlt.destinations.exceptions import DatabaseTerminalException, DatabaseTransientException, DatabaseUndefinedRelation
from dlt.destinations.typing import DBApi, DBApiCursor, DBTransaction
from dlt.destinations.sql_client import DBApiCursorImpl, SqlClientBase, raise_database_error, raise_open_connection_error

from dlt.destinations.mssql.configuration import MsSqlCredentials
from dlt.destinations.mssql import capabilities

class PymssqlClient(SqlClientBase[pymssql.Connection], DBTransaction):

    dbapi: ClassVar[DBApi] = pymssql
    capabilities: ClassVar[DestinationCapabilitiesContext] = capabilities()

    def __init__(self, dataset_name: str, credentials: MsSqlCredentials) -> None:
        super().__init__(credentials.database, dataset_name)
        self._conn: pymssql.Connection = None
        self.credentials = credentials

    def open_connection(self) -> pymssql.Connection:
        self._conn = pymssql.connect(
            server=self.credentials.host,
            user=self.credentials.username,
            password=self.credentials.password,
            database=self.credentials.database,
            port=self.credentials.port,
            as_dict=False
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
    def native_connection(self) -> pymssql.Connection:
        return self._conn

    # @raise_database_error
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
        curr: DBApiCursor = None
        db_args = args if args else kwargs if kwargs else None
        with self._conn.cursor() as curr:
            try:
                curr.execute(query, db_args)
                yield DBApiCursorImpl(curr)  # type: ignore
            except pymssql.Error as outer:
                raise outer

    def fully_qualified_dataset_name(self, escape: bool = True) -> str:
        return self.capabilities.escape_identifier(self.dataset_name) if escape else self.dataset_name

    @classmethod
    def _make_database_exception(cls, ex: Exception) -> Exception:
        if isinstance(ex, pymssql.Error):
            if ex.args[0] == 208:
                return DatabaseUndefinedRelation(ex)
        return DatabaseTerminalException(ex)

    @staticmethod
    def is_dbapi_exception(ex: Exception) -> bool:
        return isinstance(ex, pymssql.Error)
