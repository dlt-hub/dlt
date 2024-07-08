import platform

from dlt.common.destination import DestinationCapabilitiesContext

if platform.python_implementation() == "PyPy":
    import psycopg2cffi as psycopg2
    from psycopg2cffi.sql import SQL, Composed, Composable
else:
    import psycopg2
    from psycopg2.sql import SQL, Composed, Composable

from contextlib import contextmanager
from typing import Any, AnyStr, ClassVar, Iterator, Optional, Sequence

from dlt.destinations.exceptions import (
    DatabaseTerminalException,
    DatabaseTransientException,
    DatabaseUndefinedRelation,
)
from dlt.destinations.typing import DBApi, DBApiCursor, DBTransaction
from dlt.destinations.sql_client import (
    DBApiCursorImpl,
    SqlClientBase,
    raise_database_error,
    raise_open_connection_error,
)

from dlt.destinations.impl.postgres.configuration import PostgresCredentials


class Psycopg2SqlClient(SqlClientBase["psycopg2.connection"], DBTransaction):
    dbapi: ClassVar[DBApi] = psycopg2

    def __init__(
        self,
        dataset_name: str,
        staging_dataset_name: str,
        credentials: PostgresCredentials,
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        super().__init__(credentials.database, dataset_name, staging_dataset_name, capabilities)
        self._conn: psycopg2.connection = None
        self.credentials = credentials

    def open_connection(self) -> "psycopg2.connection":
        self._conn = psycopg2.connect(
            dsn=self.credentials.to_native_representation(),
            options=f"-c search_path={self.fully_qualified_dataset_name()},public",
        )
        # we'll provide explicit transactions see _reset
        self._reset_connection()
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
        self._conn.rollback()
        self._conn.autocommit = True

    @property
    def native_connection(self) -> "psycopg2.connection":
        return self._conn

    # @raise_database_error
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
        curr: DBApiCursor = None
        db_args = args if args else kwargs if kwargs else None
        with self._conn.cursor() as curr:
            try:
                curr.execute(query, db_args)
                yield DBApiCursorImpl(curr)  # type: ignore
            except psycopg2.Error as outer:
                try:
                    self._reset_connection()
                except psycopg2.Error:
                    self.close_connection()
                    self.open_connection()
                raise outer

    def execute_fragments(
        self, fragments: Sequence[AnyStr], *args: Any, **kwargs: Any
    ) -> Optional[Sequence[Sequence[Any]]]:
        # compose the statements using psycopg2 library
        composed = Composed(sql if isinstance(sql, Composable) else SQL(sql) for sql in fragments)
        return self.execute_sql(composed, *args, **kwargs)

    def _reset_connection(self) -> None:
        # self._conn.autocommit = True
        self._conn.reset()
        self._conn.autocommit = True

    @classmethod
    def _make_database_exception(cls, ex: Exception) -> Exception:
        if isinstance(ex, (psycopg2.errors.UndefinedTable, psycopg2.errors.InvalidSchemaName)):
            raise DatabaseUndefinedRelation(ex)
        if isinstance(
            ex,
            (
                psycopg2.OperationalError,
                psycopg2.InternalError,
                psycopg2.errors.SyntaxError,
                psycopg2.errors.UndefinedFunction,
            ),
        ):
            term = cls._maybe_make_terminal_exception_from_data_error(ex)
            if term:
                return term
            else:
                return DatabaseTransientException(ex)
        elif isinstance(
            ex, (psycopg2.DataError, psycopg2.ProgrammingError, psycopg2.IntegrityError)
        ):
            return DatabaseTerminalException(ex)
        elif isinstance(ex, TypeError):
            # psycopg2 raises TypeError on malformed query parameters
            return DatabaseTransientException(psycopg2.ProgrammingError(ex))
        elif cls.is_dbapi_exception(ex):
            return DatabaseTransientException(ex)
        else:
            return ex

    @staticmethod
    def _maybe_make_terminal_exception_from_data_error(
        pg_ex: psycopg2.DataError,
    ) -> Optional[Exception]:
        return None

    @staticmethod
    def is_dbapi_exception(ex: Exception) -> bool:
        return isinstance(ex, psycopg2.Error)
