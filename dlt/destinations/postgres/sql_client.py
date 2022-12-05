import platform

if platform.python_implementation() == "PyPy":
    import psycopg2cffi as psycopg2
    from psycopg2cffi.sql import SQL, Identifier, Literal as SQLLiteral
else:
    import psycopg2
    from psycopg2.sql import SQL, Identifier, Literal as SQLLiteral

from contextlib import contextmanager
from typing import Any, AnyStr, Iterator, Optional, Sequence

from dlt.common.configuration.specs import PostgresCredentials

from dlt.destinations.exceptions import DatabaseTerminalException, DatabaseTransientException, DatabaseUndefinedRelation
from dlt.destinations.typing import DBCursor
from dlt.destinations.sql_client import SqlClientBase, raise_database_error, raise_open_connection_error


class Psycopg2SqlClient(SqlClientBase["psycopg2.connection"]):

    def __init__(self, dataset_name: str, credentials: PostgresCredentials) -> None:
        super().__init__(dataset_name)
        self._conn: psycopg2.connection = None
        self.credentials = credentials

    def open_connection(self) -> None:
        self._conn = psycopg2.connect(
                             dsn=self.credentials.to_native_representation(),
                             options=f"-c search_path={self.fully_qualified_dataset_name()},public"
                             )
        # we'll provide explicit transactions see _reset
        self._reset_connection()

    @raise_open_connection_error
    def close_connection(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    @property
    def native_connection(self) -> "psycopg2.connection":
        return self._conn

    def has_dataset(self) -> bool:
        query = """
                SELECT 1
                    FROM INFORMATION_SCHEMA.SCHEMATA
                    WHERE schema_name = {};
                """
        rows = self.execute_sql(SQL(query).format(SQLLiteral(self.fully_qualified_dataset_name())))
        return len(rows) > 0

    def create_dataset(self) -> None:
        self.execute_sql(
            SQL("CREATE SCHEMA {};").format(Identifier(self.fully_qualified_dataset_name()))
            )

    def drop_dataset(self) -> None:
        self.execute_sql(
            SQL("DROP SCHEMA {} CASCADE;").format(Identifier(self.fully_qualified_dataset_name()))
            )

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
    def execute_query(self, query: AnyStr, *args: Any, **kwargs: Any) -> Iterator[DBCursor]:
        curr: DBCursor = None
        db_args = args if args else kwargs if kwargs else None
        with self._conn.cursor() as curr:
            try:
                curr.execute(query, db_args)
                yield curr
            except psycopg2.Error as outer:
                try:
                    self._conn.rollback()
                    self._reset_connection()
                except psycopg2.Error:
                    self.close_connection()
                    self.open_connection()

                raise outer

    def fully_qualified_dataset_name(self) -> str:
        return self.dataset_name

    def _reset_connection(self) -> None:
        # self._conn.autocommit = True
        self._conn.reset()
        self._conn.autocommit = True

    @classmethod
    def _make_database_exception(cls, ex: Exception) -> Exception:
        if isinstance(ex, (psycopg2.errors.UndefinedTable, psycopg2.errors.InvalidSchemaName)):
            raise DatabaseUndefinedRelation(ex)
        elif isinstance(ex, (psycopg2.OperationalError, psycopg2.InternalError, psycopg2.errors.SyntaxError, psycopg2.errors.UndefinedFunction)):
            term = cls._maybe_make_terminal_exception_from_data_error(ex)
            if term:
                return term
            else:
                return DatabaseTransientException(ex)
        elif isinstance(ex, (psycopg2.DataError, psycopg2.ProgrammingError, psycopg2.IntegrityError)):
            return DatabaseTerminalException(ex)
        elif isinstance(ex, TypeError):
            # psycopg2 raises TypeError on malformed query parameters
            return DatabaseTransientException(psycopg2.ProgrammingError(ex))
        elif cls.is_dbapi_exception(ex):
            return DatabaseTransientException(ex)
        else:
            return ex

    @staticmethod
    def _maybe_make_terminal_exception_from_data_error(pg_ex: psycopg2.DataError) -> Optional[Exception]:
        return None

    @staticmethod
    def is_dbapi_exception(ex: Exception) -> bool:
        return isinstance(ex, psycopg2.Error)
