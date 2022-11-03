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

from dlt.load.typing import DBCursor
from dlt.load.sql_client import SqlClientBase


class Psycopg2SqlClient(SqlClientBase["psycopg2.connection"]):

    def __init__(self, default_dataset_name: str, credentials: PostgresCredentials) -> None:
        super().__init__(default_dataset_name)
        self._conn: psycopg2.connection = None
        self.credentials = credentials

    def open_connection(self) -> None:
        self._conn = psycopg2.connect(
                             dsn=self.credentials.to_native_representation(),
                             options=f"-c search_path={self.fully_qualified_dataset_name()},public"
                             )
        # we'll provide explicit transactions
        self._conn.reset()
        self._conn.autocommit = True

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

    def execute_sql(self, sql: AnyStr, *args: Any, **kwargs: Any) -> Optional[Sequence[Sequence[Any]]]:
        curr: DBCursor = None
        with self._conn.cursor() as curr:
            try:
                curr.execute(sql, *args, **kwargs)
                if curr.description is None:
                    return None
                else:
                    f = curr.fetchall()
                    return f
            except psycopg2.Error as outer:
                try:
                    self._conn.rollback()
                    self._conn.reset()
                except psycopg2.Error:
                    self.close_connection()
                    self.open_connection()
                raise outer

    @contextmanager
    def execute_query(self, query: AnyStr, *args: Any, **kwargs: Any) -> Iterator[DBCursor]:
        curr: DBCursor = None
        with self._conn.cursor() as curr:
            try:
                curr.execute(query, *args, **kwargs)
                yield curr
            except psycopg2.Error as outer:
                try:
                    self._conn.rollback()
                    self._conn.reset()
                except psycopg2.Error:
                    self.close_connection()
                    self.open_connection()
                raise outer

    def fully_qualified_dataset_name(self) -> str:
        return self.default_dataset_name
