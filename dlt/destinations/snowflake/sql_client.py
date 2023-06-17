from contextlib import contextmanager
from typing import Any, AnyStr, ClassVar, Iterator, Optional, Sequence, List

import snowflake.connector as snowflake_lib

from dlt.common.destination import DestinationCapabilitiesContext
from dlt.destinations.exceptions import DatabaseTerminalException, DatabaseTransientException, DatabaseUndefinedRelation
from dlt.destinations.sql_client import DBApiCursorImpl, SqlClientBase, raise_database_error, raise_open_connection_error
from dlt.destinations.typing import DBApi, DBApiCursor, DBTransaction
from dlt.destinations.snowflake.configuration import SnowflakeCredentials
from dlt.destinations.snowflake import capabilities


class SnowflakeSqlClient(SqlClientBase[snowflake_lib.SnowflakeConnection], DBTransaction):

    dbapi: ClassVar[DBApi] = snowflake_lib
    capabilities: ClassVar[DestinationCapabilitiesContext] = capabilities()

    def __init__(self, dataset_name: str, credentials: SnowflakeCredentials) -> None:
        super().__init__(dataset_name)
        self._conn: snowflake_lib.SnowflakeConnection = None
        self.credentials = credentials

    def open_connection(self) -> snowflake_lib.SnowflakeConnection:
        self._conn = snowflake_lib.connect(
            user=self.credentials.username,
            password=self.credentials.password,
            account=self.credentials.host,
            warehouse=self.credentials.warehouse,
            database=self.credentials.database,
            schema=self.fully_qualified_dataset_name()
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
            self._conn.autocommit(False)
            yield self
            self.commit_transaction()
        except Exception:
            self.rollback_transaction()
            raise

    @raise_database_error
    def commit_transaction(self) -> None:
        self._conn.commit()
        self._conn.autocommit(True)

    @raise_database_error
    def rollback_transaction(self) -> None:
        self._conn.rollback()
        self._conn.autocommit(True)

    @property
    def native_connection(self) -> "snowflake_lib.SnowflakeConnection":
        return self._conn

    def has_dataset(self) -> bool:
        query = """
                SELECT 1
                    FROM INFORMATION_SCHEMA.SCHEMATA
                    WHERE schema_name = %s;
                """
        rows = self.execute_sql(query, self.fully_qualified_dataset_name(escape=False))
        return len(rows) > 0

    def create_dataset(self) -> None:
        self.execute_sql("CREATE SCHEMA %s" % self.fully_qualified_dataset_name())

    def drop_dataset(self) -> None:
        self.execute_sql("DROP SCHEMA %s CASCADE;" % self.fully_qualified_dataset_name())

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
        with self._conn.cursor() as curr:  # type: ignore[assignment]
            try:
                curr.execute(query, db_args)
                yield SnowflakeCursorImpl(curr)  # type: ignore[abstract]
            except snowflake_lib.Error as outer:
                try:
                    self._reset_connection()
                except snowflake_lib.Error:
                    self.close_connection()
                    self.open_connection()
                raise outer

    def execute_fragments(self, fragments: Sequence[AnyStr], *args: Any, **kwargs: Any) -> Optional[Sequence[Sequence[Any]]]:
        results: List[Sequence[Any]] = []
        for statement in fragments:
            result = self.execute_sql(statement, *args, **kwargs)
            if result:
                results.append(result)
        return results or None

    def fully_qualified_dataset_name(self, escape: bool = True) -> str:
        # Always escape for uppercase
        if escape:
            return self.capabilities.escape_identifier(self.dataset_name)
        return self.dataset_name.upper()

    def _reset_connection(self) -> None:
        self._conn.rollback()
        self._conn.autocommit(True)

    @classmethod
    def _make_database_exception(cls, ex: Exception) -> Exception:
        if isinstance(ex, snowflake_lib.errors.ProgrammingError):
            if ex.sqlstate in {'42S02', '02000'}:
                return DatabaseUndefinedRelation(ex)
            elif ex.sqlstate == '22023':  # Adding non-nullable no-default column
                return DatabaseTerminalException(ex)
            elif ex.sqlstate == '42000' and ex.errno == 904:  # Invalid identifier
                return DatabaseTerminalException(ex)
            elif ex.sqlstate == "22000":
                return DatabaseTerminalException(ex)
            else:
                return DatabaseTransientException(ex)
        elif isinstance(ex, snowflake_lib.errors.IntegrityError):
            raise DatabaseTerminalException(ex)
        elif isinstance(ex, snowflake_lib.errors.DatabaseError):
            term = cls._maybe_make_terminal_exception_from_data_error(ex)
            if term:
                return term
            else:
                return DatabaseTransientException(ex)
        elif isinstance(ex, TypeError):
            # snowflake raises TypeError on malformed query parameters
            return DatabaseTransientException(snowflake_lib.errors.ProgrammingError(str(ex)))
        elif cls.is_dbapi_exception(ex):
            return DatabaseTransientException(ex)
        else:
            return ex

    @staticmethod
    def _maybe_make_terminal_exception_from_data_error(snowflake_ex: snowflake_lib.DatabaseError) -> Optional[Exception]:
        return None

    @staticmethod
    def is_dbapi_exception(ex: Exception) -> bool:
        return isinstance(ex, snowflake_lib.DatabaseError)


class SnowflakeCursorImpl(DBApiCursorImpl):
    def _get_columns(self) -> List[str]:
        return [c[0].lower() for c in self.native_cursor.description]
