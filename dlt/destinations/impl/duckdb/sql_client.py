import duckdb

from contextlib import contextmanager
from typing import Any, AnyStr, ClassVar, Iterator, Optional, Sequence
from dlt.common.destination import DestinationCapabilitiesContext

from dlt.destinations.exceptions import (
    DatabaseTerminalException,
    DatabaseTransientException,
    DatabaseUndefinedRelation,
)
from dlt.destinations.typing import DBApi, DBApiCursor, DBTransaction, DataFrame
from dlt.destinations.sql_client import (
    SqlClientBase,
    DBApiCursorImpl,
    raise_database_error,
    raise_open_connection_error,
)

from dlt.destinations.impl.duckdb.configuration import DuckDbBaseCredentials


class DuckDBDBApiCursorImpl(DBApiCursorImpl):
    """Use native duckdb data frame support if available"""

    native_cursor: duckdb.DuckDBPyConnection  # type: ignore
    vector_size: ClassVar[int] = 2048

    def df(self, chunk_size: int = None, **kwargs: Any) -> DataFrame:
        if chunk_size is None:
            return self.native_cursor.df(**kwargs)
        else:
            multiple = chunk_size // self.vector_size + (
                0 if self.vector_size % chunk_size == 0 else 1
            )
            df = self.native_cursor.fetch_df_chunk(multiple, **kwargs)
            if df.shape[0] == 0:
                return None
            else:
                return df


class DuckDbSqlClient(SqlClientBase[duckdb.DuckDBPyConnection], DBTransaction):
    dbapi: ClassVar[DBApi] = duckdb

    def __init__(
        self,
        dataset_name: str,
        staging_dataset_name: str,
        credentials: DuckDbBaseCredentials,
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        super().__init__(None, dataset_name, staging_dataset_name, capabilities)
        self._conn: duckdb.DuckDBPyConnection = None
        self.credentials = credentials

    @raise_open_connection_error
    def open_connection(self) -> duckdb.DuckDBPyConnection:
        self._conn = self.credentials.borrow_conn(read_only=self.credentials.read_only)
        # TODO: apply config settings from credentials
        self._conn.execute("PRAGMA enable_checkpoint_on_shutdown;")
        config = {
            "search_path": self.fully_qualified_dataset_name(),
            "TimeZone": "UTC",
            "checkpoint_threshold": "1gb",
        }
        if config:
            for k, v in config.items():
                try:
                    # TODO: serialize str and ints, dbapi args do not work here
                    # TODO: enable various extensions ie. parquet
                    self._conn.execute(f"SET {k} = '{v}'")
                except (duckdb.CatalogException, duckdb.BinderException):
                    pass
        return self._conn

    def close_connection(self) -> None:
        if self._conn:
            self.credentials.return_conn(self._conn)
            self._conn = None

    @contextmanager
    @raise_database_error
    def begin_transaction(self) -> Iterator[DBTransaction]:
        try:
            self._conn.begin()
            yield self
            self.commit_transaction()
        except Exception:
            # in some cases duckdb rollback the transaction automatically
            try:
                self.rollback_transaction()
            except DatabaseTransientException:
                pass
            raise

    @raise_database_error
    def commit_transaction(self) -> None:
        self._conn.commit()

    @raise_database_error
    def rollback_transaction(self) -> None:
        self._conn.rollback()

    @property
    def native_connection(self) -> duckdb.DuckDBPyConnection:
        return self._conn

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
        db_args = args if args else kwargs if kwargs else None
        if db_args:
            # TODO: must provide much better refactoring of params
            query = query.replace("%s", "?")
        try:
            self._conn.execute(query, db_args)
            yield DuckDBDBApiCursorImpl(self._conn)  # type: ignore
        except duckdb.Error as outer:
            self.close_connection()
            self.open_connection()
            raise outer

    # def execute_fragments(self, batch: Sequence[AnyStr], *args: Any, **kwargs: Any) -> Optional[Sequence[Sequence[Any]]]:
    #     # execute in a loop to avoid rewrites
    #     results = []
    #     print(batch)
    #     for sql in batch:
    #         print(f"executing in dudckdb: {sql}")
    #         result = self.execute_sql(sql, args, kwargs)
    #         if result:
    #             results.extend(result)
    #     if results:
    #         return results
    #     else:
    #         return None

    @classmethod
    def _make_database_exception(cls, ex: Exception) -> Exception:
        if isinstance(ex, (duckdb.CatalogException)):
            if "already exists" in str(ex):
                raise DatabaseTerminalException(ex)
            else:
                raise DatabaseUndefinedRelation(ex)
        elif isinstance(ex, duckdb.InvalidInputException):
            if "Catalog Error" in str(ex):
                raise DatabaseUndefinedRelation(ex)
            # duckdb raises TypeError on malformed query parameters
            return DatabaseTransientException(duckdb.ProgrammingError(ex))
        elif isinstance(
            ex,
            (
                duckdb.OperationalError,
                duckdb.InternalError,
                duckdb.SyntaxException,
                duckdb.ParserException,
            ),
        ):
            return DatabaseTransientException(ex)
        elif isinstance(ex, (duckdb.DataError, duckdb.ProgrammingError, duckdb.IntegrityError)):
            return DatabaseTerminalException(ex)
        elif cls.is_dbapi_exception(ex):
            return DatabaseTransientException(ex)
        else:
            return ex

    @staticmethod
    def is_dbapi_exception(ex: Exception) -> bool:
        return isinstance(ex, duckdb.Error)
