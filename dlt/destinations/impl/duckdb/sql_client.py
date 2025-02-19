import duckdb
import math
from contextlib import contextmanager
from typing import Any, AnyStr, ClassVar, Iterator, Optional, Sequence, Generator

from dlt.common import logger
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.destinations.exceptions import (
    DatabaseTerminalException,
    DatabaseTransientException,
    DatabaseUndefinedRelation,
)
from dlt.destinations.typing import DBApi, DBTransaction, DataFrame, ArrowTable
from dlt.destinations.sql_client import (
    SqlClientBase,
    DBApiCursorImpl,
    raise_database_error,
    raise_open_connection_error,
)

from dlt.destinations.impl.duckdb.configuration import DuckDbBaseCredentials
from dlt.common.destination.dataset import DBApiCursor


class DuckDBDBApiCursorImpl(DBApiCursorImpl):
    """Use native duckdb data frame support if available"""

    native_cursor: duckdb.DuckDBPyConnection  # type: ignore
    vector_size: ClassVar[int] = 2048  # vector size is 2048

    def _get_page_count(self, chunk_size: int) -> int:
        """get the page count for vector size"""
        if chunk_size < self.vector_size:
            return 1
        return math.floor(chunk_size / self.vector_size)

    def iter_df(self, chunk_size: int) -> Generator[DataFrame, None, None]:
        # full frame
        if not chunk_size:
            yield self.native_cursor.fetch_df()
            return
        # iterate
        while True:
            df = self.native_cursor.fetch_df_chunk(self._get_page_count(chunk_size))
            if df.shape[0] == 0:
                break
            yield df

    def iter_arrow(self, chunk_size: int) -> Generator[ArrowTable, None, None]:
        if not chunk_size:
            yield self.native_cursor.fetch_arrow_table()
            return
        # iterate
        for item in self.native_cursor.fetch_record_batch(chunk_size):
            yield ArrowTable.from_batches([item])


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

    def warn_if_catalog_equals_dataset_name(self) -> None:
        """
        Checks if the DuckDB connection's current catalog equals the dataset name (schema).
        """
        try:
            # First try to get the catalog via a function that (if available) returns the current database.
            result = self._conn.execute("SELECT current_database()").fetchone()
            if result and len(result) > 0:
                catalog = result[0]
            else:
                # fallback: use PRAGMA database_list to fetch the first (default) database name.
                result = self._conn.execute("PRAGMA database_list").fetchone()
                catalog = result[0] if result else None
        except Exception:
            return

        if catalog is None:
            return

        if catalog == self.dataset_name:
            logger.warning(
                "The current catalog (database name) '%s' is identical to the dataset name '%s'."
                " This may lead to confusion in the DuckDB binder. Consider using distinct names."
                " Most typically you use the same name for your pipeline and dataset or the same"
                " name for your destination and the dataset.",
                catalog,
                self.dataset_name,
            )

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
