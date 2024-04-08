from contextlib import contextmanager
from typing import (
    Iterator,
    AnyStr,
    Any,
    Optional,
    Sequence,
    ClassVar,
)

import clickhouse_driver  # type: ignore[import-untyped]
import clickhouse_driver.errors  # type: ignore[import-untyped]
from clickhouse_driver.dbapi import OperationalError  # type: ignore[import-untyped]
from clickhouse_driver.dbapi.extras import DictCursor  # type: ignore[import-untyped]

import dlt
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.destinations.exceptions import (
    DatabaseUndefinedRelation,
    DatabaseTransientException,
    DatabaseTerminalException,
)
from dlt.destinations.impl.clickhouse import capabilities
from dlt.destinations.impl.clickhouse.configuration import ClickHouseCredentials
from dlt.destinations.sql_client import (
    DBApiCursorImpl,
    SqlClientBase,
    raise_database_error,
    raise_open_connection_error,
)
from dlt.destinations.typing import DBTransaction, DBApi
from dlt.destinations.utils import _convert_to_old_pyformat


TRANSACTIONS_UNSUPPORTED_WARNING_MESSAGE = (
    "ClickHouse does not support transactions! Each statement is auto-committed separately."
)


class ClickHouseDBApiCursorImpl(DBApiCursorImpl):
    native_cursor: DictCursor


class ClickHouseSqlClient(
    SqlClientBase[clickhouse_driver.dbapi.connection.Connection], DBTransaction
):
    dbapi: ClassVar[DBApi] = clickhouse_driver.dbapi.connection.Connection
    capabilities: ClassVar[DestinationCapabilitiesContext] = capabilities()

    def __init__(self, dataset_name: str, credentials: ClickHouseCredentials) -> None:
        super().__init__(credentials.database, dataset_name)
        self._conn: clickhouse_driver.dbapi.connection = None
        self.credentials = credentials
        self.database_name = credentials.database

    def open_connection(self) -> clickhouse_driver.dbapi.connection.Connection:
        self._conn = clickhouse_driver.dbapi.connect(
            dsn=self.credentials.to_native_representation()
        )
        return self._conn

    @raise_open_connection_error
    def close_connection(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    @contextmanager
    @raise_database_error
    def begin_transaction(self) -> Iterator[DBTransaction]:
        yield self

    @raise_database_error
    def commit_transaction(self) -> None:
        self._conn.commit()

    @raise_database_error
    def rollback_transaction(self) -> None:
        self._conn.rollback()

    @property
    def native_connection(self) -> clickhouse_driver.dbapi.connection.Connection:
        return self._conn

    def execute_sql(
        self, sql: AnyStr, *args: Any, **kwargs: Any
    ) -> Optional[Sequence[Sequence[Any]]]:
        with self.execute_query(sql, *args, **kwargs) as curr:
            return None if curr.description is None else curr.fetchall()

    def create_dataset(self) -> None:
        # ClickHouse doesn't have schemas.
        pass

    def drop_dataset(self) -> None:
        # Since ClickHouse doesn't have schemas, we need to drop all tables in our virtual schema,
        # or collection of tables, that has the `dataset_name` as a prefix.
        to_drop_results = self.execute_sql(
            """
            SELECT name
            FROM system.tables
            WHERE database = %s
            AND name LIKE %s
            """,
            (
                self.database_name,
                f"{self.dataset_name}%",
            ),
        )
        for to_drop_result in to_drop_results:
            table = to_drop_result[0]
            # The "DROP TABLE" clause is discarded if we allow clickhouse_driver to handle parameter substitution.
            # This is because the driver incorrectly substitutes the entire query string, causing the "DROP TABLE" keyword to be omitted.
            # To resolve this, we are forced to provide the full query string here.
            self.execute_sql(
                f"""DROP TABLE {self.capabilities.escape_identifier(self.database_name)}.{self.capabilities.escape_identifier(table)} SYNC"""
            )

    @contextmanager
    @raise_database_error
    def execute_query(
        self, query: AnyStr, *args: Any, **kwargs: Any
    ) -> Iterator[ClickHouseDBApiCursorImpl]:
        assert isinstance(query, str), "Query must be a string"

        db_args = kwargs.copy()

        if args:
            query, db_args = _convert_to_old_pyformat(query, args, OperationalError)
            db_args.update(kwargs)

        # Prefix each query transaction with experimental settings.
        # These are necessary for nested datatypes to be available and other operations to work.
        query = (
            "set allow_experimental_lightweight_delete = 1;"
            "set allow_experimental_object_type = 1;"
            "set enable_http_compression= 1;"
            f"{query}"
        )
        with self._conn.cursor() as cursor:
            for query_line in query.split(";"):
                if query_line := query_line.strip():
                    try:
                        cursor.execute(query_line, db_args)
                    except KeyError as e:
                        raise DatabaseTransientException(OperationalError()) from e

            yield ClickHouseDBApiCursorImpl(cursor)  # type: ignore[abstract]

    def fully_qualified_dataset_name(self, escape: bool = True) -> str:
        if escape:
            database_name = self.capabilities.escape_identifier(self.database_name)
            dataset_name = self.capabilities.escape_identifier(f"{self.dataset_name}")
        else:
            database_name = self.database_name
            dataset_name = f"{self.dataset_name}"
        return f"{database_name}.{dataset_name}"

    def make_qualified_table_name(self, table_name: str, escape: bool = True) -> str:
        dataset_table_separator = dlt.config[
            "destination.clickhouse.credentials.dataset_table_separator"
        ]
        if escape:
            database_name = self.capabilities.escape_identifier(self.database_name)
            dataset_and_table = self.capabilities.escape_identifier(
                f"{self.dataset_name}{dataset_table_separator}{table_name}"
            )
        else:
            database_name = self.database_name
            dataset_and_table = f"{self.dataset_name}{dataset_table_separator}{table_name}"
        return f"{database_name}.{dataset_and_table}"

    @classmethod
    def _make_database_exception(cls, ex: Exception) -> Exception:  # type: ignore[return]
        if isinstance(ex, clickhouse_driver.dbapi.errors.OperationalError):
            if "Code: 57." in str(ex) or "Code: 82." in str(ex):
                raise DatabaseTerminalException(ex)
            elif "Code: 60." in str(ex) or "Code: 81." in str(ex):
                raise DatabaseUndefinedRelation(ex)
        elif isinstance(
            ex,
            (
                clickhouse_driver.dbapi.errors.OperationalError,
                clickhouse_driver.dbapi.errors.InternalError,
            ),
        ):
            if term := cls._maybe_make_terminal_exception_from_data_error():
                return term
            else:
                return DatabaseTransientException(ex)
        elif isinstance(
            ex,
            (
                clickhouse_driver.dbapi.errors.DataError,
                clickhouse_driver.dbapi.errors.ProgrammingError,
                clickhouse_driver.dbapi.errors.IntegrityError,
            ),
        ):
            return DatabaseTerminalException(ex)
        elif cls.is_dbapi_exception(ex):
            return DatabaseTransientException(ex)
        else:
            return ex

    @staticmethod
    def _maybe_make_terminal_exception_from_data_error() -> Optional[Exception]:
        return None

    @staticmethod
    def is_dbapi_exception(ex: Exception) -> bool:
        return isinstance(ex, clickhouse_driver.dbapi.Error)
