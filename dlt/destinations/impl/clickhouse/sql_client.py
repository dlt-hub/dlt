from contextlib import contextmanager
from typing import (
    Iterator,
    AnyStr,
    Any,
    Optional,
    Sequence,
    ClassVar,
    Union,
)

import clickhouse_driver  # type: ignore[import-untyped]
import clickhouse_driver.errors  # type: ignore[import-untyped]
from clickhouse_driver.dbapi import Connection  # type: ignore[import-untyped]
from clickhouse_driver.dbapi.extras import DictCursor  # type: ignore[import-untyped]

from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.runtime import logger
from dlt.destinations.exceptions import (
    DatabaseUndefinedRelation,
    DatabaseTransientException,
    DatabaseTerminalException,
)
from dlt.destinations.impl.clickhouse import capabilities
from dlt.destinations.impl.clickhouse.configuration import ClickhouseCredentials
from dlt.destinations.sql_client import (
    DBApiCursorImpl,
    SqlClientBase,
    raise_database_error,
    raise_open_connection_error,
)
from dlt.destinations.typing import DBTransaction, DBApi


TRANSACTIONS_UNSUPPORTED_WARNING_MESSAGE = (
    "Clickhouse does not support transactions! Each statement is auto-committed separately."
)


class ClickhouseDBApiCursorImpl(DBApiCursorImpl):
    native_cursor: DictCursor


class ClickhouseSqlClient(
    SqlClientBase[clickhouse_driver.dbapi.connection.Connection], DBTransaction
):
    dbapi: ClassVar[DBApi] = clickhouse_driver.dbapi.connection.Connection
    capabilities: ClassVar[DestinationCapabilitiesContext] = capabilities()

    def __init__(self, dataset_name: str, credentials: ClickhouseCredentials) -> None:
        super().__init__(credentials.database, dataset_name)
        self._conn: clickhouse_driver.dbapi.connection = None
        self.credentials = credentials
        self.database_name = credentials.database

    def open_connection(self) -> clickhouse_driver.dbapi.connection.Connection:
        self._conn = clickhouse_driver.connect(dsn=self.credentials.to_native_representation())
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
        logger.warning(TRANSACTIONS_UNSUPPORTED_WARNING_MESSAGE)

    @raise_database_error
    def commit_transaction(self) -> None:
        logger.warning(TRANSACTIONS_UNSUPPORTED_WARNING_MESSAGE)
        self._conn.commit()

    @raise_database_error
    def rollback_transaction(self) -> None:
        logger.warning(TRANSACTIONS_UNSUPPORTED_WARNING_MESSAGE)
        self._conn.rollback()

    @property
    def native_connection(self) -> clickhouse_driver.dbapi.connection.Connection:
        return self._conn

    def execute_sql(
        self, sql: AnyStr, *args: Any, **kwargs: Any
    ) -> Optional[Sequence[Sequence[Any]]]:
        with self.execute_query(sql, *args, **kwargs) as curr:
            return None if curr.description is None else curr.fetchall()

    @contextmanager
    @raise_database_error
    def execute_query(
        self, query: AnyStr, *args: Any, **kwargs: Any
    ) -> Iterator[ClickhouseDBApiCursorImpl]:
        cur: clickhouse_driver.dbapi.connection.Cursor
        with self._conn.cursor() as cur:
            try:
                # TODO: Clickhouse driver only accepts pyformat `...WHERE name=%(name)s` parameter marker arguments.
                cur.execute(query, args or (kwargs or None))
                yield ClickhouseDBApiCursorImpl(cur)  # type: ignore[abstract]
            except clickhouse_driver.dbapi.Error:
                self.close_connection()
                self.open_connection()
                raise

    def fully_qualified_dataset_name(self, escape: bool = True) -> str:
        database_name = (
            self.capabilities.escape_identifier(self.database_name)
            if escape
            else self.database_name
        )
        dataset_name = (
            self.capabilities.escape_identifier(self.dataset_name) if escape else self.dataset_name
        )
        return f"{database_name}.{dataset_name}"

    def make_qualified_table_name(self, table_name: str, escape: bool = True) -> str:
        database_name = (
            self.capabilities.escape_identifier(self.database_name)
            if escape
            else self.database_name
        )
        dataset_table_name = (
            self.capabilities.escape_identifier(f"{self.dataset_name}_{table_name}")
            if escape
            else f"{self.dataset_name}_{table_name}"
        )
        return f"{database_name}.{dataset_table_name}"

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
            if term := cls._maybe_make_terminal_exception_from_data_error(ex):
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
    def _maybe_make_terminal_exception_from_data_error(
        ex: Union[
            clickhouse_driver.dbapi.errors.DataError, clickhouse_driver.dbapi.errors.InternalError
        ]
    ) -> Optional[Exception]:
        return None

    @staticmethod
    def is_dbapi_exception(ex: Exception) -> bool:
        return isinstance(ex, clickhouse_driver.dbapi.Error)
