from contextlib import contextmanager
from typing import (
    Iterator,
    AnyStr,
    Any,
    ContextManager,
    Optional,
    Sequence,
    ClassVar,
)

import clickhouse_driver.dbapi as clickhouse_lib  # type: ignore[import-untyped]
from clickhouse_driver.dbapi.connection import Connection  # type: ignore[import-untyped]
from clickhouse_driver.dbapi.extras import DictCursor  # type: ignore[import-untyped]

from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.runtime import logger
from dlt.destinations.impl.clickhouse import capabilities
from dlt.destinations.sql_client import DBApiCursorImpl, SqlClientBase, raise_database_error
from dlt.destinations.typing import DBTransaction, DBApiCursor, TNativeConn, DBApi


class ClickhouseDBApiCursorImpl(DBApiCursorImpl):
    native_cursor: DictCursor


class ClickhouseSqlClient(SqlClientBase[Connection], DBTransaction):
    dbapi: ClassVar[DBApi] = clickhouse_lib
    capabilities: ClassVar[DestinationCapabilitiesContext] = capabilities()

    @property
    def native_connection(self) -> TNativeConn:  # type: ignore
        pass

    def open_connection(self) -> Connection:
        pass

    def close_connection(self) -> None:
        pass

    def execute_sql(
        self, sql: AnyStr, *args: Any, **kwargs: Any
    ) -> Optional[Sequence[Sequence[Any]]]:
        pass

    def execute_query(
        self, query: AnyStr, *args: Any, **kwargs: Any
    ) -> ContextManager[DBApiCursor]:
        pass

    def fully_qualified_dataset_name(self, escape: bool = True) -> str:
        pass

    @staticmethod
    def _make_database_exception(ex: Exception) -> Exception:
        pass

    @contextmanager
    @raise_database_error
    def begin_transaction(self) -> Iterator[DBTransaction]:
        logger.warning(
            "Clickhouse does not support transactions! Each SQL statement is auto-committed"
            " separately."
        )
        yield self

    @raise_database_error
    def rollback_transaction(self) -> None:
        raise NotImplementedError("You cannot rollback Clickhouse SQL statements.")


class TransactionsNotImplementedError(NotImplementedError):
    def __init__(self) -> None:
        super().__init__("Clickhouse does not support transaction management.")
