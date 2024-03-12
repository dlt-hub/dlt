from typing import AnyStr, Any, ContextManager, Optional, Sequence

import clickhouse_driver
from clickhouse_driver.dbapi.extras import DictCursor

from dlt.destinations.sql_client import DBApiCursorImpl, SqlClientBase
from dlt.destinations.typing import DBTransaction, DBApiCursor, TNativeConn


class ClickhouseDBApiCursorImpl(DBApiCursorImpl):
    native_cursor: DictCursor


class ClickhouseSqlClient(SqlClientBase[clickhouse_driver.Client], DBTransaction):
    def open_connection(self) -> TNativeConn:
        pass

    def close_connection(self) -> None:
        pass

    def begin_transaction(self) -> ContextManager[DBTransaction]:
        pass

    @property
    def native_connection(self) -> TNativeConn:
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


class TransactionsNotImplementedError(NotImplementedError):
    def __init__(self) -> None:
        super().__init__(
            "Clickhouse does not support transaction management."
        )
