from typing import Iterator, Any, Optional
from math import ceil
from dlt.common.libs.sql_alchemy import Select
from .schema_types import SelectClause
from sqlalchemy.engine import Connection
from sqlalchemy.sql.expression import func, ColumnElement
from dlt.common import logger


class TablePaginator:
    def __init__(
        self,
        query: SelectClause,
        conn: Connection,
        page_size: int,
        pk_columns: Optional[list[ColumnElement[Any]]] = None,
    ) -> None:
        if not isinstance(query, Select):
            raise TypeError("The query adapter is currently not compatible with pagination")
        self._page_size: int = page_size
        self._conn = conn
        self._current_page: int = 0
        self._n_rows: int = self._total_rows(conn=conn, query=query)
        self._total_pages: int = ceil(self._n_rows / self._page_size)
        self._query = query.limit(self._page_size)
        if pk_columns:
            self._query = self._query.order_by(*pk_columns)
        logger.info("Initial pagination query: {self._query}")

    def __iter__(self) -> Iterator[Any]:
        return self

    def _total_rows(self, conn: Connection, query: Select) -> int:
        count_q = query.with_only_columns(func.count(), maintain_column_froms=True)
        n_rows = conn.execute(count_q).scalar()
        if isinstance(n_rows, int):
            return n_rows
        elif n_rows is None:
            return 0
        else:
            raise TypeError("Pagination initalisation failed due to invalid type for count query")

    def _query_offset(self) -> int:
        return self._current_page * self._page_size

    def has_next(self) -> bool:
        return self._current_page < self._total_pages

    def __next__(self) -> Any:
        if self.has_next():
            query = self._query.offset(self._query_offset())
            logger.info(
                f"Pagination query {self._current_page} of {self._total_pages}: "
                f"{query.compile(compile_kwargs={'literal_binds': True})}"
            )
            self._current_page += 1
            return self._conn.execute(query)
        else:
            raise StopIteration()
