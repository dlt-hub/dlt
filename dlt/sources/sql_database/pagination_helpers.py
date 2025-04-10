from typing import Iterator, Any, Optional
from dlt.common.libs.sql_alchemy import Select
from .schema_types import SelectClause
from math import floor
from sqlalchemy.engine import Connection, CursorResult
from sqlalchemy.sql.expression import ColumnElement
from dlt.common import logger
from dlt.common.data_types.type_helpers import coerce_value
import os


class TablePaginator:
    def __init__(
        self,
        query: SelectClause,
        conn: Connection,
        page_size: int,
        pk_columns: Optional[list[ColumnElement[Any]]] = None,
    ) -> None:
        """Paginator object for SQL queries.

        Args:
            query: SelectClause generated from TableLoader
            conn: SQLAlchemy connection object for the source DB
            page_size: Number of rows to return at most on each iteration
            pk_columns: Optional list of primary keys for the table, with a sorting order
        """
        if not isinstance(query, Select):
            raise TypeError("The query adapter is currently not compatible with pagination")
        self._page_size: int = page_size
        self._conn = conn
        self._query = query.limit(self._page_size)
        # Sorting by PK is needed to avoid draws on rows with the same value of the cursor
        # Does not replace existing order, just appends new columns to the existing ORDER BY
        if pk_columns:
            self._query = self._query.order_by(*pk_columns)
        logger.info(
            "Initial pagination query:"
            f" {self._query.compile(compile_kwargs={'literal_binds': True})}"
        )
        # Keep track of the rows returned for pagination
        self._current_offset = 0
        # Flags whether the last page has already been returned (when the number of rows returned < page_size)
        self._last_page = False
        # Keeps track of the possible retries of the query when it fails
        self._retries = coerce_value("bigint", "text", os.environ.get("PAGINATION_RETRIES", "2"))
        # Defines a backoff rate, which is used to reduce the page size when the query is retried
        self._backoff = coerce_value("decimal", "text", os.environ.get("PAGINATION_BACKOFF", "0.1"))

    def __iter__(self) -> Iterator[Any]:
        return self

    def _get_new_offset(self) -> int:
        return floor(self._current_offset + self._page_size)

    def _set_offset(self, offset: int) -> None:
        self._current_offset = offset

    def _page_backoff_possible(self) -> bool:
        """Page backoff is meant for cases where the DB has restrictions not only on the number of rows,
        but also on the size of the page in memory. Will update the page size to page_size * backoff_rate.
        """
        if self._retries == 0:
            return False
        # Adapt page size with backoff rate
        self._page_size = int(self._page_size * self._backoff)
        logger.warning(f"Retrying query with backoff. New page size {self._page_size}")
        self._retries -= 1
        # Update LIMIT clause with new page size
        self._query = self._query.limit(self._page_size)
        return True

    def _make_and_execute_query(self) -> CursorResult:
        """First calculate the expected offset of the query, then execute the query.
        If the query fails, then check if page backoff is possible and execute the new query.
        Otherwise, update the offset to the one used when executing the query. This is to avoid
        updating the offset when a query fails, leading to skipped rows.
        """
        new_offset = self._get_new_offset()
        try:
            self._query = self._query.offset(new_offset)
            result = self._conn.execute(self._query)
        except Exception as e:
            if self._page_backoff_possible():
                return self._make_and_execute_query()
            else:
                raise e
        self._set_offset(offset=new_offset)
        return result

    def __next__(self) -> CursorResult:
        """Check whether the last page has already been returned and stop the iterator.
        Otherwise execute the query, check that rowcount is available (to keep track of returned rows)
        and return the page."""
        if self._last_page:
            raise StopIteration()
        result = self._make_and_execute_query()
        logger.info(
            f"Pagination query: {self._query.compile(compile_kwargs={'literal_binds': True})}"
        )
        if result.rowcount == -1:
            raise RuntimeError(
                "Pagination not possible: SQLAlchemy: DBAPI does not support rowcount for SELECT"
                " statements"
            )
        if result.rowcount < self._page_size:
            self._last_page = True
        return result
