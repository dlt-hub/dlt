from __future__ import annotations

import abc
from typing import (
    TYPE_CHECKING,
    Any,
    AnyStr,
    Generator,
    Literal,
    Optional,
    Protocol,
)

from dlt.common.schema.typing import TTableSchemaColumns

if TYPE_CHECKING:
    from dlt.common.libs.pandas import DataFrame
    from dlt.common.libs.pyarrow import Table as ArrowTable

TFilterOperation = Literal["eq", "ne", "gt", "lt", "gte", "lte", "in", "not_in"]


class SupportsDataAccess(Protocol):
    """Common data access protocol shared between dbapi cursors and relations"""

    @property
    def columns_schema(self) -> TTableSchemaColumns:
        """
        Returns the expected columns schema for the result of the relation. Column types are discovered with
        sql glot query analysis and lineage. dlt hints for columns are kept in some cases. Refere to <docs-page> for more details.
        """
        ...

    def df(self, chunk_size: Optional[int] = None) -> Optional[DataFrame]:
        """Fetches the results as arrow table. Uses the native pandas implementation of the destination client cursor if available.

        Args:
            chunk_size (Optional[int]): The number of rows to fetch for this call. Defaults to None which will fetch all rows.

        Returns:
            Optional[DataFrame]: A data frame with query results.
        """
        ...

    def arrow(self, chunk_size: Optional[int] = None) -> Optional[ArrowTable]:
        """Fetches the results as arrow table. Uses the native arrow implementation of the destination client cursor if available.

        Args:
            chunk_size (Optional[int]): The number of rows to fetch for this call. Defaults to None which will fetch all rows.

        Returns:
            Optional[ArrowTable]: An arrow table with query results.
        """
        ...

    def iter_df(self, chunk_size: int) -> Generator[DataFrame, None, None]:
        """Iterates over data frames of 'chunk_size' items. Uses the native pandas implementation of the destination client cursor if available.

        Args:
            chunk_size (int): The number of rows to fetch for each iteration.

        Returns:
            Generator[DataFrame, None, None]: A generator of data frames with query results.
        """
        ...

    def iter_arrow(self, chunk_size: int) -> Generator[ArrowTable, None, None]:
        """Iterates over arrow tables of 'chunk_size' items. Uses the native arrow implementation of the destination client cursor if available.

        Args:
            chunk_size (int): The number of rows to fetch for each iteration.

        Returns:
            Generator[ArrowTable, None, None]: A generator of arrow tables with query results.
        """
        ...

    def fetchall(self) -> list[tuple[Any, ...]]:
        """Fetches all items as a list of python tuples. Uses the native dbapi fetchall implementation of the destination client cursor.

        Returns:
            list[tuple[Any, ...]]: A list of python tuples w
        """
        ...

    def fetchmany(self, chunk_size: int) -> list[tuple[Any, ...]]:
        """Fetches the first 'chunk_size' items as a list of python tuples. Uses the native dbapi fetchmany implementation of the destination client cursor.

        Args:
            chunk_size (int): The number of rows to fetch for this call.

        Returns:
            list[tuple[Any, ...]]: A list of python tuples with query results.
        """
        ...

    def iter_fetch(self, chunk_size: int) -> Generator[list[tuple[Any, ...]], Any, Any]:
        """Iterates in lists of Python tuples in 'chunk_size' chunks. Uses the native dbapi fetchmany implementation of the destination client cursor.

        Args:
            chunk_size (int): The number of rows to fetch for each iteration.

        Returns:
            Generator[list[tuple[Any, ...]], Any, Any]: A generator of lists of python tuples with query results.
        """
        ...

    def fetchone(self) -> Optional[tuple[Any, ...]]:
        """Fetches the first item as a python tuple. Uses the native dbapi fetchone implementation of the destination client cursor.

        Returns:
            Optional[tuple[Any, ...]]: A python tuple with the first item of the query results.
        """
        ...


class DBApiCursorProtocol(SupportsDataAccess, Protocol):
    """Protocol for the DBAPI cursor"""

    description: tuple[Any, ...]
    native_cursor: "DBApiCursor"
    """Cursor implementation native to current destination"""

    @property
    def columns_schema(self) -> TTableSchemaColumns: ...

    def execute(self, query: AnyStr, *args: Any, **kwargs: Any) -> None:
        """Execute a query on the cursor"""
        ...

    def close(self) -> None:
        """Close the cursor"""
        ...


class DBApiCursor(abc.ABC, DBApiCursorProtocol):
    """Protocol for the DBAPI cursor"""

    description: tuple[Any, ...]
    native_cursor: "DBApiCursor"
    """Cursor implementation native to current destination"""

    def __init__(self) -> None:
        super().__init__()
        self._columns_schema: TTableSchemaColumns = {}

    @property
    def columns_schema(self) -> TTableSchemaColumns:
        return self._columns_schema

    @columns_schema.setter
    def columns_schema(self, value: TTableSchemaColumns) -> None:
        self._columns_schema = value

    def execute(self, query: AnyStr, *args: Any, **kwargs: Any) -> None:
        """Execute a query on the cursor"""

    def close(self) -> None:
        """Close the cursor"""
