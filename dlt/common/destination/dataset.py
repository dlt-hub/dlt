from __future__ import annotations

import abc
from types import TracebackType
from typing import (
    TYPE_CHECKING,
    overload,
    Any,
    AnyStr,
    Generator,
    Literal,
    Optional,
    Protocol,
    Sequence,
    Type,
    Union,
)

import sqlglot.expressions as sge
from sqlglot.schema import Schema as SQLGlotSchema
from sqlglot.expressions import ExpOrStr as SqlglotExprOrStr

from dlt.common.typing import Self, TSortOrder
from dlt.common.schema.schema import Schema
from dlt.common.schema.typing import TTableSchema, TTableSchemaColumns
from dlt.common.libs.sqlglot import TSqlGlotDialect

if TYPE_CHECKING:
    from dlt.common.libs.pandas import DataFrame
    from dlt.common.libs.pyarrow import Table as ArrowTable
    from dlt.helpers.ibis import BaseBackend as IbisBackend, Table as IbisTable, Expr as IbisExpr
    from dlt.common.destination.client import SupportsOpenTables


TFilterOperation = Literal["eq", "ne", "gt", "lt", "gte", "lte", "in", "not_in"]


class DataAccess(Protocol):
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


class Relation(DataAccess, Protocol):
    """A readable relation retrieved from a destination that supports it"""

    schema: TTableSchema
    """The schema of the relation"""

    def scalar(self) -> Any:
        """fetch first value of first column on first row as python primitive

        Returns:
            Any: The first value of the first column on the first row as a python primitive.
        """
        ...

    # modifying access parameters
    def limit(self, limit: int, **kwargs: Any) -> Self:
        """Returns a new relation with the limit applied.

        Args:
            limit (int): The number of rows to fetch.
            **kwargs (Any): Additional keyword arguments to pass to the limit implementation of the destination client cursor.

        Returns:
            Self: The relation with the limit applied.
        """
        ...

    def head(self, limit: int = 5) -> Self:
        """By default returns a relation with the first 5 rows selected.

        Args:
            limit (int): The number of rows to fetch.

        Returns:
            Self: The relation with the limit applied.
        """
        ...

    def select(self, *columns: str) -> Self:
        """Returns a new relation with the given columns selected.

        Args:
            *columns (str): The columns to select.

        Returns:
            Self: The relation with the columns selected.
        """
        ...

    def max(self) -> Self:  # noqa: A003
        """Returns a new relation with the MAX aggregate applied.
        Exactly one column must be selected.

        Returns:
            Self: The relation with the MAX aggregate expression.
        """
        ...

    def min(self) -> Self:  # noqa: A003
        """Returns a new relation with the MIN aggregate applied.
        Exactly one column must be selected.

        Returns:
            Self: The relation with the MIN aggregate expression.
        """
        ...

    @overload
    def where(self, column_or_expr: SqlglotExprOrStr) -> Self: ...

    @overload
    def where(
        self,
        column_or_expr: str,
        operator: TFilterOperation,
        value: Any,
    ) -> Self: ...

    def where(
        self,
        column_or_expr: SqlglotExprOrStr,
        operator: Optional[TFilterOperation] = None,
        value: Optional[Any] = None,
    ) -> Self:
        """Returns a new relation with the given where clause applied. Same as .filter().

        Args:
            column_or_expr (SqlglotExprOrStr): The column to filter on. Alternatively, the SQL expression or string representing a custom WHERE clause.
            operator (Optional[TFilterOperation]): The operator to use. Available operations are: eq, ne, gt, lt, gte, lte, in, not_in
            value (Optional[Any]): The value to filter on.

        Returns:
            Self: A copy of the relation with the where clause applied.
        """
        ...

    def filter(  # noqa: A003
        self,
        column_name: str,
        operator: TFilterOperation,
        value: Any,
    ) -> Self:
        """Returns a new relation with the given where clause applied. Same as .where().

        Args:
            column_name (str): The column to filter on.
            operator (TFilterOperation): The operator to use. Available operations are: eq, ne, gt, lt, gte, lte, in, not_in
            value (Any): The value to filter on.

        Returns:
            Self: A copy of the relation with the where clause applied.
        """
        ...

    def order_by(self, column_name: str, direction: TSortOrder = "asc") -> Self:
        """Returns a new relation with the given order by clause applied.

        Args:
            column_name (str): The column to order by.
            direction (TSortOrder, optional): The direction to order by: "asc"/"desc". Defaults to "asc".

        Returns:
            Self: A copy of the relation with the order by clause applied.
        """
        ...

    @overload
    def __getitem__(self, columns: str) -> Self: ...

    @overload
    def __getitem__(self, columns: Sequence[str]) -> Self: ...

    def __getitem__(self, columns: Union[str, Sequence[str]]) -> Self:
        """Returns a new relation with the given columns selected.

        Args:
            columns (Union[str, Sequence[str]]): The columns to select.

        Returns:
            Self: The relation with the columns selected.
        """
        ...

    def __copy__(self) -> Self:
        """create a copy of the relation object

        Returns:
            Self: The copy of the relation object
        """
        ...


class DBApiCursorProtocol(DataAccess, Protocol):
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


class Dataset(Protocol):
    """A readable dataset retrieved from a destination, has support for creating readable relations for a query or table"""

    @property
    def schema(self) -> Schema:
        """Returns the schema of the dataset, will fetch the schema from the destination

        Returns:
            Schema: The schema of the dataset
        """
        ...

    @property
    def sqlglot_schema(self) -> SQLGlotSchema:
        """Returns the computed and cached sqlglot schema of the dataset

        Returns:
            SQLGlotSchema: The sqlglot schema of the dataset
        """
        ...

    @property
    def dataset_name(self) -> str:
        """Returns the name of the dataset

        Returns:
            str: The name of the dataset
        """
        ...

    def __call__(
        self,
        query: Union[str, sge.Select, IbisExpr],
        query_dialect: Optional[TSqlGlotDialect] = None,
        _execute_raw_query: bool = False,
    ) -> Relation:
        """Returns a readable relation for a given sql query

        Args:
            query (Union[str, sge.Select, IbisExpr]): The sql query to base the relation on. Can be a raw sql query, a sqlglot select expression or an ibis expression.
            query_dialect (Optional[TSqlGlotDialect]): The dialect of the query. Defaults to the dataset's destination dialect. You can use this to write queries in a different dialect than the destination.
                This settings will only be user fo the initial parsing of the query. When executing the query, the query will be executed in the underlying destination dialect.
            _execute_raw_query (bool, optional): Whether to run the query as is (raw)or perform query normalization and lineage. Experimental.

        Returns:
            Relation: The readable relation for the query
        """
        ...

    def query(
        self,
        query: Union[str, sge.Select, IbisExpr],
        query_dialect: Optional[TSqlGlotDialect] = None,
        _execute_raw_query: bool = False,
    ) -> Relation:
        """Returns a readable relation for a given sql query

        Args:
            query (Union[str, sge.Select, IbisExpr]): The sql query to base the relation on. Can be a raw sql query, a sqlglot select expression or an ibis expression.
            query_dialect (Optional[TSqlGlotDialect]): The dialect of the query. Defaults to the dataset's destination dialect. You can use this to write queries in a different dialect than the destination.
                This settings will only be user fo the initial parsing of the query. When executing the query, the query will be executed in the underlying destination dialect.
            _execute_raw_query (bool, optional): Whether to run the query as is (raw)or perform query normalization and lineage. Experimental.

        Returns:
            Relation: The readable relation for the query
        """
        ...

    @overload
    def table(self, table_name: str) -> Relation: ...

    @overload
    def table(self, table_name: str, table_type: Literal["relation"]) -> Relation: ...

    @overload
    def table(self, table_name: str, table_type: Literal["ibis"]) -> IbisTable: ...

    def table(
        self, table_name: str, table_type: Literal["relation", "ibis"] = "relation"
    ) -> Union[Relation, IbisTable]:
        """Returns an object representing a table named `table_name`

        Args:
            table_name (str): The name of the table
            table_type (Literal["relation", "ibis"], optional): The type of the table. Defaults to "relation" if not specified. If "ibis" is specified, you will get an unbound ibis table.

        Returns:
            Union[Relation, IbisTable]: The object representing the table
        """
        ...

    def __getitem__(self, table: str) -> Relation:
        """Returns a readable relation for the table named `table`

        Args:
            table (str): The name of the table

        Returns:
            Relation: The readable relation for the table
        """
        ...

    def __getattr__(self, table: str) -> Relation:
        """Returns a readable relation for the table named `table`

        Args:
            table (str): The name of the table

        Returns:
            Relation: The readable relation for the table
        """
        ...

    def __enter__(self) -> Self:
        """Context manager to keep the connection to the destination open between queries"""
        ...

    def __exit__(
        self, exc_type: Type[BaseException], exc_val: BaseException, exc_tb: TracebackType
    ) -> None:
        """Context manager to keep the connection to the destination open between queries"""
        ...

    def ibis(self) -> IbisBackend:
        """Returns a connected ibis backend for the dataset. Not implemented for all destinations.

        Returns:
            IbisBackend: The ibis backend for the dataset
        """
        ...

    def row_counts(
        self,
        *,
        data_tables: bool = True,
        dlt_tables: bool = False,
        table_names: Optional[list[str]] = None,
        load_id: Optional[str] = None,
    ) -> Relation:
        """Returns the row counts of the dataset

        Args:
            data_tables (bool): Whether to include data tables. Defaults to True.
            dlt_tables (bool): Whether to include dlt tables. Defaults to False.
            table_names (Optional[list[str]]): The names of the tables to include. Defaults to None. Will override data_tables and dlt_tables if set
            load_id (Optional[str]): If set, only count rows associated with a given load id. Will exclude tables that do not have a load id.
        Returns:
            Relation: The row counts of the dataset as ReadableRelation
        """
        ...
