from typing import (
    Optional,
    Sequence,
    Union,
    List,
    Any,
    Generator,
    TYPE_CHECKING,
    Protocol,
    Tuple,
    AnyStr,
    overload,
)

from dlt.common.typing import Self, Generic, TypeVar
from dlt.common.exceptions import MissingDependencyException
from dlt.common.schema.schema import Schema
from dlt.common.schema.typing import TTableSchemaColumns


if TYPE_CHECKING:
    from dlt.common.libs.pandas import DataFrame
    from dlt.common.libs.pyarrow import Table as ArrowTable
    from dlt.helpers.ibis import BaseBackend as IbisBackend
else:
    DataFrame = Any
    ArrowTable = Any
    IbisBackend = Any


class SupportsReadableRelation(Protocol):
    """A readable relation retrieved from a destination that supports it"""

    columns_schema: TTableSchemaColumns
    """Returns the expected columns schema for the result of the relation. Column types are discovered with
    sql glot query analysis and lineage. dlt hints for columns are kept in some cases. Refere to <docs-page> for more details.
    """

    def query(self) -> Any:
        """Returns the sql query that represents the relation

        Returns:
            Any: The sql query that represents the relation
        """

    def df(self, chunk_size: int = None) -> Optional[DataFrame]:
        """Fetches the results as arrow table. Uses the native pandas implementation of the destination client cursor if available.

        Args:
            chunk_size (int, optional): The number of rows to fetch for this call. Defaults to None which will fetch all rows.

        Returns:
            Optional[DataFrame]: A data frame with query results.
        """

    def arrow(self, chunk_size: int = None) -> Optional[ArrowTable]:
        """Fetches the results as arrow table. Uses the native arrow implementation of the destination client cursor if available.

        Args:
            chunk_size (int, optional): The number of rows to fetch for this call. Defaults to None which will fetch all rows.

        Returns:
            Optional[ArrowTable]: An arrow table with query results.
        """

    def iter_df(self, chunk_size: int) -> Generator[DataFrame, None, None]:
        """Iterates over data frames of 'chunk_size' items. Uses the native pandas implementation of the destination client cursor if available.

        Args:
            chunk_size (int): The number of rows to fetch for each iteration.

        Returns:
            Generator[DataFrame, None, None]: A generator of data frames with query results.
        """

    def iter_arrow(self, chunk_size: int) -> Generator[ArrowTable, None, None]:
        """Iterates over arrow tables of 'chunk_size' items. Uses the native arrow implementation of the destination client cursor if available.

        Args:
            chunk_size (int): The number of rows to fetch for each iteration.

        Returns:
            Generator[ArrowTable, None, None]: A generator of arrow tables with query results.
        """

    def fetchall(self) -> List[Tuple[Any, ...]]:
        """Fetches all items as a list of python tuples. Uses the native dbapi fetchall implementation of the destination client cursor.

        Returns:
            List[Tuple[Any, ...]]: A list of python tuples with query results.
        """

    def fetchmany(self, chunk_size: int) -> List[Tuple[Any, ...]]:
        """Fetches the first 'chunk_size' items as a list of python tuples. Uses the native dbapi fetchmany implementation of the destination client cursor.

        Args:
            chunk_size (int): The number of rows to fetch for this call.

        Returns:
            List[Tuple[Any, ...]]: A list of python tuples with query results.
        """

    def iter_fetch(self, chunk_size: int) -> Generator[List[Tuple[Any, ...]], Any, Any]:
        """Iterates in lists of Python tuples in 'chunk_size' chunks. Uses the native dbapi fetchmany implementation of the destination client cursor.

        Args:
            chunk_size (int): The number of rows to fetch for each iteration.

        Returns:
            Generator[List[Tuple[Any, ...]], Any, Any]: A generator of lists of python tuples with query results.
        """

    def fetchone(self) -> Optional[Tuple[Any, ...]]:
        """Fetches the first item as a python tuple. Uses the native dbapi fetchone implementation of the destination client cursor.

        Returns:
            Optional[Tuple[Any, ...]]: A python tuple with the first item of the query results.
        """

    # modifying access parameters
    def limit(self, limit: int, **kwargs: Any) -> Self:
        """Returns a new relation with the limit applied.

        Args:
            limit (int): The number of rows to fetch.
            **kwargs (Any): Additional keyword arguments to pass to the limit implementation of the destination client cursor.

        Returns:
            Self: The relation with the limit applied.
        """

    def head(self, limit: int = 5) -> Self:
        """By default returns a relation with the first 5 rows selected.

        Args:
            limit (int): The number of rows to fetch.

        Returns:
            Self: The relation with the limit applied.
        """

    def select(self, *columns: str) -> Self:
        """Returns a new relation with the given columns selected.

        Args:
            *columns (str): The columns to select.

        Returns:
            Self: The relation with the columns selected.
        """

    @overload
    def __getitem__(self, column: str) -> Self: ...

    @overload
    def __getitem__(self, columns: Sequence[str]) -> Self: ...

    def __getitem__(self, columns: Union[str, Sequence[str]]) -> Self:
        """Returns a new relation with the given columns selected.

        Args:
            columns (Union[str, Sequence[str]]): The columns to select.

        Returns:
            Self: The relation with the columns selected.
        """

    def __getattr__(self, attr: str) -> Any:
        """get an attribute of the relation

        Args:
            attr (str): The attribute to get.

        Returns:
            Any: The attribute of the relation
        """

    def __copy__(self) -> Self:
        """create a copy of the relation object

        Returns:
            Self: The copy of the relation object
        """


class DBApiCursor(SupportsReadableRelation):
    """Protocol for DBAPI cursor"""

    description: Tuple[Any, ...]

    native_cursor: "DBApiCursor"
    """Cursor implementation native to current destination"""

    def execute(self, query: AnyStr, *args: Any, **kwargs: Any) -> None: ...
    def close(self) -> None: ...


TReadableRelation = TypeVar("TReadableRelation", bound=SupportsReadableRelation, covariant=True)


class SupportsReadableDataset(Generic[TReadableRelation], Protocol):
    """A readable dataset retrieved from a destination, has support for creating readable relations for a query or table"""

    @property
    def schema(self) -> Schema:
        """Returns the schema of the dataset, will fetch the schema from the destination

        Returns:
            Schema: The schema of the dataset
        """

    @property
    def dataset_name(self) -> str:
        """Returns the name of the dataset

        Returns:
            str: The name of the dataset
        """

    def __call__(self, query: Any) -> SupportsReadableRelation:
        """Returns a readable relation for a given sql query

        Args:
            query (Any): The sql query to base the relation on

        Returns:
            SupportsReadableRelation: The readable relation for the query
        """

    def __getitem__(self, table: str) -> TReadableRelation:
        """Returns a readable relation for the table named `table`

        Args:
            table (str): The name of the table

        Returns:
            TReadableRelation: The readable relation for the table
        """

    def __getattr__(self, table: str) -> TReadableRelation:
        """Returns a readable relation for the table named `table`

        Args:
            table (str): The name of the table

        Returns:
            TReadableRelation: The readable relation for the table
        """

    def ibis(self) -> IbisBackend:
        """Returns a connected ibis backend for the dataset. Not implemented for all destinations.

        Returns:
            IbisBackend: The ibis backend for the dataset
        """

    def row_counts(
        self, *, data_tables: bool = True, dlt_tables: bool = False, table_names: List[str] = None
    ) -> SupportsReadableRelation:
        """Returns the row counts of the dataset

        Args:
            data_tables (bool, optional): Whether to include data tables. Defaults to True.
            dlt_tables (bool, optional): Whether to include dlt tables. Defaults to False.
            table_names (List[str], optional): The names of the tables to include. Defaults to None. Will override data_tables and dlt_tables if set

        Returns:
            SupportsReadableRelation: The row counts of the dataset as ReadableRelation
        """
        ...
