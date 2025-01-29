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

from dlt.common.exceptions import MissingDependencyException
from dlt.common.schema.schema import Schema
from dlt.common.schema.typing import TTableSchemaColumns


if TYPE_CHECKING:
    try:
        from dlt.common.libs.pandas import DataFrame
        from dlt.common.libs.pyarrow import Table as ArrowTable
        from dlt.helpers.ibis import BaseBackend as IbisBackend
    except MissingDependencyException:
        DataFrame = Any
        ArrowTable = Any
        IbisBackend = Any
else:
    DataFrame = Any
    ArrowTable = Any
    IbisBackend = Any


class SupportsReadableRelation(Protocol):
    """A readable relation retrieved from a destination that supports it"""

    columns_schema: TTableSchemaColumns
    """Known dlt table columns for this relation"""

    def df(self, chunk_size: int = None) -> Optional[DataFrame]:
        """Fetches the results as data frame. For large queries the results may be chunked

        Fetches the results into a data frame. The default implementation uses helpers in `pandas.io.sql` to generate Pandas data frame.
        This function will try to use native data frame generation for particular destination. For `BigQuery`: `QueryJob.to_dataframe` is used.
        For `duckdb`: `DuckDBPyConnection.df'

        Args:
            chunk_size (int, optional): Will chunk the results into several data frames. Defaults to None
            **kwargs (Any): Additional parameters which will be passed to native data frame generation function.

        Returns:
            Optional[DataFrame]: A data frame with query results. If chunk_size > 0, None will be returned if there is no more data in results
        """
        ...

    # accessing data
    def arrow(self, chunk_size: int = None) -> Optional[ArrowTable]:
        """fetch arrow table of first 'chunk_size' items"""
        ...

    def iter_df(self, chunk_size: int) -> Generator[DataFrame, None, None]:
        """iterate over data frames tables of 'chunk_size' items"""
        ...

    def iter_arrow(self, chunk_size: int) -> Generator[ArrowTable, None, None]:
        """iterate over arrow tables of 'chunk_size' items"""
        ...

    def fetchall(self) -> List[Tuple[Any, ...]]:
        """fetch all items as list of python tuples"""
        ...

    def fetchmany(self, chunk_size: int) -> List[Tuple[Any, ...]]:
        """fetch first 'chunk_size' items  as list of python tuples"""
        ...

    def iter_fetch(self, chunk_size: int) -> Generator[List[Tuple[Any, ...]], Any, Any]:
        """iterate in lists of python tuples in 'chunk_size' chunks"""
        ...

    def fetchone(self) -> Optional[Tuple[Any, ...]]:
        """fetch first item as python tuple"""
        ...

    # modifying access parameters
    def limit(self, limit: int, **kwargs: Any) -> "SupportsReadableRelation":
        """limit the result to 'limit' items"""
        ...

    def head(self, limit: int = 5) -> "SupportsReadableRelation":
        """limit the result to 5 items by default"""
        ...

    def select(self, *columns: str) -> "SupportsReadableRelation":
        """set which columns will be selected"""
        ...

    @overload
    def __getitem__(self, column: str) -> "SupportsReadableRelation": ...

    @overload
    def __getitem__(self, columns: Sequence[str]) -> "SupportsReadableRelation": ...

    def __getitem__(self, columns: Union[str, Sequence[str]]) -> "SupportsReadableRelation":
        """set which columns will be selected"""
        ...

    def __getattr__(self, attr: str) -> Any:
        """get an attribute of the relation"""
        ...

    def __copy__(self) -> "SupportsReadableRelation":
        """create a copy of the relation object"""
        ...


class DBApiCursor(SupportsReadableRelation):
    """Protocol for DBAPI cursor"""

    description: Tuple[Any, ...]

    native_cursor: "DBApiCursor"
    """Cursor implementation native to current destination"""

    def execute(self, query: AnyStr, *args: Any, **kwargs: Any) -> None: ...
    def close(self) -> None: ...


class SupportsReadableDataset(Protocol):
    """A readable dataset retrieved from a destination, has support for creating readable relations for a query or table"""

    @property
    def schema(self) -> Schema: ...

    def __call__(self, query: Any) -> SupportsReadableRelation: ...

    def __getitem__(self, table: str) -> SupportsReadableRelation: ...

    def __getattr__(self, table: str) -> SupportsReadableRelation: ...

    def ibis(self) -> IbisBackend: ...

    def row_counts(
        self, *, data_tables: bool = True, dlt_tables: bool = False, table_names: List[str] = None
    ) -> SupportsReadableRelation: ...
