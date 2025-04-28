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
    runtime_checkable,
)

from abc import ABC, abstractmethod

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


class SupportsReadableRelation(ABC):
    """A readable relation retrieved from a destination that supports it"""

    columns_schema: TTableSchemaColumns
    """Known dlt table columns for this relation"""

    def query(self) -> Any:
        """Represents relation as a query, currently always SQL"""
        raise NotImplementedError("Query is not supported for this relation")

    # TODO think for a better name that matches the type `TTableSchemaColumns`
    # `compute_table_columns_schema()` ?
    def compute_columns_schema(self, **kwargs: Any) -> TTableSchemaColumns:
        """Return the expected dlt schema of the execution result of self.query()"""
        raise NotImplementedError("Compute columns schema is not supported for this relation")

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
        raise NotImplementedError("DF is not supported for this relation")

    def arrow(self, chunk_size: int = None) -> Optional[ArrowTable]:
        """fetch arrow table of first 'chunk_size' items"""
        raise NotImplementedError("Arrow is not supported for this relation")

    def iter_df(self, chunk_size: int) -> Generator[DataFrame, None, None]:
        """iterate over data frames tables of 'chunk_size' items"""
        raise NotImplementedError("Iter df is not supported for this relation")

    def iter_arrow(self, chunk_size: int) -> Generator[ArrowTable, None, None]:
        """iterate over arrow tables of 'chunk_size' items"""
        raise NotImplementedError("Iter arrow is not supported for this relation")

    def fetchall(self) -> List[Tuple[Any, ...]]:
        """fetch all items as list of python tuples"""
        raise NotImplementedError("Fetch all is not supported for this relation")

    def fetchmany(self, chunk_size: int) -> List[Tuple[Any, ...]]:
        """fetch first 'chunk_size' items  as list of python tuples"""
        raise NotImplementedError("Fetch many is not supported for this relation")

    def iter_fetch(self, chunk_size: int) -> Generator[List[Tuple[Any, ...]], Any, Any]:
        """iterate in lists of python tuples in 'chunk_size' chunks"""
        raise NotImplementedError("Iter fetch is not supported for this relation")

    def fetchone(self) -> Optional[Tuple[Any, ...]]:
        """fetch first item as python tuple"""
        raise NotImplementedError("Fetch one is not supported for this relation")

    def scalar(self) -> Any:
        """fetch first value of first column on first row as python primitive"""
        return self.fetchone()[0]

    # modifying access parameters
    def limit(self, limit: int, **kwargs: Any) -> Self:
        """limit the result to 'limit' items"""
        raise NotImplementedError("Limit is not supported for this relation")

    def head(self, limit: int = 5) -> Self:
        """limit the result to 5 items by default"""
        raise NotImplementedError("Head is not supported for this relation")

    def select(self, *columns: str) -> Self:
        """set which columns will be selected"""
        raise NotImplementedError("Select is not supported for this relation")

    @overload
    def __getitem__(self, column: str) -> Self: ...

    @overload
    def __getitem__(self, columns: Sequence[str]) -> Self: ...

    def __getitem__(self, columns: Union[str, Sequence[str]]) -> Self:
        """set which columns will be selected"""
        raise NotImplementedError("Select is not supported for this relation")

    def __getattr__(self, attr: str) -> Any:
        """get an attribute of the relation"""
        raise NotImplementedError("Get attribute is not supported for this relation")

    def __copy__(self) -> Self:
        """create a copy of the relation object"""
        raise NotImplementedError("Copy is not supported for this relation")


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
    def schema(self) -> Schema: ...

    @property
    def dataset_name(self) -> str: ...

    def __call__(self, query: Any) -> SupportsReadableRelation: ...

    def __getitem__(self, table: str) -> TReadableRelation: ...

    def __getattr__(self, table: str) -> TReadableRelation: ...

    def ibis(self) -> IbisBackend: ...

    def row_counts(
        self, *, data_tables: bool = True, dlt_tables: bool = False, table_names: List[str] = None
    ) -> SupportsReadableRelation: ...
