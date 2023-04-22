from typing import Any, AnyStr, List, Type, Optional, Protocol, Tuple, TypeVar
try:
    from pandas import DataFrame
except ImportError:
    DataFrame: Type[Any] = None  # type: ignore

# native connection
TNativeConn = TypeVar("TNativeConn", bound=Any)

class DBTransaction(Protocol):
    def commit_transaction(self) -> None:
        ...

    def rollback_transaction(self) -> None:
        ...


class DBApi(Protocol):
    threadsafety: int
    apilevel: str
    paramstyle: str


class DBApiCursor(Protocol):
    """Protocol for DBAPI cursor"""
    description: Tuple[Any, ...]

    native_cursor: "DBApiCursor"
    """Cursor implementation native to current destination"""

    def execute(self, query: AnyStr, *args: Any, **kwargs: Any) -> None:
        ...
    def fetchall(self) -> List[Tuple[Any, ...]]:
        ...
    def fetchmany(self, size: int = ...) -> List[Tuple[Any, ...]]:
        ...
    def fetchone(self) -> Optional[Tuple[Any, ...]]:
        ...
    def close(self) -> None:
        ...

    def df(self, chunk_size: int = None, **kwargs: None) -> Optional[DataFrame]:
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

