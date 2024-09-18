from typing import Any, AnyStr, List, Type, Optional, Protocol, Tuple, TypeVar, Generator


# native connection
TNativeConn = TypeVar("TNativeConn", bound=Any)

try:
    from pandas import DataFrame
except ImportError:
    DataFrame: Type[Any] = None  # type: ignore

try:
    from pyarrow import Table as ArrowTable
except ImportError:
    ArrowTable: Type[Any] = None  # type: ignore


class DBTransaction(Protocol):
    def commit_transaction(self) -> None: ...
    def rollback_transaction(self) -> None: ...


class DBApi(Protocol):
    threadsafety: int
    apilevel: str
    paramstyle: str
