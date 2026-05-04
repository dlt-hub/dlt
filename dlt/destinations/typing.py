from typing import Any, AnyStr, List, Optional, Protocol, Tuple, TypeVar, Generator, TYPE_CHECKING


# native connection
TNativeConn = TypeVar("TNativeConn", bound=Any)

if TYPE_CHECKING:
    from pandas import DataFrame
    from pyarrow import Table as ArrowTable
else:
    DataFrame = Any
    ArrowTable = Any


class DBTransaction(Protocol):
    def commit_transaction(self) -> None: ...
    def rollback_transaction(self) -> None: ...


class DBApi(Protocol):
    threadsafety: int
    apilevel: str
    paramstyle: str
