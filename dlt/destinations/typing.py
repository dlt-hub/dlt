from typing import Any, AnyStr, List, Type, Optional, Protocol, Tuple, TypeVar, Generator

from dlt.common.typing import DataFrame, ArrowTable
from dlt.common.destination.reference import SupportsReadableRelation

# native connection
TNativeConn = TypeVar("TNativeConn", bound=Any)


class DBTransaction(Protocol):
    def commit_transaction(self) -> None: ...

    def rollback_transaction(self) -> None: ...


class DBApi(Protocol):
    threadsafety: int
    apilevel: str
    paramstyle: str


class DBApiCursor(SupportsReadableRelation):
    """Protocol for DBAPI cursor"""

    description: Tuple[Any, ...]

    native_cursor: "DBApiCursor"
    """Cursor implementation native to current destination"""

    def execute(self, query: AnyStr, *args: Any, **kwargs: Any) -> None: ...
    def close(self) -> None: ...
