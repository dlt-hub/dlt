from typing import Any, AnyStr, List, Literal, Optional, Protocol, Tuple, TypeVar

# native connection
TNativeConn = TypeVar("TNativeConn", bound=Any)


# class DbApiConnection(Protocol):
#     pass


class DBTransaction(Protocol):
    def commit_transaction(self) -> None:
        ...

    def rollback_transaction(self) -> None:
        ...


class DBApi(Protocol):
    threadsafety: bool
    apilevel: bool
    paramstyle: str


class DBApiCursor(Protocol):
    """Protocol for DBApi cursor"""
    closed: Any
    connection: Any
    query: Any
    description: Tuple[Any, ...]

    def execute(self, query: AnyStr, *args: Any, **kwargs: Any ) -> None:
        ...
    def fetchall(self) -> List[Tuple[Any, ...]]:
        ...
    def fetchmany(self, size: int = ...) -> List[Tuple[Any, ...]]:
        ...
    def fetchone(self) -> Optional[Tuple[Any, ...]]:
        ...
