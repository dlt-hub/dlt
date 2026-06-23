from typing import Any, AnyStr, Protocol, TypeVar


# native connection
TNativeConn = TypeVar("TNativeConn", bound=Any)


class DBTransaction(Protocol):
    def commit_transaction(self) -> None: ...
    def rollback_transaction(self) -> None: ...


class DBApi(Protocol):
    threadsafety: int
    apilevel: str
    paramstyle: str
