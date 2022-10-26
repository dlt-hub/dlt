from typing import Any, AnyStr, List, Literal, Optional, Tuple, TypeVar

# native connection
TNativeConn = TypeVar("TNativeConn", bound="object")


# type for dbapi cursor
class DBCursor:
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
