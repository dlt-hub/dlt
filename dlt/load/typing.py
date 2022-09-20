from typing import Any, AnyStr, List, Literal, Optional, Tuple, TypeVar, TypedDict

from dlt.common.data_writers import TLoaderFileFormat


LoadJobStatus = Literal["running", "failed", "retry", "completed"]
# native connection
TNativeConn = TypeVar("TNativeConn", bound="object")


class TLoaderCapabilities(TypedDict):
    preferred_loader_file_format: TLoaderFileFormat
    supported_loader_file_formats: List[TLoaderFileFormat]
    max_identifier_length: int
    max_column_length: int
    max_query_length: int
    is_max_query_length_in_bytes: bool
    max_text_data_type_length: int
    is_max_text_data_type_length_in_bytes: bool


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
