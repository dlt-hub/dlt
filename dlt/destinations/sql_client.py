from abc import ABC, abstractmethod
from contextlib import contextmanager
from types import TracebackType
from typing import Any, ContextManager, Generic, Iterator, Optional, Sequence, Tuple, Type, AnyStr, Protocol

from dlt.destinations.typing import TNativeConn, DBCursor


class SqlClientBase(ABC, Generic[TNativeConn]):
    def __init__(self, default_dataset_name: str) -> None:
        if not default_dataset_name:
            raise ValueError(default_dataset_name)
        self.default_dataset_name = default_dataset_name

    @abstractmethod
    def open_connection(self) -> None:
        pass

    @abstractmethod
    def close_connection(self) -> None:
        pass

    def __enter__(self) -> "SqlClientBase[TNativeConn]":
        self.open_connection()
        return self

    def __exit__(self, exc_type: Type[BaseException], exc_val: BaseException, exc_tb: TracebackType) -> None:
        self.close_connection()

    @abstractmethod
    def native_connection(self) -> TNativeConn:
        pass

    @abstractmethod
    def has_dataset(self) -> bool:
        pass

    @abstractmethod
    def create_dataset(self) -> None:
        pass

    @abstractmethod
    def drop_dataset(self) -> None:
        pass

    @abstractmethod
    def execute_sql(self, sql: AnyStr, *args: Any, **kwargs: Any) -> Optional[Sequence[Sequence[Any]]]:
        pass

    @abstractmethod
    def execute_query(self, query: AnyStr, *args: Any, **kwargs: Any) -> ContextManager[DBCursor]:
        pass

    @abstractmethod
    def fully_qualified_dataset_name(self) -> str:
        pass

    def make_qualified_table_name(self, table_name: str) -> str:
        return f"{self.fully_qualified_dataset_name()}.{table_name}"

    @contextmanager
    def with_alternative_dataset_name(self, dataset_name: str) -> Iterator["SqlClientBase[TNativeConn]"]:
        current_dataset_name = self.default_dataset_name
        try:
            self.default_dataset_name = dataset_name
            yield self
        finally:
            # restore previous dataset name
            self.default_dataset_name = current_dataset_name
