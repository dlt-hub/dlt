from abc import ABC, abstractmethod
from contextlib import contextmanager
from types import TracebackType
from typing import Any, ContextManager, Generic, Iterator, Optional, Sequence, Tuple, Type, AnyStr, Protocol
from pathlib import Path

from dlt.common.schema import Schema
from dlt.common.schema.typing import TTableSchema
from dlt.common.typing import ConfigValue
from dlt.common.configuration.specs import DestinationCapabilitiesContext

from dlt.load.configuration import DestinationClientConfiguration
from dlt.load.typing import TLoadJobStatus, TNativeConn, DBCursor


class LoadJob:
    """Represents a job that loads a single file

        Each job starts in "running" state and ends in one of terminal states: "retry", "failed" or "completed".
        Each job is uniquely identified by a file name. The file is guaranteed to exist in "running" state. In terminal state, the file may not be present.
        In "running" state, the loader component periodically gets the state via `status()` method. When terminal state is reached, load job is discarded and not called again.
        `exception` method is called to get error information in "failed" and "retry" states.

        The `__init__` method is responsible to put the Job in "running" state. It may raise `LoadClientTerminalException` and `LoadClientTransientException` tp
        immediately transition job into "failed" or "retry" state respectively.
    """
    def __init__(self, file_name: str) -> None:
        """
        File name is also a job id (or job id is deterministically derived) so it must be globally unique
        """
        self._file_name = file_name

    @abstractmethod
    def status(self) -> TLoadJobStatus:
        pass

    @abstractmethod
    def file_name(self) -> str:
        pass

    @abstractmethod
    def exception(self) -> str:
        pass


class JobClientBase(ABC):
    def __init__(self, schema: Schema, config: DestinationClientConfiguration) -> None:
        self.schema = schema
        self.config = config

    @abstractmethod
    def initialize_storage(self) -> None:
        pass

    @abstractmethod
    def update_storage_schema(self) -> None:
        pass

    @abstractmethod
    def start_file_load(self, table: TTableSchema, file_path: str) -> LoadJob:
        pass

    @abstractmethod
    def restore_file_load(self, file_path: str) -> LoadJob:
        pass

    @abstractmethod
    def complete_load(self, load_id: str) -> None:
        pass

    @abstractmethod
    def __enter__(self) -> "JobClientBase":
        pass

    @abstractmethod
    def __exit__(self, exc_type: Type[BaseException], exc_val: BaseException, exc_tb: TracebackType) -> None:
        pass

    @classmethod
    @abstractmethod
    def capabilities(cls) -> DestinationCapabilitiesContext:
        pass

    # @classmethod
    # @abstractmethod
    # def configure(cls, initial_values: StrAny = None) -> Tuple[BaseConfiguration, CredentialsConfiguration]:
    #     pass


class DestinationReference(Protocol):
    def capabilities(self) -> DestinationCapabilitiesContext:
        ...

    def client(self, schema: Schema, initial_config: DestinationClientConfiguration = ConfigValue) -> "JobClientBase":
        ...

    def spec(self) -> Type[DestinationClientConfiguration]:
        ...


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
