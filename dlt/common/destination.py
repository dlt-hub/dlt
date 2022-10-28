from abc import ABC, abstractmethod
from importlib import import_module
from types import TracebackType
from typing import ClassVar, List, Optional, Literal, Type, Protocol, Union, TYPE_CHECKING

from dlt.common.schema import Schema
from dlt.common.schema.typing import TTableSchema
from dlt.common.typing import ConfigValue
from dlt.common.configuration import configspec
from dlt.common.configuration.specs import BaseConfiguration, CredentialsConfiguration, ContainerInjectableContext


# known loader file formats
# jsonl - new line separated json documents
# puae-jsonl - internal extract -> normalize format bases on jsonl
# insert_values - insert SQL statements
TLoaderFileFormat = Literal["jsonl", "puae-jsonl", "insert_values"]


@configspec(init=True)
class DestinationCapabilitiesContext(ContainerInjectableContext):
    """Injectable destination capabilities required for many Pipeline stages ie. normalize"""
    preferred_loader_file_format: TLoaderFileFormat
    supported_loader_file_formats: List[TLoaderFileFormat]
    max_identifier_length: int
    max_column_length: int
    max_query_length: int
    is_max_query_length_in_bytes: bool
    max_text_data_type_length: int
    is_max_text_data_type_length_in_bytes: bool

    # do not allow to create default value, destination caps must be always explicitly inserted into container
    can_create_default: ClassVar[bool] = False


@configspec(init=True)
class DestinationClientConfiguration(BaseConfiguration):
    destination_name: str = None  # which destination to load data to
    credentials: Optional[CredentialsConfiguration]

    if TYPE_CHECKING:
        def __init__(self, destination_name: str = None, credentials: Optional[CredentialsConfiguration] = None) -> None:
            ...


@configspec(init=True)
class DestinationClientDwhConfiguration(DestinationClientConfiguration):
    dataset_name: str = None  # dataset name in the destination to load data to, for schemas that are not default schema, it is used as dataset prefix
    default_schema_name: Optional[str] = None  # name of default schema to be used to name effective dataset to load data to

    if TYPE_CHECKING:
        def __init__(
            self,
            destination_name: str = None,
            credentials: Optional[CredentialsConfiguration] = None,
            dataset_name: str = None,
            default_schema_name: Optional[str] = None
        ) -> None:
            ...


TLoadJobStatus = Literal["running", "failed", "retry", "completed"]


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


class DestinationReference(Protocol):
    def capabilities(self) -> DestinationCapabilitiesContext:
        ...

    def client(self, schema: Schema, initial_config: DestinationClientConfiguration = ConfigValue) -> "JobClientBase":
        ...

    def spec(self) -> Type[DestinationClientConfiguration]:
        ...


def resolve_destination_reference(destination: Union[None, str, DestinationReference]) -> DestinationReference:
    if destination is None:
        return None

    if isinstance(destination, str):
        # TODO: figure out if this is full module path name or name of one of the known destinations
        # if destination is a str, get destination reference by dynamically importing module from known location
        return import_module(f"dlt.load.{destination}")

    return destination