from abc import ABC, abstractmethod
from importlib import import_module
from types import TracebackType, ModuleType
from typing import ClassVar, Final, Optional, Literal, Sequence, Iterable, Type, Protocol, Union, TYPE_CHECKING, cast

from dlt.common import logger
from dlt.common.exceptions import IdentifierTooLongException, InvalidDestinationReference, UnknownDestinationModule
from dlt.common.schema import Schema, TTableSchema, TSchemaTables
from dlt.common.schema.exceptions import InvalidDatasetName
from dlt.common.configuration import configspec
from dlt.common.configuration.specs import BaseConfiguration, CredentialsConfiguration
from dlt.common.configuration.accessors import config
from dlt.common.destination.capabilities import DestinationCapabilitiesContext
from dlt.common.schema.utils import is_complete_column
from dlt.common.storages import FileStorage
from dlt.common.storages.load_storage import ParsedLoadJobFileName


@configspec(init=True)
class DestinationClientConfiguration(BaseConfiguration):
    destination_name: str = None  # which destination to load data to
    credentials: Optional[CredentialsConfiguration]

    if TYPE_CHECKING:
        def __init__(self, destination_name: str = None, credentials: Optional[CredentialsConfiguration] = None) -> None:
            ...


@configspec(init=True)
class DestinationClientDwhConfiguration(DestinationClientConfiguration):
    # keep default/initial value if present
    dataset_name: Final[str] = None
    """dataset name in the destination to load data to, for schemas that are not default schema, it is used as dataset prefix"""
    default_schema_name: Optional[str] = None
    """name of default schema to be used to name effective dataset to load data to"""

    if TYPE_CHECKING:
        def __init__(
            self,
            destination_name: str = None,
            credentials: Optional[CredentialsConfiguration] = None,
            dataset_name: str = None,
            default_schema_name: Optional[str] = None
        ) -> None:
            ...


TLoadJobState = Literal["running", "failed", "retry", "completed"]


class LoadJob:
    """Represents a job that loads a single file

        Each job starts in "running" state and ends in one of terminal states: "retry", "failed" or "completed".
        Each job is uniquely identified by a file name. The file is guaranteed to exist in "running" state. In terminal state, the file may not be present.
        In "running" state, the loader component periodically gets the state via `status()` method. When terminal state is reached, load job is discarded and not called again.
        `exception` method is called to get error information in "failed" and "retry" states.

        The `__init__` method is responsible to put the Job in "running" state. It may raise `LoadClientTerminalException` and `LoadClientTransientException` to
        immediately transition job into "failed" or "retry" state respectively.
    """
    def __init__(self, file_name: str) -> None:
        """
        File name is also a job id (or job id is deterministically derived) so it must be globally unique
        """
        # ensure file name
        assert file_name == FileStorage.get_file_name_from_file_path(file_name)
        self._file_name = file_name
        self._parsed_file_name = ParsedLoadJobFileName.parse(file_name)

    @abstractmethod
    def state(self) -> TLoadJobState:
        """Returns current state. Should poll external resource if necessary."""
        pass

    def file_name(self) -> str:
        """A name of the job file"""
        return self._file_name

    def job_id(self) -> str:
        """The job id that is derived from the file name"""
        return self._parsed_file_name.job_id()

    def job_file_info(self) -> ParsedLoadJobFileName:
        return self._parsed_file_name

    @abstractmethod
    def exception(self) -> str:
        """The exception associated with failed or retry states"""
        pass


class NewLoadJob(LoadJob):
    """Adds a trait that allows to save new job file"""

    @abstractmethod
    def new_file_path(self) -> str:
        """Path to a newly created temporary job file. If empty, no followup job should be created"""
        pass


class FollowupJob:
    """Adds a trait that allows to create a followup job"""
    pass


class JobClientBase(ABC):

    capabilities: ClassVar[DestinationCapabilitiesContext] = None

    def __init__(self, schema: Schema, config: DestinationClientConfiguration) -> None:
        self.schema = schema
        self.config = config

    @abstractmethod
    def initialize_storage(self, staging: bool = False, truncate_tables: Iterable[str] = None) -> None:
        """Prepares storage to be used ie. creates database schema or file system folder. Creates a staging storage if `staging` flag is true. Truncates requested tables.
        """
        pass

    @abstractmethod
    def is_storage_initialized(self, staging: bool = False) -> bool:
        """Returns if storage is ready to be read/written. Checks staging storage if `staging` flag is true"""
        pass

    def update_storage_schema(self, staging: bool = False, only_tables: Iterable[str] = None, expected_update: TSchemaTables = None) -> Optional[TSchemaTables]:
        """Updates storage to the current schema.

        Implementations should not assume that `expected_update` is the exact difference between destination state and the self.schema. This is only the case if
        destination has single writer and no other processes modify the schema.

        Args:
            staging (bool, optional): Updates the staging if True. Defaults to False.
            only_tables (Sequence[str], optional): Updates only listed tables. Defaults to None.
            expected_update (TSchemaTables, optional): Update that is expected to be applied to the destination
        Returns:
            Optional[TSchemaTables]: Returns an update that was applied at the destination.
        """
        self._verify_schema()
        return expected_update

    @abstractmethod
    def start_file_load(self, table: TTableSchema, file_path: str) -> LoadJob:
        """Creates a job for a particular `table` with content in `file_path`"""
        pass

    @abstractmethod
    def restore_file_load(self, file_path: str) -> LoadJob:
        pass

    @abstractmethod
    def create_merge_job(self, table_chain: Sequence[TTableSchema]) -> NewLoadJob:
        """Creates a table merge job without executing it. The `table_chain` contains a list of tables, ordered by ancestry, that should be merged.
        Clients that cannot merge should return None
        """
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

    def _verify_schema(self) -> None:
        """Verifies and cleans up a schema before loading

        * Checks all table and column name lengths against destination capabilities and raises on too long identifiers
        * Removes and warns on (unbound) incomplete columns
        """

        for table in self.schema.data_tables():
            table_name = table["name"]
            if len(table_name) > self.capabilities.max_identifier_length:
                raise IdentifierTooLongException(self.config.destination_name, "table", table_name, self.capabilities.max_identifier_length)
            for column_name, column in dict(table["columns"]).items():
                if len(column_name) > self.capabilities.max_column_identifier_length:
                    raise IdentifierTooLongException(
                        self.config.destination_name,
                        "column",
                        f"{table_name}.{column_name}",
                        self.capabilities.max_column_identifier_length
                    )
                if not is_complete_column(column):
                    logger.warning(f"A column {column_name} in table {table_name} in schema {self.schema.name} is incomplete. It was not bound to the data during normalizations stage and its data type is unknown. Did you add this column manually in code ie. as a merge key?")
                    table["columns"].pop(column_name)

    @staticmethod
    def make_dataset_name(schema: Schema, dataset_name: str, default_schema_name: str) -> str:
        """Builds full db dataset (dataset) name out of (normalized) default dataset and schema name"""
        if not schema.name:
            raise ValueError("schema_name is None or empty")
        if not dataset_name:
            raise ValueError("dataset_name is None or empty")
        norm_name = schema.naming.normalize_identifier(dataset_name)
        if norm_name != dataset_name:
            raise InvalidDatasetName(dataset_name, norm_name)
        # if default schema is None then suffix is not added
        if default_schema_name is not None and schema.name != default_schema_name:
            norm_name += "_" + schema.name

        return norm_name


TDestinationReferenceArg = Union["DestinationReference", ModuleType, None, str]


class DestinationReference(Protocol):
    __name__: str

    def capabilities(self) -> DestinationCapabilitiesContext:
        ...

    def client(self, schema: Schema, initial_config: DestinationClientConfiguration = config.value) -> "JobClientBase":
        ...

    def spec(self) -> Type[DestinationClientConfiguration]:
        ...

    @staticmethod
    def from_name(destination: TDestinationReferenceArg) -> "DestinationReference":
        if destination is None:
            return None

        # if destination is a str, get destination reference by dynamically importing module
        if isinstance(destination, str):
            try:
                if "." in destination:
                    # this is full module name
                    destination_ref = cast(DestinationReference, import_module(destination))
                else:
                    # from known location
                    destination_ref = cast(DestinationReference, import_module(f"dlt.destinations.{destination}"))
            except ImportError:
                raise UnknownDestinationModule(destination)
        else:
            destination_ref = cast(DestinationReference, destination)

        # make sure the reference is correct
        try:
            c = destination_ref.spec()
            c.credentials
        except Exception:
            raise InvalidDestinationReference(destination)

        return destination_ref
