from abc import ABC, abstractmethod, abstractproperty
from importlib import import_module
from types import TracebackType, ModuleType
from typing import ClassVar, Final, Optional, NamedTuple, Literal, Sequence, Iterable, Type, Protocol, Union, TYPE_CHECKING, cast, List, ContextManager, Dict, Any
from contextlib import contextmanager
import datetime  # noqa: 251

from dlt.common import logger
from dlt.common.exceptions import IdentifierTooLongException, InvalidDestinationReference, UnknownDestinationModule
from dlt.common.schema import Schema, TTableSchema, TSchemaTables
from dlt.common.schema.typing import TWriteDisposition
from dlt.common.schema.exceptions import InvalidDatasetName
from dlt.common.configuration import configspec
from dlt.common.configuration.specs import BaseConfiguration, CredentialsConfiguration
from dlt.common.configuration.accessors import config
from dlt.common.destination.capabilities import DestinationCapabilitiesContext
from dlt.common.schema.utils import is_complete_column
from dlt.common.storages import FileStorage
from dlt.common.storages.load_storage import ParsedLoadJobFileName
from dlt.common.utils import get_module_name
from dlt.common.configuration.specs import GcpCredentials, AwsCredentialsWithoutDefaults

TLoaderReplaceStrategy = Literal["truncate-and-insert", "insert-from-staging", "staging-optimized"]


class StorageSchemaInfo(NamedTuple):
    version_hash: str
    schema_name: str
    version: int
    engine_version: str
    inserted_at: datetime.datetime
    schema: str

class StateInfo(NamedTuple):
    version: int
    engine_version: int
    pipeline_name: str
    state: str
    created_at: datetime.datetime
    dlt_load_id: str = None

@configspec
class DestinationClientConfiguration(BaseConfiguration):
    destination_name: str = None  # which destination to load data to
    credentials: Optional[CredentialsConfiguration]

    def fingerprint(self) -> str:
        """Returns a destination fingerprint which is a hash of selected configuration fields. ie. host in case of connection string"""
        return ""

    def __str__(self) -> str:
        """Return displayable destination location"""
        return str(self.credentials)

    if TYPE_CHECKING:
        def __init__(self, destination_name: str = None, credentials: Optional[CredentialsConfiguration] = None
) -> None:
            ...


@configspec
class DestinationClientDwhConfiguration(DestinationClientConfiguration):
    """Configuration of a destination that supports datasets/schemas"""

    dataset_name: Final[str] = None
    """dataset name in the destination to load data to, for schemas that are not default schema, it is used as dataset prefix"""
    default_schema_name: Optional[str] = None
    """name of default schema to be used to name effective dataset to load data to"""
    replace_strategy: TLoaderReplaceStrategy = "truncate-and-insert"
    """How to handle replace disposition for this destination, can be classic or staging"""

    def normalize_dataset_name(self, schema: Schema) -> str:
        """Builds full db dataset (schema) name out of configured dataset name and schema name: {dataset_name}_{schema.name}. The resulting name is normalized.

           If default schema name is None or equals schema.name, the schema suffix is skipped.
        """
        if not schema.name:
            raise ValueError("schema_name is None or empty")

        # if default schema is None then suffix is not added
        if self.default_schema_name is not None and schema.name != self.default_schema_name:
            # also normalize schema name. schema name is Python identifier and here convention may be different
            return schema.naming.normalize_table_identifier((self.dataset_name or "") + "_" + schema.name)

        return self.dataset_name if not self.dataset_name else schema.naming.normalize_table_identifier(self.dataset_name)

    if TYPE_CHECKING:
        def __init__(
            self,
            destination_name: str = None,
            credentials: Optional[CredentialsConfiguration] = None,
            dataset_name: str = None,
            default_schema_name: Optional[str] = None,
        ) -> None:
            ...

@configspec
class DestinationClientStagingConfiguration(DestinationClientDwhConfiguration):
    """Configuration of a staging destination, able to store files with desired `layout` at `bucket_url`.

       Also supports datasets and can act as standalone destination.
    """
    as_staging: bool = False
    bucket_url: str = None
    # layout of the destination files
    layout: str = "{table_name}/{load_id}.{file_id}.{ext}"

    if TYPE_CHECKING:
        def __init__(
            self,
            destination_name: str = None,
            credentials: Union[AwsCredentialsWithoutDefaults, GcpCredentials] = None,
            dataset_name: str = None,
            default_schema_name: Optional[str] = None,
            as_staging: bool = False,
            bucket_url: str = None,
            layout: str =  None
        ) -> None:
            ...

@configspec
class DestinationClientDwhWithStagingConfiguration(DestinationClientDwhConfiguration):
    """Configuration of a destination that can take data from staging destination"""
    staging_config: Optional[DestinationClientStagingConfiguration] = None
    """configuration of the staging, if present, injected at runtime"""
    if TYPE_CHECKING:
        def __init__(
            self,
            destination_name: str = None,
            credentials: Optional[CredentialsConfiguration] = None,
            dataset_name: str = None,
            default_schema_name: Optional[str] = None,
            staging_config: Optional[DestinationClientStagingConfiguration] = None
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
    def create_followup_jobs(self, next_state: str) -> List[NewLoadJob]:
        return []


class JobClientBase(ABC):

    capabilities: ClassVar[DestinationCapabilitiesContext] = None

    def __init__(self, schema: Schema, config: DestinationClientConfiguration) -> None:
        self.schema = schema
        self.config = config

    @abstractmethod
    def initialize_storage(self, truncate_tables: Iterable[str] = None) -> None:
        """Prepares storage to be used ie. creates database schema or file system folder. Truncates requested tables.
        """
        pass

    @abstractmethod
    def is_storage_initialized(self) -> bool:
        """Returns if storage is ready to be read/written."""
        pass

    @abstractmethod
    def drop_storage(self) -> None:
        """Brings storage back into not initialized state. Typically data in storage is destroyed."""
        pass

    def update_stored_schema(self, only_tables: Iterable[str] = None, expected_update: TSchemaTables = None) -> Optional[TSchemaTables]:
        """Updates storage to the current schema.

        Implementations should not assume that `expected_update` is the exact difference between destination state and the self.schema. This is only the case if
        destination has single writer and no other processes modify the schema.

        Args:
            only_tables (Sequence[str], optional): Updates only listed tables. Defaults to None.
            expected_update (TSchemaTables, optional): Update that is expected to be applied to the destination
        Returns:
            Optional[TSchemaTables]: Returns an update that was applied at the destination.
        """
        self._verify_schema()
        return expected_update

    @abstractmethod
    def start_file_load(self, table: TTableSchema, file_path: str, load_id: str) -> LoadJob:
        """Creates and starts a load job for a particular `table` with content in `file_path`"""
        pass

    @abstractmethod
    def restore_file_load(self, file_path: str) -> LoadJob:
        """Finds and restores already started loading job identified by `file_path` if destination supports it."""
        pass

    def get_truncate_destination_table_dispositions(self) -> List[TWriteDisposition]:
        # in the base job, all replace strategies are treated the same, see filesystem for example
        return ["replace"]

    def create_table_chain_completed_followup_jobs(self, table_chain: Sequence[TTableSchema]) -> List[NewLoadJob]:
        """Creates a list of followup jobs that should be executed after a table chain is completed"""
        return []

    @abstractmethod
    def complete_load(self, load_id: str) -> None:
        """Marks the load package with `load_id` as completed in the destination. Before such commit is done, the data with `load_id` is invalid."""
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


class WithStateSync(ABC):

    @abstractmethod
    def get_stored_schema(self) -> Optional[StorageSchemaInfo]:
        """Retrieves newest schema from destination storage"""
        pass

    @abstractmethod
    def get_stored_schema_by_hash(self, version_hash: str) -> StorageSchemaInfo:
        pass

    @abstractmethod
    def get_stored_state(self, pipeline_name: str) -> Optional[StateInfo]:
        """Loads compressed state from destination storage"""
        pass


class WithStagingDataset(ABC):
    """Adds capability to use staging dataset and request it from the loader"""

    @abstractmethod
    def get_stage_dispositions(self) -> List[TWriteDisposition]:
        """Returns a list of write dispositions that require staging dataset"""
        return []

    @abstractmethod
    def with_staging_dataset(self)-> ContextManager["JobClientBase"]:
        """Executes job client methods on staging dataset"""
        return self  # type: ignore


TDestinationReferenceArg = Union["DestinationReference", ModuleType, None, str]


class DestinationReference(Protocol):
    __name__: str
    """Name of the destination"""

    def capabilities(self) -> DestinationCapabilitiesContext:
        """Destination capabilities ie. supported loader file formats, identifier name lengths, naming conventions, escape function etc."""

    def client(self, schema: Schema, initial_config: DestinationClientConfiguration = config.value) -> "JobClientBase":
        """A job client responsible for starting and resuming load jobs"""

    def spec(self) -> Type[DestinationClientConfiguration]:
        """A spec of destination configuration that also contains destination credentials"""

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
                if "." in destination:
                    raise UnknownDestinationModule(destination)
                else:
                    # allow local external module imported without dot
                    try:
                        destination_ref = cast(DestinationReference, import_module(destination))
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

    @staticmethod
    def to_name(destination: TDestinationReferenceArg) -> str:
        if isinstance(destination, ModuleType):
            return get_module_name(destination)
        return destination.split(".")[-1]  # type: ignore
