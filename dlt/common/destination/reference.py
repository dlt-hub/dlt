from abc import ABC, abstractmethod
import dataclasses
from importlib import import_module
from types import TracebackType
from typing import (
    Callable,
    ClassVar,
    Optional,
    NamedTuple,
    Literal,
    Sequence,
    Iterable,
    Type,
    Union,
    List,
    ContextManager,
    Dict,
    Any,
    TypeVar,
    Generic,
)
from typing_extensions import Annotated
import datetime  # noqa: 251
from copy import deepcopy
import inspect

from dlt.common import logger
from dlt.common.schema import Schema, TTableSchema, TSchemaTables
from dlt.common.schema.typing import MERGE_STRATEGIES
from dlt.common.schema.exceptions import SchemaException
from dlt.common.schema.utils import (
    get_write_disposition,
    get_table_format,
    get_columns_names_with_prop,
    has_column_with_prop,
    get_first_column_name_with_prop,
)
from dlt.common.configuration import configspec, resolve_configuration, known_sections, NotResolved
from dlt.common.configuration.specs import BaseConfiguration, CredentialsConfiguration
from dlt.common.configuration.accessors import config
from dlt.common.destination.capabilities import DestinationCapabilitiesContext
from dlt.common.destination.exceptions import (
    IdentifierTooLongException,
    InvalidDestinationReference,
    UnknownDestinationModule,
    DestinationSchemaTampered,
)
from dlt.common.schema.utils import is_complete_column
from dlt.common.schema.exceptions import UnknownTableException
from dlt.common.storages import FileStorage
from dlt.common.storages.load_storage import ParsedLoadJobFileName

TLoaderReplaceStrategy = Literal["truncate-and-insert", "insert-from-staging", "staging-optimized"]
TDestinationConfig = TypeVar("TDestinationConfig", bound="DestinationClientConfiguration")
TDestinationClient = TypeVar("TDestinationClient", bound="JobClientBase")
TDestinationDwhClient = TypeVar("TDestinationDwhClient", bound="DestinationClientDwhConfiguration")

DEFAULT_FILE_LAYOUT = "{table_name}/{load_id}.{file_id}.{ext}"


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
    destination_type: Annotated[str, NotResolved()] = dataclasses.field(
        default=None, init=False, repr=False, compare=False
    )  # which destination to load data to
    credentials: Optional[CredentialsConfiguration] = None
    destination_name: Optional[str] = (
        None  # name of the destination, if not set, destination_type is used
    )
    environment: Optional[str] = None

    def fingerprint(self) -> str:
        """Returns a destination fingerprint which is a hash of selected configuration fields. ie. host in case of connection string"""
        return ""

    def __str__(self) -> str:
        """Return displayable destination location"""
        return str(self.credentials)

    def on_resolved(self) -> None:
        self.destination_name = self.destination_name or self.destination_type


@configspec
class DestinationClientDwhConfiguration(DestinationClientConfiguration):
    """Configuration of a destination that supports datasets/schemas"""

    dataset_name: Annotated[str, NotResolved()] = dataclasses.field(
        default=None, init=False, repr=False, compare=False
    )  # dataset cannot be resolved
    """dataset name in the destination to load data to, for schemas that are not default schema, it is used as dataset prefix"""
    default_schema_name: Annotated[Optional[str], NotResolved()] = dataclasses.field(
        default=None, init=False, repr=False, compare=False
    )
    """name of default schema to be used to name effective dataset to load data to"""
    replace_strategy: TLoaderReplaceStrategy = "truncate-and-insert"
    """How to handle replace disposition for this destination, can be classic or staging"""

    def _bind_dataset_name(
        self: TDestinationDwhClient, dataset_name: str, default_schema_name: str = None
    ) -> TDestinationDwhClient:
        """Binds the dataset and default schema name to the configuration

        This method is intended to be used internally.
        """
        self.dataset_name = dataset_name
        self.default_schema_name = default_schema_name
        return self

    def normalize_dataset_name(self, schema: Schema) -> str:
        """Builds full db dataset (schema) name out of configured dataset name and schema name: {dataset_name}_{schema.name}. The resulting name is normalized.

        If default schema name is None or equals schema.name, the schema suffix is skipped.
        """
        if not schema.name:
            raise ValueError("schema_name is None or empty")

        # if default schema is None then suffix is not added
        if self.default_schema_name is not None and schema.name != self.default_schema_name:
            # also normalize schema name. schema name is Python identifier and here convention may be different
            return schema.naming.normalize_table_identifier(
                (self.dataset_name or "") + "_" + schema.name
            )

        return (
            self.dataset_name
            if not self.dataset_name
            else schema.naming.normalize_table_identifier(self.dataset_name)
        )


@configspec
class DestinationClientStagingConfiguration(DestinationClientDwhConfiguration):
    """Configuration of a staging destination, able to store files with desired `layout` at `bucket_url`.

    Also supports datasets and can act as standalone destination.
    """

    as_staging: bool = False
    bucket_url: str = None
    # layout of the destination files
    layout: str = DEFAULT_FILE_LAYOUT


@configspec
class DestinationClientDwhWithStagingConfiguration(DestinationClientDwhConfiguration):
    """Configuration of a destination that can take data from staging destination"""

    staging_config: Optional[DestinationClientStagingConfiguration] = None
    """configuration of the staging, if present, injected at runtime"""


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
        """The job id that is derived from the file name and does not changes during job lifecycle"""
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

    def create_followup_jobs(self, final_state: TLoadJobState) -> List[NewLoadJob]:
        """Return list of new jobs. `final_state` is state to which this job transits"""
        return []


class DoNothingJob(LoadJob):
    """The most lazy class of dlt"""

    def __init__(self, file_path: str) -> None:
        super().__init__(FileStorage.get_file_name_from_file_path(file_path))

    def state(self) -> TLoadJobState:
        # this job is always done
        return "completed"

    def exception(self) -> str:
        # this part of code should be never reached
        raise NotImplementedError()


class DoNothingFollowupJob(DoNothingJob, FollowupJob):
    """The second most lazy class of dlt"""

    pass


class JobClientBase(ABC):
    capabilities: ClassVar[DestinationCapabilitiesContext] = None

    def __init__(self, schema: Schema, config: DestinationClientConfiguration) -> None:
        self.schema = schema
        self.config = config

    @abstractmethod
    def initialize_storage(self, truncate_tables: Iterable[str] = None) -> None:
        """Prepares storage to be used ie. creates database schema or file system folder. Truncates requested tables."""
        pass

    @abstractmethod
    def is_storage_initialized(self) -> bool:
        """Returns if storage is ready to be read/written."""
        pass

    @abstractmethod
    def drop_storage(self) -> None:
        """Brings storage back into not initialized state. Typically data in storage is destroyed."""
        pass

    def update_stored_schema(
        self,
        only_tables: Iterable[str] = None,
        expected_update: TSchemaTables = None,
    ) -> Optional[TSchemaTables]:
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
        # make sure that schema being saved was not modified from the moment it was loaded from storage
        version_hash = self.schema.version_hash
        if self.schema.is_modified:
            raise DestinationSchemaTampered(
                self.schema.name, version_hash, self.schema.stored_version_hash
            )
        return expected_update

    @abstractmethod
    def start_file_load(self, table: TTableSchema, file_path: str, load_id: str) -> LoadJob:
        """Creates and starts a load job for a particular `table` with content in `file_path`"""
        pass

    @abstractmethod
    def restore_file_load(self, file_path: str) -> LoadJob:
        """Finds and restores already started loading job identified by `file_path` if destination supports it."""
        pass

    def should_truncate_table_before_load(self, table: TTableSchema) -> bool:
        return table["write_disposition"] == "replace"

    def create_table_chain_completed_followup_jobs(
        self, table_chain: Sequence[TTableSchema]
    ) -> List[NewLoadJob]:
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
    def __exit__(
        self, exc_type: Type[BaseException], exc_val: BaseException, exc_tb: TracebackType
    ) -> None:
        pass

    def _verify_schema(self) -> None:
        """Verifies and cleans up a schema before loading

        * Checks all table and column name lengths against destination capabilities and raises on too long identifiers
        * Removes and warns on (unbound) incomplete columns
        """

        for table in self.schema.data_tables():
            table_name = table["name"]
            if len(table_name) > self.capabilities.max_identifier_length:
                raise IdentifierTooLongException(
                    self.config.destination_type,
                    "table",
                    table_name,
                    self.capabilities.max_identifier_length,
                )
            if table.get("write_disposition") == "merge":
                if "x-merge-strategy" in table and table["x-merge-strategy"] not in MERGE_STRATEGIES:  # type: ignore[typeddict-item]
                    raise SchemaException(
                        f'"{table["x-merge-strategy"]}" is not a valid merge strategy. '  # type: ignore[typeddict-item]
                        f"""Allowed values: {', '.join(['"' + s + '"' for s in MERGE_STRATEGIES])}."""
                    )
                if (
                    table.get("x-merge-strategy") == "delete-insert"
                    and not has_column_with_prop(table, "primary_key")
                    and not has_column_with_prop(table, "merge_key")
                ):
                    logger.warning(
                        f"Table {table_name} has `write_disposition` set to `merge`"
                        " and `merge_strategy` set to `delete-insert`, but no primary or"
                        " merge keys defined."
                        " dlt will fall back to `append` for this table."
                    )
            if has_column_with_prop(table, "hard_delete"):
                if len(get_columns_names_with_prop(table, "hard_delete")) > 1:
                    raise SchemaException(
                        f'Found multiple "hard_delete" column hints for table "{table_name}" in'
                        f' schema "{self.schema.name}" while only one is allowed:'
                        f' {", ".join(get_columns_names_with_prop(table, "hard_delete"))}.'
                    )
                if table.get("write_disposition") in ("replace", "append"):
                    logger.warning(
                        f"""The "hard_delete" column hint for column "{get_first_column_name_with_prop(table, 'hard_delete')}" """
                        f'in table "{table_name}" with write disposition'
                        f' "{table.get("write_disposition")}"'
                        f' in schema "{self.schema.name}" will be ignored.'
                        ' The "hard_delete" column hint is only applied when using'
                        ' the "merge" write disposition.'
                    )
            if has_column_with_prop(table, "dedup_sort"):
                if len(get_columns_names_with_prop(table, "dedup_sort")) > 1:
                    raise SchemaException(
                        f'Found multiple "dedup_sort" column hints for table "{table_name}" in'
                        f' schema "{self.schema.name}" while only one is allowed:'
                        f' {", ".join(get_columns_names_with_prop(table, "dedup_sort"))}.'
                    )
                if table.get("write_disposition") in ("replace", "append"):
                    logger.warning(
                        f"""The "dedup_sort" column hint for column "{get_first_column_name_with_prop(table, 'dedup_sort')}" """
                        f'in table "{table_name}" with write disposition'
                        f' "{table.get("write_disposition")}"'
                        f' in schema "{self.schema.name}" will be ignored.'
                        ' The "dedup_sort" column hint is only applied when using'
                        ' the "merge" write disposition.'
                    )
                if table.get("write_disposition") == "merge" and not has_column_with_prop(
                    table, "primary_key"
                ):
                    logger.warning(
                        f"""The "dedup_sort" column hint for column "{get_first_column_name_with_prop(table, 'dedup_sort')}" """
                        f'in table "{table_name}" with write disposition'
                        f' "{table.get("write_disposition")}"'
                        f' in schema "{self.schema.name}" will be ignored.'
                        ' The "dedup_sort" column hint is only applied when a'
                        " primary key has been specified."
                    )
            for column_name, column in dict(table["columns"]).items():
                if len(column_name) > self.capabilities.max_column_identifier_length:
                    raise IdentifierTooLongException(
                        self.config.destination_type,
                        "column",
                        f"{table_name}.{column_name}",
                        self.capabilities.max_column_identifier_length,
                    )
                if not is_complete_column(column):
                    logger.warning(
                        f"A column {column_name} in table {table_name} in schema"
                        f" {self.schema.name} is incomplete. It was not bound to the data during"
                        " normalizations stage and its data type is unknown. Did you add this"
                        " column manually in code ie. as a merge key?"
                    )

    def prepare_load_table(
        self, table_name: str, prepare_for_staging: bool = False
    ) -> TTableSchema:
        try:
            # make a copy of the schema so modifications do not affect the original document
            table = deepcopy(self.schema.tables[table_name])
            # add write disposition if not specified - in child tables
            if "write_disposition" not in table:
                table["write_disposition"] = get_write_disposition(self.schema.tables, table_name)
            if "table_format" not in table:
                table["table_format"] = get_table_format(self.schema.tables, table_name)
            return table
        except KeyError:
            raise UnknownTableException(table_name)


class WithStateSync(ABC):
    @abstractmethod
    def get_stored_schema(self) -> Optional[StorageSchemaInfo]:
        """Retrieves newest schema from destination storage"""
        pass

    @abstractmethod
    def get_stored_schema_by_hash(self, version_hash: str) -> StorageSchemaInfo:
        """retrieves the stored schema by hash"""
        pass

    @abstractmethod
    def get_stored_state(self, pipeline_name: str) -> Optional[StateInfo]:
        """Loads compressed state from destination storage"""
        pass


class WithStagingDataset(ABC):
    """Adds capability to use staging dataset and request it from the loader"""

    @abstractmethod
    def should_load_data_to_staging_dataset(self, table: TTableSchema) -> bool:
        return False

    @abstractmethod
    def with_staging_dataset(self) -> ContextManager["JobClientBase"]:
        """Executes job client methods on staging dataset"""
        return self  # type: ignore


class SupportsStagingDestination:
    """Adds capability to support a staging destination for the load"""

    def should_load_data_to_staging_dataset_on_staging_destination(
        self, table: TTableSchema
    ) -> bool:
        return False

    def should_truncate_table_before_load_on_staging_destination(self, table: TTableSchema) -> bool:
        # the default is to truncate the tables on the staging destination...
        return True


# TODO: type Destination properly
TDestinationReferenceArg = Union[
    str, "Destination[Any, Any]", Callable[..., "Destination[Any, Any]"], None
]


class Destination(ABC, Generic[TDestinationConfig, TDestinationClient]):
    """A destination factory that can be partially pre-configured
    with credentials and other config params.
    """

    config_params: Optional[Dict[str, Any]] = None

    def __init__(self, **kwargs: Any) -> None:
        # Create initial unresolved destination config
        # Argument defaults are filtered out here because we only want arguments passed explicitly
        # to supersede config from the environment or pipeline args
        sig = inspect.signature(self.__class__.__init__)
        params = sig.parameters
        self.config_params = {
            k: v for k, v in kwargs.items() if k not in params or v != params[k].default
        }

    @property
    @abstractmethod
    def spec(self) -> Type[TDestinationConfig]:
        """A spec of destination configuration that also contains destination credentials"""
        ...

    @abstractmethod
    def capabilities(self) -> DestinationCapabilitiesContext:
        """Destination capabilities ie. supported loader file formats, identifier name lengths, naming conventions, escape function etc."""
        ...

    @property
    def destination_name(self) -> str:
        """The destination name will either be explicitly set while creating the destination or will be taken from the type"""
        return self.config_params.get("destination_name") or self.to_name(self.destination_type)

    @property
    def destination_type(self) -> str:
        full_path = self.__class__.__module__ + "." + self.__class__.__qualname__
        return Destination.normalize_type(full_path)

    @property
    def destination_description(self) -> str:
        return f"{self.destination_name}({self.destination_type})"

    @property
    @abstractmethod
    def client_class(self) -> Type[TDestinationClient]:
        """A job client class responsible for starting and resuming load jobs"""
        ...

    def configuration(self, initial_config: TDestinationConfig) -> TDestinationConfig:
        """Get a fully resolved destination config from the initial config"""
        config = resolve_configuration(
            initial_config,
            sections=(known_sections.DESTINATION, self.destination_name),
            # Already populated values will supersede resolved env config
            explicit_value=self.config_params,
        )
        return config

    @staticmethod
    def to_name(ref: TDestinationReferenceArg) -> str:
        if ref is None:
            raise InvalidDestinationReference(ref)
        if isinstance(ref, str):
            return ref.rsplit(".", 1)[-1]
        if callable(ref):
            ref = ref()
        return ref.destination_name

    @staticmethod
    def normalize_type(destination_type: str) -> str:
        """Normalizes destination type string into a canonical form. Assumes that type names without dots correspond to build in destinations."""
        if "." not in destination_type:
            destination_type = "dlt.destinations." + destination_type
        # the next two lines shorten the dlt internal destination paths to dlt.destinations.<destination_type>
        name = Destination.to_name(destination_type)
        destination_type = destination_type.replace(
            f"dlt.destinations.impl.{name}.factory.", "dlt.destinations."
        )
        return destination_type

    @staticmethod
    def from_reference(
        ref: TDestinationReferenceArg,
        credentials: Optional[CredentialsConfiguration] = None,
        destination_name: Optional[str] = None,
        environment: Optional[str] = None,
        **kwargs: Any,
    ) -> Optional["Destination[DestinationClientConfiguration, JobClientBase]"]:
        """Instantiate destination from str reference.
        The ref can be a destination name or import path pointing to a destination class (e.g. `dlt.destinations.postgres`)
        """
        # if we only get a name but no ref, we assume that the name is the destination_type
        if ref is None and destination_name is not None:
            ref = destination_name
        if ref is None:
            return None
        # evaluate callable returning Destination
        if callable(ref):
            ref = ref()
        if isinstance(ref, Destination):
            if credentials or destination_name or environment:
                logger.warning(
                    "Cannot override credentials, destination_name or environment when passing a"
                    " Destination instance, these values will be ignored."
                )
            return ref
        if not isinstance(ref, str):
            raise InvalidDestinationReference(ref)
        try:
            module_path, attr_name = Destination.normalize_type(ref).rsplit(".", 1)
            dest_module = import_module(module_path)
        except ModuleNotFoundError as e:
            raise UnknownDestinationModule(ref) from e

        try:
            factory: Type[Destination[DestinationClientConfiguration, JobClientBase]] = getattr(
                dest_module, attr_name
            )
        except AttributeError as e:
            raise UnknownDestinationModule(ref) from e
        if credentials:
            kwargs["credentials"] = credentials
        if destination_name:
            kwargs["destination_name"] = destination_name
        if environment:
            kwargs["environment"] = environment
        try:
            dest = factory(**kwargs)
            dest.spec
        except Exception as e:
            raise InvalidDestinationReference(ref) from e
        return dest

    def client(
        self, schema: Schema, initial_config: TDestinationConfig = config.value
    ) -> TDestinationClient:
        """Returns a configured instance of the destination's job client"""
        return self.client_class(schema, self.configuration(initial_config))


TDestination = Destination[DestinationClientConfiguration, JobClientBase]
