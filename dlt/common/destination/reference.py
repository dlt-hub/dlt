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

from dlt.common import logger, pendulum
from dlt.common.configuration.specs.base_configuration import extract_inner_hint
from dlt.common.destination.utils import verify_schema_capabilities
from dlt.common.exceptions import TerminalValueError
from dlt.common.metrics import LoadJobMetrics
from dlt.common.normalizers.naming import NamingConvention
from dlt.common.schema import Schema, TTableSchema, TSchemaTables
from dlt.common.schema.utils import (
    get_file_format,
    get_write_disposition,
    get_table_format,
    get_merge_strategy,
)
from dlt.common.configuration import configspec, resolve_configuration, known_sections, NotResolved
from dlt.common.configuration.specs import BaseConfiguration, CredentialsConfiguration
from dlt.common.destination.capabilities import DestinationCapabilitiesContext
from dlt.common.destination.exceptions import (
    InvalidDestinationReference,
    UnknownDestinationModule,
    DestinationSchemaTampered,
    DestinationTransientException,
    DestinationTerminalException,
)
from dlt.common.schema.exceptions import UnknownTableException
from dlt.common.storages import FileStorage
from dlt.common.storages.load_storage import ParsedLoadJobFileName
from dlt.common.storages.load_package import LoadJobInfo, TPipelineStateDoc

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

    @classmethod
    def from_normalized_mapping(
        cls, normalized_doc: Dict[str, Any], naming_convention: NamingConvention
    ) -> "StorageSchemaInfo":
        """Instantiate this class from mapping where keys are normalized according to given naming convention

        Args:
            normalized_doc: Mapping with normalized keys (e.g. {Version: ..., SchemaName: ...})
            naming_convention: Naming convention that was used to normalize keys

        Returns:
            StorageSchemaInfo: Instance of this class
        """
        return cls(
            version_hash=normalized_doc[naming_convention.normalize_identifier("version_hash")],
            schema_name=normalized_doc[naming_convention.normalize_identifier("schema_name")],
            version=normalized_doc[naming_convention.normalize_identifier("version")],
            engine_version=normalized_doc[naming_convention.normalize_identifier("engine_version")],
            inserted_at=normalized_doc[naming_convention.normalize_identifier("inserted_at")],
            schema=normalized_doc[naming_convention.normalize_identifier("schema")],
        )


@dataclasses.dataclass
class StateInfo:
    version: int
    engine_version: int
    pipeline_name: str
    state: str
    created_at: datetime.datetime
    version_hash: Optional[str] = None
    _dlt_load_id: Optional[str] = None

    def as_doc(self) -> TPipelineStateDoc:
        doc: TPipelineStateDoc = dataclasses.asdict(self)  # type: ignore[assignment]
        if self._dlt_load_id is None:
            doc.pop("_dlt_load_id")
        if self.version_hash is None:
            doc.pop("version_hash")
        return doc

    @classmethod
    def from_normalized_mapping(
        cls, normalized_doc: Dict[str, Any], naming_convention: NamingConvention
    ) -> "StateInfo":
        """Instantiate this class from mapping where keys are normalized according to given naming convention

        Args:
            normalized_doc: Mapping with normalized keys (e.g. {Version: ..., PipelineName: ...})
            naming_convention: Naming convention that was used to normalize keys

        Returns:
            StateInfo: Instance of this class
        """
        return cls(
            version=normalized_doc[naming_convention.normalize_identifier("version")],
            engine_version=normalized_doc[naming_convention.normalize_identifier("engine_version")],
            pipeline_name=normalized_doc[naming_convention.normalize_identifier("pipeline_name")],
            state=normalized_doc[naming_convention.normalize_identifier("state")],
            created_at=normalized_doc[naming_convention.normalize_identifier("created_at")],
            version_hash=normalized_doc.get(naming_convention.normalize_identifier("version_hash")),
            _dlt_load_id=normalized_doc.get(naming_convention.normalize_identifier("_dlt_load_id")),
        )


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

    @classmethod
    def credentials_type(
        cls, config: "DestinationClientConfiguration" = None
    ) -> Type[CredentialsConfiguration]:
        """Figure out credentials type, using hint resolvers for dynamic types

        For correct type resolution of filesystem, config should have bucket_url populated
        """
        key = "credentials"
        type_ = cls.get_resolvable_fields()[key]
        if key in cls.__hint_resolvers__ and config is not None:
            try:
                # Type hint for this field is created dynamically
                type_ = cls.__hint_resolvers__[key](config)
            except Exception:
                # we suppress failed hint resolutions
                pass
        return extract_inner_hint(type_)


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
    staging_dataset_name_layout: str = "%s_staging"
    """Layout for staging dataset, where %s is replaced with dataset name. placeholder is optional"""
    enable_dataset_name_normalization: bool = True
    """Whether to normalize the dataset name. Affects staging dataset as well."""

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
        dataset_name = self._make_dataset_name(schema.name)
        if not dataset_name:
            return dataset_name
        else:
            return (
                schema.naming.normalize_table_identifier(dataset_name)
                if self.enable_dataset_name_normalization
                else dataset_name
            )

    def normalize_staging_dataset_name(self, schema: Schema) -> str:
        """Builds staging dataset name out of dataset_name and staging_dataset_name_layout."""
        if "%s" in self.staging_dataset_name_layout:
            # if dataset name is empty, staging dataset name is also empty
            dataset_name = self._make_dataset_name(schema.name)
            if not dataset_name:
                return dataset_name
            # fill the placeholder
            dataset_name = self.staging_dataset_name_layout % dataset_name
        else:
            # no placeholder, then layout is a full name. so you can have a single staging dataset
            dataset_name = self.staging_dataset_name_layout

        return (
            schema.naming.normalize_table_identifier(dataset_name)
            if self.enable_dataset_name_normalization
            else dataset_name
        )

    def _make_dataset_name(self, schema_name: str) -> str:
        if not schema_name:
            raise ValueError("schema_name is None or empty")

        # if default schema is None then suffix is not added
        if self.default_schema_name is not None and schema_name != self.default_schema_name:
            return (self.dataset_name or "") + "_" + schema_name

        return self.dataset_name


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
    truncate_tables_on_staging_destination_before_load: bool = True
    """If dlt should truncate the tables on staging destination before loading data."""


TLoadJobState = Literal["ready", "running", "failed", "retry", "completed"]


class LoadJob(ABC):
    """
    A stateful load job, represents one job file
    """

    def __init__(self, file_path: str) -> None:
        self._file_path = file_path
        self._file_name = FileStorage.get_file_name_from_file_path(file_path)
        # NOTE: we only accept a full filepath in the constructor
        assert self._file_name != self._file_path
        self._parsed_file_name = ParsedLoadJobFileName.parse(self._file_name)
        self._started_at: pendulum.DateTime = None
        self._finished_at: pendulum.DateTime = None

    def job_id(self) -> str:
        """The job id that is derived from the file name and does not changes during job lifecycle"""
        return self._parsed_file_name.job_id()

    def file_name(self) -> str:
        """A name of the job file"""
        return self._file_name

    def job_file_info(self) -> ParsedLoadJobFileName:
        return self._parsed_file_name

    @abstractmethod
    def state(self) -> TLoadJobState:
        """Returns current state. Should poll external resource if necessary."""
        pass

    @abstractmethod
    def exception(self) -> str:
        """The exception associated with failed or retry states"""
        pass

    def metrics(self) -> Optional[LoadJobMetrics]:
        """Returns job execution metrics"""
        return LoadJobMetrics(
            self._parsed_file_name.job_id(),
            self._file_path,
            self._parsed_file_name.table_name,
            self._started_at,
            self._finished_at,
            self.state(),
            None,
        )


class RunnableLoadJob(LoadJob, ABC):
    """Represents a runnable job that loads a single file

    Each job starts in "running" state and ends in one of terminal states: "retry", "failed" or "completed".
    Each job is uniquely identified by a file name. The file is guaranteed to exist in "running" state. In terminal state, the file may not be present.
    In "running" state, the loader component periodically gets the state via `status()` method. When terminal state is reached, load job is discarded and not called again.
    `exception` method is called to get error information in "failed" and "retry" states.

    The `__init__` method is responsible to put the Job in "running" state. It may raise `LoadClientTerminalException` and `LoadClientTransientException` to
    immediately transition job into "failed" or "retry" state respectively.
    """

    def __init__(self, file_path: str) -> None:
        """
        File name is also a job id (or job id is deterministically derived) so it must be globally unique
        """
        # ensure file name
        super().__init__(file_path)
        self._state: TLoadJobState = "ready"
        self._exception: Exception = None

        # variables needed by most jobs, set by the loader in set_run_vars
        self._schema: Schema = None
        self._load_table: TTableSchema = None
        self._load_id: str = None
        self._job_client: "JobClientBase" = None

    def set_run_vars(self, load_id: str, schema: Schema, load_table: TTableSchema) -> None:
        """
        called by the loader right before the job is run
        """
        self._load_id = load_id
        self._schema = schema
        self._load_table = load_table

    @property
    def load_table_name(self) -> str:
        return self._load_table["name"]

    def run_managed(
        self,
        job_client: "JobClientBase",
    ) -> None:
        """
        wrapper around the user implemented run method
        """
        # only jobs that are not running or have not reached a final state
        # may be started
        assert self._state in ("ready", "retry")
        self._job_client = job_client

        # filepath is now moved to running
        try:
            self._state = "running"
            self._started_at = pendulum.now()
            self._job_client.prepare_load_job_execution(self)
            self.run()
            self._state = "completed"
        except (DestinationTerminalException, TerminalValueError) as e:
            self._state = "failed"
            self._exception = e
            logger.exception(f"Terminal exception in job {self.job_id()} in file {self._file_path}")
        except (DestinationTransientException, Exception) as e:
            self._state = "retry"
            self._exception = e
            logger.exception(
                f"Transient exception in job {self.job_id()} in file {self._file_path}"
            )
        finally:
            self._finished_at = pendulum.now()
            # sanity check
            assert self._state in ("completed", "retry", "failed")

    @abstractmethod
    def run(self) -> None:
        """
        run the actual job, this will be executed on a thread and should be implemented by the user
        exception will be handled outside of this function
        """
        raise NotImplementedError()

    def state(self) -> TLoadJobState:
        """Returns current state. Should poll external resource if necessary."""
        return self._state

    def exception(self) -> str:
        """The exception associated with failed or retry states"""
        return str(self._exception)


class FollowupJobRequest:
    """Base class for follow up jobs that should be created"""

    @abstractmethod
    def new_file_path(self) -> str:
        """Path to a newly created temporary job file. If empty, no followup job should be created"""
        pass


class HasFollowupJobs:
    """Adds a trait that allows to create single or table chain followup jobs"""

    def create_followup_jobs(self, final_state: TLoadJobState) -> List[FollowupJobRequest]:
        """Return list of jobs requests for jobs that should be created. `final_state` is state to which this job transits"""
        return []


class JobClientBase(ABC):
    def __init__(
        self,
        schema: Schema,
        config: DestinationClientConfiguration,
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        self.schema = schema
        self.config = config
        self.capabilities = capabilities

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
    def create_load_job(
        self, table: TTableSchema, file_path: str, load_id: str, restore: bool = False
    ) -> LoadJob:
        """Creates a load job for a particular `table` with content in `file_path`"""
        pass

    def prepare_load_job_execution(  # noqa: B027, optional override
        self, job: RunnableLoadJob
    ) -> None:
        """Prepare the connected job client for the execution of a load job (used for query tags in sql clients)"""
        pass

    def should_truncate_table_before_load(self, table: TTableSchema) -> bool:
        return table["write_disposition"] == "replace"

    def create_table_chain_completed_followup_jobs(
        self,
        table_chain: Sequence[TTableSchema],
        completed_table_chain_jobs: Optional[Sequence[LoadJobInfo]] = None,
    ) -> List[FollowupJobRequest]:
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
        """Verifies schema before loading"""
        if exceptions := verify_schema_capabilities(
            self.schema, self.capabilities, self.config.destination_type, warnings=False
        ):
            for exception in exceptions:
                logger.error(str(exception))
            raise exceptions[0]

    def prepare_load_table(
        self, table_name: str, prepare_for_staging: bool = False
    ) -> TTableSchema:
        try:
            # make a copy of the schema so modifications do not affect the original document
            table = deepcopy(self.schema.tables[table_name])
            # add write disposition if not specified - in child tables
            if "write_disposition" not in table:
                table["write_disposition"] = get_write_disposition(self.schema.tables, table_name)
            if "x-merge-strategy" not in table:
                table["x-merge-strategy"] = get_merge_strategy(self.schema.tables, table_name)  # type: ignore[typeddict-unknown-key]
            if "table_format" not in table:
                table["table_format"] = get_table_format(self.schema.tables, table_name)
            if "file_format" not in table:
                table["file_format"] = get_file_format(self.schema.tables, table_name)
            return table
        except KeyError:
            raise UnknownTableException(self.schema.name, table_name)


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


class SupportsStagingDestination(ABC):
    """Adds capability to support a staging destination for the load"""

    def should_load_data_to_staging_dataset_on_staging_destination(
        self, table: TTableSchema
    ) -> bool:
        """If set to True, and staging destination is configured, the data will be loaded to staging dataset on staging destination
        instead of a regular dataset on staging destination. Currently it is used by Athena Iceberg which uses staging dataset
        on staging destination to copy data to iceberg tables stored on regular dataset on staging destination.
        The default is to load data to regular dataset on staging destination from where warehouses like Snowflake (that have their
        own storage) will copy data.
        """
        return False

    @abstractmethod
    def should_truncate_table_before_load_on_staging_destination(self, table: TTableSchema) -> bool:
        """If set to True, data in `table` will be truncated on staging destination (regular dataset). This is the default behavior which
        can be changed with a config flag.
        For Athena + Iceberg this setting is always False - Athena uses regular dataset to store Iceberg tables and we avoid touching it.
        For Athena we truncate those tables only on "replace" write disposition.
        """
        pass


# TODO: type Destination properly
TDestinationReferenceArg = Union[
    str, "Destination[Any, Any]", Callable[..., "Destination[Any, Any]"], None
]


class Destination(ABC, Generic[TDestinationConfig, TDestinationClient]):
    """A destination factory that can be partially pre-configured
    with credentials and other config params.
    """

    config_params: Dict[str, Any]
    """Explicit config params, overriding any injected or default values."""
    caps_params: Dict[str, Any]
    """Explicit capabilities params, overriding any default values for this destination"""

    def __init__(self, **kwargs: Any) -> None:
        # Create initial unresolved destination config
        # Argument defaults are filtered out here because we only want arguments passed explicitly
        # to supersede config from the environment or pipeline args
        sig = inspect.signature(self.__class__.__init__)
        params = sig.parameters

        # get available args
        spec = self.spec
        spec_fields = spec.get_resolvable_fields()
        caps_fields = DestinationCapabilitiesContext.get_resolvable_fields()

        # remove default kwargs
        kwargs = {k: v for k, v in kwargs.items() if k not in params or v != params[k].default}

        # warn on unknown params
        for k in list(kwargs):
            if k not in spec_fields and k not in caps_fields:
                logger.warning(
                    f"When initializing destination factory of type {self.destination_type},"
                    f" argument {k} is not a valid field in {spec.__name__} or destination"
                    " capabilities"
                )
                kwargs.pop(k)

        self.config_params = {k: v for k, v in kwargs.items() if k in spec_fields}
        self.caps_params = {k: v for k, v in kwargs.items() if k in caps_fields}

    @property
    @abstractmethod
    def spec(self) -> Type[TDestinationConfig]:
        """A spec of destination configuration that also contains destination credentials"""
        ...

    def capabilities(
        self, config: Optional[TDestinationConfig] = None, naming: Optional[NamingConvention] = None
    ) -> DestinationCapabilitiesContext:
        """Destination capabilities ie. supported loader file formats, identifier name lengths, naming conventions, escape function etc.
        Explicit caps arguments passed to the factory init and stored in `caps_params` are applied.

        If `config` is provided, it is used to adjust the capabilities, otherwise the explicit config composed just of `config_params` passed
          to factory init is applied
        If `naming` is provided, the case sensitivity and case folding are adjusted.
        """
        caps = self._raw_capabilities()
        caps.update(self.caps_params)
        # get explicit config if final config not passed
        if config is None:
            # create mock credentials to avoid credentials being resolved
            init_config = self.spec()
            init_config.update(self.config_params)
            credentials = self.spec.credentials_type(init_config)()
            credentials.__is_resolved__ = True
            config = self.spec(credentials=credentials)
            try:
                config = self.configuration(config, accept_partial=True)
            except Exception:
                # in rare cases partial may fail ie. when invalid native value is present
                # in that case we fallback to "empty" config
                pass
        return self.adjust_capabilities(caps, config, naming)

    @abstractmethod
    def _raw_capabilities(self) -> DestinationCapabilitiesContext:
        """Returns raw capabilities, before being adjusted with naming convention and config"""
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

    def configuration(
        self, initial_config: TDestinationConfig, accept_partial: bool = False
    ) -> TDestinationConfig:
        """Get a fully resolved destination config from the initial config"""

        config = resolve_configuration(
            initial_config or self.spec(),
            sections=(known_sections.DESTINATION, self.destination_name),
            # Already populated values will supersede resolved env config
            explicit_value=self.config_params,
            accept_partial=accept_partial,
        )
        return config

    def client(
        self, schema: Schema, initial_config: TDestinationConfig = None
    ) -> TDestinationClient:
        """Returns a configured instance of the destination's job client"""
        config = self.configuration(initial_config)
        return self.client_class(schema, config, self.capabilities(config, schema.naming))

    @classmethod
    def adjust_capabilities(
        cls,
        caps: DestinationCapabilitiesContext,
        config: TDestinationConfig,
        naming: Optional[NamingConvention],
    ) -> DestinationCapabilitiesContext:
        """Adjust the capabilities to match the case sensitivity as requested by naming convention."""
        # if naming not provided, skip the adjustment
        if not naming or not naming.is_case_sensitive:
            # all destinations are configured to be case insensitive so there's nothing to adjust
            return caps
        if not caps.has_case_sensitive_identifiers:
            if caps.casefold_identifier is str:
                logger.info(
                    f"Naming convention {naming.name()} is case sensitive but the destination does"
                    " not support case sensitive identifiers. Nevertheless identifier casing will"
                    " be preserved in the destination schema."
                )
            else:
                logger.warn(
                    f"Naming convention {naming.name()} is case sensitive but the destination does"
                    " not support case sensitive identifiers. Destination will case fold all the"
                    f" identifiers with {caps.casefold_identifier}"
                )
        else:
            # adjust case folding to store casefold identifiers in the schema
            if caps.casefold_identifier is not str:
                caps.casefold_identifier = str
                logger.info(
                    f"Enabling case sensitive identifiers for naming convention {naming.name()}"
                )
        return caps

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
        """Normalizes destination type string into a canonical form. Assumes that type names without dots correspond to built in destinations."""
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
        credentials: Optional[Any] = None,
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


TDestination = Destination[DestinationClientConfiguration, JobClientBase]
