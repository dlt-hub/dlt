from abc import ABC, abstractmethod
import dataclasses

from types import TracebackType
from typing import (
    Optional,
    NamedTuple,
    Literal,
    Sequence,
    Iterable,
    Type,
    List,
    ContextManager,
    Dict,
    Any,
    TypeVar,
    Tuple,
)
from typing_extensions import Annotated
import datetime  # noqa: 251

from dlt.common import logger, pendulum
from dlt.common.configuration.specs.base_configuration import extract_inner_hint
from dlt.common.configuration import configspec, NotResolved
from dlt.common.configuration.specs import BaseConfiguration, CredentialsConfiguration
from dlt.common.destination.typing import PreparedTableSchema
from dlt.common.destination.utils import (
    resolve_replace_strategy,
    verify_schema_capabilities,
    verify_supported_data_types,
)
from dlt.common.exceptions import TerminalException
from dlt.common.metrics import LoadJobMetrics
from dlt.common.normalizers.naming import NamingConvention

from dlt.common.schema import Schema, TSchemaTables
from dlt.common.schema.typing import (
    C_DLT_LOAD_ID,
    TLoaderReplaceStrategy,
    TTableFormat,
)
from dlt.common.destination.capabilities import DestinationCapabilitiesContext
from dlt.common.destination.exceptions import (
    DestinationSchemaTampered,
    DestinationTransientException,
)
from dlt.common.destination.utils import prepare_load_table
from dlt.common.storages import FileStorage
from dlt.common.storages.load_storage import ParsedLoadJobFileName
from dlt.common.storages.load_package import LoadJobInfo, TPipelineStateDoc
from dlt.common.typing import is_optional_type

TDestinationDwhClient = TypeVar("TDestinationDwhClient", bound="DestinationClientDwhConfiguration")

DEFAULT_FILE_LAYOUT = "{table_name}/{load_id}.{file_id}.{ext}"


class StorageSchemaInfo(NamedTuple):
    version_hash: str
    schema_name: str
    version: int
    engine_version: int
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

    def to_normalized_mapping(self, naming_convention: NamingConvention) -> Dict[str, Any]:
        """Convert this instance to mapping where keys are normalized according to given naming convention

        Args:
            naming_convention: Naming convention that should be used to normalize keys

        Returns:
            Dict[str, Any]: Mapping with normalized keys (e.g. {Version: ..., SchemaName: ...})
        """
        return {
            naming_convention.normalize_identifier(key): value
            for key, value in self._asdict().items()
        }


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
            doc.pop(C_DLT_LOAD_ID)  # type: ignore[misc]
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
            _dlt_load_id=normalized_doc.get(naming_convention.normalize_identifier(C_DLT_LOAD_ID)),
        )


@configspec
class DestinationClientConfiguration(BaseConfiguration):
    destination_type: Annotated[str, NotResolved()] = dataclasses.field(
        default=None, init=False, repr=False, compare=False
    )  # which destination to load data to
    credentials: Optional[CredentialsConfiguration] = None
    destination_name: Optional[str] = None  # name of the destination
    environment: Optional[str] = None

    def fingerprint(self) -> str:
        """Returns a destination fingerprint which is a hash of selected configuration fields. ie. host in case of connection string"""
        return ""

    def __str__(self) -> str:
        """Return displayable destination location"""
        return str(self.credentials)

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
    replace_strategy: Optional[TLoaderReplaceStrategy] = None
    """How to handle replace disposition for this destination, uses first strategy from caps if not declared"""
    staging_dataset_name_layout: str = "%s_staging"
    """Layout for staging dataset, where %s is replaced with dataset name. placeholder is optional"""
    enable_dataset_name_normalization: bool = True
    """Whether to normalize the dataset name. Affects staging dataset as well."""
    info_tables_query_threshold: int = 1000
    """Threshold for information schema tables query, if exceeded tables will be filtered in code."""

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
            return self._normalize_identifier(dataset_name, schema.naming)

    def normalize_staging_dataset_name(self, schema: Schema) -> str:
        """Builds staging dataset name out of dataset_name and staging_dataset_name_layout."""
        if "%s" in self.staging_dataset_name_layout:
            # staging dataset name is never empty, otherwise table names must clash
            dataset_name = self._make_dataset_name(schema.name)
            # fill the placeholder
            dataset_name = self.staging_dataset_name_layout % (dataset_name or "")
        else:
            # no placeholder, then layout is a full name. so you can have a single staging dataset
            dataset_name = self.staging_dataset_name_layout

        return self._normalize_identifier(dataset_name, schema.naming)

    def _normalize_identifier(self, identifier: str, naming: NamingConvention) -> str:
        norm_ident = (
            naming.normalize_table_identifier(identifier)
            if self.enable_dataset_name_normalization
            else identifier
        )
        if norm_ident != identifier:
            logger.warning(
                f"Due to normalization dataset name got changed from '{identifier}' to"
                f" '{norm_ident} which will be used to create db schemas or folders. `dataset_name`"
                " field in the pipeline instance will not be changed. We suggest that you use"
                " dataset names that do not need to be normalized or disable dataset name"
                " normalization via `enable_dataset_name_normalization` on destination"
                " configuration."
            )
        return norm_ident

    @classmethod
    def needs_dataset_name(cls) -> bool:
        """Checks if configuration requires dataset name to be present. Empty datasets are allowed
        ie. for schema-less destinations like weaviate or clickhouse
        """
        fields = cls.get_resolvable_fields()
        dataset_name_type = fields["dataset_name"]
        return not is_optional_type(dataset_name_type)

    def _make_dataset_name(self, schema_name: str) -> str:
        if not schema_name:
            raise ValueError("`schema_name` is `None` or empty")

        # if default schema is None then suffix is not added
        if self.default_schema_name is not None and schema_name != self.default_schema_name:
            return (self.dataset_name or "") + "_" + schema_name
        return self.dataset_name


@configspec
class DestinationClientStagingConfiguration(DestinationClientDwhConfiguration):
    """Configuration of a staging destination, able to store files with desired `layout` at `bucket_url`.

    Also supports datasets and can act as standalone destination.
    """

    as_staging_destination: bool = False
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
        self._exception: BaseException = None

        # variables needed by most jobs, set by the loader in set_run_vars
        self._schema: Schema = None
        self._load_table: PreparedTableSchema = None
        self._load_id: str = None
        self._job_client: "JobClientBase" = None

    def set_run_vars(self, load_id: str, schema: Schema, load_table: PreparedTableSchema) -> None:
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
        from dlt.common.runtime import signals

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
        except (TerminalException, AssertionError) as e:
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
            if self._state != "retry":
                # wake up waiting threads
                signals.wake_all()

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
    def initialize_storage(self, truncate_tables: Optional[Iterable[str]] = None) -> None:
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

    def verify_schema(
        self, only_tables: Iterable[str] = None, new_jobs: Iterable[ParsedLoadJobFileName] = None
    ) -> List[PreparedTableSchema]:
        """Verifies schema before loading, returns a list of verified loaded tables."""
        if exceptions := verify_schema_capabilities(
            self.schema,
            self.capabilities,
            self.config.destination_type,
            warnings=False,
        ):
            for exception in exceptions:
                logger.error(str(exception))
            raise exceptions[0]

        prepared_tables = [
            self.prepare_load_table(table_name)
            for table_name in set(
                list(only_tables or []) + self.schema.data_table_names(seen_data_only=True)
            )
        ]
        if exceptions := verify_supported_data_types(
            prepared_tables,
            new_jobs,
            self.capabilities,
            self.config.destination_type,
            warnings=False,
        ):
            for exception in exceptions:
                logger.error(str(exception))
            raise exceptions[0]
        return prepared_tables

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
        # make sure that schema being saved was not modified from the moment it was loaded from storage
        version_hash = self.schema.version_hash
        if self.schema.is_modified:
            raise DestinationSchemaTampered(
                self.schema.name, version_hash, self.schema.stored_version_hash
            )
        return expected_update

    def prepare_load_table(self, table_name: str) -> PreparedTableSchema:
        """Prepares a table schema to be loaded by filling missing hints and doing other modifications requires by given destination.

        Returns: prepared table, note: table schema for `table_name` is cloned
        """
        return prepare_load_table(
            self.schema.tables, self.schema.get_table(table_name), self.capabilities
        )

    @abstractmethod
    def create_load_job(
        self, table: PreparedTableSchema, file_path: str, load_id: str, restore: bool = False
    ) -> LoadJob:
        """Creates a load job for a particular `table` with content in `file_path`. Table is already prepared to be loaded."""
        pass

    def prepare_load_job_execution(  # noqa: B027, optional override
        self, job: RunnableLoadJob
    ) -> None:
        """Prepare the connected job client for the execution of a load job (used for query tags in sql clients)"""
        pass

    def should_truncate_table_before_load(self, table_name: str) -> bool:
        return self.prepare_load_table(table_name)["write_disposition"] == "replace"

    def create_table_chain_completed_followup_jobs(
        self,
        table_chain: Sequence[PreparedTableSchema],
        completed_table_chain_jobs: Optional[Sequence[LoadJobInfo]] = None,
    ) -> List[FollowupJobRequest]:
        """Creates a list of followup jobs that should be executed after a table chain is completed. Tables are already prepared to be loaded."""
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


class WithStateSync(ABC):
    @abstractmethod
    def get_stored_schema(self, schema_name: str = None) -> Optional[StorageSchemaInfo]:
        """
        Retrieves newest schema with given name from destination storage
        If no name is provided, the newest schema found is retrieved.
        """
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
    def should_load_data_to_staging_dataset(self, table_name: str) -> bool:
        return False

    @abstractmethod
    def with_staging_dataset(self) -> ContextManager["JobClientBase"]:
        """Executes job client methods on staging dataset"""
        return self  # type: ignore


class SupportsStagingDestination(ABC):
    """Adds capability to support a staging destination for the load"""

    def should_load_data_to_staging_dataset_on_staging_destination(self, table_name: str) -> bool:
        """If set to True, and staging destination is configured, the data will be loaded to staging dataset on staging destination
        instead of a regular dataset on staging destination. Currently it is used by Athena Iceberg which uses staging dataset
        on staging destination to copy data to iceberg tables stored on regular dataset on staging destination.
        The default is to load data to regular dataset on staging destination from where warehouses like Snowflake (that have their
        own storage) will copy data.
        """
        return False

    @abstractmethod
    def should_truncate_table_before_load_on_staging_destination(self, table_name: str) -> bool:
        """If set to True, data in `table` will be truncated on staging destination (regular dataset). This is the default behavior which
        can be changed with a config flag.
        For Athena + Iceberg this setting is always False - Athena uses regular dataset to store Iceberg tables and we avoid touching it.
        For Athena we truncate those tables only on "replace" write disposition.
        """
        pass


class SupportsOpenTables(ABC):
    """Provides access to data stored in one of open table formats (iceberg or delta) and intended to
    be implemented by job clients.

    """

    @abstractmethod
    def get_open_table_catalog(self, table_format: TTableFormat, catalog_name: str = None) -> Any:
        """Gets the catalog that keeps tables' metadata. Currently only pyiceberg Catalog is supported"""

    @abstractmethod
    def get_open_table_location(self, table_format: TTableFormat, table_name: str) -> str:
        """Computes location in which table is stored which is typically a "folder" with table
        data and metadata. Does not verify if table exists.
        Returns:
            str: fully formed url with table location
        """

    @abstractmethod
    def load_open_table(self, table_format: TTableFormat, table_name: str, **kwargs: Any) -> Any:
        """Loads table `table_name` metadata via catalog or directly and returns populated and authenticated
        table client. Currently pyiceberg Table or DeltaTable is returned.
        * table must be present in schema of job client
        * table must physically exist in storage
        * table may be present in associated catalog and may be automatically registered if destination configuration allows for that
        * otherwise table is not found

        raised DestinationUndefinedEntity if table not found
        """

    @abstractmethod
    def is_open_table(self, table_format: TTableFormat, table_name: str) -> bool:
        """Checks if `table_name` is stored with open table format `table_format`. Does not load table. Does not check if
        table exists
        """
