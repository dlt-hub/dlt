import contextlib
import os
from contextlib import contextmanager
from copy import deepcopy, copy
from functools import wraps
from typing import (
    Any,
    Callable,
    ClassVar,
    List,
    Iterator,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    cast,
    ContextManager,
    Union,
    TYPE_CHECKING,
)

import dlt
from dlt.common import logger
from dlt.common.json import json
from dlt.common.pendulum import pendulum
from dlt.common.exceptions import ValueErrorWithKnownValues
from dlt.common.configuration import inject_section, known_sections
from dlt.common.configuration.specs import RuntimeConfiguration
from dlt.common.configuration.container import Container
from dlt.common.configuration.exceptions import (
    ContextDefaultCannotBeCreated,
)
from dlt.common.destination.dataset import Dataset
from dlt.common.configuration.specs.config_section_context import ConfigSectionContext
from dlt.common.destination.exceptions import (
    DestinationIncompatibleLoaderFileFormatException,
    DestinationNoStagingMode,
    DestinationUndefinedEntity,
)
from dlt.common.runtime import signals, apply_runtime_config
from dlt.common.schema.typing import (
    TSchemaTables,
    TTableFormat,
    TWriteDispositionConfig,
    TAnySchemaColumns,
    TSchemaContract,
)
from dlt.common.schema.utils import normalize_schema_name
from dlt.common.storages.exceptions import LoadPackageNotFound
from dlt.common.typing import ConfigValue, TFun, TSecretStrValue, TColumnNames, get_type_hints
from dlt.common.runners import pool_runner as runner
from dlt.common.storages import (
    LiveSchemaStorage,
    NormalizeStorage,
    LoadStorage,
    SchemaStorage,
    FileStorage,
    NormalizeStorageConfiguration,
    SchemaStorageConfiguration,
    LoadStorageConfiguration,
    PackageStorage,
    LoadJobInfo,
    LoadPackageInfo,
    WithLocalFiles,
)
from dlt.common.destination import (
    Destination,
    TDestinationReferenceArg,
    DestinationCapabilitiesContext,
    merge_caps_file_formats,
    AnyDestination,
    LOADER_FILE_FORMATS,
    TLoaderFileFormat,
)
from dlt.common.destination.client import (
    DestinationClientDwhConfiguration,
    WithStateSync,
    JobClientBase,
    DestinationClientStagingConfiguration,
)
from dlt.common.destination.exceptions import SqlClientNotAvailable, FSClientNotAvailable
from dlt.common.normalizers.naming import NamingConvention
from dlt.common.pipeline import (
    ExtractInfo,
    LoadInfo,
    NormalizeInfo,
    PipelineContext,
    TStepInfo,
    SupportsPipeline,
    TLastRunContext,
    TPipelineLocalState,
    TPipelineState,
    StateInjectableContext,
    TStepMetrics,
    WithStepInfo,
    TRefreshMode,
)
from dlt.common.schema import Schema
from dlt.common.utils import is_interactive, simple_repr, without_none
from dlt.common.warnings import deprecated, Dlt04DeprecationWarning
from dlt.common.versioned_state import json_encode_state, json_decode_state

from dlt.extract import DltSource
from dlt.extract.exceptions import SourceExhausted
from dlt.extract.extract import Extract, data_to_sources
from dlt.normalize import Normalize
from dlt.normalize.configuration import NormalizeConfiguration
from dlt.destinations.sql_client import SqlClientBase, WithSqlClient
from dlt.destinations.fs_client import FSClientBase
from dlt.destinations.job_client_impl import SqlJobClientBase
from dlt.destinations.dataset import (
    dataset,
    get_destination_clients,
)

from dlt.load.configuration import LoaderConfiguration
from dlt.load import Load

from dlt.pipeline.configuration import PipelineConfiguration
from dlt.pipeline.progress import _Collector, _NULL_COLLECTOR
from dlt.pipeline.exceptions import (
    CannotRestorePipelineException,
    InvalidPipelineName,
    PipelineConfigMissing,
    PipelineNeverRan,
    PipelineNotActive,
    PipelineStepFailed,
)
from dlt.pipeline.trace import (
    PipelineTrace,
    PipelineStepTrace,
    load_trace,
    merge_traces,
    start_trace,
    start_trace_step,
    end_trace_step,
    end_trace,
)
from dlt.pipeline.typing import TPipelineStep
from dlt.pipeline.state_sync import (
    PIPELINE_STATE_ENGINE_VERSION,
    bump_pipeline_state_version_if_modified,
    load_pipeline_state_from_destination,
    mark_state_extracted,
    migrate_pipeline_state,
    state_resource,
    default_pipeline_state,
)
from dlt.common.storages.load_package import TLoadPackageState
from dlt.pipeline.helpers import refresh_source

if TYPE_CHECKING:
    from dlt import Dataset
else:
    Dataset = Any


TWithLocalFiles = TypeVar("TWithLocalFiles", bound=WithLocalFiles)


def with_state_sync(may_extract_state: bool = False) -> Callable[[TFun], TFun]:
    def decorator(f: TFun) -> TFun:
        @wraps(f)
        def _wrap(self: "Pipeline", *args: Any, **kwargs: Any) -> Any:
            # activate pipeline so right state is always provided
            self.activate()

            # backup and restore state
            should_extract_state = may_extract_state and self.config.restore_from_destination
            with self.managed_state(extract_state=should_extract_state):
                return f(self, *args, **kwargs)

        return _wrap  # type: ignore

    return decorator


def with_schemas_sync(f: TFun) -> TFun:
    @wraps(f)
    def _wrap(self: "Pipeline", *args: Any, **kwargs: Any) -> Any:
        for name in self._schema_storage.live_schemas:
            # refresh live schemas in storage or import schema path
            self._schema_storage.commit_live_schema(name)
        try:
            rv = f(self, *args, **kwargs)
        except Exception:
            # because we committed live schema before calling f, we may safely
            # drop all changes in live schemas
            for name in list(self._schema_storage.live_schemas.keys()):
                try:
                    schema = self._schema_storage.load_schema(name)
                    schema.replace_schema_content(schema, link_to_replaced_schema=False)
                except FileNotFoundError:
                    # no storage schema yet so pop live schema (created in call to f)
                    self._schema_storage.live_schemas.pop(name, None)
            # NOTE: with_state_sync will restore schema_names and default_schema_name
            # so we do not need to do that here
            raise
        else:
            # save modified live schemas
            for name, schema in self._schema_storage.live_schemas.items():
                # also save import schemas only here
                self._schema_storage.save_import_schema_if_not_exists(schema)
                # only now save the schema, already linked to itself if saved as import schema
                self._schema_storage.commit_live_schema(name)
            # refresh list of schemas if any new schemas are added
            self.schema_names = self._list_schemas_sorted()
            return rv

    return _wrap  # type: ignore


def with_runtime_trace(send_state: bool = False) -> Callable[[TFun], TFun]:
    def decorator(f: TFun) -> TFun:
        @wraps(f)
        def _wrap(self: "Pipeline", *args: Any, **kwargs: Any) -> Any:
            trace: PipelineTrace = self._trace
            trace_step: PipelineStepTrace = None
            step_info: Any = None
            is_new_trace = self._trace is None and self.config.enable_runtime_trace

            # create a new trace if we enter a traced function and there's no current trace
            if is_new_trace:
                self._trace = trace = start_trace(cast(TPipelineStep, f.__name__), self)

            try:
                # start a trace step for wrapped function
                if trace:
                    trace_step = start_trace_step(trace, cast(TPipelineStep, f.__name__), self)

                step_info = f(self, *args, **kwargs)
                return step_info
            except Exception as ex:
                step_info = ex  # step info is an exception
                raise
            finally:
                try:
                    if trace_step:
                        # if there was a step, finish it
                        self._trace = end_trace_step(
                            self._trace, trace_step, self, step_info, send_state
                        )
                    if is_new_trace:
                        assert trace.transaction_id == self._trace.transaction_id, (
                            f"Messed up trace reference {self._trace.transaction_id} vs"
                            f" {trace.transaction_id}"
                        )
                        trace = end_trace(
                            trace, self, self._pipeline_storage.storage_path, send_state
                        )
                finally:
                    # always end trace
                    if is_new_trace:
                        assert (
                            self._trace.transaction_id == trace.transaction_id
                        ), f"Messed up trace reference {id(self._trace)} vs {id(trace)}"
                        # if we end new trace that had only 1 step, add it to previous trace
                        # this way we combine several separate calls to extract, normalize, load as single trace
                        # the trace of "run" has many steps and will not be merged
                        self._last_trace = merge_traces(self._last_trace, trace)
                        self._trace = None

        return _wrap  # type: ignore

    return decorator


def with_config_section(
    sections: Tuple[str, ...], merge_func: ConfigSectionContext.TMergeFunc = None
) -> Callable[[TFun], TFun]:
    def decorator(f: TFun) -> TFun:
        @wraps(f)
        def _wrap(self: "Pipeline", *args: Any, **kwargs: Any) -> Any:
            # add section context to the container to be used by all configuration without explicit sections resolution
            with inject_section(
                ConfigSectionContext(
                    pipeline_name=self.pipeline_name, sections=sections, merge_style=merge_func
                )
            ):
                return f(self, *args, **kwargs)

        return _wrap  # type: ignore

    return decorator


class Pipeline(SupportsPipeline):
    STATE_FILE: ClassVar[str] = "state.json"
    STATE_PROPS: ClassVar[List[str]] = list(
        set(get_type_hints(TPipelineState).keys())
        - {
            "sources",
            "destination_type",
            "destination_name",
            "staging_type",
            "staging_name",
            "destinations",
        }
    )
    LOCAL_STATE_PROPS: ClassVar[List[str]] = list(get_type_hints(TPipelineLocalState).keys())
    DEFAULT_DATASET_SUFFIX: ClassVar[str] = "_dataset"

    # default_schema_name: str
    schema_names: List[str]
    dev_mode: bool
    must_attach_to_local_pipeline: bool
    pipelines_dir: str
    """A directory where the pipelines' working directories are created"""
    _destination: AnyDestination
    _staging: AnyDestination
    """The destination reference which is the Destination Class. `destination.destination_name` returns the name string"""
    is_active: bool
    """Tells if instance is currently active and available via dlt.pipeline()"""
    collector: _Collector
    config: PipelineConfiguration
    refresh: Optional[TRefreshMode]

    def __init__(
        self,
        pipeline_name: str,
        pipelines_dir: str,
        pipeline_salt: TSecretStrValue,
        destination: AnyDestination,
        staging: AnyDestination,
        dataset_name: str,
        import_schema_path: str,
        export_schema_path: str,
        dev_mode: bool,
        progress: _Collector,
        must_attach_to_local_pipeline: bool,
        config: PipelineConfiguration,
        runtime: RuntimeConfiguration,
        refresh: Optional[TRefreshMode] = None,
    ) -> None:
        """Initializes the Pipeline class which implements `dlt` pipeline. Please use `pipeline` function in `dlt` module to create a new Pipeline instance."""
        self.default_schema_name = None
        self.schema_names = []
        self.first_run = False
        self.dataset_name: str = None
        self.is_active = False
        self.last_run_context: TLastRunContext = None

        self.pipeline_salt = pipeline_salt
        self.config = config
        self.runtime_config = runtime
        self.run_context = config.pluggable_run_context.context
        self.dev_mode = dev_mode
        self.collector = progress or _NULL_COLLECTOR
        self._destination = None
        self._staging = None
        self.refresh = refresh

        self._container = Container()
        self._pipeline_instance_id = self._create_pipeline_instance_id()
        self._pipeline_storage: FileStorage = None
        self._schema_storage: LiveSchemaStorage = None
        self._schema_storage_config: SchemaStorageConfiguration = None
        self._trace: PipelineTrace = None
        self._last_trace: PipelineTrace = None
        self._state_restored: bool = False

        # initialize pipeline working dir
        self._init_working_dir(pipeline_name, pipelines_dir)

        with self.managed_state() as state:
            self._configure(import_schema_path, export_schema_path, must_attach_to_local_pipeline)
            # changing the destination could be dangerous if pipeline has pending load packages
            self._set_destinations(destination=destination, staging=staging, initializing=True)
            # set the pipeline properties from state, destination and staging will not be set
            self._state_to_props(state)
            # TODO: compare run_context with last_run_context restored from props. warn on changed uri
            # we overwrite the state with the values from init
            self._set_dataset_name(dataset_name)

    def drop(self, pipeline_name: str = None) -> "Pipeline":
        """Deletes local pipeline state, schemas and any working files. Re-initializes
           all internal fields via __init__. If `pipeline_name` is specified that is
           different from the current name, new pipeline instance is created, activated and returned.
           Note that original pipeline is still dropped.

        Args:
            pipeline_name (str): Optional. New pipeline name. Creates and activates new instance

        Returns:
            "Pipeline": returns self
        """
        if self.is_active:
            self.deactivate()
        # reset the pipeline working dir
        self._create_pipeline()
        self.__init__(  # type: ignore[misc]
            self.pipeline_name,
            self.pipelines_dir,
            self.pipeline_salt,
            self._destination,
            self._staging,
            self.dataset_name,
            self._schema_storage.config.import_schema_path,
            self._schema_storage.config.export_schema_path,
            self.dev_mode,
            self.collector,
            False,
            self.config,
            self.runtime_config,
        )
        if pipeline_name is not None and pipeline_name != self.pipeline_name:
            self = self.__class__(
                pipeline_name,
                self.pipelines_dir,
                self.pipeline_salt,
                deepcopy(self._destination),
                deepcopy(self._staging),
                self.dataset_name,
                self._schema_storage.config.import_schema_path,
                self._schema_storage.config.export_schema_path,
                self.dev_mode,
                deepcopy(self.collector),
                False,
                self.config,
                self.runtime_config,
            )
        # activate (possibly new) self
        self.activate()
        return self

    @with_runtime_trace()
    @with_schemas_sync  # this must precede with_state_sync
    @with_state_sync(may_extract_state=True)
    @with_config_section((known_sections.EXTRACT,))
    def extract(
        self,
        data: Any,
        *,
        table_name: str = None,
        parent_table_name: str = None,
        write_disposition: TWriteDispositionConfig = None,
        columns: TAnySchemaColumns = None,
        primary_key: TColumnNames = None,
        schema: Schema = None,
        max_parallel_items: int = ConfigValue,
        workers: int = ConfigValue,
        table_format: TTableFormat = None,
        schema_contract: TSchemaContract = None,
        refresh: Optional[TRefreshMode] = None,
        loader_file_format: Optional[TLoaderFileFormat] = None,
    ) -> ExtractInfo:
        """Extracts the `data` and prepare it for the normalization. Does not require destination or credentials to be configured. See `run` method for the arguments' description."""
        if loader_file_format and loader_file_format not in LOADER_FILE_FORMATS:
            raise ValueErrorWithKnownValues(
                "loader_file_format", loader_file_format, LOADER_FILE_FORMATS
            )

        with self._maybe_destination_capabilities() as caps:
            if caps:
                self._verify_destination_capabilities(caps, loader_file_format)

        # create extract storage to which all the sources will be extracted
        extract_step = Extract(
            self._schema_storage,
            self._normalize_storage_config(),
            self.collector,
            original_data=data,
        )
        try:
            with self._maybe_destination_capabilities():
                # extract all sources
                for source in data_to_sources(
                    data,
                    self,
                    schema=schema,
                    table_name=table_name,
                    parent_table_name=parent_table_name,
                    write_disposition=write_disposition,
                    columns=columns,
                    primary_key=primary_key,
                    schema_contract=schema_contract,
                    table_format=table_format,
                    loader_file_format=loader_file_format,
                ):
                    if source.exhausted:
                        raise SourceExhausted(source.name)

                    self._extract_source(
                        extract_step,
                        source,
                        max_parallel_items,
                        workers,
                        refresh=refresh or self.refresh,
                    )

                # this will update state version hash so it will not be extracted again by with_state_sync
                self._bump_version_and_extract_state(
                    self._container[StateInjectableContext].state,
                    self.config.restore_from_destination,
                    extract_step,
                )
                # commit load packages with state
                extract_step.commit_packages()
                return self._get_step_info(extract_step)
        except Exception as exc:
            # emit step info
            step_info = self._get_step_info(extract_step)
            current_load_id = step_info.loads_ids[-1] if len(step_info.loads_ids) > 0 else None
            raise PipelineStepFailed(
                self,
                "extract",
                current_load_id,
                exc,
                step_info,
            ) from exc

    @with_runtime_trace()
    @with_schemas_sync
    @with_config_section((known_sections.NORMALIZE,))
    def normalize(self, workers: int = 1) -> NormalizeInfo:
        """Normalizes the data prepared with `extract` method, infers the schema and creates load packages for the `load` method. Requires `destination` to be known."""
        if is_interactive():
            workers = 1
        # check if any schema is present, if not then no data was extracted
        if not self.default_schema_name:
            return None

        # make sure destination capabilities are available
        self._get_destination_capabilities()

        # create default normalize config
        normalize_config = NormalizeConfiguration(
            workers=workers,
            _schema_storage_config=self._schema_storage_config,
            _normalize_storage_config=self._normalize_storage_config(),
            _load_storage_config=self._load_storage_config(),
        )

        # run with destination context
        with self._maybe_destination_capabilities() as caps:
            self._verify_destination_capabilities(caps, None)

            # shares schema storage with the pipeline so we do not need to install
            normalize_step: Normalize = Normalize(
                collector=self.collector,
                config=normalize_config,
                schema_storage=self._schema_storage,
            )
            try:
                with signals.delayed_signals():
                    runner.run_pool(normalize_step.config, normalize_step)
                return self._get_step_info(normalize_step)
            except Exception as n_ex:
                step_info = self._get_step_info(normalize_step)
                raise PipelineStepFailed(
                    self,
                    "normalize",
                    normalize_step.current_load_id,
                    n_ex,
                    step_info,
                ) from n_ex

    @with_runtime_trace(send_state=True)
    @with_state_sync()
    @with_config_section((known_sections.LOAD,))
    def load(
        self,
        destination: TDestinationReferenceArg = None,
        dataset_name: str = None,
        credentials: Any = None,
        *,
        workers: int = 20,
        raise_on_failed_jobs: bool = ConfigValue,
    ) -> LoadInfo:
        """Loads the packages prepared by `normalize` method into the `dataset_name` at `destination`, optionally using provided `credentials`"""
        # set destination and default dataset if provided (this is the reason we have state sync here)
        self._set_destinations(
            destination=destination, destination_credentials=credentials, staging=None
        )
        self._set_dataset_name(dataset_name)

        # check if any schema is present, if not then no data was extracted
        if not self.default_schema_name:
            return None

        # make sure that destination is set and client is importable and can be instantiated
        client, staging_client = self._get_destination_clients()

        # create default loader config and the loader
        load_config = LoaderConfiguration(
            workers=workers,
            raise_on_failed_jobs=raise_on_failed_jobs,
            _load_storage_config=self._load_storage_config(),
        )
        load_step: Load = Load(
            self._destination,
            staging_destination=self._staging,
            collector=self.collector,
            is_storage_owner=False,
            config=load_config,
            initial_client_config=client.config,
            initial_staging_client_config=staging_client.config if staging_client else None,
        )
        try:
            with signals.delayed_signals():
                runner.run_pool(load_step.config, load_step)
            info: LoadInfo = self._get_step_info(load_step)
            self._update_last_run_context()
            return info
        except Exception as l_ex:
            step_info = self._get_step_info(load_step)
            raise PipelineStepFailed(
                self, "load", load_step.current_load_id, l_ex, step_info
            ) from l_ex

    @with_runtime_trace()
    @with_config_section(("run",))
    def run(
        self,
        data: Any = None,
        *,
        destination: TDestinationReferenceArg = None,
        staging: TDestinationReferenceArg = None,
        dataset_name: str = None,
        credentials: Any = None,
        table_name: str = None,
        write_disposition: TWriteDispositionConfig = None,
        columns: TAnySchemaColumns = None,
        primary_key: TColumnNames = None,
        schema: Schema = None,
        loader_file_format: TLoaderFileFormat = None,
        table_format: TTableFormat = None,
        schema_contract: TSchemaContract = None,
        refresh: TRefreshMode = None,
    ) -> LoadInfo:
        """Loads the data from `data` argument into the destination specified in `destination` and dataset specified in `dataset_name`.

        #### Note:
        This method will `extract` the data from the `data` argument, infer the schema, `normalize` the data into a load package (ie. jsonl or PARQUET files representing tables) and then `load` such packages into the `destination`.

        The data may be supplied in several forms:
        * a `list` or `Iterable` of any JSON-serializable objects ie. `dlt.run([1, 2, 3], table_name="numbers")`
        * any `Iterator` or a function that yield (`Generator`) ie. `dlt.run(range(1, 10), table_name="range")`
        * a function or a list of functions decorated with @dlt.resource ie. `dlt.run([chess_players(title="GM"), chess_games()])`
        * a function or a list of functions decorated with @dlt.source.

        Please note that `dlt` deals with `bytes`, `datetime`, `decimal` and `uuid` objects so you are free to load documents containing ie. binary data or dates.

        #### Execution:
        The `run` method will first use `sync_destination` method to synchronize pipeline state and schemas with the destination. You can disable this behavior with `restore_from_destination` configuration option.
        Next it will make sure that data from the previous is fully processed. If not, `run` method normalizes, loads pending data items and **exits**
        If there was no pending data, new data from `data` argument is extracted, normalized and loaded.

        Args:
            data (Any): Data to be loaded to destination

            destination (TDestinationReferenceArg, optional): A name of the destination to which dlt will load the data, or a destination module imported from `dlt.destination`.
                If not provided, the value passed to `dlt.pipeline` will be used.

            staging (TDestinationReferenceArg, optional): A name of the staging destination to which dlt will load the data temporarily before it is loaded to the destination, can also
                be a module imported from `dlt.destination`.

            dataset_name (str, optional): A name of the dataset to which the data will be loaded. A dataset is a logical group of tables ie. `schema` in relational databases or folder grouping many files.
                If not provided, the value passed to `dlt.pipeline` will be used. If not provided at all then defaults to the `pipeline_name`

            credentials (Any, optional): Credentials for the `destination` ie. database connection string or a dictionary with google cloud credentials.
                In most cases should be set to None, which lets `dlt` to use `secrets.toml` or environment variables to infer right credentials values.

            table_name (str, optional): The name of the table to which the data should be loaded within the `dataset`. This argument is required for a `data` that is a list/Iterable or Iterator without `__name__` attribute.
                The behavior of this argument depends on the type of the `data`:
                * generator functions - the function name is used as table name, `table_name` overrides this default
                * `@dlt.resource` - resource contains the full table schema and that includes the table name. `table_name` will override this property. Use with care!
                * `@dlt.source` - source contains several resources each with a table schema. `table_name` will override all table names within the source and load the data into single table.

            write_disposition (TWriteDispositionConfig, optional): Controls how to write data to a table. Accepts a shorthand string literal or configuration dictionary.
                Allowed shorthand string literals: `append` will always add new data at the end of the table. `replace` will replace existing data with new data. `skip` will prevent data from loading. "merge" will deduplicate and merge data based on "primary_key" and "merge_key" hints. Defaults to "append".
                Write behaviour can be further customized through a configuration dictionary. For example, to obtain an SCD2 table provide `write_disposition={"disposition": "merge", "strategy": "scd2"}`.
                Please note that in case of `dlt.resource` the table schema value will be overwritten and in case of `dlt.source`, the values in all resources will be overwritten.

            columns (TAnySchemaColumns, optional): A list of column schemas. Typed dictionary describing column names, data types, write disposition and performance hints that gives you full control over the created table schema.

            primary_key (TColumnNames, optional): A column name or a list of column names that comprise a private key. Typically used with "merge" write disposition to deduplicate loaded data.

            schema (Schema, optional): An explicit `Schema` object in which all table schemas will be grouped. By default `dlt` takes the schema from the source (if passed in `data` argument) or creates a default one itself.

            loader_file_format (TLoaderFileFormat, optional): The file format the loader will use to create the load package. Not all file_formats are compatible with all destinations. Defaults to the preferred file format of the selected destination.

            table_format (TTableFormat, optional): Can be "delta" or "iceberg". The table format used by the destination to store tables. Currently you can select table format on filesystem and Athena destinations.

            schema_contract (TSchemaContract, optional): On override for the schema contract settings, this will replace the schema contract settings for all tables in the schema. Defaults to None.

            refresh (TRefreshMode, optional): Fully or partially reset sources before loading new data in this run. The following refresh modes are supported:
                * `drop_sources` - Drop tables and source and resource state for all sources currently being processed in `run` or `extract` methods of the pipeline. (Note: schema history is erased)
                * `drop_resources`-  Drop tables and resource state for all resources being processed. Source level state is not modified. (Note: schema history is erased)
                * `drop_data` - Wipe all data and resource state for all resources being processed. Schema is not modified.

        Raises:
            PipelineStepFailed: when a problem happened during `extract`, `normalize` or `load` steps.

        Returns:
            LoadInfo: Information on loaded data including the list of package ids and failed job statuses. Please not that `dlt` will not raise if a single job terminally fails. Such information is provided via LoadInfo.
        """

        signals.raise_if_signalled()
        self.activate()
        self._set_destinations(
            destination=destination, destination_credentials=credentials, staging=staging
        )
        self._set_dataset_name(dataset_name)

        # sync state with destination
        if (
            self.config.restore_from_destination
            and not self.dev_mode
            and not self._state_restored
            and (self._destination or destination)
        ):
            self._sync_destination(destination, staging, dataset_name)
            # sync only once
            self._state_restored = True
        # normalize and load pending data
        if self.list_extracted_load_packages():
            self.normalize()
        if self.list_normalized_load_packages():
            # if there were any pending loads, load them and **exit**
            if data is not None:
                logger.warn(
                    "The pipeline `run` method will now load the pending load packages. The data"
                    " you passed to the run function will not be loaded. In order to do that you"
                    " must run the pipeline again"
                )
            return self.load(destination, dataset_name, credentials=credentials)

        # extract from the source
        if data is not None:
            self.extract(
                data,
                table_name=table_name,
                write_disposition=write_disposition,
                columns=columns,
                primary_key=primary_key,
                schema=schema,
                table_format=table_format,
                schema_contract=schema_contract,
                refresh=refresh or self.refresh,
                loader_file_format=loader_file_format,
            )
            self.normalize()
            return self.load(destination, dataset_name, credentials=credentials)
        else:
            return None

    @with_config_section(sections=(), merge_func=ConfigSectionContext.prefer_existing)
    def sync_destination(
        self,
        destination: TDestinationReferenceArg = None,
        staging: TDestinationReferenceArg = None,
        dataset_name: str = None,
    ) -> None:
        """Synchronizes pipeline state with the `destination`'s state kept in `dataset_name`

        #### Note:
        Attempts to restore pipeline state and schemas from the destination. Requires the state that is present at the destination to have a higher version number that state kept locally in working directory.
        In such a situation the local state, schemas and intermediate files with the data will be deleted and replaced with the state and schema present in the destination.

        A special case where the pipeline state exists locally but the dataset does not exist at the destination will wipe out the local state.

        Note: this method is executed by the `run` method before any operation on data. Use `restore_from_destination` configuration option to disable that behavior.

        """
        return self._sync_destination(
            destination=destination, staging=staging, dataset_name=dataset_name
        )

    @with_schemas_sync
    def _sync_destination(
        self,
        destination: TDestinationReferenceArg = None,
        staging: TDestinationReferenceArg = None,
        dataset_name: str = None,
    ) -> None:
        self._set_destinations(destination=destination, staging=staging)
        self._set_dataset_name(dataset_name)

        state = self._get_state()
        state_changed = False
        try:
            try:
                restored_schemas: Sequence[Schema] = None

                remote_state = self._restore_state_from_destination()

                # if remote state is newer or same
                # TODO: check if remote_state["_state_version"] is not in 10 recent version. then we know remote is newer.
                if remote_state and remote_state["_state_version"] >= state["_state_version"]:
                    state_changed = remote_state["_version_hash"] != state.get("_version_hash")
                    if state_changed:
                        # see if state didn't change the pipeline name
                        if state["pipeline_name"] != remote_state["pipeline_name"]:
                            raise CannotRestorePipelineException(
                                state["pipeline_name"],
                                self.pipelines_dir,
                                "destination state contains state for pipeline with name"
                                f" {remote_state['pipeline_name']}",
                            )
                        # if state was modified force get all schemas
                        restored_schemas = self._get_schemas_from_destination(
                            remote_state["schema_names"], always_download=True
                        )
                        # TODO: we should probably wipe out pipeline here
                        if self.has_pending_data:
                            logger.warning(
                                f"Pipeline {self.pipeline_name} got restored from destination"
                                " including new version of pipeline state but it has pending load"
                                " packages that were not yet normalized or loaded. If those"
                                " packages contain extracted state or schema migrations - those"
                                " will not be affected and will still be loaded to destination."
                            )
                # if we didn't full refresh schemas, get only missing schemas
                if restored_schemas is None:
                    restored_schemas = self._get_schemas_from_destination(
                        state["schema_names"], always_download=False
                    )
                # commit all the changes locally
                if state_changed:
                    # use remote state as state
                    remote_state["_local"] = state["_local"]
                    state = remote_state
                    # set the pipeline props from merged state
                    self._state_to_props(state)
                    # add that the state is already extracted
                    mark_state_extracted(state, state["_version_hash"])
                    # on merge schemas are replaced so we delete all old versions
                    self._schema_storage.clear_storage()
                for schema in restored_schemas:
                    self._schema_storage.save_schema(schema)
                # if the remote state is present then unset first run and update last run context
                if remote_state is not None:
                    self._update_last_run_context()
            except DestinationUndefinedEntity:
                # storage not present. wipe the pipeline if pipeline not new
                # do it only if pipeline has any data
                if self.has_data:
                    should_wipe = False
                    if self.default_schema_name is None:
                        should_wipe = True
                    else:
                        with self._get_destination_clients()[0] as job_client:
                            # and storage is not initialized
                            should_wipe = not job_client.is_storage_initialized()
                    if should_wipe:
                        # reset pipeline
                        self._wipe_working_folder()
                        state = self._get_state()
                        self._configure(
                            self._schema_storage_config.import_schema_path,
                            self._schema_storage_config.export_schema_path,
                            False,
                        )
            # write the state back
            self._props_to_state(state)
            # verify state
            if state_default_schema_name := state.get("default_schema_name"):
                # at least empty list is present
                state_schemas = state["schema_names"]
                if state_default_schema_name not in state_schemas:
                    new_default_schema_name: Optional[str] = (
                        state_schemas[0] if len(state_schemas) > 0 else None
                    )
                    logger.warning(
                        f"Pipeline {self.pipeline_name} was restored from destination with"
                        " inconsistent state. Default schema name"
                        f" {state_default_schema_name} could not be found and downloaded from the"
                        " destination. "
                        + (
                            f"Default schema was set to {new_default_schema_name}."
                            if new_default_schema_name
                            else "Default schema was removed."
                        )
                    )
                    self.default_schema_name = new_default_schema_name  # type: ignore[assignment]
                    state["default_schema_name"] = new_default_schema_name
            bump_pipeline_state_version_if_modified(state)
            self._save_state(state)
        except Exception as ex:
            raise PipelineStepFailed(self, "sync", None, ex, None) from ex

    def activate(self) -> None:
        """Activates the pipeline

        The active pipeline is used as a context for several `dlt` components. It provides state to sources and resources evaluated outside of
        `pipeline.run` and `pipeline.extract` method. For example, if the source you use is accessing state in `dlt.source` decorated function, the state is provided
        by active pipeline.

        The name of active pipeline is used when resolving secrets and config values as the optional most outer section during value lookup. For example if pipeline
        with name `chess_pipeline` is active and `dlt` looks for `BigQuery` configuration, it will look in `chess_pipeline.destination.bigquery.credentials` first and then in
        `destination.bigquery.credentials`.

        Active pipeline also provides the current DestinationCapabilitiesContext to other components ie. Schema instances. Among others, it sets the naming convention
        and maximum identifier length.

        Only one pipeline is active at a given time.

        Pipeline created or attached with `dlt.pipeline`/'dlt.attach` is automatically activated. `run`, `load` and `extract` methods also activate pipeline.
        """
        Container()[PipelineContext].activate(self)

    def deactivate(self) -> None:
        """Deactivates the pipeline

        Pipeline must be active in order to use this method. Please refer to `activate()` method for the explanation of active pipeline concept.
        """
        if not self.is_active:
            raise PipelineNotActive(self.pipeline_name)
        Container()[PipelineContext].deactivate()

    @property
    def has_data(self) -> bool:
        """Tells if the pipeline contains any data: schemas, extracted files, load packages or loaded packages in the destination"""
        return (
            not self.first_run
            or bool(self.schema_names)
            or len(self.list_extracted_load_packages()) > 0
            or len(self.list_normalized_load_packages()) > 0
        )

    @property
    def has_pending_data(self) -> bool:
        """Tells if the pipeline contains any pending packages to be normalized or loaded"""
        return (
            len(self.list_normalized_load_packages()) > 0
            or len(self.list_extracted_load_packages()) > 0
        )

    @property
    def schemas(self) -> SchemaStorage:
        return self._schema_storage

    @property
    def default_schema(self) -> Schema:
        return self.schemas[self.default_schema_name]

    @property
    def state(self) -> TPipelineState:
        """Returns a dictionary with the pipeline state"""
        return self._get_state()

    @property
    def naming(self) -> NamingConvention:
        """Returns naming convention of the default schema"""
        return self._get_schema_or_create().naming

    @property
    def last_trace(self) -> PipelineTrace:
        """Returns or loads last trace generated by pipeline. The trace is loaded from standard location."""
        if self._last_trace:
            return self._last_trace
        return load_trace(self.working_dir)

    def __repr__(self) -> str:
        kwargs = {
            "pipeline_name": self.pipeline_name,
            "destination": self._destination.destination_name if self._destination else None,
            "staging": self._staging.destination_name if self._staging else None,
            "dataset_name": self.dataset_name,
            "default_schema_name": self.default_schema_name,
            "schema_names": self.schema_names if self.schema_names else None,
            "first_run": self.first_run if self.first_run else None,
            "dev_mode": self.dev_mode if self.dev_mode else None,
            # we check for `not is_active` which is the less common case
            "is_active": self.is_active if not self.is_active else None,
            "pipelines_dir": self.pipelines_dir,
            "working_dir": self.working_dir,
        }
        return simple_repr("dlt.pipeline", **without_none(kwargs))

    @deprecated(
        "Please use list_extracted_load_packages instead. Flat extracted storage format got dropped"
        " in dlt 0.4.0",
        category=Dlt04DeprecationWarning,
    )
    def list_extracted_resources(self) -> Sequence[str]:
        """Returns a list of all the files with extracted resources that will be normalized."""
        return self._get_normalize_storage().list_files_to_normalize_sorted()

    def list_extracted_load_packages(self) -> Sequence[str]:
        """Returns a list of all load packages ids that are or will be normalized."""
        return self._get_normalize_storage().extracted_packages.list_packages()

    def list_normalized_load_packages(self) -> Sequence[str]:
        """Returns a list of all load packages ids that are or will be loaded."""
        return self._get_load_storage().list_normalized_packages()

    def list_completed_load_packages(self) -> Sequence[str]:
        """Returns a list of all load package ids that are completely loaded"""
        return self._get_load_storage().list_loaded_packages()

    def get_load_package_info(self, load_id: str) -> LoadPackageInfo:
        """Returns information on extracted/normalized/completed package with given load_id, all jobs and their statuses."""
        try:
            return self._get_load_storage().get_load_package_info(load_id)
        except LoadPackageNotFound:
            return self._get_normalize_storage().extracted_packages.get_load_package_info(load_id)

    def get_load_package_state(self, load_id: str) -> TLoadPackageState:
        """Returns information on extracted/normalized/completed package with given load_id, all jobs and their statuses."""
        return self._get_load_storage().get_load_package_state(load_id)

    def list_failed_jobs_in_package(self, load_id: str) -> Sequence[LoadJobInfo]:
        """List all failed jobs and associated error messages for a specified `load_id`"""
        return self._get_load_storage().get_load_package_info(load_id).jobs.get("failed_jobs", [])

    def drop_pending_packages(self, with_partial_loads: bool = True) -> None:
        """Deletes all extracted and normalized packages, including those that are partially loaded by default"""
        # delete normalized packages
        load_storage = self._get_load_storage()
        for load_id in load_storage.normalized_packages.list_packages():
            package_info = load_storage.normalized_packages.get_load_package_info(load_id)
            if PackageStorage.is_package_partially_loaded(package_info) and not with_partial_loads:
                continue
            load_storage.normalized_packages.delete_package(load_id)
        # delete extracted files
        normalize_storage = self._get_normalize_storage()
        for load_id in normalize_storage.extracted_packages.list_packages():
            normalize_storage.extracted_packages.delete_package(load_id)

    @with_schemas_sync
    def sync_schema(self, schema_name: str = None) -> TSchemaTables:
        """Synchronizes the schema `schema_name` with the destination. If no name is provided, the default schema will be synchronized."""
        if not schema_name and not self.default_schema_name:
            raise PipelineConfigMissing(
                self.pipeline_name,
                "default_schema_name",
                "load",
                "Pipeline contains no schemas. Please extract any data with `extract` or `run`"
                " methods.",
            )

        schema = self.schemas[schema_name] if schema_name else self.default_schema
        with self._get_destination_clients(schema)[0] as client:
            client.initialize_storage()
            return client.update_stored_schema()

    def set_local_state_val(self, key: str, value: Any) -> None:
        """Sets value in local state. Local state is not synchronized with destination."""
        try:
            # get managed state that is read/write
            state = self._container[StateInjectableContext].state
            state["_local"][key] = value  # type: ignore
        except ContextDefaultCannotBeCreated:
            state = self._get_state()
            state["_local"][key] = value  # type: ignore
            self._save_state(state)

    def get_local_state_val(self, key: str) -> Any:
        """Gets value from local state. Local state is not synchronized with destination."""
        try:
            # get managed state that is read/write
            state = self._container[StateInjectableContext].state
        except ContextDefaultCannotBeCreated:
            state = self._get_state()
        return state["_local"][key]  # type: ignore

    def _update_last_run_context(self) -> None:
        """
        Persist the directory context in local state.
        Safe-no-op if the pipeline is not active or if run_context cannot be resolved.
        """
        self.first_run = False
        self.last_run_context = {
            "settings_dir": os.path.abspath(self.run_context.settings_dir),
            "local_dir": os.path.abspath(self.run_context.local_dir),
            "run_dir": os.path.abspath(self.run_context.run_dir),
            "uri": self.run_context.uri,
        }

    @with_config_section(sections=(), merge_func=ConfigSectionContext.prefer_existing)
    def sql_client(self, schema_name: str = None) -> SqlClientBase[Any]:
        """Returns a sql client configured to query/change the destination and dataset that were used to load the data.
        Use the client with `with` statement to manage opening and closing connection to the destination:
        >>> with pipeline.sql_client() as client:
        >>>     with client.execute_query(
        >>>         "SELECT id, name, email FROM customers WHERE id = %s", 10
        >>>     ) as cursor:
        >>>         print(cursor.fetchall())

        The client is authenticated and defaults all queries to dataset_name used by the pipeline. You can provide alternative
        `schema_name` which will be used to normalize dataset name.
        """
        schema = self._get_schema_or_create(schema_name)
        client = self._get_destination_clients(schema)[0]
        if isinstance(client, WithSqlClient):
            return client.sql_client
        else:
            raise SqlClientNotAvailable(self.pipeline_name, self._destination.destination_name)

    def _fs_client(self, schema_name: str = None) -> FSClientBase:
        """Returns a filesystem client configured to point to the right folder / bucket for each table.
        For example you may read all parquet files as bytes for one table with the following code:
        >>> files = pipeline._fs_client.list_table_files("customers")
        >>> for file in files:
        >>>     file_bytes = pipeline.fs_client.read_bytes(file)
        >>>     # now you can read them into a pyarrow table for example
        >>>     import pyarrow.parquet as pq
        >>>     table = pq.read_table(io.BytesIO(file_bytes))
        NOTE: This currently is considered a private endpoint and will become stable after we have decided on the
        interface of FSClientBase.
        """
        client = self.destination_client(schema_name)
        if isinstance(client, FSClientBase):
            return client
        raise FSClientNotAvailable(self.pipeline_name, self._destination.destination_name)

    @with_config_section(sections=(), merge_func=ConfigSectionContext.prefer_existing)
    def destination_client(self, schema_name: str = None) -> JobClientBase:
        """Get the destination job client for the configured destination
        Use the client with `with` statement to manage opening and closing connection to the destination:
        >>> with pipeline.destination_client() as client:
        >>>     client.drop_storage()  # removes storage which typically wipes all data in it

        The client is authenticated. You can provide alternative `schema_name` which will be used to normalize dataset name.
        If no schema name is provided and no default schema is present in the pipeline, and ad hoc schema will be created and discarded after use.
        """
        schema = self._get_schema_or_create(schema_name)
        return self._get_destination_clients(schema)[0]

    @property
    def destination(self) -> AnyDestination:
        return self._destination

    @destination.setter
    def destination(self, new_value: AnyDestination) -> None:
        self._destination = new_value
        self._on_set_destination(new_value)

    @property
    def staging(self) -> AnyDestination:
        return self._staging

    @staging.setter
    def staging(self, new_value: AnyDestination) -> None:
        self._staging = new_value
        self._on_set_destination(new_value)

    def _verify_destination_capabilities(
        self,
        caps: DestinationCapabilitiesContext,
        loader_file_format: TLoaderFileFormat,
    ) -> None:
        # verify loader file format
        if loader_file_format and loader_file_format not in caps.supported_loader_file_formats:
            raise DestinationIncompatibleLoaderFileFormatException(
                self._destination.destination_name,
                (self._staging.destination_name if self._staging else None),
                loader_file_format,
                set(caps.supported_loader_file_formats),
            )

    def _on_set_destination(self, new_value: AnyDestination) -> None:
        """Called when destination changes"""
        if issubclass(new_value.spec, WithLocalFiles):
            config = WithLocalFiles()
            config = self._bind_local_files(config)
            # bind config fields with pipeline context so local files are created at deterministic location
            for field in WithLocalFiles.__annotations__:
                if config[field] is not None:
                    new_value.config_params[field] = config[field]

    def _bind_local_files(self, local_files: TWithLocalFiles) -> TWithLocalFiles:
        # get context for local files from pipeline
        local_files.pipeline_working_dir = self.working_dir
        try:
            local_files.legacy_db_path = self.get_local_state_val("duckdb_database")
        except KeyError:
            pass
        local_files.local_dir = self.get_local_state_val("initial_cwd")
        local_files.pipeline_name = self.pipeline_name
        return local_files

    def _get_schema_or_create(self, schema_name: str = None) -> Schema:
        if schema_name:
            return self.schemas[schema_name]
        if self.default_schema_name:
            return self.default_schema
        with self._maybe_destination_capabilities():
            return Schema(self.pipeline_name)

    def _get_normalize_storage(self) -> NormalizeStorage:
        return NormalizeStorage(True, self._normalize_storage_config())

    def _get_load_storage(self) -> LoadStorage:
        caps = self._get_destination_capabilities()
        return LoadStorage(
            True,
            caps.supported_loader_file_formats,
            self._load_storage_config(),
        )

    def _normalize_storage_config(self) -> NormalizeStorageConfiguration:
        return NormalizeStorageConfiguration(
            normalize_volume_path=os.path.join(self.working_dir, "normalize")
        )

    def _load_storage_config(self) -> LoadStorageConfiguration:
        return LoadStorageConfiguration(load_volume_path=os.path.join(self.working_dir, "load"))

    def _init_working_dir(self, pipeline_name: str, pipelines_dir: str) -> None:
        self.pipeline_name = pipeline_name
        self.pipelines_dir = pipelines_dir
        self._validate_pipeline_name()
        # compute the folder that keeps all of the pipeline state
        self.working_dir = os.path.join(pipelines_dir, pipeline_name)
        # create pipeline storage, do not create working dir yet
        self._pipeline_storage = FileStorage(self.working_dir, makedirs=False)
        # if full refresh was requested, wipe out all data from working folder, if exists
        if self.dev_mode:
            self._wipe_working_folder()

    def _configure(
        self, import_schema_path: str, export_schema_path: str, must_attach_to_local_pipeline: bool
    ) -> None:
        # create schema storage and folders
        self._schema_storage_config = SchemaStorageConfiguration(
            schema_volume_path=os.path.join(self.working_dir, "schemas"),
            import_schema_path=import_schema_path,
            export_schema_path=export_schema_path,
        )
        # create default configs
        self._normalize_storage_config()
        self._load_storage_config()

        # are we running again?
        has_state = self._pipeline_storage.has_file(Pipeline.STATE_FILE)
        if must_attach_to_local_pipeline and not has_state:
            raise CannotRestorePipelineException(
                self.pipeline_name,
                self.pipelines_dir,
                f"the pipeline was not found in {self.working_dir}.",
            )

        self.must_attach_to_local_pipeline = must_attach_to_local_pipeline
        # attach to pipeline if folder exists and contains state
        if has_state:
            self._attach_pipeline()
        else:
            # this will erase the existing working folder
            self._create_pipeline()

        # create schema storage
        self._schema_storage = LiveSchemaStorage(self._schema_storage_config, makedirs=True)

    def _create_pipeline(self) -> None:
        self._wipe_working_folder()
        self._pipeline_storage.create_folder("", exists_ok=False)
        self.default_schema_name = None
        self.schema_names = []
        self.first_run = True

    def _wipe_working_folder(self) -> None:
        # kill everything inside the working folder
        if self._pipeline_storage.has_folder(""):
            self._pipeline_storage.delete_folder("", recursively=True, delete_ro=True)

    def _attach_pipeline(self) -> None:
        pass

    def _extract_source(
        self,
        extract: Extract,
        source: DltSource,
        max_parallel_items: int,
        workers: int,
        refresh: Optional[TRefreshMode] = None,
        load_package_state_update: Optional[TLoadPackageState] = None,
    ) -> str:
        load_package_state_update = copy(load_package_state_update or {})
        # discover the existing pipeline schema
        try:
            # all live schemas are initially committed and during the extract will accumulate changes in memory
            # line below may create another live schema if source schema is not a part of storage
            # this will (1) look for import schema if present
            # (2) load import schema an overwrite pipeline schema if import schema modified
            # (3) load pipeline schema if no import schema is present

            # keep schema created by the source so we can apply changes from it later
            source_schema = source.schema
            # use existing pipeline schema as the source schema, clone until extraction complete
            source.schema = self.schemas[source.schema.name].clone()
            # refresh the pipeline schema ie. to drop certain tables before any normalizes change
            if refresh:
                # NOTE: we use original pipeline schema to detect dropped/truncated tables so we can drop
                # the original names, before eventual new naming convention is applied
                load_package_state_update.update(deepcopy(refresh_source(self, source, refresh)))
                if refresh == "drop_sources":
                    # replace the whole source AFTER we got tables to drop
                    source.schema = source_schema
            # NOTE: we do pass any programmatic changes from source schema to pipeline schema except settings below
            # TODO: enable when we have full identifier lineage and we are able to merge table identifiers
            if type(source.schema.naming) is not type(source_schema.naming):  # noqa
                source.schema_contract = source_schema.settings.get("schema_contract")
            else:
                source.schema.update_schema(source_schema)
        except FileNotFoundError:
            if refresh is not None:
                logger.info(
                    f"Refresh flag {refresh} has no effect on source {source.name} because the"
                    " source is extracted for a first time"
                )

        # update the normalizers to detect any conflicts early
        source.schema.update_normalizers()

        # extract into pipeline schema
        load_id = extract.extract(
            source, max_parallel_items, workers, load_package_state_update=load_package_state_update
        )

        # update live schema but not update the store yet
        source.schema = self._schema_storage.set_live_schema(source.schema)

        # set as default if this is first schema in pipeline
        if not self.default_schema_name:
            # this performs additional validations as schema contains the naming module
            self._set_default_schema_name(source.schema)

        return load_id

    def _get_destination_clients(
        self,
        schema: Schema = None,
    ) -> Tuple[JobClientBase, JobClientBase]:
        if not self._destination:
            raise PipelineConfigMissing(
                self.pipeline_name,
                "destination",
                "load",
                "Please provide `destination` argument to `pipeline`, `run` or `load` method"
                " directly or via .dlt/config.toml file or environment variable.",
            )

        destination_client, staging_client = get_destination_clients(
            schema=schema or self.default_schema,
            destination=self._destination,
            destination_dataset_name=self.dataset_name,
            staging=self._staging,
            # in case of destination that does not need dataset name, we still must
            # provide one to staging
            # TODO: allow for separate staging_dataset_name, that will require to migrate pipeline state
            #   to store it.
            staging_dataset_name=self.dataset_name or self._make_dataset_name(None, self._staging),
            multi_dataset_default_schema_name=(
                self.default_schema_name if not self.config.use_single_dataset else None
            ),
        )

        if isinstance(destination_client.config, DestinationClientStagingConfiguration):
            if not self.dataset_name and self.dev_mode:
                logger.warning(
                    "`dev_mode=True` may not work if `dataset_name` is not set. "
                    "Please set `dataset_name` in `dlt.pipeline(...)` or `Pipeline.run(...)`."
                )

        return destination_client, staging_client

    def _get_destination_capabilities(self) -> DestinationCapabilitiesContext:
        if not self._destination:
            raise PipelineConfigMissing(
                self.pipeline_name,
                "destination",
                "normalize",
                "Please provide `destination` argument to `pipeline`, `run` or `load` method"
                " directly or via .dlt/config.toml file or environment variable.",
            )

        # check if default schema is present
        if (
            self.default_schema_name is not None
            and self.default_schema_name in self._schema_storage
        ):
            naming = self.default_schema.naming
        else:
            naming = None

        return self._destination.capabilities(naming=naming)

    def _get_staging_capabilities(self) -> Optional[DestinationCapabilitiesContext]:
        if self._staging is None:
            return None
        # check if default schema is present
        if (
            self.default_schema_name is not None
            and self.default_schema_name in self._schema_storage
        ):
            naming = self.default_schema.naming
        else:
            naming = None
        return self._staging.capabilities(naming=naming)

    def _validate_pipeline_name(self) -> None:
        try:
            FileStorage.validate_file_name_component(self.pipeline_name)
        except ValueError as ve_ex:
            raise InvalidPipelineName(self.pipeline_name, str(ve_ex))

    def _set_context(self, is_active: bool) -> None:
        if not self.is_active and is_active:
            # initialize runtime if not active previously
            apply_runtime_config(self.runtime_config)

        self.is_active = is_active
        if is_active:
            # set destination context on activation
            if self._destination:
                # inject capabilities context
                self._container[DestinationCapabilitiesContext] = (
                    self._get_destination_capabilities()
                )
        else:
            # remove destination context on deactivation
            if DestinationCapabilitiesContext in self._container:
                del self._container[DestinationCapabilitiesContext]

    def _set_destinations(
        self,
        destination: TDestinationReferenceArg,
        destination_name: Optional[str] = None,
        staging: Optional[TDestinationReferenceArg] = None,
        staging_name: Optional[str] = None,
        initializing: bool = False,
        destination_credentials: Any = None,
    ) -> None:
        destination_changed = destination is not None and destination != self._destination
        # set destination if provided but do not swap if factory is the same
        if destination_changed:
            self._destination = Destination.from_reference(
                destination, destination_name=destination_name
            )

        if (
            self._destination
            and not self._destination.capabilities().supported_loader_file_formats
            and not staging
            and not self._staging
        ):
            logger.warning(
                f"The destination {self._destination.destination_name} requires the filesystem"
                " staging destination to be set, but it was not provided. Setting it to"
                " 'filesystem'."
            )
            staging = "filesystem"
            staging_name = "filesystem"

        staging_changed = staging is not None and staging != self._staging
        if staging_changed:
            staging_module = Destination.from_reference(staging, destination_name=staging_name)
            if staging_module and not issubclass(
                staging_module.spec, DestinationClientStagingConfiguration
            ):
                raise DestinationNoStagingMode(staging_module.destination_name)
            # set via property
            self.staging = staging_module

        if staging_changed or destination_changed:
            # make sure that capabilities can be generated
            with self._maybe_destination_capabilities():
                # update normalizers in all live schemas, only when destination changed
                if destination_changed and not initializing:
                    for schema in self._schema_storage.live_schemas.values():
                        schema.update_normalizers()
            # set new context
            if not initializing:
                self._set_context(is_active=True)
        # apply explicit credentials
        if self._destination:
            if destination_credentials:
                self._destination.config_params["credentials"] = destination_credentials
            # set via property
            self.destination = self._destination

    @contextmanager
    def _maybe_destination_capabilities(
        self,
    ) -> Iterator[DestinationCapabilitiesContext]:
        caps: DestinationCapabilitiesContext = None
        injected_caps: ContextManager[DestinationCapabilitiesContext] = None
        try:
            if self._destination:
                destination_caps = self._get_destination_capabilities()
                stage_caps = self._get_staging_capabilities()
                injected_caps = self._container.injectable_context(destination_caps)
                caps = injected_caps.__enter__()

                caps.preferred_loader_file_format, caps.supported_loader_file_formats = (
                    merge_caps_file_formats(
                        self._destination.destination_name,
                        (self._staging.destination_name if self._staging else None),
                        destination_caps,
                        stage_caps,
                    )
                )
            yield caps
        finally:
            if injected_caps:
                injected_caps.__exit__(None, None, None)

    def _set_dataset_name(self, new_dataset_name: Optional[str]) -> None:
        if new_dataset_name or not self.dataset_name:
            self.dataset_name = self._make_dataset_name(new_dataset_name, self._destination)

    def _make_dataset_name(
        self, new_dataset_name: Optional[str], destination: Optional[AnyDestination]
    ) -> str:
        """Generates dataset name for the pipeline based on `new_dataset_name`
        1. if name is not provided, default name is created
        2. for destinations that do not need dataset names, def. name is not created
        3. we add serial number in dev mode
        4. we apply layout from pipeline config if present
        """
        if not new_dataset_name:
            # dataset name is required but not provided - generate the default now
            destination_needs_dataset = False
            if destination and issubclass(destination.spec, DestinationClientDwhConfiguration):
                destination_needs_dataset = destination.spec.needs_dataset_name()
            # if destination is not specified - generate dataset
            if destination_needs_dataset:
                new_dataset_name = self.pipeline_name + self.DEFAULT_DATASET_SUFFIX

        if not new_dataset_name:
            return new_dataset_name

        # in case of dev_mode add unique suffix
        if self.dev_mode:
            # dataset must be specified
            # double _ is not allowed
            if new_dataset_name.endswith("_"):
                new_dataset_name += self._pipeline_instance_id[1:]
            else:
                new_dataset_name += self._pipeline_instance_id

        # normalizes the dataset name using the dataset_name_layout
        if self.config.dataset_name_layout:
            new_dataset_name = self.config.dataset_name_layout % new_dataset_name
        return new_dataset_name

    def _set_default_schema_name(self, schema: Schema) -> None:
        assert self.default_schema_name is None
        self.default_schema_name = schema.name

    def _create_pipeline_instance_id(self) -> str:
        return pendulum.now().format("_YYYYMMDDhhmmss")

    @with_schemas_sync
    @with_state_sync()
    def _inject_schema(self, schema: Schema) -> None:
        """Injects a schema into the pipeline. Existing schema will be overwritten"""
        schema.update_normalizers()
        self._schema_storage.save_schema(schema)
        if not self.default_schema_name:
            self._set_default_schema_name(schema)

    def _get_step_info(self, step: WithStepInfo[TStepMetrics, TStepInfo]) -> TStepInfo:
        return step.get_step_info(self)

    def _get_state(self) -> TPipelineState:
        try:
            state = json_decode_state(self._pipeline_storage.load(Pipeline.STATE_FILE))
            migrated_state = migrate_pipeline_state(
                self.pipeline_name,
                state,
                state["_state_engine_version"],
                PIPELINE_STATE_ENGINE_VERSION,
            )
            # TODO: move to a migration. this change is local and too small to justify
            # engine upgrade
            _local = migrated_state["_local"]
            if "initial_cwd" not in _local:
                _local["initial_cwd"] = os.path.abspath(dlt.current.run_context().local_dir)
            return migrated_state
        except FileNotFoundError:
            # do not set the state hash, this will happen on first merge
            return default_pipeline_state()

    def _restore_state_from_destination(self) -> Optional[TPipelineState]:
        # if state is not present locally, take the state from the destination
        dataset_name = self.dataset_name
        use_single_dataset = self.config.use_single_dataset
        try:
            # force the main dataset to be used
            self.config.use_single_dataset = True
            schema_name = normalize_schema_name(self.pipeline_name)
            with self._maybe_destination_capabilities():
                schema = Schema(schema_name)
            with self._get_destination_clients(schema)[0] as job_client:
                if isinstance(job_client, WithStateSync):
                    state = load_pipeline_state_from_destination(self.pipeline_name, job_client)
                    if state is None:
                        logger.info(
                            "The state was not found in the destination"
                            f" {self._destination.destination_description}:{dataset_name}"
                        )
                    else:
                        logger.info(
                            "The state was restored from the destination"
                            f" {self._destination.destination_description}:{dataset_name}"
                        )
                else:
                    state = None
                    logger.info(
                        "Destination does not support state sync"
                        f" {self._destination.destination_description}:{dataset_name}"
                    )
            return state
        finally:
            # restore the use_single_dataset option
            self.config.use_single_dataset = use_single_dataset

    def _get_schemas_from_destination(
        self, schema_names: Sequence[str], always_download: bool = False
    ) -> Sequence[Schema]:
        # check which schemas are present in the pipeline and restore missing schemas
        restored_schemas: List[Schema] = []
        for schema_name in schema_names:
            with self._maybe_destination_capabilities():
                schema = Schema(schema_name)

            if not self._schema_storage.has_schema(schema.name) or always_download:
                with self._get_destination_clients(schema)[0] as job_client:
                    if not isinstance(job_client, WithStateSync):
                        logger.info(
                            "Destination does not support restoring of pipeline state"
                            f" {self._destination.destination_name}"
                        )
                        return restored_schemas
                    schema_info = job_client.get_stored_schema(schema_name)
                    if schema_info is None:
                        logger.warning(
                            f"The schema {schema.name} was not found in the destination"
                            f" {self._destination.destination_name}:{self.dataset_name}. "
                            " Pipeline state indicated that this schema should be preset. "
                            "New or imported schema will be used instead. "
                        )
                        # try to import schema
                        with contextlib.suppress(FileNotFoundError):
                            schema = self._schema_storage.load_schema(schema.name)
                        # imported or new/empty schema will be added to restored schemas
                    else:
                        schema = Schema.from_dict(json.loads(schema_info.schema))
                        logger.info(
                            f"The schema {schema.name} version {schema.version} hash"
                            f" {schema.stored_version_hash} was restored from the destination"
                            f" {self._destination.destination_name}:{self.dataset_name}"
                        )

                    restored_schemas.append(schema)
        return restored_schemas

    @contextmanager
    def managed_state(self, *, extract_state: bool = False) -> Iterator[TPipelineState]:
        """Puts pipeline state in managed mode, where yielded state changes will be persisted or fully roll-backed on exception.

        Makes the state to be available via StateInjectableContext
        """
        state = self._get_state()
        try:
            # add the state to container as a context
            with self._container.injectable_context(StateInjectableContext(state=state)):
                yield state
        except Exception:
            backup_state = self._get_state()
            # restore original pipeline props
            self._state_to_props(backup_state)
            # raise original exception
            raise
        else:
            # this modifies state in place
            self._bump_version_and_extract_state(state, extract_state)
            # so we save modified state here
            self._save_state(state)

    def _state_to_props(self, state: TPipelineState) -> None:
        """Write `state` to pipeline props."""
        for prop in Pipeline.STATE_PROPS:
            if prop in state and not prop.startswith("_"):
                setattr(self, prop, state[prop])  # type: ignore
        for prop in Pipeline.LOCAL_STATE_PROPS:
            if prop in state["_local"] and not prop.startswith("_"):
                setattr(self, prop, state["_local"][prop])  # type: ignore
        # staging and destination are taken from state only if not yet set in the pipeline
        if not self._destination:
            self._set_destinations(
                destination=state.get("destination_type"),
                destination_name=state.get("destination_name"),
                staging=state.get("staging_type"),
                staging_name=state.get("staging_name"),
            )
        else:
            # issue warnings that state destination/staging got ignored
            state_destination = state.get("destination_type")
            if state_destination:
                if self._destination.destination_type != state_destination:
                    logger.warning(
                        f"The destination {state_destination}:{state.get('destination_name')} in"
                        " state differs from destination"
                        f" {self._destination.destination_type}:{self._destination.destination_name} in"
                        " pipeline and will be ignored"
                    )
                    state_staging = state.get("staging_type")
                    if state_staging:
                        logger.warning(
                            "The state staging destination"
                            f" {state_staging}:{state.get('staging_name')} is ignored"
                        )

    def _props_to_state(self, state: TPipelineState) -> TPipelineState:
        """Write pipeline props to `state`, returns it for chaining"""
        for prop in Pipeline.STATE_PROPS:
            if not prop.startswith("_"):
                state[prop] = getattr(self, prop)  # type: ignore
        for prop in Pipeline.LOCAL_STATE_PROPS:
            if not prop.startswith("_"):
                state["_local"][prop] = getattr(self, prop)  # type: ignore
        if self._destination:
            state["destination_type"] = self._destination.destination_type
            state["destination_name"] = self._destination.configured_name
        if self._staging:
            state["staging_type"] = self._staging.destination_type
            state["staging_name"] = self._staging.configured_name
        state["schema_names"] = self._list_schemas_sorted()
        return state

    def _save_and_extract_state_and_schema(
        self,
        state: TPipelineState,
        schema: Schema,
        load_package_state_update: Optional[TLoadPackageState] = None,
    ) -> None:
        """Save given state + schema and extract creating a new load package

        Args:
            state (TPipelineState): The new pipeline state, replaces the current state
            schema (Schema): The new source schema, replaces current schema of the same name
            load_package_state_update (Optional[TLoadPackageState]): Dict which items will be included in the load package state
        """
        self.schemas.save_schema(schema)
        with self.managed_state() as old_state:
            old_state.update(state)

        self._bump_version_and_extract_state(
            state,
            extract_state=True,
            load_package_state_update=load_package_state_update,
            schema=schema,
        )

    def _bump_version_and_extract_state(
        self,
        state: TPipelineState,
        extract_state: bool,
        extract: Extract = None,
        load_package_state_update: Optional[TLoadPackageState] = None,
        schema: Optional[Schema] = None,
    ) -> None:
        """Merges existing state into `state` and extracts state using `storage` if extract_state is True.

        Storage will be created on demand. In that case the extracted package will be immediately committed.
        """
        _, hash_, _ = bump_pipeline_state_version_if_modified(self._props_to_state(state))
        should_extract = hash_ != state["_local"].get("_last_extracted_hash")
        if should_extract and extract_state:
            extract_ = extract or Extract(self._schema_storage, self._normalize_storage_config())
            # create or get load package upfront to get load_id to create state doc
            schema = schema or self.default_schema
            # note that we preferably retrieve existing package for `schema`
            # same thing happens in extract_.extract so the load_id is preserved
            load_id = extract_.extract_storage.create_load_package(
                schema, reuse_exiting_package=True
            )
            data, doc = state_resource(state, load_id)
            # keep the original data to be used in the metrics
            if extract_.original_data is None:
                extract_.original_data = data
            # append pipeline state to package state
            load_package_state_update = load_package_state_update or {}
            load_package_state_update["pipeline_state"] = doc
            self._extract_source(
                extract_,
                data_to_sources(data, self, schema=schema)[0],
                1,
                1,
                load_package_state_update=load_package_state_update,
            )
            # set state to be extracted
            mark_state_extracted(state, hash_)
            # commit only if we created storage
            if not extract:
                extract_.commit_packages()

    def _list_schemas_sorted(self) -> List[str]:
        """Lists schema names sorted to have deterministic state"""
        return sorted(self._schema_storage.list_schemas())

    def _save_state(self, state: TPipelineState) -> None:
        self._pipeline_storage.save(Pipeline.STATE_FILE, json_encode_state(state))

    def __getstate__(self) -> Any:
        # pickle only the SupportsPipeline protocol fields
        return {
            "pipeline_name": self.pipeline_name,
            "default_schema_name": self.default_schema_name,
            "dataset_name": self.dataset_name,
            "working_dir": self.working_dir,
        }

    # NOTE: I expect that we'll merge all relations into one. and then we'll be able to get rid
    #  of overload and dataset_type

    def dataset(self, schema: Union[Schema, str, None] = None) -> Dataset:
        """Returns a dataset object for querying the destination data.

        Args:
            schema (Union[Schema, str, None]): Schema name or Schema object to use. If None, uses the default schema if set.
        Returns:
            Dataset: A dataset object that supports querying the destination data.
        """

        if not self._destination:
            raise PipelineConfigMissing(
                self.pipeline_name,
                "destination",
                "dataset",
                "Please provide `destination` argument to `pipeline` method"
                " directly or via .dlt config.toml file or environment variable.",
            )

        if isinstance(schema, Schema):
            logger.info(
                f"Make sure that tables declared in explicit schema {schema.name} are present on"
                f" dataset {self.dataset_name}"
            )
        elif isinstance(schema, str):
            if schema not in self.schemas:
                logger.info(
                    f"Schema {schema} not found in the pipeline, deferring to destination, this"
                    " will load a schema of this name from the destination or use an empty schema"
                    " with this name."
                )
            else:
                schema = self.schemas[schema]

        elif self.default_schema_name:
            schema = self.default_schema

        return dataset(
            self._destination,
            self.dataset_name,
            schema=schema,
        )
