import contextlib
import os
import datetime  # noqa: 251
from contextlib import contextmanager
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
    cast,
    get_type_hints,
    ContextManager,
)

from dlt import version
from dlt.common import json, logger, pendulum
from dlt.common.configuration import inject_section, known_sections
from dlt.common.configuration.specs import RunConfiguration, CredentialsConfiguration
from dlt.common.configuration.container import Container
from dlt.common.configuration.exceptions import (
    ConfigFieldMissingException,
    ContextDefaultCannotBeCreated,
)
from dlt.common.configuration.specs.config_section_context import ConfigSectionContext
from dlt.common.configuration.resolve import initialize_credentials
from dlt.common.exceptions import (
    DestinationLoadingViaStagingNotSupported,
    DestinationLoadingWithoutStagingNotSupported,
    DestinationNoStagingMode,
    MissingDependencyException,
    DestinationUndefinedEntity,
    DestinationIncompatibleLoaderFileFormatException,
)
from dlt.common.normalizers import explicit_normalizers, import_normalizers
from dlt.common.runtime import signals, initialize_runtime
from dlt.common.schema.typing import (
    TColumnNames,
    TSchemaTables,
    TWriteDisposition,
    TAnySchemaColumns,
    TSchemaContract,
)
from dlt.common.schema.utils import normalize_schema_name
from dlt.common.storages.exceptions import LoadPackageNotFound
from dlt.common.typing import DictStrStr, TFun, TSecretValue, is_optional_type
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
)
from dlt.common.destination import DestinationCapabilitiesContext, TDestination
from dlt.common.destination.reference import (
    DestinationClientDwhConfiguration,
    WithStateSync,
    Destination,
    JobClientBase,
    DestinationClientConfiguration,
    TDestinationReferenceArg,
    DestinationClientStagingConfiguration,
    DestinationClientStagingConfiguration,
    DestinationClientDwhWithStagingConfiguration,
)
from dlt.common.destination.capabilities import INTERNAL_LOADER_FILE_FORMATS
from dlt.common.pipeline import (
    ExtractInfo,
    LoadInfo,
    NormalizeInfo,
    PipelineContext,
    StepInfo,
    TStepInfo,
    SupportsPipeline,
    TPipelineLocalState,
    TPipelineState,
    StateInjectableContext,
    TStepMetrics,
    WithStepInfo,
)
from dlt.common.schema import Schema
from dlt.common.utils import is_interactive
from dlt.common.data_writers import TLoaderFileFormat
from dlt.common.warnings import deprecated, Dlt04DeprecationWarning

from dlt.extract import DltSource
from dlt.extract.exceptions import SourceExhausted
from dlt.extract.extract import Extract, data_to_sources
from dlt.normalize import Normalize
from dlt.normalize.configuration import NormalizeConfiguration
from dlt.destinations.sql_client import SqlClientBase
from dlt.destinations.job_client_impl import SqlJobClientBase
from dlt.load.configuration import LoaderConfiguration
from dlt.load import Load

from dlt.pipeline.configuration import PipelineConfiguration
from dlt.pipeline.progress import _Collector, _NULL_COLLECTOR
from dlt.pipeline.exceptions import (
    CannotRestorePipelineException,
    InvalidPipelineName,
    PipelineConfigMissing,
    PipelineNotActive,
    PipelineStepFailed,
    SqlClientNotAvailable,
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
    STATE_ENGINE_VERSION,
    bump_version_if_modified,
    load_state_from_destination,
    migrate_state,
    state_resource,
    json_encode_state,
    json_decode_state,
)
from dlt.pipeline.warnings import credentials_argument_deprecated


def with_state_sync(may_extract_state: bool = False) -> Callable[[TFun], TFun]:
    def decorator(f: TFun) -> TFun:
        @wraps(f)
        def _wrap(self: "Pipeline", *args: Any, **kwargs: Any) -> Any:
            # activate pipeline so right state is always provided
            self.activate()
            # backup and restore state
            should_extract_state = may_extract_state and self.config.restore_from_destination
            with self.managed_state(extract_state=should_extract_state) as state:
                # add the state to container as a context
                with self._container.injectable_context(StateInjectableContext(state=state)):
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
                    self._schema_storage.update_live_schema(schema, can_create_new=False)
                except FileNotFoundError:
                    # no storage schema yet so pop live schema (created in call to f)
                    self._schema_storage.live_schemas.pop(name, None)
            # NOTE: with_state_sync will restore schema_names and default_schema_name
            # so we do not need to do that here
            raise
        else:
            # save modified live schemas
            for name, schema in self._schema_storage.live_schemas.items():
                self._schema_storage.commit_live_schema(name)
                # also save import schemas only here
                self._schema_storage.save_import_schema_if_not_exists(schema)
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


def with_config_section(sections: Tuple[str, ...]) -> Callable[[TFun], TFun]:
    def decorator(f: TFun) -> TFun:
        @wraps(f)
        def _wrap(self: "Pipeline", *args: Any, **kwargs: Any) -> Any:
            # add section context to the container to be used by all configuration without explicit sections resolution
            with inject_section(
                ConfigSectionContext(pipeline_name=self.pipeline_name, sections=sections)
            ):
                return f(self, *args, **kwargs)

        return _wrap  # type: ignore

    return decorator


class Pipeline(SupportsPipeline):
    STATE_FILE: ClassVar[str] = "state.json"
    STATE_PROPS: ClassVar[List[str]] = list(
        set(get_type_hints(TPipelineState).keys())
        - {"sources", "destination_type", "destination_name", "staging_type", "staging_name"}
    )
    LOCAL_STATE_PROPS: ClassVar[List[str]] = list(get_type_hints(TPipelineLocalState).keys())
    DEFAULT_DATASET_SUFFIX: ClassVar[str] = "_dataset"

    pipeline_name: str
    """Name of the pipeline"""
    default_schema_name: str = None
    schema_names: List[str] = []
    first_run: bool = False
    """Indicates a first run of the pipeline, where run ends with successful loading of the data"""
    full_refresh: bool
    must_attach_to_local_pipeline: bool
    pipelines_dir: str
    """A directory where the pipelines' working directories are created"""
    working_dir: str
    """A working directory of the pipeline"""
    destination: TDestination = None
    staging: TDestination = None
    """The destination reference which is the Destination Class. `destination.destination_name` returns the name string"""
    dataset_name: str = None
    """Name of the dataset to which pipeline will be loaded to"""
    credentials: Any = None
    is_active: bool = False
    """Tells if instance is currently active and available via dlt.pipeline()"""
    collector: _Collector
    config: PipelineConfiguration
    runtime_config: RunConfiguration

    def __init__(
        self,
        pipeline_name: str,
        pipelines_dir: str,
        pipeline_salt: TSecretValue,
        destination: TDestination,
        staging: TDestination,
        dataset_name: str,
        credentials: Any,
        import_schema_path: str,
        export_schema_path: str,
        full_refresh: bool,
        progress: _Collector,
        must_attach_to_local_pipeline: bool,
        config: PipelineConfiguration,
        runtime: RunConfiguration,
    ) -> None:
        """Initializes the Pipeline class which implements `dlt` pipeline. Please use `pipeline` function in `dlt` module to create a new Pipeline instance."""
        self.pipeline_salt = pipeline_salt
        self.config = config
        self.runtime_config = runtime
        self.full_refresh = full_refresh
        self.collector = progress or _NULL_COLLECTOR
        self.destination = None
        self.staging = None

        self._container = Container()
        self._pipeline_instance_id = self._create_pipeline_instance_id()
        self._pipeline_storage: FileStorage = None
        self._schema_storage: LiveSchemaStorage = None
        self._schema_storage_config: SchemaStorageConfiguration = None
        self._trace: PipelineTrace = None
        self._last_trace: PipelineTrace = None
        self._state_restored: bool = False

        initialize_runtime(self.runtime_config)
        # initialize pipeline working dir
        self._init_working_dir(pipeline_name, pipelines_dir)

        with self.managed_state() as state:
            # changing the destination could be dangerous if pipeline has pending load packages
            self._set_destinations(destination=destination, staging=staging)
            # set the pipeline properties from state, destination and staging will not be set
            self._state_to_props(state)
            # we overwrite the state with the values from init
            self._set_dataset_name(dataset_name)
            self.credentials = credentials
            self._configure(import_schema_path, export_schema_path, must_attach_to_local_pipeline)

    def drop(self, pipeline_name: str = None) -> "Pipeline":
        """Deletes local pipeline state, schemas and any working files.

        Args:
            pipeline_name (str): Optional. New pipeline name.
        """
        # reset the pipeline working dir
        self._create_pipeline()
        # clone the pipeline
        return Pipeline(
            pipeline_name or self.pipeline_name,
            self.pipelines_dir,
            self.pipeline_salt,
            self.destination,
            self.staging,
            self.dataset_name,
            self.credentials,
            self._schema_storage.config.import_schema_path,
            self._schema_storage.config.export_schema_path,
            self.full_refresh,
            self.collector,
            False,
            self.config,
            self.runtime_config,
        )

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
        write_disposition: TWriteDisposition = None,
        columns: TAnySchemaColumns = None,
        primary_key: TColumnNames = None,
        schema: Schema = None,
        max_parallel_items: int = None,
        workers: int = None,
        schema_contract: TSchemaContract = None,
    ) -> ExtractInfo:
        """Extracts the `data` and prepare it for the normalization. Does not require destination or credentials to be configured. See `run` method for the arguments' description."""
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
                    schema,
                    table_name,
                    parent_table_name,
                    write_disposition,
                    columns,
                    primary_key,
                    schema_contract,
                ):
                    if source.exhausted:
                        raise SourceExhausted(source.name)
                    self._extract_source(extract_step, source, max_parallel_items, workers)
                # extract state
                if self.config.restore_from_destination:
                    # this will update state version hash so it will not be extracted again by with_state_sync
                    self._bump_version_and_extract_state(
                        self._container[StateInjectableContext].state, True, extract_step
                    )
                # commit load packages
                extract_step.commit_packages()
                return self._get_step_info(extract_step)
        except Exception as exc:
            step_info = self._get_step_info(extract_step)
            raise PipelineStepFailed(
                self,
                "extract",
                extract_step.current_load_id,
                exc,
                step_info,
            ) from exc

    @with_runtime_trace()
    @with_schemas_sync
    @with_config_section((known_sections.NORMALIZE,))
    def normalize(
        self, workers: int = 1, loader_file_format: TLoaderFileFormat = None
    ) -> NormalizeInfo:
        """Normalizes the data prepared with `extract` method, infers the schema and creates load packages for the `load` method. Requires `destination` to be known."""
        if is_interactive():
            workers = 1
        if loader_file_format and loader_file_format in INTERNAL_LOADER_FILE_FORMATS:
            raise ValueError(f"{loader_file_format} is one of internal dlt file formats.")
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
        with self._maybe_destination_capabilities(loader_file_format=loader_file_format):
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
    @with_schemas_sync
    @with_state_sync()
    @with_config_section((known_sections.LOAD,))
    def load(
        self,
        destination: TDestinationReferenceArg = None,
        dataset_name: str = None,
        credentials: Any = None,
        *,
        workers: int = 20,
        raise_on_failed_jobs: bool = False,
    ) -> LoadInfo:
        """Loads the packages prepared by `normalize` method into the `dataset_name` at `destination`, using provided `credentials`"""
        # set destination and default dataset if provided (this is the reason we have state sync here)
        self._set_destinations(destination=destination, staging=None)
        self._set_dataset_name(dataset_name)

        credentials_argument_deprecated("pipeline.load", credentials, destination)

        self.credentials = credentials or self.credentials

        # check if any schema is present, if not then no data was extracted
        if not self.default_schema_name:
            return None

        # make sure that destination is set and client is importable and can be instantiated
        client, staging_client = self._get_destination_clients(self.default_schema)

        # create default loader config and the loader
        load_config = LoaderConfiguration(
            workers=workers,
            raise_on_failed_jobs=raise_on_failed_jobs,
            _load_storage_config=self._load_storage_config(),
        )
        load_step: Load = Load(
            self.destination,
            staging_destination=self.staging,
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
            self.first_run = False
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
        write_disposition: TWriteDisposition = None,
        columns: TAnySchemaColumns = None,
        primary_key: TColumnNames = None,
        schema: Schema = None,
        loader_file_format: TLoaderFileFormat = None,
        schema_contract: TSchemaContract = None,
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

        #### Args:
            data (Any): Data to be loaded to destination

            destination (str | DestinationReference, optional): A name of the destination to which dlt will load the data, or a destination module imported from `dlt.destination`.
            If not provided, the value passed to `dlt.pipeline` will be used.

            dataset_name (str, optional):A name of the dataset to which the data will be loaded. A dataset is a logical group of tables ie. `schema` in relational databases or folder grouping many files.
            If not provided, the value passed to `dlt.pipeline` will be used. If not provided at all then defaults to the `pipeline_name`


            credentials (Any, optional): Credentials for the `destination` ie. database connection string or a dictionary with google cloud credentials.
            In most cases should be set to None, which lets `dlt` to use `secrets.toml` or environment variables to infer right credentials values.

            table_name (str, optional): The name of the table to which the data should be loaded within the `dataset`. This argument is required for a `data` that is a list/Iterable or Iterator without `__name__` attribute.
            The behavior of this argument depends on the type of the `data`:
            * generator functions: the function name is used as table name, `table_name` overrides this default
            * `@dlt.resource`: resource contains the full table schema and that includes the table name. `table_name` will override this property. Use with care!
            * `@dlt.source`: source contains several resources each with a table schema. `table_name` will override all table names within the source and load the data into single table.

            write_disposition (Literal["skip", "append", "replace", "merge"], optional): Controls how to write data to a table. `append` will always add new data at the end of the table. `replace` will replace existing data with new data. `skip` will prevent data from loading. "merge" will deduplicate and merge data based on "primary_key" and "merge_key" hints. Defaults to "append".
            Please note that in case of `dlt.resource` the table schema value will be overwritten and in case of `dlt.source`, the values in all resources will be overwritten.

            columns (Sequence[TColumnSchema], optional): A list of column schemas. Typed dictionary describing column names, data types, write disposition and performance hints that gives you full control over the created table schema.

            primary_key (str | Sequence[str]): A column name or a list of column names that comprise a private key. Typically used with "merge" write disposition to deduplicate loaded data.

            schema (Schema, optional): An explicit `Schema` object in which all table schemas will be grouped. By default `dlt` takes the schema from the source (if passed in `data` argument) or creates a default one itself.

            loader_file_format (Literal["jsonl", "insert_values", "parquet"], optional). The file format the loader will use to create the load package. Not all file_formats are compatible with all destinations. Defaults to the preferred file format of the selected destination.

            schema_contract (TSchemaContract, optional): On override for the schema contract settings, this will replace the schema contract settings for all tables in the schema. Defaults to None.

        Raises:
            PipelineStepFailed when a problem happened during `extract`, `normalize` or `load` steps.
        Returns:
            LoadInfo: Information on loaded data including the list of package ids and failed job statuses. Please not that `dlt` will not raise if a single job terminally fails. Such information is provided via LoadInfo.
        """
        signals.raise_if_signalled()
        self.activate()
        self._set_destinations(destination=destination, staging=staging)
        self._set_dataset_name(dataset_name)

        credentials_argument_deprecated("pipeline.run", credentials, self.destination)

        # sync state with destination
        if (
            self.config.restore_from_destination
            and not self.full_refresh
            and not self._state_restored
            and (self.destination or destination)
        ):
            self.sync_destination(destination, staging, dataset_name)
            # sync only once
            self._state_restored = True

        # normalize and load pending data
        if self.list_extracted_load_packages():
            self.normalize(loader_file_format=loader_file_format)
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
                schema_contract=schema_contract,
            )
            self.normalize(loader_file_format=loader_file_format)
            return self.load(destination, dataset_name, credentials=credentials)
        else:
            return None

    @with_schemas_sync
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
        self._set_destinations(destination=destination, staging=staging)
        self._set_dataset_name(dataset_name)

        state = self._get_state()
        state_changed = False
        try:
            try:
                restored_schemas: Sequence[Schema] = None
                remote_state = self._restore_state_from_destination()

                # if remote state is newer or same
                # print(f'REMOTE STATE: {(remote_state or {}).get("_state_version")} >= {state["_state_version"]}')
                # TODO: check if remote_state["_state_version"] is not in 10 recent version. then we know remote is newer.
                if remote_state and remote_state["_state_version"] >= state["_state_version"]:
                    state_changed = remote_state["_version_hash"] != state.get("_version_hash")
                    # print(f"MERGED STATE: {bool(merged_state)}")
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
                    state["_local"]["_last_extracted_hash"] = state["_version_hash"]
                    state["_local"]["_last_extracted_at"] = pendulum.now()
                    # on merge schemas are replaced so we delete all old versions
                    self._schema_storage.clear_storage()
                for schema in restored_schemas:
                    self._schema_storage.save_schema(schema)
                # if the remote state is present then unset first run
                if remote_state is not None:
                    self.first_run = False
            except DestinationUndefinedEntity:
                # storage not present. wipe the pipeline if pipeline not new
                # do it only if pipeline has any data
                if self.has_data:
                    should_wipe = False
                    if self.default_schema_name is None:
                        should_wipe = True
                    else:
                        with self._get_destination_clients(self.default_schema)[0] as job_client:
                            # and storage is not initialized
                            should_wipe = not job_client.is_storage_initialized()
                    if should_wipe:
                        # reset pipeline
                        self._wipe_working_folder()
                        state = self._get_state()
                        self._configure(
                            self._schema_storage_config.export_schema_path,
                            self._schema_storage_config.import_schema_path,
                            False,
                        )

            # write the state back
            self._props_to_state(state)
            bump_version_if_modified(state)
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
        """Tells if the pipeline contains any extracted files or pending load packages"""
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
    def last_trace(self) -> PipelineTrace:
        """Returns or loads last trace generated by pipeline. The trace is loaded from standard location."""
        if self._last_trace:
            return self._last_trace
        return load_trace(self.working_dir)

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
    def sync_schema(self, schema_name: str = None, credentials: Any = None) -> TSchemaTables:
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
        client_config = self._get_destination_client_initial_config(credentials)
        with self._get_destination_clients(schema, client_config)[0] as client:
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

    def sql_client(self, schema_name: str = None, credentials: Any = None) -> SqlClientBase[Any]:
        """Returns a sql client configured to query/change the destination and dataset that were used to load the data.
        Use the client with `with` statement to manage opening and closing connection to the destination:
        >>> with pipeline.sql_client() as client:
        >>>     with client.execute_query(
        >>>         "SELECT id, name, email FROM customers WHERE id = %s", 10
        >>>     ) as cursor:
        >>>         print(cursor.fetchall())

        The client is authenticated and defaults all queries to dataset_name used by the pipeline. You can provide alternative
        `schema_name` which will be used to normalize dataset name and alternative `credentials`.
        """
        # if not self.default_schema_name and not schema_name:
        #     raise PipelineConfigMissing(
        #         self.pipeline_name,
        #         "default_schema_name",
        #         "load",
        #         "Sql Client is not available in a pipeline without a default schema. Extract some data first or restore the pipeline from the destination using 'restore_from_destination' flag. There's also `_inject_schema` method for advanced users."
        #     )
        schema = self._get_schema_or_create(schema_name)
        return self._sql_job_client(schema, credentials).sql_client

    def destination_client(self, schema_name: str = None, credentials: Any = None) -> JobClientBase:
        """Get the destination job client for the configured destination
        Use the client with `with` statement to manage opening and closing connection to the destination:
        >>> with pipeline.destination_client() as client:
        >>>     client.drop_storage()  # removes storage which typically wipes all data in it

        The client is authenticated. You can provide alternative `schema_name` which will be used to normalize dataset name and alternative `credentials`.
        If no schema name is provided and no default schema is present in the pipeline, and ad hoc schema will be created and discarded after use.
        """
        schema = self._get_schema_or_create(schema_name)
        client_config = self._get_destination_client_initial_config(credentials)
        return self._get_destination_clients(schema, client_config)[0]

    def _get_schema_or_create(self, schema_name: str = None) -> Schema:
        if schema_name:
            return self.schemas[schema_name]
        if self.default_schema_name:
            return self.default_schema
        with self._maybe_destination_capabilities():
            return Schema(self.pipeline_name)

    def _sql_job_client(self, schema: Schema, credentials: Any = None) -> SqlJobClientBase:
        client_config = self._get_destination_client_initial_config(credentials)
        client = self._get_destination_clients(schema, client_config)[0]
        if isinstance(client, SqlJobClientBase):
            return client
        else:
            raise SqlClientNotAvailable(self.pipeline_name, self.destination.destination_name)

    def _get_normalize_storage(self) -> NormalizeStorage:
        return NormalizeStorage(True, self._normalize_storage_config())

    def _get_load_storage(self) -> LoadStorage:
        caps = self._get_destination_capabilities()
        return LoadStorage(
            True,
            caps.preferred_loader_file_format,
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
        if self.full_refresh:
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
        self, extract: Extract, source: DltSource, max_parallel_items: int, workers: int
    ) -> str:
        # discover the existing pipeline schema
        try:
            # all live schemas are initially committed and during the extract will accumulate changes in memory
            # if schema is committed try to take schema from storage
            if self._schema_storage.is_live_schema_committed(source.schema.name):
                # this will (1) save live schema if modified (2) look for import schema if present
                # (3) load import schema an overwrite pipeline schema if import schema modified
                # (4) load pipeline schema if no import schema is present
                pipeline_schema = self.schemas.load_schema(source.schema.name)
            else:
                # if schema is not committed we know we are in process of extraction
                pipeline_schema = self.schemas[source.schema.name]
            pipeline_schema = pipeline_schema.clone()  # use clone until extraction complete
            # apply all changes in the source schema to pipeline schema
            # NOTE: we do not apply contracts to changes done programmatically
            pipeline_schema.update_schema(source.schema)
            # replace schema in the source
            source.schema = pipeline_schema
        except FileNotFoundError:
            pass

        # extract into pipeline schema
        load_id = extract.extract(source, max_parallel_items, workers)

        # save import with fully discovered schema
        # NOTE: moved to with_schema_sync, remove this if all test pass
        # self._schema_storage.save_import_schema_if_not_exists(source.schema)

        # update live schema but not update the store yet
        self._schema_storage.update_live_schema(source.schema)

        # set as default if this is first schema in pipeline
        if not self.default_schema_name:
            # this performs additional validations as schema contains the naming module
            self._set_default_schema_name(source.schema)

        return load_id

    def _get_destination_client_initial_config(
        self, destination: TDestination = None, credentials: Any = None, as_staging: bool = False
    ) -> DestinationClientConfiguration:
        destination = destination or self.destination
        if not destination:
            raise PipelineConfigMissing(
                self.pipeline_name,
                "destination",
                "load",
                "Please provide `destination` argument to `pipeline`, `run` or `load` method"
                " directly or via .dlt config.toml file or environment variable.",
            )
        # create initial destination client config
        client_spec = destination.spec
        # initialize explicit credentials
        if not as_staging:
            # explicit credentials passed to dlt.pipeline should not be applied to staging
            credentials = credentials or self.credentials
        if credentials is not None and not isinstance(credentials, CredentialsConfiguration):
            # use passed credentials as initial value. initial value may resolve credentials
            credentials = initialize_credentials(
                client_spec.get_resolvable_fields()["credentials"], credentials
            )

        # this client support many schemas and datasets
        if issubclass(client_spec, DestinationClientDwhConfiguration):
            if not self.dataset_name and self.full_refresh:
                logger.warning(
                    "Full refresh may not work if dataset name is not set. Please set the"
                    " dataset_name argument in dlt.pipeline or run method"
                )
            # set default schema name to load all incoming data to a single dataset, no matter what is the current schema name
            default_schema_name = (
                None if self.config.use_single_dataset else self.default_schema_name
            )

            if issubclass(client_spec, DestinationClientStagingConfiguration):
                return client_spec(
                    dataset_name=self.dataset_name,
                    default_schema_name=default_schema_name,
                    credentials=credentials,
                    as_staging=as_staging,
                )
            return client_spec(
                dataset_name=self.dataset_name,
                default_schema_name=default_schema_name,
                credentials=credentials,
            )

        return client_spec(credentials=credentials)

    def _get_destination_clients(
        self,
        schema: Schema,
        initial_config: DestinationClientConfiguration = None,
        initial_staging_config: DestinationClientConfiguration = None,
    ) -> Tuple[JobClientBase, JobClientBase]:
        try:
            # resolve staging config in order to pass it to destination client config
            staging_client = None
            if self.staging:
                if not initial_staging_config:
                    # this is just initial config - without user configuration injected
                    initial_staging_config = self._get_destination_client_initial_config(
                        self.staging, as_staging=True
                    )
                # create the client - that will also resolve the config
                staging_client = self.staging.client(schema, initial_staging_config)
            if not initial_config:
                # config is not provided then get it with injected credentials
                initial_config = self._get_destination_client_initial_config(self.destination)
            # attach the staging client config to destination client config - if its type supports it
            if (
                self.staging
                and isinstance(initial_config, DestinationClientDwhWithStagingConfiguration)
                and isinstance(staging_client.config, DestinationClientStagingConfiguration)
            ):
                initial_config.staging_config = staging_client.config
            # create instance with initial_config properly set
            client = self.destination.client(schema, initial_config)
            return client, staging_client
        except ModuleNotFoundError:
            client_spec = self.destination.spec()
            raise MissingDependencyException(
                f"{client_spec.destination_type} destination",
                [f"{version.DLT_PKG_NAME}[{client_spec.destination_type}]"],
                "Dependencies for specific destinations are available as extras of dlt",
            )

    def _get_destination_capabilities(self) -> DestinationCapabilitiesContext:
        if not self.destination:
            raise PipelineConfigMissing(
                self.pipeline_name,
                "destination",
                "normalize",
                "Please provide `destination` argument to `pipeline`, `run` or `load` method"
                " directly or via .dlt config.toml file or environment variable.",
            )
        return self.destination.capabilities()

    def _get_staging_capabilities(self) -> Optional[DestinationCapabilitiesContext]:
        return self.staging.capabilities() if self.staging is not None else None

    def _validate_pipeline_name(self) -> None:
        try:
            FileStorage.validate_file_name_component(self.pipeline_name)
        except ValueError as ve_ex:
            raise InvalidPipelineName(self.pipeline_name, str(ve_ex))

    def _make_schema_with_default_name(self) -> Schema:
        """Make a schema from the pipeline name using the name normalizer. "_pipeline" suffix is removed if present"""
        if self.pipeline_name.endswith("_pipeline"):
            schema_name = self.pipeline_name[:-9]
        else:
            schema_name = self.pipeline_name
        return Schema(normalize_schema_name(schema_name))

    def _set_context(self, is_active: bool) -> None:
        self.is_active = is_active
        if is_active:
            # set destination context on activation
            if self.destination:
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
    ) -> None:
        # destination_mod = DestinationReference.from_name(destination)
        if destination:
            self.destination = Destination.from_reference(
                destination, destination_name=destination_name
            )

        if (
            self.destination
            and not self.destination.capabilities().supported_loader_file_formats
            and not staging
            and not self.staging
        ):
            logger.warning(
                f"The destination {self.destination.destination_name} requires the filesystem"
                " staging destination to be set, but it was not provided. Setting it to"
                " 'filesystem'."
            )
            staging = "filesystem"
            staging_name = "filesystem"

        if staging:
            staging_module = Destination.from_reference(staging, destination_name=staging_name)
            if staging_module and not issubclass(
                staging_module.spec, DestinationClientStagingConfiguration
            ):
                raise DestinationNoStagingMode(staging_module.destination_name)
            self.staging = staging_module

        with self._maybe_destination_capabilities():
            # default normalizers must match the destination
            self._set_default_normalizers()

    @contextmanager
    def _maybe_destination_capabilities(
        self, loader_file_format: TLoaderFileFormat = None
    ) -> Iterator[DestinationCapabilitiesContext]:
        try:
            caps: DestinationCapabilitiesContext = None
            injected_caps: ContextManager[DestinationCapabilitiesContext] = None
            if self.destination:
                destination_caps = self._get_destination_capabilities()
                stage_caps = self._get_staging_capabilities()
                injected_caps = self._container.injectable_context(destination_caps)
                caps = injected_caps.__enter__()

                caps.preferred_loader_file_format = self._resolve_loader_file_format(
                    self.destination.destination_name,
                    (
                        # DestinationReference.to_name(self.destination),
                        self.staging.destination_name
                        if self.staging
                        else None
                    ),
                    # DestinationReference.to_name(self.staging) if self.staging else None,
                    destination_caps,
                    stage_caps,
                    loader_file_format,
                )
                caps.supported_loader_file_formats = (
                    destination_caps.supported_staging_file_formats if stage_caps else None
                ) or destination_caps.supported_loader_file_formats
            yield caps
        finally:
            if injected_caps:
                injected_caps.__exit__(None, None, None)

    @staticmethod
    def _resolve_loader_file_format(
        destination: str,
        staging: str,
        dest_caps: DestinationCapabilitiesContext,
        stage_caps: DestinationCapabilitiesContext,
        file_format: TLoaderFileFormat,
    ) -> TLoaderFileFormat:
        possible_file_formats = dest_caps.supported_loader_file_formats
        if stage_caps:
            if not dest_caps.supported_staging_file_formats:
                raise DestinationLoadingViaStagingNotSupported(destination)
            possible_file_formats = [
                f
                for f in dest_caps.supported_staging_file_formats
                if f in stage_caps.supported_loader_file_formats
            ]
        if not file_format:
            if not stage_caps:
                if not dest_caps.preferred_loader_file_format:
                    raise DestinationLoadingWithoutStagingNotSupported(destination)
                file_format = dest_caps.preferred_loader_file_format
            elif stage_caps and dest_caps.preferred_staging_file_format in possible_file_formats:
                file_format = dest_caps.preferred_staging_file_format
            else:
                file_format = possible_file_formats[0] if len(possible_file_formats) > 0 else None
        if file_format not in possible_file_formats:
            raise DestinationIncompatibleLoaderFileFormatException(
                destination,
                staging,
                file_format,
                set(possible_file_formats) - INTERNAL_LOADER_FILE_FORMATS,
            )
        return file_format

    def _set_default_normalizers(self) -> None:
        _, self._default_naming, _ = import_normalizers(explicit_normalizers())

    def _set_dataset_name(self, new_dataset_name: str) -> None:
        if not new_dataset_name and not self.dataset_name:
            # dataset name is required but not provided - generate the default now
            destination_needs_dataset = False
            if self.destination:
                fields = self.destination.spec().get_resolvable_fields()
                dataset_name_type = fields.get("dataset_name")
                # if dataset is required (default!) we create a default dataset name
                destination_needs_dataset = dataset_name_type is not None and not is_optional_type(
                    dataset_name_type
                )
            # if destination is not specified - generate dataset
            if not self.destination or destination_needs_dataset:
                new_dataset_name = self.pipeline_name + self.DEFAULT_DATASET_SUFFIX

        if not new_dataset_name:
            return

        # in case of full refresh add unique suffix
        if self.full_refresh:
            # dataset must be specified
            # double _ is not allowed
            if new_dataset_name.endswith("_"):
                new_dataset_name += self._pipeline_instance_id[1:]
            else:
                new_dataset_name += self._pipeline_instance_id
        self.dataset_name = new_dataset_name

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
            return migrate_state(
                self.pipeline_name, state, state["_state_engine_version"], STATE_ENGINE_VERSION
            )
        except FileNotFoundError:
            # do not set the state hash, this will happen on first merge
            return {
                "_state_version": 0,
                "_state_engine_version": STATE_ENGINE_VERSION,
                "_local": {"first_run": True},
            }
            # state["_version_hash"] = generate_version_hash(state)
            # return state

    def _optional_sql_job_client(self, schema_name: str) -> Optional[SqlJobClientBase]:
        try:
            return self._sql_job_client(Schema(schema_name))
        except PipelineConfigMissing as pip_ex:
            # fallback to regular init if destination not configured
            logger.info(f"Sql Client not available: {pip_ex}")
        except SqlClientNotAvailable:
            # fallback is sql client not available for destination
            logger.info("Client not available because destination does not support sql client")
        except ConfigFieldMissingException:
            # probably credentials are missing
            logger.info("Client not available due to missing credentials")
        return None

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
                    state = load_state_from_destination(self.pipeline_name, job_client)
                    if state is None:
                        logger.info(
                            "The state was not found in the destination"
                            f" {self.destination.destination_description}:{dataset_name}"
                        )
                    else:
                        logger.info(
                            "The state was restored from the destination"
                            f" {self.destination.destination_description}:{dataset_name}"
                        )
                else:
                    state = None
                    logger.info(
                        "Destination does not support state sync"
                        f" {self.destination.destination_description}:{dataset_name}"
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
                            f" {self.destination.destination_name}"
                        )
                        return restored_schemas
                    schema_info = job_client.get_stored_schema()
                    if schema_info is None:
                        logger.info(
                            f"The schema {schema.name} was not found in the destination"
                            f" {self.destination.destination_name}:{self.dataset_name}"
                        )
                        # try to import schema
                        with contextlib.suppress(FileNotFoundError):
                            self._schema_storage.load_schema(schema.name)
                    else:
                        schema = Schema.from_dict(json.loads(schema_info.schema))
                        logger.info(
                            f"The schema {schema.name} version {schema.version} hash"
                            f" {schema.stored_version_hash} was restored from the destination"
                            f" {self.destination.destination_name}:{self.dataset_name}"
                        )
                        restored_schemas.append(schema)
        return restored_schemas

    @contextmanager
    def managed_state(self, *, extract_state: bool = False) -> Iterator[TPipelineState]:
        # load or restore state
        state = self._get_state()
        # TODO: we should backup schemas here
        try:
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
        if not self.destination:
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
                if self.destination.destination_type != state_destination:
                    logger.warning(
                        f"The destination {state_destination}:{state.get('destination_name')} in"
                        " state differs from destination"
                        f" {self.destination.destination_type}:{self.destination.destination_name} in"
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
        if self.destination:
            state["destination_type"] = self.destination.destination_type
            state["destination_name"] = self.destination.destination_name
        if self.staging:
            state["staging_type"] = self.staging.destination_type
            state["staging_name"] = self.staging.destination_name
        state["schema_names"] = self._list_schemas_sorted()
        return state

    def _bump_version_and_extract_state(
        self, state: TPipelineState, extract_state: bool, extract: Extract = None
    ) -> None:
        """Merges existing state into `state` and extracts state using `storage` if extract_state is True.

        Storage will be created on demand. In that case the extracted package will be immediately committed.
        """
        _, hash_, _ = bump_version_if_modified(self._props_to_state(state))
        should_extract = hash_ != state["_local"].get("_last_extracted_hash")
        if should_extract and extract_state:
            data = state_resource(state)
            extract_ = extract or Extract(
                self._schema_storage, self._normalize_storage_config(), original_data=data
            )
            self._extract_source(extract_, data_to_sources(data, self)[0], 1, 1)
            state["_local"]["_last_extracted_at"] = pendulum.now()
            state["_local"]["_last_extracted_hash"] = hash_
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
        return {"pipeline_name": self.pipeline_name}
