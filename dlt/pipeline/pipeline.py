import contextlib
import os
from contextlib import contextmanager
from functools import wraps
from collections.abc import Sequence as C_Sequence
from typing import Any, Callable, ClassVar, Dict, List, Iterator, Mapping, Optional, Sequence, Tuple, get_type_hints, overload

from dlt.common import json, logger, signals, pendulum
from dlt.common.configuration.container import Container
from dlt.common.configuration.exceptions import ConfigFieldMissingException
from dlt.common.configuration.specs.config_namespace_context import ConfigNamespacesContext
from dlt.common.exceptions import MissingDependencyException
from dlt.common.runners.runnable import Runnable
from dlt.common.schema.exceptions import InvalidDatasetName
from dlt.common.schema.typing import TColumnSchema, TWriteDisposition
from dlt.common.schema.utils import default_normalizers, import_normalizers
from dlt.common.storages.load_storage import LoadStorage
from dlt.common.typing import TFun, TSecretValue

from dlt.common.runners import pool_runner as runner, TRunMetrics, initialize_runner
from dlt.common.storages import LiveSchemaStorage, NormalizeStorage

from dlt.common.configuration import inject_namespace
from dlt.common.configuration.specs import RunConfiguration, NormalizeVolumeConfiguration, SchemaVolumeConfiguration, LoadVolumeConfiguration, PoolRunnerConfiguration
from dlt.common.destination import DestinationCapabilitiesContext, DestinationReference, JobClientBase, DestinationClientConfiguration, DestinationClientDwhConfiguration, TDestinationReferenceArg
from dlt.common.pipeline import LoadInfo, TPipelineState
from dlt.common.schema import Schema
from dlt.common.storages.file_storage import FileStorage
from dlt.common.utils import is_interactive

from dlt.extract.exceptions import SourceExhausted
from dlt.extract.extract import ExtractorStorage, extract
from dlt.extract.source import DltResource, DltSource
from dlt.normalize import Normalize
from dlt.normalize.configuration import NormalizeConfiguration
from dlt.destinations.sql_client import SqlClientBase
from dlt.destinations.job_client_impl import SqlJobClientBase
from dlt.load.configuration import LoaderConfiguration
from dlt.load import Load

from dlt.pipeline.exceptions import CannotRestorePipelineException, InvalidPipelineName, PipelineConfigMissing, PipelineStepFailed, SqlClientNotAvailable
from dlt.pipeline.typing import TPipelineStep
from dlt.pipeline.state import STATE_ENGINE_VERSION, load_state_from_destination, merge_state_if_changed, state_resource, StateInjectableContext


def with_state_sync(extract_state: bool = False) -> Callable[[TFun], TFun]:

    def decorator(f: TFun) -> TFun:
        @wraps(f)
        def _wrap(self: "Pipeline", *args: Any, **kwargs: Any) -> Any:
            # backup and restore state
            with self._managed_state(extract_state=extract_state) as state:
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
        return f(self, *args, **kwargs)

    return _wrap  # type: ignore

def with_config_namespace(namespaces: Tuple[str, ...]) -> Callable[[TFun], TFun]:

    def decorator(f: TFun) -> TFun:

        @wraps(f)
        def _wrap(self: "Pipeline", *args: Any, **kwargs: Any) -> Any:
            # add namespace context to the container to be used by all configuration without explicit namespaces resolution
            with inject_namespace(ConfigNamespacesContext(pipeline_name=self.pipeline_name, namespaces=namespaces)):
                return f(self, *args, **kwargs)

        return _wrap  # type: ignore

    return decorator


class Pipeline:

    STATE_FILE: ClassVar[str] = "state.json"
    STATE_PROPS: ClassVar[List[str]] = list(get_type_hints(TPipelineState).keys())

    pipeline_name: str
    default_schema_name: str = None
    schema_names: List[str] = []
    full_refresh: bool
    working_dir: str
    pipeline_root: str
    destination: DestinationReference = None
    dataset_name: str = None
    credentials: Any = None

    def __init__(
            self,
            pipeline_name: str,
            working_dir: str,
            pipeline_salt: TSecretValue,
            destination: DestinationReference,
            dataset_name: str,
            credentials: Any,
            import_schema_path: str,
            export_schema_path: str,
            full_refresh: bool,
            must_attach_to_local_pipeline: bool,
            restore_from_destination: bool,
            runtime: RunConfiguration
        ) -> None:
        self.pipeline_salt = pipeline_salt
        self.runtime_config = runtime
        self.full_refresh = full_refresh

        self._container = Container()
        # TODO: pass default normalizers as context or as config with defaults
        self._default_naming, _ = import_normalizers(default_normalizers())
        self._pipeline_instance_id = pendulum.now().format("_YYYYMMDDhhmmss")
        self._pipeline_storage: FileStorage = None
        self._schema_storage: LiveSchemaStorage = None
        self._schema_storage_config: SchemaVolumeConfiguration = None
        self._normalize_storage_config: NormalizeVolumeConfiguration = None
        self._load_storage_config: LoadVolumeConfiguration = None

        initialize_runner(self.runtime_config)
        # initialize pipeline working dir
        self._init_working_dir(pipeline_name, working_dir)

        # set destination and dataset, those may be overwritten by state but we'll need them to eventually restore state
        self.destination = destination or self.destination  # changing the destination could be dangerous if pipeline has not loaded items
        self._set_dataset_name(dataset_name)
        self.credentials = credentials

        # do not restore from destination if full refresh or must must attach to local working folder
        restore_from_destination = not must_attach_to_local_pipeline and not full_refresh and restore_from_destination
        # require dataset name to be present if restoring
        if restore_from_destination and not dataset_name:
            raise PipelineConfigMissing(self.pipeline_name, "dataset_name", "init", "When requesting the pipeline to be restored from the destination, 'dataset_name` must be passed explicitly or configured via config providers.")
        with self._managed_state(restore_from_destination=restore_from_destination) as state:
            # see if state didn't change the pipeline name
            if "pipeline_name" in state and pipeline_name != state["pipeline_name"]:
                raise CannotRestorePipelineException(pipeline_name, working_dir, f"working directory contains state for pipeline with name {self.pipeline_name}")
            # at this moment state is recovered so we overwrite the state with the values from init
            self.destination = destination or self.destination  # changing the destination could be dangerous if pipeline has not loaded items
            self._set_dataset_name(dataset_name)
            self._configure(import_schema_path, export_schema_path, must_attach_to_local_pipeline)
            self._restore_schemas_from_destination()

    def drop(self) -> "Pipeline":
        """Deletes existing pipeline state, schemas and drops datasets at the destination if present"""
        # if self.destination:
        #     # drop the data for all known schemas
        #     for schema in self._schema_storage:
        #         with self._get_destination_client(self._schema_storage.load_schema(schema)) as client:
        #             client.initialize_storage(wipe_data=True)
        # reset the pipeline working dir
        self._create_pipeline()
        # clone the pipeline
        return Pipeline(
            self.pipeline_name,
            self.working_dir,
            self.pipeline_salt,
            self.destination,
            self.dataset_name,
            self.credentials,
            self._schema_storage.config.import_schema_path,
            self._schema_storage.config.export_schema_path,
            self.full_refresh,
            False,
            False,
            self.runtime_config
        )


    # @overload
    # def extract(
    #     self,
    #     data: Union[Iterator[TResolvableDataItem], Iterable[TResolvableDataItem]],
    #     table_name = None,
    #     write_disposition = None,
    #     parent = None,
    #     columns = None,
    #     max_parallel_data_items: int =  20,
    #     schema: Schema = None
    # ) -> None:
    #     ...

    # @overload
    # def extract(
    #     self,
    #     data: DltSource,
    #     max_parallel_iterators: int = 1,
    #     max_parallel_data_items: int =  20,
    #     schema: Schema = None
    # ) -> None:
    #     ...

    @with_schemas_sync  # this must precede with_state_sync
    @with_state_sync(extract_state=True)  # extract pipeline state with the package
    @with_config_namespace(("extract",))
    def extract(
        self,
        data: Any,
        *,
        table_name: str = None,
        parent_table_name: str = None,
        write_disposition: TWriteDisposition = None,
        columns: Sequence[TColumnSchema] = None,
        schema: Schema = None,
        max_parallel_items: int = 100,
        workers: int = 5
    ) -> None:

        def apply_hint_args(resource: DltResource) -> None:
            columns_dict = None
            if columns:
                columns_dict = {c["name"]:c for c in columns}
            # apply hints only if any of the hints is present, table_name must be always present
            if table_name or parent_table_name or write_disposition or columns:
                resource.apply_hints(table_name or resource.name, parent_table_name, write_disposition, columns_dict)

        def choose_schema() -> Schema:
            if schema:
                return schema
            if self.default_schema_name:
                return self.default_schema
            return self._make_schema_with_default_name()

        effective_schema = choose_schema()

        # a list of sources or a list of resources may be passed as data
        sources: List[DltSource] = []

        def item_to_source(data_item: Any) -> DltSource:
            if isinstance(data_item, DltSource):
                # if schema is explicit then override source schema
                if schema:
                    data_item.schema = schema
                # try to apply hints to resources
                resources = data_item.resources.values()
                for r in resources:
                    apply_hint_args(r)
                return data_item

            if isinstance(data_item, DltResource):
                # apply hints
                apply_hint_args(data_item)
                # package resource in source
                return DltSource(effective_schema.name, effective_schema, [data_item])

            # iterator/iterable/generator
            # create resource first without table template
            resource = DltResource.from_data(data_item, name=table_name)
            # apply hints
            apply_hint_args(resource)
            # wrap resource in source
            return DltSource(effective_schema.name, effective_schema, [resource])

        if isinstance(data, C_Sequence) and len(data) > 0:
            # if first element is source or resource
            if isinstance(data[0], DltResource):
                sources.append(item_to_source(DltSource(effective_schema.name, effective_schema, data)))
            elif isinstance(data[0], DltSource):
                for s in data:
                    sources.append(item_to_source(s))
            else:
                sources.append(item_to_source(data))
        else:
            sources.append(item_to_source(data))

        try:
            # extract all sources
            for s in sources:
                if s.exhausted:
                    raise SourceExhausted(s.name)
                self._extract_source(s, max_parallel_items, workers)
        except Exception as exc:
            # TODO: provide metrics from extractor
            raise PipelineStepFailed(self.pipeline_name, "extract", exc, runner.LAST_RUN_METRICS) from exc


    @with_schemas_sync
    @with_config_namespace(("normalize",))
    def normalize(self, workers: int = 1) -> None:
        if is_interactive() and workers > 1:
            raise NotImplementedError("Do not use normalize workers in interactive mode ie. in notebook")
        # check if any schema is present, if not then no data was extracted
        if not self.default_schema_name:
            return

        # get destination capabilities
        destination_caps = self._get_destination_capabilities()
        # create default normalize config
        normalize_config = NormalizeConfiguration(
            is_single_run=True,
            exit_on_exception=True,
            workers=workers,
            pool_type="none" if workers == 1 else "process",
            _schema_storage_config=self._schema_storage_config,
            _normalize_storage_config=self._normalize_storage_config,
            _load_storage_config=self._load_storage_config
        )
        # run with destination context
        with self._container.injectable_context(destination_caps):
            # shares schema storage with the pipeline so we do not need to install
            normalize = Normalize(config=normalize_config, schema_storage=self._schema_storage)
            self._run_step_in_pool("normalize", normalize, normalize.config)

    @with_state_sync()
    @with_schemas_sync
    @with_config_namespace(("load",))
    def load(
        self,
        destination: DestinationReference = None,
        dataset_name: str = None,
        credentials: Any = None,
        # raise_on_failed_jobs = False,
        # raise_on_incompatible_schema = False,
        # always_wipe_storage: bool = False,
        *,
        workers: int = 20
    ) -> LoadInfo:

        # if destination is a string then resolve it
        if isinstance(destination, str):
            destination = DestinationReference.from_name(destination)

        # set destination and default dataset if provided
        self.destination = destination or self.destination
        self._set_dataset_name(dataset_name)
        self.credentials = credentials
        # check if any schema is present, if not then no data was extracted
        if not self.default_schema_name:
            return None

        # make sure that destination is set and client is importable and can be instantiated
        client_initial_config = self._get_destination_client_initial_config()
        self._get_destination_client(self.default_schema, client_initial_config)

        # create initial loader config and the loader
        load_config = LoaderConfiguration(
            is_single_run=True,
            exit_on_exception=True,
            workers=workers,
            always_wipe_storage=False,
            _load_storage_config=self._load_storage_config
        )
        load = Load(self.destination, is_storage_owner=False, config=load_config, initial_client_config=client_initial_config)
        try:
            self._run_step_in_pool("load", load, load.config)
            return self._get_load_info(load)
        except PipelineStepFailed as pipex:
            pipex.step_info = self._get_load_info(load)
            raise

    @with_config_namespace(("run",))
    def run(
        self,
        data: Any = None,
        *,
        destination: TDestinationReferenceArg = None,
        dataset_name: str = None,
        credentials: Any = None,
        table_name: str = None,
        write_disposition: TWriteDisposition = None,
        columns: Sequence[TColumnSchema] = None,
        schema: Schema = None
    ) -> LoadInfo:
        # set destination and default dataset if provided
        destination = DestinationReference.from_name(destination)
        self.destination = destination or self.destination
        self._set_dataset_name(dataset_name)

        # normalize and load pending data
        self.normalize()
        info = self.load(destination, dataset_name, credentials=credentials)

        # extract from the source
        if data:
            self.extract(data, table_name=table_name, write_disposition=write_disposition, columns=columns, schema=schema)
            self.normalize()
            return self.load(destination, dataset_name, credentials=credentials)
        else:
            return info

    @property
    def schemas(self) -> Mapping[str, Schema]:
        return self._schema_storage

    @property
    def default_schema(self) -> Schema:
        return self.schemas[self.default_schema_name]

    @property
    def state(self) -> TPipelineState:
        return self._get_state()

    @property
    def last_run_exception(self) -> BaseException:
        return runner.LAST_RUN_EXCEPTION

    def list_extracted_resources(self) -> Sequence[str]:
        return self._get_normalize_storage().list_files_to_normalize_sorted()

    def list_normalized_load_packages(self) -> Sequence[str]:
        return self._get_load_storage().list_packages()

    def list_completed_load_packages(self) -> Sequence[str]:
        return self._get_load_storage().list_completed_packages()

    def list_failed_jobs_in_package(self, load_id: str) -> Sequence[Tuple[str, str]]:
        storage = self._get_load_storage()
        failed_jobs: List[Tuple[str, str]] = []
        for file in storage.list_completed_failed_jobs(load_id):
            if not file.endswith(".exception"):
                try:
                    failed_message = storage.storage.load(file + ".exception")
                except FileNotFoundError:
                    failed_message = None
                failed_jobs.append((storage.storage.make_full_path(file), failed_message))
        return failed_jobs

    def sync_schema(self, schema_name: str = None, credentials: Any = None) -> None:
        schema = self.schemas[schema_name] if schema_name else self.default_schema
        with self._get_destination_client(schema, self._get_destination_client_initial_config(credentials)) as client:
            client.initialize_storage()
            client.update_storage_schema()

    def sql_client(self, schema_name: str = None, credentials: Any = None) -> SqlClientBase[Any]:
        if not self.default_schema_name:
            raise PipelineConfigMissing(self.pipeline_name, "default_schema_name", "load", "Sql Client is not available in a pipeline without a default schema. Extract some data first or restore the pipeline from the destination using 'restore_from_destination' flag. There's also `_inject_schema` method for advanced users.")
        schema = self.schemas[schema_name] if schema_name else self.default_schema
        return self._sql_job_client(schema, credentials).sql_client

    def _sql_job_client(self, schema: Schema, credentials: Any = None) -> SqlJobClientBase:
        client = self._get_destination_client(schema, self._get_destination_client_initial_config(credentials))
        if isinstance(client, SqlJobClientBase):
            return client
        else:
            raise SqlClientNotAvailable(self.pipeline_name, self.destination.__name__)

    def _get_normalize_storage(self) -> NormalizeStorage:
        return NormalizeStorage(True, self._normalize_storage_config)

    def _get_load_storage(self) -> LoadStorage:
        caps = self._get_destination_capabilities()
        return LoadStorage(True, caps.preferred_loader_file_format, caps.supported_loader_file_formats, self._load_storage_config)

    def _init_working_dir(self, pipeline_name: str, working_dir: str) -> None:
        self.pipeline_name = pipeline_name
        self.working_dir = working_dir
        self._validate_pipeline_name()
        # compute the folder that keeps all of the pipeline state
        self.pipeline_root = os.path.join(working_dir, pipeline_name)
        # create pipeline storage, do not create working dir yet
        self._pipeline_storage = FileStorage(self.pipeline_root, makedirs=False)
        # if full refresh was requested, wipe out all data from working folder, if exists
        if self.full_refresh:
            self._wipe_working_folder()

    def _configure(self, import_schema_path: str, export_schema_path: str, must_attach_to_local_pipeline: bool) -> None:
        # create schema storage and folders
        self._schema_storage_config = SchemaVolumeConfiguration(
            schema_volume_path=os.path.join(self.pipeline_root, "schemas"),
            import_schema_path=import_schema_path,
            export_schema_path=export_schema_path
        )
        # create default configs
        self._normalize_storage_config = NormalizeVolumeConfiguration(normalize_volume_path=os.path.join(self.pipeline_root, "normalize"))
        self._load_storage_config = LoadVolumeConfiguration(load_volume_path=os.path.join(self.pipeline_root, "load"),)

        # are we running again?
        has_state = self._pipeline_storage.has_file(Pipeline.STATE_FILE)
        if must_attach_to_local_pipeline and not has_state:
            raise CannotRestorePipelineException(self.pipeline_name, self.working_dir, f"the pipeline was not found in {self.pipeline_root}.")

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

    def _wipe_working_folder(self) -> None:
        # kill everything inside the working folder
        if self._pipeline_storage.has_folder(""):
            self._pipeline_storage.delete_folder("", recursively=True)

    def _attach_pipeline(self) -> None:
        pass

    def _extract_source(self, source: DltSource, max_parallel_items: int, workers: int) -> None:
        # discover the schema from source
        source_schema = source.discover_schema()
        pipeline_schema: Schema = None
        with contextlib.suppress(FileNotFoundError):
            pipeline_schema = self._schema_storage.load_schema(source_schema.name)
        should_initialize_import = False

        # if source schema does not exist in the pipeline
        if source_schema.name not in self._schema_storage:
            # possibly initialize the import schema if it is a new schema
            should_initialize_import = True
            # save schema into the pipeline
            self._schema_storage.save_schema(source_schema)
        pipeline_schema = self._schema_storage[source_schema.name]

        # and set as default if this is first schema in pipeline
        if not self.default_schema_name:
            # this performs additional validations as schema contains the naming module
            self._set_default_schema_name(source_schema)

        # get the current schema and merge tables from source_schema, we'll not merge the high level props
        for table in source_schema.all_tables():
            pipeline_schema.update_schema(pipeline_schema.normalize_table_identifiers(table))

        self._iterate_source(source, pipeline_schema, max_parallel_items, workers)

        # initialize import with fully discovered schema
        if should_initialize_import:
            self._schema_storage.initialize_import_schema(pipeline_schema)

    def _iterate_source(self, source: DltSource, pipeline_schema: Schema, max_parallel_items: int, workers: int) -> None:
        # TODO: create source context
        # iterate over all items in the pipeline and update the schema if dynamic table hints were present
        storage = ExtractorStorage(self._normalize_storage_config)
        # inject the config namespace with the current source name
        with inject_namespace(ConfigNamespacesContext(namespaces=("sources", source.name))):
            extractor = extract(source, storage, max_parallel_items=max_parallel_items, workers=workers)
            # source iterates
            source.exhausted = True
            for _, partials in extractor.items():
                for partial in partials:
                    pipeline_schema.update_schema(pipeline_schema.normalize_table_identifiers(partial))

    def _run_step_in_pool(self, step: TPipelineStep, runnable: Runnable[Any], config: PoolRunnerConfiguration) -> int:
        try:
            ec = runner.run_pool(config, runnable)
            # in any other case we raise if runner exited with status failed
            if runner.LAST_RUN_METRICS.has_failed:
                raise PipelineStepFailed(self.pipeline_name, step, self.last_run_exception, runner.LAST_RUN_METRICS)
            return ec
        except Exception as r_ex:
            # if EXIT_ON_EXCEPTION flag is set, exception will bubble up directly
            raise PipelineStepFailed(self.pipeline_name, step, self.last_run_exception, runner.LAST_RUN_METRICS) from r_ex
        finally:
            signals.raise_if_signalled()

    def _run_f_in_pool(self, run_f: Callable[..., Any], config: PoolRunnerConfiguration) -> int:

        def _run(_: Any) -> TRunMetrics:
            rv = run_f()
            if isinstance(rv, TRunMetrics):
                return rv
            if isinstance(rv, int):
                pending = rv
            else:
                pending = 1
            return TRunMetrics(False, False, int(pending))

        # run the fun
        ec = runner.run_pool(config, _run)
        # ec > 0 - signalled
        # -1 - runner was not able to start

        if runner.LAST_RUN_METRICS is not None and runner.LAST_RUN_METRICS.has_failed:
            raise self.last_run_exception
        return ec

    def _get_destination_client_initial_config(self, credentials: Any = None) -> DestinationClientConfiguration:
        if not self.destination:
            raise PipelineConfigMissing(
                self.pipeline_name,
                "destination",
                "load",
                "Please provide `destination` argument to `pipeline`, `run` or `load` method directly or via .dlt config.toml file or environment variable."
            )
        # create initial destination client config
        client_spec = self.destination.spec()
        if issubclass(client_spec, DestinationClientDwhConfiguration):
            # client support schemas and datasets
            return client_spec(dataset_name=self.dataset_name, default_schema_name=self.default_schema_name, credentials=credentials or self.credentials)
        else:
            return client_spec(credentials=credentials or self.credentials)

    def _get_destination_client(self, schema: Schema, initial_config: DestinationClientConfiguration = None) -> JobClientBase:
        try:
            # config is not provided then get it with injected credentials
            if not initial_config:
                initial_config = self._get_destination_client_initial_config()
            return self.destination.client(schema, initial_config)
        except ImportError:
            client_spec = self.destination.spec()
            raise MissingDependencyException(
                f"{client_spec.destination_name} destination",
                [f"{logger.DLT_PKG_NAME}[{client_spec.destination_name}]"],
                "Dependencies for specific destinations are available as extras of python-dlt"
            )

    def _get_destination_capabilities(self) -> DestinationCapabilitiesContext:
        if not self.destination:
                raise PipelineConfigMissing(
                    self.pipeline_name,
                    "destination",
                    "normalize",
                    "Please provide `destination` argument to `pipeline`, `run` or `load` method directly or via .dlt config.toml file or environment variable."
                )
        return self.destination.capabilities()

    def _validate_pipeline_name(self) -> None:
        try:
            FileStorage.validate_file_name_component(self.pipeline_name)
        except ValueError as ve_ex:
            raise InvalidPipelineName(self.pipeline_name, str(ve_ex))

    def _make_schema_with_default_name(self) -> Schema:
        # make a schema from the pipeline name using the name normalizer
        return Schema(self.pipeline_name, normalize_name=True)

    def _validate_dataset_name(self, dataset_name: str) -> None:
        normalized_name = self._default_naming.normalize_schema_name(dataset_name)
        if normalized_name != dataset_name:
            raise InvalidDatasetName(dataset_name, normalized_name)

    def _set_dataset_name(self, dataset_name: str) -> None:
        if not dataset_name:
            if not self.dataset_name:
                # set default dataset name from pipeline name
                dataset_name = self._default_naming.normalize_schema_name(self.pipeline_name)
            else:
                return

        # in case of full refresh add unique suffix
        if self.full_refresh:
            dataset_name += self._pipeline_instance_id
        self._validate_dataset_name(dataset_name)
        self.dataset_name = dataset_name

    def _set_default_schema_name(self, schema: Schema) -> None:
        assert self.default_schema_name is None
        self.default_schema_name = schema.name

    @with_state_sync()
    def _inject_schema(self, schema: Schema) -> None:
        """Injects a schema into the pipeline. Existing schema will be overwritten"""
        self._schema_storage.save_schema(schema)
        if not self.default_schema_name:
            self._set_default_schema_name(schema)

    def _get_load_info(self, load: Load) -> LoadInfo:
        # TODO: Load must provide a clear interface to get last loads and metrics
        # TODO: LoadInfo must hold many packages with different schemas and datasets
        load_ids = load._processed_load_ids
        failed_packages: Dict[str, Sequence[Tuple[str, str]]] = {}
        for load_id, metrics in load_ids.items():
            if metrics:
                # get failed packages only when package finished
                failed_packages[load_id] = self.list_failed_jobs_in_package(load_id)
        dataset_name = None
        if isinstance(load.initial_client_config, DestinationClientDwhConfiguration):
            dataset_name = load.initial_client_config.dataset_name

        return LoadInfo(
            self,
            load.initial_client_config.destination_name,
            str(load.initial_client_config.credentials),
            dataset_name,
            {load_id: bool(metrics) for load_id, metrics in load_ids.items()},
            failed_packages
        )

    def _get_state(self, restore_from_destination: bool = False) -> TPipelineState:
        try:
            state: TPipelineState = json.loads(self._pipeline_storage.load(Pipeline.STATE_FILE))
        except FileNotFoundError:
            if restore_from_destination:
                state = self._restore_state_from_destination()
            else:
                state = None

        # initial state
        if not state:
            state = {
                "_state_version": 0,
                "_state_engine_version": STATE_ENGINE_VERSION
            }
        return state

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
        try:
            # patch the default schema name so we load exactly in the dataset
            assert self.default_schema_name is None
            self.default_schema_name = dataset_name
            job_client = self._optional_sql_job_client(dataset_name)
            if job_client:
                with job_client.sql_client as sql_client:
                    state = load_state_from_destination(self.pipeline_name, sql_client)
                    if state is None:
                        logger.info(f"The state was not found in the destination {self.destination.__name__}:{dataset_name}")
                    else:
                        logger.info(f"The state was restored from the destination {self.destination.__name__}:{dataset_name}")
                    return state
            else:
                return None
        finally:
            self.default_schema_name = None

    def _restore_schemas_from_destination(self) -> None:
        # check which schemas are present in the pipeline and restore missing schemas
        # existing_schemas = set(self._schema_storage.list_schemas())
        for schema_name in self.schema_names:
            if not self._schema_storage.has_schema(schema_name):
                # assume client always exist
                with self._optional_sql_job_client(schema_name) as job_client:
                    schema_info = job_client.get_newest_schema_from_storage()
                    if schema_info is None:
                        logger.info(f"The schema {schema_name} was not found in the destination {self.destination.__name__}:{job_client.sql_client.default_dataset_name}. Default schema was placed instead")
                        # try to import schema
                        try:
                            self._schema_storage.load_schema(schema_name)
                        except FileNotFoundError:
                            self._schema_storage.save_schema(job_client.schema)
                    else:
                        logger.info(f"The schema {schema_name} was restored from the destination {self.destination.__name__}:{job_client.sql_client.default_dataset_name}")
                        schema = Schema.from_dict(json.loads(schema_info.schema))
                        self._schema_storage.save_schema(schema)

    @contextmanager
    def _managed_state(self, *, restore_from_destination: bool = False, extract_state: bool = False) -> Iterator[TPipelineState]:
        # load current state
        state = self._get_state(restore_from_destination)

        # write props to pipeline variables
        for prop in Pipeline.STATE_PROPS:
            if prop in state and not prop.startswith("_"):
                setattr(self, prop, state.get(prop))
        if "destination" in state:
            self.destination = DestinationReference.from_name(self.destination)

        try:
            yield state
        except Exception:
            # currently do nothing - state is not preserved in memory, only saved
            raise
        else:
            # update state props
            for prop in Pipeline.STATE_PROPS:
                if not prop.startswith("_"):
                    state[prop] = getattr(self, prop)  # type: ignore
            if self.destination:
                state["destination"] = self.destination.__name__
            state["schema_names"] = self._schema_storage.list_schemas()

            backup_state = self._get_state()
            merged_state = merge_state_if_changed(backup_state, state)
            # state was changed or current state was not yet extracted and there's request to extract state
            if (merged_state or "_last_extracted_at" not in state) and extract_state:
                merged_state = merged_state or state
                merged_state["_last_extracted_at"] = pendulum.now()
                # this will extract the state into current load package and update the schema with the _dlt_pipeline_state table
                # note: the schema will be persisted because the schema saving decorator is over the state manager decorator for extract
                state_source = DltSource("pipeline_state", self.default_schema, [state_resource(merged_state)])
                self._iterate_source(state_source, self.default_schema, 1, 1)

            if not extract_state and merged_state:
                merged_state.pop("_last_extracted_at", None)

            # if state was changed, save it
            if merged_state:
                self._pipeline_storage.save(Pipeline.STATE_FILE, json.dumps(merged_state))
