import os
from contextlib import contextmanager
from copy import deepcopy
from functools import wraps
from collections.abc import Sequence as C_Sequence
from typing import Any, Callable, ClassVar, List, Iterable, Iterator, Generator, Mapping, NewType, Optional, Sequence, Tuple, Type, TypedDict, Union, get_type_hints, overload

from dlt.common import json, logger, signals
from dlt.common.configuration.container import Container
from dlt.common.configuration.specs.config_namespace_context import ConfigNamespacesContext
from dlt.common.runners.runnable import Runnable
from dlt.common.schema.typing import TColumnSchema, TWriteDisposition
from dlt.common.source import DLT_METADATA_FIELD, TResolvableDataItem, with_table_name
from dlt.common.storages.load_storage import LoadStorage
from dlt.common.typing import DictStrAny, StrAny, TFun, TSecretValue, TAny

from dlt.common.runners import pool_runner as runner, TRunMetrics, initialize_runner
from dlt.common.storages import LiveSchemaStorage, NormalizeStorage

from dlt.common.configuration import inject_namespace
from dlt.common.configuration.specs import RunConfiguration, NormalizeVolumeConfiguration, SchemaVolumeConfiguration, LoadVolumeConfiguration, PoolRunnerConfiguration
from dlt.common.destination import DestinationCapabilitiesContext, DestinationReference, JobClientBase, DestinationClientConfiguration, DestinationClientDwhConfiguration
from dlt.common.schema import Schema, utils as schema_utils
from dlt.common.storages.file_storage import FileStorage
from dlt.common.utils import is_interactive
from dlt.extract.extract import ExtractorStorage, extract
from dlt.load.job_client_impl import SqlJobClientBase

from dlt.normalize import Normalize
from dlt.load.sql_client import SqlClientBase
from dlt.load.configuration import LoaderConfiguration
from dlt.load import Load
from dlt.normalize.configuration import NormalizeConfiguration

from dlt.pipeline.exceptions import PipelineConfigMissing, MissingDependencyException, PipelineStepFailed, SqlClientNotAvailable
from dlt.extract.sources import DltResource, DltSource, TTableSchemaTemplate
from dlt.pipeline.typing import TPipelineStep, TPipelineState
from dlt.pipeline.configuration import StateInjectableContext


class Pipeline:

    STATE_FILE: ClassVar[str] = "state.json"
    STATE_PROPS: ClassVar[List[str]] = list(get_type_hints(TPipelineState).keys())

    pipeline_name: str
    default_schema_name: str
    always_drop_pipeline: bool
    working_dir: str

    def __init__(
            self,
            pipeline_name: str,
            working_dir: str,
            pipeline_secret: TSecretValue,
            destination: DestinationReference,
            dataset_name: str,
            import_schema_path: str,
            export_schema_path: str,
            always_drop_pipeline: bool,
            runtime: RunConfiguration
        ) -> None:
        self.pipeline_secret = pipeline_secret
        self.runtime_config = runtime
        self.destination = destination
        self.dataset_name = dataset_name
        self.root_folder: str = None

        self._container = Container()
        self._state: TPipelineState = {}  # type: ignore
        self._pipeline_storage: FileStorage = None
        self._schema_storage: LiveSchemaStorage = None
        # self._pool_config: PoolRunnerConfiguration = None
        self._schema_storage_config: SchemaVolumeConfiguration = None
        self._normalize_storage_config: NormalizeVolumeConfiguration = None
        self._load_storage_config: LoadVolumeConfiguration = None

        initialize_runner(self.runtime_config)
        self._configure(pipeline_name, working_dir, import_schema_path, export_schema_path, always_drop_pipeline)

    def with_state_sync(f: TFun) -> TFun:

        @wraps(f)
        def _wrap(self: "Pipeline", *args: Any, **kwargs: Any) -> Any:
            # backup and restore state
            with self._managed_state() as state:
                # add the state to container as a context
                with self._container.injectable_context(StateInjectableContext(state=state)):
                    return f(self, *args, **kwargs)

        return _wrap

    def with_schemas_sync(f: TFun) -> TFun:

        @wraps(f)
        def _wrap(self: "Pipeline", *args: Any, **kwargs: Any) -> Any:
            for name in self._schema_storage.live_schemas:
                # refresh live schemas in storage or import schema path
                self._schema_storage.commit_live_schema(name)
            return f(self, *args, **kwargs)

        return _wrap

    def with_config_namespace(namespaces: Tuple[str, ...]) -> Callable[[TFun], TFun]:

        def decorator(f: TFun) -> TFun:

            @wraps(f)
            def _wrap(self: "Pipeline", *args: Any, **kwargs: Any) -> Any:
                # add namespace context to the container to be used by all configuration without explicit namespaces resolution
                with inject_namespace(ConfigNamespacesContext(pipeline_name=self.pipeline_name, namespaces=namespaces)):
                    return f(self, *args, **kwargs)

            return _wrap

        return decorator


    def drop(self) -> "Pipeline":
        """Deletes existing pipeline state, schemas and drops datasets at the destination if present"""
        # drop the data for all known schemas
        for schema in self._schema_storage:
            with self._get_destination_client(schema) as client:
                client.initialize_storage(wipe_data=True)
        # reset the pipeline working dir
        self._create_pipeline()
        # clone the pipeline
        return Pipeline(
            self.pipeline_name,
            self.working_dir,
            self.pipeline_secret,
            self.destination,
            self.dataset_name,
            self._schema_storage.config.import_schema_path,
            self._schema_storage.config.export_schema_path,
            self.always_drop_pipeline,
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

    @with_schemas_sync
    @with_state_sync
    @with_config_namespace(("extract",))
    def extract(
        self,
        data: Any,
        table_name: str,
        parent_table_name: str = None,
        write_disposition: TWriteDisposition = None,
        columns: Sequence[TColumnSchema] = None,
        schema: Schema = None,
        *,
        max_parallel_items: int = 100,
        workers: int = 5
    ) -> None:


        # def has_hint_args() -> bool:
        #     return table_name or parent_table_name or write_disposition or schema

        def apply_hint_args(resource: DltResource) -> None:
            resource.apply_hints(table_name, parent_table_name, write_disposition, columns)

        def choose_schema() -> Schema:
            if schema:
                return schema
            if self.default_schema_name:
                return self.default_schema
            return Schema(self.pipeline_name)

        # a list of sources or a list of resources may be passed as data
        sources: List[DltSource] = []

        def item_to_source(data_item: Any) -> DltSource:
            if isinstance(data_item, DltSource):
                # if schema is explicit then override source schema
                if schema:
                    data_item.schema = schema
                # try to apply hints to resources
                resources = data_item.resources
                for r in resources:
                    apply_hint_args(r)
                return data_item

            if isinstance(data_item, DltResource):
                # apply hints
                apply_hint_args(data_item)
                # package resource in source
                return DltSource(choose_schema(), [data_item])

            # iterator/iterable/generator
            # create resource first without table template
            resource = DltResource.from_data(data_item, name=table_name)
            # apply hints
            apply_hint_args(resource)
            # wrap resource in source
            return DltSource(choose_schema(), [resource])

        if isinstance(data, C_Sequence) and len(data) > 0:
            # if first element is source or resource
            if isinstance(data[0], DltResource):
                sources.append(item_to_source(DltSource(choose_schema(), data)))
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
                self._extract_source(s, max_parallel_items, workers)
        except Exception as exc:
            raise PipelineStepFailed("extract", self.last_run_exception, runner.LAST_RUN_METRICS) from exc


    @with_schemas_sync
    @with_config_namespace(("normalize",))
    def normalize(self, workers: int = 1, dry_run: bool = False) -> None:
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
            schema_storage_config=self._schema_storage_config,
            normalize_storage_config=self._normalize_storage_config,
            load_storage_config=self._load_storage_config
        )
        # run with destination context
        with self._container.injectable_context(destination_caps):
            # shares schema storage with the pipeline so we do not need to install
            normalize = Normalize(config=normalize_config, schema_storage=self._schema_storage)
            self._run_step_in_pool("normalize", normalize, normalize.config)

    @with_schemas_sync
    @with_state_sync
    @with_config_namespace(("load",))
    def load(
        self,
        destination: DestinationReference = None,
        dataset_name: str = None,
        credentials: Any = None,
        # raise_on_failed_jobs = False,
        # raise_on_incompatible_schema = False,
        always_wipe_storage = False,
        *,
        workers: int = 20
    ) -> None:

        # set destination and default dataset if provided
        self.destination = destination or self.destination
        self.dataset_name = dataset_name or self.dataset_name
        # check if any schema is present, if not then no data was extracted
        if not self.default_schema_name:
            return

        # make sure that destination is set and client is importable and can be instantiated
        client_initial_config = self._get_destination_client_initial_config(credentials)
        self._get_destination_client(self.default_schema, client_initial_config)

        # create initial loader config and the loader
        load_config = LoaderConfiguration(
            is_single_run=True,
            exit_on_exception=True,
            workers=workers,
            always_wipe_storage=always_wipe_storage or self.always_drop_pipeline,
            load_storage_config=self._load_storage_config
        )
        load = Load(self.destination, is_storage_owner=False, config=load_config, initial_client_config=client_initial_config)
        self._run_step_in_pool("load", load, load.config)

    @with_config_namespace(("run",))
    def run(
        self,
        source: Any = None,
        destination: DestinationReference = None,
        dataset_name: str = None,
        table_name: str = None,
        write_disposition: TWriteDisposition = None,
        columns: Sequence[TColumnSchema] = None,
        schema: Schema = None
    ) -> None:
        # set destination and default dataset if provided
        self.destination = destination or self.destination
        self.dataset_name = dataset_name or self.dataset_name
        # normalize and load pending data
        self.normalize()
        self.load(destination, dataset_name)

        # extract from the source
        if source:
            self.extract(source, table_name, write_disposition, None, columns, schema)
            self.normalize()
            self.load(destination, dataset_name)

    @property
    def schemas(self) -> Mapping[str, Schema]:
        return self._schema_storage

    @property
    def default_schema(self) -> Schema:
        return self.schemas[self.default_schema_name]

    @property
    def last_run_exception(self) -> BaseException:
        return runner.LAST_RUN_EXCEPTION

    def list_extracted_resources(self) -> Sequence[str]:
        return self._get_normalize_storage().list_files_to_normalize_sorted()

    def list_normalized_load_packages(self) -> Sequence[str]:
        return self._get_load_storage().load_storage.list_packages()

    def list_completed_load_packages(self) -> Sequence[str]:
        return self._get_load_storage().load_storage.list_completed_packages()

    def list_failed_jobs_in_package(self, load_id: str) -> Sequence[Tuple[str, str]]:
        storage = self._get_load_storage()
        failed_jobs: List[Tuple[str, str]] = []
        for file in storage.load_storage.list_completed_failed_jobs(load_id):
            if not file.endswith(".exception"):
                try:
                    failed_message = storage.storage.load(file + ".exception")
                except FileNotFoundError:
                    failed_message = None
                failed_jobs.append((file, failed_message))
        return failed_jobs

    def sync_schema(self, schema_name: str = None) -> None:
        with self._get_destination_client(self.schemas[schema_name]) as client:
            client.initialize_storage(wipe_data=self.always_drop_pipeline)
            client.update_storage_schema()

    def sql_client(self, schema_name: str = None) -> SqlClientBase[Any]:
        with self._get_destination_client(self.schemas[schema_name]) as client:
            if isinstance(client, SqlJobClientBase):
                return client.sql_client
            else:
                raise SqlClientNotAvailable(self.destination.name())

    def _get_normalize_storage(self) -> NormalizeStorage:
        return NormalizeStorage(True, self._normalize_storage_config)

    def _get_load_storage(self) -> LoadStorage:
        caps = self._get_destination_capabilities()
        return LoadStorage(True, caps.preferred_loader_file_format, caps.supported_loader_file_formats, self._load_storage_config)

    @with_state_sync
    def _configure(self, pipeline_name: str, working_dir: str, import_schema_path: str, export_schema_path: str, always_drop_pipeline: bool) -> None:
        self.pipeline_name = pipeline_name
        self.working_dir = working_dir
        self.always_drop_pipeline = always_drop_pipeline

        # compute the folder that keeps all of the pipeline state
        FileStorage.validate_file_name_component(self.pipeline_name)
        self.root_folder = os.path.join(self.working_dir, self.pipeline_name)
        # create default configs
        # self._pool_config = PoolRunnerConfiguration(is_single_run=True, exit_on_exception=True)
        self._schema_storage_config = SchemaVolumeConfiguration(
            schema_volume_path=os.path.join(self.root_folder, "schemas"),
            import_schema_path=import_schema_path,
            export_schema_path=export_schema_path
        )
        self._normalize_storage_config = NormalizeVolumeConfiguration(normalize_volume_path=os.path.join(self.root_folder, "normalize"))
        self._load_storage_config = LoadVolumeConfiguration(load_volume_path=os.path.join(self.root_folder, "load"),)

        # create pipeline working dir
        self._pipeline_storage = FileStorage(self.root_folder, makedirs=False)

        # restore pipeline if folder exists and contains state
        if self._pipeline_storage.has_file(Pipeline.STATE_FILE) and not always_drop_pipeline:
            self._restore_pipeline()
        else:
            # this will erase the existing working folder
            self._create_pipeline()

        # create schema storage
        self._schema_storage = LiveSchemaStorage(self._schema_storage_config, makedirs=True)

    def _create_pipeline(self) -> None:
        # kill everything inside the working folder
        if self._pipeline_storage.has_folder(""):
            self._pipeline_storage.delete_folder("", recursively=True)
        self._pipeline_storage.create_folder("", exists_ok=False)

    def _restore_pipeline(self) -> None:
        self._restore_state()

    def _restore_state(self) -> None:
        self._state.clear()  # type: ignore
        restored_state: TPipelineState = json.loads(self._pipeline_storage.load(Pipeline.STATE_FILE))
        self._state.update(restored_state)

    def _extract_source(self, source: DltSource, max_parallel_items: int, workers: int) -> None:
        # discover the schema from source
        source_schema = source.discover_schema()

        # iterate over all items in the pipeline and update the schema if dynamic table hints were present
        storage = ExtractorStorage(self._normalize_storage_config)
        for _, partials in extract(source, storage, max_parallel_items=max_parallel_items, workers=workers).items():
            for partial in partials:
                source_schema.update_schema(source_schema.normalize_table_identifiers(partial))

        # if source schema does not exist in the pipeline
        if source_schema.name not in self._schema_storage:
            # possibly initialize the import schema if it is a new schema
            self._schema_storage.initialize_import_if_new(source_schema)
            # save schema into the pipeline
            self._schema_storage.save_schema(source_schema)
            # and set as default if this is first schema in pipeline
            if not self.default_schema_name:
                self.default_schema_name = source_schema.name


    def _run_step_in_pool(self, step: TPipelineStep, runnable: Runnable[Any], config: PoolRunnerConfiguration) -> int:
        try:
            ec = runner.run_pool(config, runnable)
            # in any other case we raise if runner exited with status failed
            if runner.LAST_RUN_METRICS.has_failed:
                raise PipelineStepFailed(step, self.last_run_exception, runner.LAST_RUN_METRICS)
            return ec
        except Exception as r_ex:
            # if EXIT_ON_EXCEPTION flag is set, exception will bubble up directly
            raise PipelineStepFailed(step, self.last_run_exception, runner.LAST_RUN_METRICS) from r_ex
        finally:
            signals.raise_if_signalled()

    def _run_f_in_pool(self, run_f: Callable[..., Any], config: PoolRunnerConfiguration) -> int:
        # internal runners should work in single mode
        self._loader_instance.config.is_single_run = True
        self._loader_instance.config.exit_on_exception = True
        self._normalize_instance.config.is_single_run = True
        self._normalize_instance.config.exit_on_exception = True

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
                "destination",
                "load",
                "Please provide `destination` argument to `config` or `load` method or via pipeline config file or environment var."
            )
        dataset_name = self._get_dataset_name()
        # create initial destination client config
        client_spec = self.destination.spec()
        if issubclass(client_spec, DestinationClientDwhConfiguration):
            # client support schemas and datasets
            return client_spec(dataset_name=dataset_name, default_schema_name=self.default_schema_name, credentials=credentials)
        else:
            return client_spec(credentials=credentials)

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
                [f"python-dlt[{client_spec.destination_name}]"],
                "Dependencies for specific destinations are available as extras of python-dlt"
            )

    def _get_destination_capabilities(self) -> DestinationCapabilitiesContext:
        if not self.destination:
                raise PipelineConfigMissing(
                    "destination",
                    "normalize",
                    "Please provide `destination` argument to `config` or `load` method or via pipeline config file or environment var."
                )
        return self.destination.capabilities()

    def _get_dataset_name(self) -> str:
        return self.dataset_name or self.pipeline_name

    @contextmanager
    def _managed_state(self) -> Iterator[TPipelineState]:
        # write props to pipeline variables
        for prop in Pipeline.STATE_PROPS:
            setattr(self, prop, self._state.get(prop))
        # backup the state
        backup_state = deepcopy(self._state)
        try:
            yield self._state
        except Exception:
            # restore old state
            self._state.clear()  # type: ignore
            self._state.update(backup_state)
            raise
        else:
            # update state props
            for prop in Pipeline.STATE_PROPS:
                self._state[prop] = getattr(self, prop)
            # compare backup and new state, save only if different
            new_state = json.dumps(self._state)
            old_state = json.dumps(backup_state)
            # persist old state
            if new_state != old_state:
                self._pipeline_storage.save(Pipeline.STATE_FILE, new_state)

    @property
    def has_pending_loads(self) -> bool:
        # TODO: check if has pending normalizer and loader data
        pass
