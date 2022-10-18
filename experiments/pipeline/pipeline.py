import os
from contextlib import contextmanager
from copy import deepcopy
from functools import wraps
from typing import Any, Callable, ClassVar, List, Iterable, Iterator, Generator, Mapping, NewType, Optional, Sequence, Tuple, Type, TypedDict, Union, get_type_hints, overload

from dlt.common import json, logger, signals
from dlt.common.configuration.container import Container
from dlt.common.configuration.specs.config_namespace_context import ConfigNamespacesContext
from dlt.common.runners.runnable import Runnable
from dlt.common.sources import DLT_METADATA_FIELD, TResolvableDataItem, with_table_name
from dlt.common.typing import DictStrAny, StrAny, TFun, TSecretValue, TAny

from dlt.common.runners import pool_runner as runner, TRunMetrics, initialize_runner
from dlt.common.storages import LiveSchemaStorage, NormalizeStorage

from dlt.common.configuration import inject_namespace
from dlt.common.configuration.specs import RunConfiguration, NormalizeVolumeConfiguration, SchemaVolumeConfiguration, LoadVolumeConfiguration, PoolRunnerConfiguration, DestinationCapabilitiesContext
from dlt.common.schema.schema import Schema
from dlt.common.storages.file_storage import FileStorage
from dlt.common.utils import is_interactive
from dlt.extract.extract import ExtractorStorage, extract

from dlt.normalize import Normalize
from dlt.load.client_base import DestinationReference, JobClientBase, SqlClientBase
from dlt.load.configuration import DestinationClientConfiguration, DestinationClientDwhConfiguration, LoaderConfiguration
from dlt.load import Load
from dlt.normalize.configuration import NormalizeConfiguration

from experiments.pipeline.exceptions import PipelineConfigMissing, MissingDependencyException, PipelineStepFailed
from dlt.extract.sources import DltResource, DltSource, TTableSchemaTemplate
from experiments.pipeline.typing import TPipelineStep, TPipelineState
from experiments.pipeline.configuration import StateInjectableContext


class Pipeline:

    STATE_FILE: ClassVar[str] = "state.json"
    STATE_PROPS: ClassVar[List[str]] = list(get_type_hints(TPipelineState).keys())

    pipeline_name: str
    dataset_name: str
    default_schema_name: str
    working_dir: str

    def __init__(self, pipeline_name: str, working_dir: str, pipeline_secret: TSecretValue, destination: DestinationReference, runtime: RunConfiguration):
        self.pipeline_secret = pipeline_secret
        self.runtime_config = runtime
        self.destination = destination
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
        self._configure(pipeline_name, working_dir)

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

    @with_state_sync
    def _configure(self, pipeline_name: str, working_dir: str) -> None:
        self.pipeline_name = pipeline_name
        self.working_dir = working_dir

        # compute the folder that keeps all of the pipeline state
        FileStorage.validate_file_name_component(self.pipeline_name)
        self.root_folder = os.path.join(self.working_dir, self.pipeline_name)
        # create default configs
        # self._pool_config = PoolRunnerConfiguration(is_single_run=True, exit_on_exception=True)
        self._schema_storage_config = SchemaVolumeConfiguration(schema_volume_path = os.path.join(self.root_folder, "schemas"))
        self._normalize_storage_config = NormalizeVolumeConfiguration(normalize_volume_path=os.path.join(self.root_folder, "normalize"))
        self._load_storage_config = LoadVolumeConfiguration(load_volume_path=os.path.join(self.root_folder, "load"),)

        # create pipeline working dir
        self._pipeline_storage = FileStorage(self.root_folder, makedirs=False)

        # restore pipeline if folder exists and contains state
        if self._pipeline_storage.has_file(Pipeline.STATE_FILE):
            self._restore_pipeline()
        else:
            self._create_pipeline()

        # create schema storage
        self._schema_storage = LiveSchemaStorage(self._schema_storage_config, makedirs=True)


    def drop(self) -> "Pipeline":
        """Deletes existing pipeline state, schemas and drops datasets at the destination if present"""
        pass


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
        data: Union[DltSource, DltResource, Iterator[TResolvableDataItem], Iterable[TResolvableDataItem]],
        table_name = None,
        write_disposition = None,
        parent = None,
        columns = None,
        schema: Schema = None,
        *,
        max_parallel_items: int = 100,
        workers: int = 5
    ) -> None:

        def only_data_args(with_schema: bool) -> None:
            if not table_name or not write_disposition or not parent or not columns:
                raise InvalidExtractArguments(with_schema)
            if not with_schema and not schema:
                raise InvalidExtractArguments(with_schema)

        def choose_schema() -> Schema:
            if schema:
                return schema
            if self.default_schema_name:
                return self.default_schema
            return Schema(self.pipeline_name)

        source: DltSource = None

        if isinstance(data, DltSource):
            # already a source
            only_data_args(with_schema=False)
            source = data
        elif isinstance(data, DltResource):
            # package resource in source
            only_data_args(with_schema=True)
            source = DltSource(choose_schema(), [data])
        else:
            table_schema: TTableSchemaTemplate = {
                "name": table_name,
                "parent": parent,
                "write_disposition": write_disposition,
                "columns": columns
            }
            # convert iterable to resource
            data = DltResource.from_data(data, name=table_name, table_schema_template=table_schema)
            # wrap resource in source
            source = DltSource(choose_schema(), [data])

        try:
            self._extract_source(source, max_parallel_items, workers)
        except Exception as exc:
            raise PipelineStepFailed("extract", self.last_run_exception, runner.LAST_RUN_METRICS) from exc


    @with_schemas_sync
    @with_config_namespace(("normalize",))
    def normalize(self, workers: int = 1, dry_run: bool = False) -> None:
        if is_interactive() and workers > 1:
            raise NotImplementedError("Do not use normalize workers in interactive mode ie. in notebook")

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
        # always_drop_dataset = False,
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
            load_storage_config=self._load_storage_config
        )
        load = Load(self.destination, is_storage_owner=False, config=load_config, initial_client_config=client_initial_config)
        self._run_step_in_pool("load", load, load.config)

    @property
    def schemas(self) -> Mapping[str, Schema]:
        return self._schema_storage

    @property
    def default_schema(self) -> Schema:
        return self.schemas[self.default_schema_name]

    @property
    def last_run_exception(self) -> BaseException:
        return runner.LAST_RUN_EXCEPTION

    def _create_pipeline(self) -> None:
        self._pipeline_storage.create_folder(".", exists_ok=True)

    def _restore_pipeline(self) -> None:
        self._restore_state()

    def _restore_state(self) -> None:
        self._state.clear()  # type: ignore
        restored_state: TPipelineState = json.loads(self._pipeline_storage.load(Pipeline.STATE_FILE))
        self._state.update(restored_state)

    def _extract_source(self, source: DltSource, max_parallel_items: int, workers: int) -> None:
        storage = ExtractorStorage(self._normalize_storage_config)

        for _, partials in extract(source, storage, max_parallel_items=max_parallel_items, workers=workers).items():
            for partial in partials:
                source.schema.update_schema(source.schema.normalize_table_identifiers(partial))

        # save schema and set as default if this is first one
        self._schema_storage.save_schema(source.schema)
        if not self.default_schema_name:
            self.default_schema_name = source.schema.name

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

    def _get_destination_client_initial_config(self, credentials: Any) -> DestinationClientConfiguration:
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
