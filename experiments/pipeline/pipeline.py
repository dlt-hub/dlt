import os
import tempfile
from contextlib import contextmanager
from copy import deepcopy
from functools import wraps
from typing import Any, Dict, Iterable, Iterator, Mapping, NewType, Optional, Type, TypedDict, Union, overload
from operator import itemgetter
from prometheus_client import REGISTRY

from dlt.common import json
from dlt.common.configuration.schema_volume_configuration import SchemaVolumeConfiguration

from dlt.common.runners import pool_runner as runner, TRunArgs, TRunMetrics
from dlt.common.dataset_writers import TLoaderFileFormat
from dlt.common.schema.utils import normalize_schema_name
from dlt.common.storages.normalize_storage import NormalizeStorage
from dlt.common.storages.schema_storage import SchemaStorage
from dlt.common.typing import DictStrAny, StrAny, TFun, TSecretValue
from dlt.common.configuration import RunConfiguration, NormalizeVolumeConfiguration, ProductionNormalizeVolumeConfiguration
from dlt.common.schema.schema import Schema
from dlt.common.file_storage import FileStorage
from dlt.common.utils import is_interactive, uniq_id

from dlt.extractors.extractor_storage import ExtractorStorageBase
from dlt.load.typing import TLoaderCapabilities
from dlt.normalize.configuration import NormalizeConfiguration, configuration as normalize_configuration
from dlt.normalize import Normalize
from dlt.load.client_base import SqlClientBase, SqlJobClientBase
from dlt.load.configuration import LoaderConfiguration, configuration as loader_configuration
from dlt.load import loader

from experiments.pipeline.configuration import get_config
from experiments.pipeline.exceptions import InvalidPipelineContextException, PipelineConfigMissing, PipelineConfiguredException, MissingDependencyException, PipelineStepFailed
from experiments.pipeline.sources import SourceTables, TDataItem


TConnectionString = NewType("TConnectionString",  str)
TSourceState = NewType("TSourceState", DictStrAny)

TCredentials = Union[TConnectionString, StrAny]

class TPipelineState(TypedDict):
    pipeline_name: str
    default_dataset_name: str
    # is_transient: bool
    default_schema_name: Optional[str]
    # pipeline_secret: TSecretValue
    destination_name: Optional[str]
    # schema_sync_path: Optional[str]


# class TPipelineState()
#     sources: Dict[str, TSourceState]


class PipelineConfiguration(RunConfiguration):
    WORKING_DIR: Optional[str] = None
    PIPELINE_SECRET: Optional[TSecretValue] = None
    OVERWRITE_EXISTING: bool = False
    # LOADER_TYPE: Optional[str] = None
    # SYNC_SCHEMA_DIR: Optional[str] = None

    @classmethod
    def check_integrity(cls) -> None:
        if cls.PIPELINE_SECRET:
            cls.PIPELINE_SECRET = uniq_id()


class Pipeline:

    ACTIVE_INSTANCE: "Pipeline" = None

    def __new__(cls: Type["Pipeline"]) -> "Pipeline":
        cls.ACTIVE_INSTANCE = super().__new__(cls)
        return cls.ACTIVE_INSTANCE

    def __init__(self):
        # pipeline is not configured yet
        self.is_configured = False
        # self.pipeline_name: str = None
        self.pipeline_secret: str = None
        # self.default_schema_name: str = None
        # self.default_dataset_name: str = None
        self.working_dir: str = None
        self.is_transient: bool = None
        self.pipeline_storage: FileStorage = None
        self.credentials: TCredentials = None
        self.state: TPipelineState = {}

        self._extractor_storage: ExtractorStorageBase = None
        self._schema_storage: SchemaStorage = None

    def only_not_configured(f: TFun) -> TFun:

        @wraps(f)
        def _wrap(self: "Pipeline", *args: Any, **kwargs: Any) -> Any:
            if self.is_configured:
                raise PipelineConfiguredException(f.__name__)
            return f(self, *args, **kwargs)

        return _wrap

    def maybe_default_config(f: TFun) -> TFun:

        @wraps(f)
        def _wrap(self: "Pipeline", *args: Any, **kwargs: Any) -> Any:
            if not self.is_configured:
                self.configure()
            return f(self, *args, **kwargs)

        return _wrap

    def with_state_sync(f: TFun) -> TFun:

        @wraps(f)
        def _wrap(self: "Pipeline", *args: Any, **kwargs: Any) -> Any:
            with self._managed_state():
                return f(self, *args, **kwargs)

        return _wrap

    @only_not_configured
    @with_state_sync
    def configure(self,
        pipeline_name: str = None,
        working_dir: str = None,
        pipeline_secret: TSecretValue = None,
        overwrite_existing: bool = False,
        import_schema_path: str = None,
        export_schema_path: str = None,
        # initial_state: TPipelineState = None,
        destination_name: str = None,
        # credentials: TCredentials = None,
        log_level: str = "INFO"
    ) -> None:
        # cannot has_pending_loads: cannot reconfigure when loads are pending (?)

        # go through configuration to resolve config via registered providers
        C = get_config(PipelineConfiguration, PipelineConfiguration, initial_values=locals())
        pipeline_name, working_dir, pipeline_secret, overwrite_existing = itemgetter("pipeline_name", "working_dir", "pipeline_secret", "overwrite_existing")(C.as_dict())
        # get partial config for load
        C = get_config(LoaderConfiguration, initial_values={"client_type": destination_name}, accept_partial=True)
        destination_name = itemgetter("client_type")(C.as_dict())

        # create root storage with full pipeline state
        if not working_dir:
            working_dir = tempfile.mkdtemp()
            self.is_transient = True
        else:
            self.is_transient = False

        self.pipeline_storage = FileStorage(working_dir, makedirs=False)
        self.working_dir = self.pipeline_storage.storage_path
        # self.pipeline_name = pipeline_name
        # self.default_dataset_name = normalize_schema_name(name)
        self.pipeline_secret = pipeline_secret
        # self.destination_name = destination_name
        # self.credentials = credentials

        self.state = {
            # "default_dataset_name": normalize_schema_name(name),
            "destination_name": destination_name,
            # "is_transient": is_transient,
            "pipeline_name": pipeline_name,
            # "schema_sync_path": schema_sync_path
        }

        # restore pipeline if folder exists and is not empty
        # if self.pipeline_storage.has_folder("."):
        #     if overwrite_existing:
        #         self.pipeline_storage.delete_folder(".")
        #     elif not self.pipeline_storage.is_empty("."):
        #         self._restore_pipeline()

        # create new pipeline
        # if not self.is_ready:
        self._create_pipeline()
        # create schema storage
        self._schema_storage = self._ensure_schema_storage(import_schema_path, export_schema_path)
        # create extractor storage
        self._extractor_storage = ExtractorStorageBase(
            "1.0.0",
            True,
            FileStorage(os.path.join(self.working_dir, "extract"), makedirs=True),
            self._ensure_normalize_storage()
        )

        self.is_configured = True
        runner.initialize_runner(C, TRunArgs(True, 0))

    @only_not_configured
    def restore(self, working_dir: str) -> None:
        pass

    @overload
    def extract(
        self,
        data: Union[Iterator[TDataItem], Iterable[TDataItem]],
        table_name = None,
        write_disposition = None,
        parent = None,
        columns = None,
        max_parallel_data_items: int =  20,
        schema: Schema = None
    ) -> None:
        ...

    @overload
    def extract(
        self,
        data: SourceTables,
        max_parallel_iterators: int = 1,
        max_parallel_data_items: int =  20,
        schema: Schema = None
    ) -> None:
        ...

    @maybe_default_config
    # @with_schemas_sync
    @with_state_sync
    def extract(
        self,
        data: Union[SourceTables, Iterator[TDataItem], Iterable[TDataItem]],
        table_name = None,
        write_disposition = None,
        parent = None,
        columns = None,
        max_parallel_iterators: int = 1,
        max_parallel_data_items: int =  20,
        schema: Schema = None
    ) -> None:
        self._schema_storage.save_schema(schema)
        self.state["default_dataset_name"] = schema.name

        if isinstance(data, SourceTables):
            # extract many
            pass
        else:
            # extract single
            pass

    @maybe_default_config
    # @with_schemas_sync
    @with_state_sync
    def extract_many() -> None:
        pass

    # @with_schemas_sync
    def normalize(self, dry_run: bool = False, workers: int = 1, max_events_in_chunk: int = 100000) -> None:
        if is_interactive() and workers > 1:
            raise NotImplementedError("Do not use workers in interactive mode ie. in notebook")
        # set parameters to be passed to config
        normalize = self._configure_normalize({
            "WORKERS": workers,
            "MAX_EVENTS_IN_CHUNK": max_events_in_chunk,
            "POOL_TYPE": "thread" if workers == 1 else "process"
        })
        runner.run_pool(normalize.CONFIG, normalize)
        if runner.LAST_RUN_METRICS.has_failed:
            raise PipelineStepFailed("normalize", self.last_run_exception, runner.LAST_RUN_METRICS)

    @with_state_sync
    def load(
        self,
        destination_name: str = None,
        credentials: TCredentials = None,
        raise_on_failed_jobs = False,
        raise_on_incompatible_schema = False,
        always_drop_dataset = False,
        dry_run: bool = False,
        max_parallel_loads: int = 20,
        normalize_workers: int = 1
    ) -> None:
        # check if anything to normalize
        # then load
        pass

    def activate(self) -> None:
        # make this instance the active one
        pass

    @property
    def schemas(self) -> Mapping[str, Schema]:
        return self._schema_storage

    @property
    def default_schema(self) -> Schema:
        return self.schemas[self.state.get("default_schema_name")]

    def _create_pipeline(self):
        pass


    def _restore_pipeline(self):
        # pipeline state takes precedence over passed settings
        # schemas are preserved
        # loader type
        pass

    def _ensure_normalize_storage(self) -> NormalizeStorage:
        C = get_config(NormalizeVolumeConfiguration, ProductionNormalizeVolumeConfiguration, initial_values=self._common_initial())
        return NormalizeStorage(True, C)

    def _ensure_schema_storage(self, import_schema_path: str = None, export_schema_path: str = None) -> SchemaStorage:
        initial = {"SCHEMA_VOLUME_PATH": os.path.join(self.working_dir, "schemas")}
        initial.update(locals())
        C = get_config(SchemaVolumeConfiguration, initial_values=initial)
        return SchemaStorage(C, makedirs=True)

    def _configure_normalize(self, initial_values: StrAny) -> Normalize:
        destination_name = self._ensure_destination_name()
        format = self._get_loader_capabilities(destination_name)["preferred_loader_file_format"]
        # create unpacker config
        initial_values.update({
            "LOADER_FILE_FORMAT": format,
            "ADD_EVENT_JSON": False
        })
        # apply schema storage config
        initial_values.update(self._schema_storage.C.as_dict())
        # apply common initial settings
        initial_values.update(self._common_initial())
        C = normalize_configuration(initial_values=initial_values)
        # shares schema storage with the pipeline so we do not need to install
        return Normalize(C)

    def _configure_load(self, credenitials: TCredentials) -> None:
        # get destination or raise
        destination_name = self._ensure_destination_name()
        # import load client for given destination or raise
        self._get_loader_capabilities(destination_name)

        loader_initial = {
            "DELETE_COMPLETED_JOBS": True,
            "CLIENT_TYPE": destination_name
        }
        loader_initial.update(self._common_initial())

        loader_client_initial = deepcopy(credenitials)
        loader_client_initial.update({"DEFAULT_DATASET": self._ensure_default_dataset_name()})

        C = loader_configuration(initial_values=loader_initial)
        loader.configure(C, REGISTRY, client_initial_values=loader_client_initial, is_storage_owner=True)
        self._load_instance = id(loader.CONFIG)

    def _common_initial(self) -> StrAny:
        return {
            "PIPELINE_NAME": self.state["pipeline_name"],
            "EXIT_ON_EXCEPTION": True,
            "LOADING_VOLUME_PATH": os.path.join(self.working_dir, "normalized"),
            "NORMALIZE_VOLUME_PATH": os.path.join(self.working_dir, "normalize")
        }

    def _get_loader_capabilities(self, destination_name: str) -> TLoaderCapabilities:
        try:
            return loader.loader_capabilities(destination_name)
        except ImportError:
            raise MissingDependencyException(
                f"{destination_name} destination",
                [f"python-dlt[{destination_name}]"],
                "Dependencies for specific destinations are available as extras of python-dlt"
            )

    def _ensure_destination_name(self, maybe_new_destination_name: str = None) -> str:
        d_n = self.state.setdefault("destination_name", maybe_new_destination_name)
        if not d_n:
                raise PipelineConfigMissing(
                    "destination_name",
                    "normalize",
                    "Please provide `destination_name` argument to `config` or `load` method or via pipeline config file or environment var."
                )
        return d_n

    def _ensure_default_dataset_name(self, maybe_new_dataset_name: str = None) -> str:
        d_n = self.state.setdefault("default_dataset_name", maybe_new_dataset_name)
        if not d_n:
            d_n = normalize_schema_name(self.state["pipeline_name"])
        return d_n

    @contextmanager
    def _managed_state(self) -> Iterator[None]:
        backup_state = deepcopy(self.state)
        try:
            yield
        except Exception:
            # restore old state
            self.state.clear()
            self.state.update(backup_state)
            raise
        else:
            # persist old state
            self.pipeline_storage.save("state.json", json.dumps(self.state))

    @property
    def name(self) -> str:
        pass

    @property
    def is_active(self) -> str:
        return id(self) == id(Pipeline.ACTIVE_INSTANCE)

    # @property
    # def is_configured(self) -> bool:
    #     return self.

    @property
    def has_pending_loads(self) -> bool:
        # TODO: check if has pending normalizer and loader data
        pass

# active instance always present
Pipeline.ACTIVE_INSTANCE = Pipeline()
