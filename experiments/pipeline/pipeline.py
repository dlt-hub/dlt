import os
from collections import abc
import tempfile
from contextlib import contextmanager
from copy import deepcopy
from functools import wraps
from typing import Any, List, Iterable, Iterator, Mapping, NewType, Optional, Sequence, Type, TypedDict, Union, overload
from operator import itemgetter
from prometheus_client import REGISTRY

from dlt.common import json, logger, signals
from dlt.common.sources import DLT_METADATA_FIELD, with_table_name
from dlt.common.typing import DictStrAny, StrAny, TFun, TSecretValue, TAny

from dlt.common.runners import pool_runner as runner, TRunMetrics, initialize_runner
from dlt.common.schema.utils import normalize_schema_name
from dlt.common.storages import LiveSchemaStorage, NormalizeStorage

from dlt.common.configuration import make_configuration, RunConfiguration, NormalizeVolumeConfiguration, SchemaVolumeConfiguration, ProductionNormalizeVolumeConfiguration
from dlt.common.schema.schema import Schema
from dlt.common.file_storage import FileStorage
from dlt.common.utils import is_interactive, uniq_id

from dlt.extract.extractor_storage import ExtractorStorageBase
from dlt.load.typing import TLoaderCapabilities
from dlt.normalize.configuration import configuration as normalize_configuration
from dlt.normalize import Normalize
from dlt.load.client_base import SqlClientBase, SqlJobClientBase
from dlt.load.configuration import LoaderClientDwhConfiguration, configuration as loader_configuration
from dlt.load import Load

from experiments.pipeline.configuration import get_config
from experiments.pipeline.exceptions import PipelineConfigMissing, PipelineConfiguredException, MissingDependencyException, PipelineStepFailed
from experiments.pipeline.sources import DltSource, TResolvableDataItem


TConnectionString = NewType("TConnectionString",  str)
TSourceState = NewType("TSourceState", DictStrAny)

TCredentials = Union[TConnectionString, StrAny]

class TPipelineState(TypedDict):
    pipeline_name: str
    default_dataset: str
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
    drop_existing_data: bool = False

    @classmethod
    def check_integrity(cls) -> None:
        if cls.PIPELINE_SECRET:
            cls.PIPELINE_SECRET = uniq_id()


class Pipeline:

    ACTIVE_INSTANCE: "Pipeline" = None
    STATE_FILE = "state.json"

    def __new__(cls: Type["Pipeline"]) -> "Pipeline":
        cls.ACTIVE_INSTANCE = super().__new__(cls)
        return cls.ACTIVE_INSTANCE

    def __init__(self):
        # pipeline is not configured yet
        # self.is_configured = False
        # self.pipeline_name: str = None
        # self.pipeline_secret: str = None
        # self.default_schema_name: str = None
        # self.default_dataset_name: str = None
        # self.working_dir: str = None
        # self.is_transient: bool = None
        self.CONFIG: Type[PipelineConfiguration] = None
        self.root_folder: str = None

        self._initial_values: DictStrAny = {}
        self._state: TPipelineState = {}
        self._pipeline_storage: FileStorage = None
        self._extractor_storage: ExtractorStorageBase = None
        self._schema_storage: LiveSchemaStorage = None

    def only_not_configured(f: TFun) -> TFun:

        @wraps(f)
        def _wrap(self: "Pipeline", *args: Any, **kwargs: Any) -> Any:
            if self.CONFIG:
                raise PipelineConfiguredException(f.__name__)
            return f(self, *args, **kwargs)

        return _wrap

    def maybe_default_config(f: TFun) -> TFun:

        @wraps(f)
        def _wrap(self: "Pipeline", *args: Any, **kwargs: Any) -> Any:
            if not self.CONFIG:
                self.configure()
            return f(self, *args, **kwargs)

        return _wrap

    def with_state_sync(f: TFun) -> TFun:

        @wraps(f)
        def _wrap(self: "Pipeline", *args: Any, **kwargs: Any) -> Any:
            with self._managed_state():
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


    @overload
    def configure(self,
        pipeline_name: str = None,
        working_dir: str = None,
        pipeline_secret: TSecretValue = None,
        drop_existing_data: bool = False,
        import_schema_path: str = None,
        export_schema_path: str = None,
        destination_name: str = None,
        log_level: str = "INFO"
    ) -> None:
        ...


    @only_not_configured
    @with_state_sync
    def configure(self, **kwargs: Any) -> None:
        # keep the locals to be able to initialize configs at any time
        self._initial_values.update(**kwargs)
        # resolve pipeline configuration
        self.CONFIG = self._get_config(PipelineConfiguration)

        # use system temp folder if not specified
        if not self.CONFIG.WORKING_DIR:
            self.CONFIG.WORKING_DIR = tempfile.gettempdir()
        self.root_folder = os.path.join(self.CONFIG.WORKING_DIR, self.CONFIG.PIPELINE_NAME)
        self._set_common_initial_values()

        # create pipeline working dir
        self._pipeline_storage = FileStorage(self.root_folder, makedirs=False)

        # remove existing pipeline if requested
        if self._pipeline_storage.has_folder(".") and self.CONFIG.drop_existing_data:
            self._pipeline_storage.delete_folder(".")

        # restore pipeline if folder exists and contains state
        if self._pipeline_storage.has_file(Pipeline.STATE_FILE):
            self._restore_pipeline()
        else:
            self._create_pipeline()

        # create schema storage
        self._schema_storage = LiveSchemaStorage(self._get_config(SchemaVolumeConfiguration), makedirs=True)
        # create extractor storage
        self._extractor_storage = ExtractorStorageBase(
            "1.0.0",
            True,
            FileStorage(os.path.join(self.root_folder, "extract"), makedirs=True),
            self._ensure_normalize_storage()
        )

        initialize_runner(self.CONFIG)


    def _get_config(self, spec: Type[TAny], accept_partial: bool = False) -> Type[TAny]:
        print(self._initial_values)
        return make_configuration(spec, spec, initial_values=self._initial_values, accept_partial=accept_partial)


    @overload
    def extract(
        self,
        data: Union[Iterator[TResolvableDataItem], Iterable[TResolvableDataItem]],
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
        data: DltSource,
        max_parallel_iterators: int = 1,
        max_parallel_data_items: int =  20,
        schema: Schema = None
    ) -> None:
        ...

    @maybe_default_config
    @with_schemas_sync
    @with_state_sync
    def extract(
        self,
        data: Union[DltSource, Iterator[TResolvableDataItem], Iterable[TResolvableDataItem]],
        table_name = None,
        write_disposition = None,
        parent = None,
        columns = None,
        max_parallel_iterators: int = 1,
        max_parallel_data_items: int =  20,
        schema: Schema = None
    ) -> None:
        self._schema_storage.save_schema(schema)
        self._state["default_schema_name"] = schema.name
        # TODO: apply hints to table

        # check if iterator or iterable is supported
        # if isinstance(items, str) or isinstance(items, dict) or not
        # TODO: check if schema exists
        with self._managed_state():
            default_table_name = table_name or self.CONFIG.PIPELINE_NAME
            # TODO: this is not very effective - we consume iterator right away, better implementation needed where we stream iterator to files directly
            all_items: List[DictStrAny] = []
            for item in data:
                # dispatch items by type
                if callable(item):
                    item = item()
                if isinstance(item, dict):
                    all_items.append(item)
                elif isinstance(item, abc.Sequence):
                    all_items.extend(item)
                # react to CTRL-C and shutdowns from controllers
                signals.raise_if_signalled()

            try:
                self._extract_iterator(default_table_name, all_items)
            except Exception:
                raise PipelineStepFailed("extract", self.last_run_exception, runner.LAST_RUN_METRICS)

    # @maybe_default_config
    # @with_schemas_sync
    # @with_state_sync
    # def extract_many() -> None:
    #     pass

    @with_schemas_sync
    def normalize(self, dry_run: bool = False, workers: int = 1, max_events_in_chunk: int = 100000) -> None:
        if is_interactive() and workers > 1:
            raise NotImplementedError("Do not use normalize workers in interactive mode ie. in notebook")
        # set parameters to be passed to config
        normalize = self._configure_normalize({
            "WORKERS": workers,
            "POOL_TYPE": "thread" if workers == 1 else "process"
        })
        try:
            ec = runner.run_pool(normalize.CONFIG, normalize)
            # in any other case we raise if runner exited with status failed
            if runner.LAST_RUN_METRICS.has_failed:
                raise PipelineStepFailed("normalize", self.last_run_exception, runner.LAST_RUN_METRICS)
            return ec
        except Exception as r_ex:
            # if EXIT_ON_EXCEPTION flag is set, exception will bubble up directly
            raise PipelineStepFailed("normalize", self.last_run_exception, runner.LAST_RUN_METRICS) from r_ex
        finally:
            signals.raise_if_signalled()

    @with_schemas_sync
    @with_state_sync
    def load(
        self,
        destination_name: str = None,
        default_dataset: str = None,
        credentials: TCredentials = None,
        raise_on_failed_jobs = False,
        raise_on_incompatible_schema = False,
        always_drop_dataset = False,
        dry_run: bool = False,
        max_parallel_loads: int = 20,
        normalize_workers: int = 1
    ) -> None:
        self._resolve_load_client_config()
        # check if anything to normalize
        if len(self._extractor_storage.normalize_storage.list_files_to_normalize_sorted()) > 0:
            self.normalize(dry_run=dry_run, workers=normalize_workers)
        # then load
        print(locals())
        load = self._configure_load(locals(), credentials)
        runner.run_pool(load.CONFIG, load)
        if runner.LAST_RUN_METRICS.has_failed:
            raise PipelineStepFailed("load", self.last_run_exception, runner.LAST_RUN_METRICS)

    def activate(self) -> None:
        # make this instance the active one
        pass

    @property
    def schemas(self) -> Mapping[str, Schema]:
        return self._schema_storage

    @property
    def default_schema(self) -> Schema:
        return self.schemas[self._state.get("default_schema_name")]

    @property
    def last_run_exception(self) -> BaseException:
        return runner.LAST_RUN_EXCEPTION

    def _create_pipeline(self) -> None:
        self._pipeline_storage.create_folder(".", exists_ok=True)

    def _restore_pipeline(self) -> None:
        self._restore_state()

    def _ensure_normalize_storage(self) -> NormalizeStorage:
        return NormalizeStorage(True, self._get_config(NormalizeVolumeConfiguration))

    def _configure_normalize(self, initial_values: DictStrAny) -> Normalize:
        destination_name = self._ensure_destination_name()
        format = self._get_loader_capabilities(destination_name)["preferred_loader_file_format"]
        # create normalize config
        initial_values.update({
            "LOADER_FILE_FORMAT": format,
            "ADD_EVENT_JSON": False
        })
        # apply schema storage config
        # initial_values.update(self._schema_storage.C.as_dict())
        # apply common initial settings
        initial_values.update(self._initial_values)
        C = normalize_configuration(initial_values=initial_values)
        print(C.as_dict())
        # shares schema storage with the pipeline so we do not need to install
        return Normalize(C, schema_storage=self._schema_storage)

    def _configure_load(self, loader_initial: DictStrAny, credentials: TCredentials = None) -> Load:
        # get destination or raise
        destination_name = self._ensure_destination_name()
        # import load client for given destination or raise
        self._get_loader_capabilities(destination_name)
        # get default dataset or raise
        default_dataset = self._ensure_default_dataset()

        loader_initial.update({
            "DELETE_COMPLETED_JOBS": True,
            "CLIENT_TYPE": destination_name
        })
        loader_initial.update(self._initial_values)

        loader_client_initial = {
            "DEFAULT_DATASET": default_dataset,
            "DEFAULT_SCHEMA_NAME": self._state.get("default_schema_name")
        }
        if credentials:
            loader_client_initial.update(credentials)

        C = loader_configuration(initial_values=loader_initial)
        return Load(C, REGISTRY, client_initial_values=loader_client_initial, is_storage_owner=False)

    def _set_common_initial_values(self) -> None:
        self._initial_values.update({
            "IS_SINGLE_RUN": True,
            "EXIT_ON_EXCEPTION": True,
            "LOAD_VOLUME_PATH": os.path.join(self.root_folder, "load"),
            "NORMALIZE_VOLUME_PATH": os.path.join(self.root_folder, "normalize"),
            "SCHEMA_VOLUME_PATH": os.path.join(self.root_folder, "schemas")
        })

    def _get_loader_capabilities(self, destination_name: str) -> TLoaderCapabilities:
        try:
            return Load.loader_capabilities(destination_name)
        except ImportError:
            raise MissingDependencyException(
                f"{destination_name} destination",
                [f"python-dlt[{destination_name}]"],
                "Dependencies for specific destinations are available as extras of python-dlt"
            )

    def _resolve_load_client_config(self) -> Type[LoaderClientDwhConfiguration]:
        return get_config(
            LoaderClientDwhConfiguration,
            initial_values={
                "client_type": self._initial_values.get("destination_name"),
                "default_dataset": self._initial_values.get("default_dataset")
            },
            accept_partial=True
        )

    def _ensure_destination_name(self) -> str:
        d_n = self._resolve_load_client_config().CLIENT_TYPE
        if not d_n:
                raise PipelineConfigMissing(
                    "destination_name",
                    "normalize",
                    "Please provide `destination_name` argument to `config` or `load` method or via pipeline config file or environment var."
                )
        return d_n

    def _ensure_default_dataset(self) -> str:
        d_n = self._resolve_load_client_config().DEFAULT_DATASET
        if not d_n:
            d_n = normalize_schema_name(self.CONFIG.PIPELINE_NAME)
        return d_n

    def _extract_iterator(self, default_table_name: str, items: Sequence[DictStrAny]) -> None:
        try:
            for idx, i in enumerate(items):
                if not isinstance(i, dict):
                    # TODO: convert non dict types into dict
                    items[idx] = i = {"v": i}
                if DLT_METADATA_FIELD not in i or i.get(DLT_METADATA_FIELD, None) is None:
                    # set default table name
                    with_table_name(i, default_table_name)

            load_id = uniq_id()
            self._extractor_storage.save_json(f"{load_id}.json", items)
            self._extractor_storage.commit_events(
                self.default_schema.name,
                self._extractor_storage.storage.make_full_path(f"{load_id}.json"),
                default_table_name,
                len(items),
                load_id
            )

            runner.LAST_RUN_METRICS = TRunMetrics(was_idle=False, has_failed=False, pending_items=0)
        except Exception as ex:
            logger.exception("extracting iterator failed")
            runner.LAST_RUN_METRICS = TRunMetrics(was_idle=False, has_failed=True, pending_items=0)
            runner.LAST_RUN_EXCEPTION = ex
            raise

    @contextmanager
    def _managed_state(self) -> Iterator[None]:
        backup_state = deepcopy(self._state)
        try:
            yield
        except Exception:
            # restore old state
            self._state.clear()
            self._state.update(backup_state)
            raise
        else:
            # persist old state
            # TODO: compare backup and new state, save only if different
            self._pipeline_storage.save(Pipeline.STATE_FILE, json.dumps(self._state))

    def _restore_state(self) -> None:
        self._state.clear()
        restored_state: DictStrAny = json.loads(self._pipeline_storage.load(Pipeline.STATE_FILE))
        self._state.update(restored_state)

    @property
    def is_active(self) -> bool:
        return id(self) == id(Pipeline.ACTIVE_INSTANCE)

    @property
    def has_pending_loads(self) -> bool:
        # TODO: check if has pending normalizer and loader data
        pass

# active instance always present
Pipeline.ACTIVE_INSTANCE = Pipeline()
