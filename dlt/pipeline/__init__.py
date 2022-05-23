import yaml
from dataclasses import dataclass
import tempfile
import os.path
from typing import Callable, Iterator, List, Literal, Sequence, Tuple, cast
from prometheus_client import REGISTRY

from autopoiesis.common import json, runners
from autopoiesis.common.configuration import (BasicConfiguration, PoolRunnerConfiguration, UnpackingVolumeConfiguration,
                                              LoadingVolumeConfiguration, SchemaVolumeConfiguration,
                                              GcpClientConfiguration, PostgresConfiguration)
from autopoiesis.common.configuration.config_utils import TConfigSecret
from autopoiesis.common.file_storage import FileStorage
from autopoiesis.common.logger import process_internal_exception
from autopoiesis.common.runners import TRunArgs, TRunMetrics
from autopoiesis.common.schema import Schema, StoredSchema
from autopoiesis.common.typing import DictStrAny, StrAny
from autopoiesis.common.utils import uniq_id

from autopoiesis.extractors.extractor_storage import ExtractorStorageBase
from autopoiesis.unpacker import unpacker
from autopoiesis.loaders import loader


class _PipelineConfig(BasicConfiguration):
    pass


TClientType = Literal["gcp", "redshift"]

# extractor generator yields functions that returns list of items of the type (table) when called
# this allows generator to implement retry logic
TExtractorItem = Callable[[], Iterator[StrAny]]
# extractor generator yields tuples: (type of the item (table name), function defined above)
TExtractorItemWithTable = Tuple[str, TExtractorItem]
TExtractorGenerator = Callable[[DictStrAny], Iterator[TExtractorItemWithTable]]


@dataclass
class PipelineCredentials:
    client_type: TClientType


@dataclass
class GCPPipelineCredentials(PipelineCredentials):
    PROJECT_ID: str
    DATASET: str
    BQ_CRED_CLIENT_EMAIL: str
    BQ_CRED_PRIVATE_KEY: TConfigSecret = None
    TIMEOUT: float = 30.0


@dataclass
class PostgresPipelineCredentials(PipelineCredentials):
    PG_DATABASE_NAME: str
    PG_SCHEMA_PREFIX: str
    PG_USER: str
    PG_HOST: str
    PG_PASSWORD: TConfigSecret = None
    PG_PORT: int = 5439
    PG_CONNECTION_TIMEOUT: int = 15


class Pipeline:
    def __init__(self, pipeline_name: str, log_level: str = "INFO") -> None:
        # init state
        self.pipeline_name = pipeline_name
        self.root_path: str = None
        self.root_storage: FileStorage = None
        self.credentials: PipelineCredentials = None
        self.extractor_storage: ExtractorStorageBase = None

        # patch config and initialize pipeline
        _PipelineConfig.LOG_LEVEL = log_level
        _PipelineConfig.NAME = pipeline_name
        runners.initialize_runner(_PipelineConfig, TRunArgs(True, 0))

    def create_pipeline(self, credentials: PipelineCredentials, working_dir: str = None, schema: Schema = None) -> None:
        # initialize root storage
        if not working_dir:
            working_dir = tempfile.mkdtemp()
        self.root_storage = FileStorage(working_dir, makedirs=True)
        self.root_path = self.root_storage.storage_path
        self._apply_credentials(credentials)
        self._mod_configurations()
        self._load_modules()
        self.extractor_storage = ExtractorStorageBase("1.0.0", True, FileStorage(os.path.join(self.root_path, "extractor"), makedirs=True), unpacker.unpack_storage)
        # create new schema
        if schema is None:
            schema = Schema(self.pipeline_name)
        assert schema.schema_name == self.pipeline_name
        # persist schema with the pipeline
        unpacker.schema_storage.save_store_schema(schema)

    def restore_pipeline(self, credentials: PipelineCredentials, working_dir: str) -> None:
        # do not create extractor dir - it must exist
        self.root_storage = FileStorage(working_dir, makedirs=False)
        self.root_path = self.root_storage.storage_path
        self._apply_credentials(credentials)
        self._mod_configurations()
        self._load_modules()
        self.extractor_storage = ExtractorStorageBase("1.0.0", True, FileStorage(os.path.join(self.root_path, "extractor"), makedirs=False), unpacker.unpack_storage)
        # schema must exist
        unpacker.schema_storage.load_store_schema(self.pipeline_name)

    def extract_iterator(self, table_name: str, docs_iter: Iterator[StrAny]) -> TRunMetrics:
        try:
            # extract file
            # TODO: this is not very effective - we consume iterator right away
            docs = list(docs_iter)
            for d in docs:
                d["_event_type"] = table_name  # type: ignore

            load_id = uniq_id()
            self.extractor_storage.storage.save(f"{load_id}.json", json.dumps(docs))
            self.extractor_storage.commit_events(self.pipeline_name, self.extractor_storage.storage._make_path(f"{load_id}.json"), table_name, len(docs), load_id)

            runners.LAST_RUN_METRICS = TRunMetrics(was_idle=False, has_failed=False, pending_items=0)
        except Exception as ex:
            process_internal_exception("extracting iterator failed")
            runners.LAST_RUN_METRICS = TRunMetrics(was_idle=False, has_failed=True, pending_items=0)
            runners.LAST_RUN_EXCEPTION = ex

        return runners.LAST_RUN_METRICS

    def extract_generator(self, generator: TExtractorGenerator, workers: int = 1, max_events_in_chunk: int = 40000) -> TRunMetrics:
        # currently we just re use iterator to extract from generators (no parallel processing, no generator state preserving, no chunking, no retry logic)
        # TODO: state should be retrieved: now create always empty state
        for table, items in generator({}):
            m = self.extract_iterator(table, items())
            if m.has_failed:
                return m
        # TODO: state should be preserved for the next extraction run
        return m

    def unpack(self, workers: int = 1, max_events_in_chunk: int = 100000) -> TRunMetrics:
        return self._unpack(workers, max_events_in_chunk)

    def load(self, max_parallel_loads: int = 20) -> TRunMetrics:
        loader.CONFIG.MAX_PARALLELISM = loader.CONFIG.MAX_PARALLEL_LOADS = max_parallel_loads
        runners.pool_runner(loader.CONFIG, loader.run)
        return runners.LAST_RUN_METRICS

    @property
    def last_run_exception(self) -> BaseException:
        return runners.LAST_RUN_EXCEPTION

    def list_extracted_loads(self) -> Sequence[str]:
        return unpacker.unpack_storage.list_files_to_unpack_sorted()

    def list_unpacked_loads(self) -> Sequence[str]:
        return loader.load_storage.list_loads()

    def list_completed_loads(self) -> Sequence[str]:
        return loader.load_storage.list_completed_loads()

    def get_failed_jobs(self, load_id: str) -> Sequence[Tuple[str, str]]:
        failed_jobs: List[Tuple[str, str]] = []
        for file in loader.load_storage.list_failed_jobs(load_id):
            if not file.endswith(".exception"):
                try:
                    failed_message = loader.load_storage.storage.load(file + ".exception")
                except FileNotFoundError:
                    failed_message = None
                failed_jobs.append((file, failed_message))
        return failed_jobs

    def get_current_schema(self) -> Schema:
        return unpacker.schema_storage.load_store_schema(self.pipeline_name)

    def set_current_schema(self, new_schema: Schema) -> None:
        assert new_schema.schema_name == self.pipeline_name
        unpacker.schema_storage.save_store_schema(new_schema)

    def sync_schema(self) -> None:
        schema = unpacker.schema_storage.load_store_schema(self.pipeline_name)
        with loader.create_client(schema) as client:
            client.initialize_storage()
            client.update_storage_schema()

    def _unpack(self, workers: int, max_events_in_chunk: int) -> TRunMetrics:
        unpacker.CONFIG.MAX_PARALLELISM = workers
        unpacker.CONFIG.MAX_EVENTS_IN_CHUNK = max_events_in_chunk
        runners.pool_runner(unpacker.CONFIG, unpacker.run)
        return runners.LAST_RUN_METRICS

    def _apply_credentials(self, credentials: PipelineCredentials) -> None:
        # setup singleton configurations
        if credentials.client_type == "gcp":
            gcp_credentials = cast(GCPPipelineCredentials, credentials)
            GcpClientConfiguration.DATASET = gcp_credentials.DATASET
            GcpClientConfiguration.PROJECT_ID = gcp_credentials.PROJECT_ID
            GcpClientConfiguration.BQ_CRED_CLIENT_EMAIL = gcp_credentials.BQ_CRED_CLIENT_EMAIL
            GcpClientConfiguration.BQ_CRED_PRIVATE_KEY = gcp_credentials.BQ_CRED_PRIVATE_KEY
        elif credentials.client_type == "redshift":
            postgres_credentials = cast(PostgresPipelineCredentials, credentials)
            PostgresConfiguration.PG_DATABASE_NAME = postgres_credentials.PG_DATABASE_NAME
            PostgresConfiguration.PG_SCHEMA_PREFIX = postgres_credentials.PG_SCHEMA_PREFIX
            PostgresConfiguration.PG_PASSWORD = postgres_credentials.PG_PASSWORD
            PostgresConfiguration.PG_USER = postgres_credentials.PG_USER
            PostgresConfiguration.PG_HOST = postgres_credentials.PG_HOST
            PostgresConfiguration.PG_PORT = postgres_credentials.PG_PORT
            PostgresConfiguration.PG_CONNECTION_TIMEOUT = postgres_credentials.PG_CONNECTION_TIMEOUT
        else:
            raise ValueError(credentials.client_type)
        self.credentials = credentials

    def _mod_configurations(self) -> None:
        # mod storage configurations before storages are instantiated
        PoolRunnerConfiguration.EXIT_ON_EXCEPTION = True
        LoadingVolumeConfiguration.DELETE_COMPLETED_JOBS = True

        # TODO: this is really a hack: the configs are global class variables and intended to be instantiated only once per working process, here we abuse that
        UnpackingVolumeConfiguration.UNPACKING_VOLUME_PATH = os.path.join(self.root_path, "unpacking")
        LoadingVolumeConfiguration.LOADING_VOLUME_PATH = os.path.join(self.root_path, "loading")
        SchemaVolumeConfiguration.SCHEMA_VOLUME_PATH = os.path.join(self.root_path, "schemas")

    def _load_modules(self) -> None:
        unpacker.CONFIG.ADD_EVENT_JSON = False
        if self.credentials.client_type == "gcp":
            unpacker.CONFIG.WRITER_TYPE = "jsonl"
            loader.client_module = loader.client_impl("gcp")
        else:
            unpacker.CONFIG.WRITER_TYPE = "insert_values"
            loader.client_module = loader.client_impl("redshift")

        # print(loader.client_impl.__module__.)

        # initialize unpacker
        unpacker.unpack_storage, unpacker.load_storage, unpacker.schema_storage, unpacker.load_schema_storage = unpacker.create_folders()
        unpacker.event_counter, unpacker.event_gauge, unpacker.schema_version_gauge, unpacker.load_package_counter = unpacker.create_gauges(REGISTRY)

        # initialize loader
        loader.load_storage = loader.create_folders()
        loader.load_counter, loader.job_gauge, loader.job_counter, loader.job_wait_summary = loader.create_gauges(REGISTRY)

    def _unload_modules(self) -> None:
        pass

    @staticmethod
    def load_gcp_credentials(services_path: str, dataset_prefix: str = None) -> GCPPipelineCredentials:
        assert dataset_prefix is not None

        with open(services_path, "r") as f:
            services = json.load(f)
        return GCPPipelineCredentials("gcp", services["project_id"], dataset_prefix, services["client_email"], services["private_key"])

    @staticmethod
    def save_schema_to_file(file_name: str, schema: Schema, remove_default_hints: bool = True) -> None:
        with open(file_name, "w") as f:
            f.write(schema.as_yaml(remove_default_hints=remove_default_hints))

    @staticmethod
    def load_schema_from_file(file_name: str) -> Schema:
        with open(file_name, "r") as f:
            schema_dict: StoredSchema = yaml.safe_load(f)
        return Schema.from_dict(schema_dict)

    # @staticmethod
    # def get_default_schema_dict(schema_name: str) -> StoredSchema:
    #     schema_dict = deepcopy(default_schema)
    #     schema_dict["name"] = schema_name
    #     return schema_dict
