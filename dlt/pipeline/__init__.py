import yaml
from dataclasses import dataclass, asdict as dtc_asdict
import tempfile
import os.path
from typing import Callable, Dict, Iterator, List, Literal, Sequence, Tuple
from prometheus_client import REGISTRY

from dlt.common import json, runners
from dlt.common.configuration import BasicConfiguration, make_configuration
from dlt.common.configuration.utils import TConfigSecret
from dlt.common.file_storage import FileStorage
from dlt.common.logger import process_internal_exception
from dlt.common.runners import TRunArgs, TRunMetrics
from dlt.common.schema import Schema, StoredSchema
from dlt.common.typing import DictStrAny, StrAny
from dlt.common.utils import uniq_id, is_interactive

from dlt.extractors.extractor_storage import ExtractorStorageBase
from dlt.unpacker.configuration import configuration as unpacker_configuration
from dlt.loaders.configuration import configuration as loader_configuration
from dlt.unpacker import unpacker
from dlt.loaders import loader

TClientType = Literal["gcp", "redshift"]

# extractor generator yields functions that returns list of items of the type (table) when called
# this allows generator to implement retry logic
TExtractorItem = Callable[[], Iterator[StrAny]]
# extractor generator yields tuples: (type of the item (table name), function defined above)
TExtractorItemWithTable = Tuple[str, TExtractorItem]
TExtractorGenerator = Callable[[DictStrAny], Iterator[TExtractorItemWithTable]]


@dataclass
class PipelineCredentials:
    CLIENT_TYPE: TClientType


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
        C = make_configuration(BasicConfiguration, BasicConfiguration, initial_values={
            "NAME": pipeline_name,
            "LOG_LEVEL": log_level
        })
        runners.initialize_runner(C, TRunArgs(True, 0))

    def create_pipeline(self, credentials: PipelineCredentials, working_dir: str = None, schema: Schema = None) -> None:
        # initialize root storage
        if not working_dir:
            working_dir = tempfile.mkdtemp()
        self.root_storage = FileStorage(working_dir, makedirs=True)
        self.root_path = self.root_storage.storage_path
        self.credentials = credentials
        # self._apply_credentials(credentials)
        # self._mod_configurations()
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
        self.credentials = credentials
        # self._apply_credentials(credentials)
        # self._mod_configurations()
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
                assert isinstance(d, dict), "Must pass iterator of dicts in docs_iter"
                d["_event_type"] = table_name

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
        all_tables: Dict[str, List[StrAny]] = {}
        for table, item in generator({}):
            all_tables.setdefault(table, []).extend(item())
        for table, items in all_tables.items():
            m = self.extract_iterator(table, iter(items))
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

    def list_failed_jobs(self, load_id: str) -> Sequence[Tuple[str, str]]:
        failed_jobs: List[Tuple[str, str]] = []
        for file in loader.load_storage.list_archived_failed_jobs(load_id):
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
        if is_interactive() and workers > 1:
            raise NotImplementedError("Do not use workers in interactive mode ie. in notebook")
        unpacker.CONFIG.MAX_PARALLELISM = workers
        unpacker.CONFIG.MAX_EVENTS_IN_CHUNK = max_events_in_chunk
        # switch to thread pool for single worker
        unpacker.CONFIG.POOL_TYPE = "thread" if workers == 1 else "process"
        runners.pool_runner(unpacker.CONFIG, unpacker.run)
        return runners.LAST_RUN_METRICS

    def _load_modules(self) -> None:
        # pass to all modules
        common_initial: StrAny = {
            "NAME": self.pipeline_name,
            "EXIT_ON_EXCEPTION": True,
            "LOADING_VOLUME_PATH": os.path.join(self.root_path, "loading")
        }

        # use credentials to populate loader config, it includes also client type
        loader_initial = dtc_asdict(self.credentials)
        loader_initial.update(common_initial)
        loader_initial["DELETE_COMPLETED_JOBS"] = True
        loader.CONFIG = loader_configuration(initial_values=loader_initial)
        loader.client_module = loader.client_impl(loader.CONFIG.CLIENT_TYPE)

        # create unpacker config
        unpacker_initial = {
            "UNPACKING_VOLUME_PATH": os.path.join(self.root_path, "unpacking"),
            "SCHEMA_VOLUME_PATH": os.path.join(self.root_path, "schemas"),
            "WRITER_TYPE": loader.supported_writer(),
            "ADD_EVENT_JSON": False
        }
        unpacker_initial.update(common_initial)

        # initialize unpacker
        unpacker.CONFIG = unpacker_configuration(initial_values=unpacker_initial)
        unpacker.unpack_storage, unpacker.load_storage, unpacker.schema_storage, unpacker.load_schema_storage = unpacker.create_folders()
        unpacker.event_counter, unpacker.event_gauge, unpacker.schema_version_gauge, unpacker.load_package_counter = unpacker.create_gauges(REGISTRY)

        # create loader objects
        loader.load_storage = loader.create_folders()
        loader.load_counter, loader.job_gauge, loader.job_counter, loader.job_wait_summary = loader.create_gauges(REGISTRY)

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
