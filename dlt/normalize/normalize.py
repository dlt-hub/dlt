import os
import datetime  # noqa: 251
import itertools
from typing import Callable, List, Dict, NamedTuple, Sequence, Tuple, Set, Optional
from concurrent.futures import Future, Executor

from dlt.common import logger, sleep
from dlt.common.configuration import with_config, known_sections
from dlt.common.configuration.accessors import config
from dlt.common.configuration.container import Container
from dlt.common.data_writers import DataWriterMetrics
from dlt.common.data_writers.writers import EMPTY_DATA_WRITER_METRICS
from dlt.common.destination import TLoaderFileFormat
from dlt.common.runners import TRunMetrics, Runnable, NullExecutor
from dlt.common.runtime import signals
from dlt.common.runtime.collector import Collector, NULL_COLLECTOR
from dlt.common.schema.typing import TStoredSchema
from dlt.common.schema.utils import merge_schema_updates
from dlt.common.storages import (
    NormalizeStorage,
    SchemaStorage,
    LoadStorage,
    LoadStorageConfiguration,
    NormalizeStorageConfiguration,
    ParsedLoadJobFileName,
)
from dlt.common.schema import TSchemaUpdate, Schema
from dlt.common.schema.exceptions import CannotCoerceColumnException
from dlt.common.pipeline import (
    NormalizeInfo,
    NormalizeMetrics,
    SupportsPipeline,
    WithStepInfo,
)
from dlt.common.storages.exceptions import LoadPackageNotFound
from dlt.common.storages.load_package import LoadPackageInfo
from dlt.common.utils import chunks

from dlt.normalize.configuration import NormalizeConfiguration
from dlt.normalize.exceptions import NormalizeJobFailed
from dlt.normalize.items_normalizers import (
    ParquetItemsNormalizer,
    JsonLItemsNormalizer,
    ItemsNormalizer,
)


class TWorkerRV(NamedTuple):
    schema_updates: List[TSchemaUpdate]
    file_metrics: List[DataWriterMetrics]


# normalize worker wrapping function signature
TMapFuncType = Callable[
    [Schema, str, Sequence[str]], TWorkerRV
]  # input parameters: (schema name, load_id, list of files to process)


class Normalize(Runnable[Executor], WithStepInfo[NormalizeMetrics, NormalizeInfo]):
    pool: Executor

    @with_config(spec=NormalizeConfiguration, sections=(known_sections.NORMALIZE,))
    def __init__(
        self,
        collector: Collector = NULL_COLLECTOR,
        schema_storage: SchemaStorage = None,
        config: NormalizeConfiguration = config.value,
    ) -> None:
        self.config = config
        self.collector = collector
        self.normalize_storage: NormalizeStorage = None
        self.pool = NullExecutor()
        self.load_storage: LoadStorage = None
        self.schema_storage: SchemaStorage = None

        # setup storages
        self.create_storages()
        # create schema storage with give type
        self.schema_storage = schema_storage or SchemaStorage(
            self.config._schema_storage_config, makedirs=True
        )
        super().__init__()

    def create_storages(self) -> None:
        # pass initial normalize storage config embedded in normalize config
        self.normalize_storage = NormalizeStorage(
            True, config=self.config._normalize_storage_config
        )
        # normalize saves in preferred format but can read all supported formats
        self.load_storage = LoadStorage(
            True,
            self.config.destination_capabilities.preferred_loader_file_format,
            LoadStorage.ALL_SUPPORTED_FILE_FORMATS,
            config=self.config._load_storage_config,
        )

    @staticmethod
    def w_normalize_files(
        config: NormalizeConfiguration,
        normalize_storage_config: NormalizeStorageConfiguration,
        loader_storage_config: LoadStorageConfiguration,
        stored_schema: TStoredSchema,
        load_id: str,
        extracted_items_files: Sequence[str],
    ) -> TWorkerRV:
        destination_caps = config.destination_capabilities
        schema_updates: List[TSchemaUpdate] = []
        item_normalizers: Dict[TLoaderFileFormat, ItemsNormalizer] = {}

        def _create_load_storage(file_format: TLoaderFileFormat) -> LoadStorage:
            """Creates a load storage for particular file_format"""
            # TODO: capabilities.supported_*_formats can be None, it should have defaults
            supported_formats = destination_caps.supported_loader_file_formats or []
            if file_format == "parquet":
                if file_format in supported_formats:
                    supported_formats.append(
                        "arrow"
                    )  # TODO: Hack to make load storage use the correct writer
                    file_format = "arrow"
                else:
                    # Use default storage if parquet is not supported to make normalizer fallback to read rows from the file
                    file_format = (
                        destination_caps.preferred_loader_file_format
                        or destination_caps.preferred_staging_file_format
                    )
            else:
                file_format = (
                    destination_caps.preferred_loader_file_format
                    or destination_caps.preferred_staging_file_format
                )
            return LoadStorage(False, file_format, supported_formats, loader_storage_config)

        # process all files with data items and write to buffered item storage
        with Container().injectable_context(destination_caps):
            schema = Schema.from_stored_schema(stored_schema)
            normalize_storage = NormalizeStorage(False, normalize_storage_config)

            def _get_items_normalizer(file_format: TLoaderFileFormat) -> ItemsNormalizer:
                if file_format in item_normalizers:
                    return item_normalizers[file_format]
                klass = ParquetItemsNormalizer if file_format == "parquet" else JsonLItemsNormalizer
                norm = item_normalizers[file_format] = klass(
                    _create_load_storage(file_format), normalize_storage, schema, load_id, config
                )
                return norm

            parsed_file_name: ParsedLoadJobFileName = None
            try:
                root_tables: Set[str] = set()
                for extracted_items_file in extracted_items_files:
                    parsed_file_name = ParsedLoadJobFileName.parse(extracted_items_file)
                    # normalize table name in case the normalization changed
                    # NOTE: this is the best we can do, until a full lineage information is in the schema
                    root_table_name = schema.naming.normalize_table_identifier(
                        parsed_file_name.table_name
                    )
                    root_tables.add(root_table_name)
                    normalizer = _get_items_normalizer(parsed_file_name.file_format)
                    logger.debug(
                        f"Processing extracted items in {extracted_items_file} in load_id"
                        f" {load_id} with table name {root_table_name} and schema {schema.name}"
                    )
                    partial_updates = normalizer(extracted_items_file, root_table_name)
                    schema_updates.extend(partial_updates)
                    logger.debug(f"Processed file {extracted_items_file}")
            except Exception as exc:
                job_id = parsed_file_name.job_id() if parsed_file_name else ""
                raise NormalizeJobFailed(load_id, job_id, str(exc)) from exc
            finally:
                for normalizer in item_normalizers.values():
                    normalizer.load_storage.close_writers(load_id)

        writer_metrics: List[DataWriterMetrics] = []
        for normalizer in item_normalizers.values():
            norm_metrics = normalizer.load_storage.closed_files(load_id)
            writer_metrics.extend(norm_metrics)
        logger.info(f"Processed all items in {len(extracted_items_files)} files")
        return TWorkerRV(schema_updates, writer_metrics)

    def update_table(self, schema: Schema, schema_updates: List[TSchemaUpdate]) -> None:
        for schema_update in schema_updates:
            for table_name, table_updates in schema_update.items():
                logger.info(
                    f"Updating schema for table {table_name} with {len(table_updates)} deltas"
                )
                for partial_table in table_updates:
                    # merge columns
                    schema.update_table(partial_table)

    @staticmethod
    def group_worker_files(files: Sequence[str], no_groups: int) -> List[Sequence[str]]:
        # sort files so the same tables are in the same worker
        files = list(sorted(files))

        chunk_size = max(len(files) // no_groups, 1)
        chunk_files = list(chunks(files, chunk_size))
        # distribute the remainder files to existing groups starting from the end
        remainder_l = len(chunk_files) - no_groups
        l_idx = 0
        while remainder_l > 0:
            for idx, file in enumerate(reversed(chunk_files.pop())):
                chunk_files[-l_idx - idx - remainder_l].append(file)  # type: ignore
            remainder_l -= 1
            l_idx = idx + 1
        return chunk_files

    def map_parallel(self, schema: Schema, load_id: str, files: Sequence[str]) -> TWorkerRV:
        workers: int = getattr(self.pool, "_max_workers", 1)
        chunk_files = self.group_worker_files(files, workers)
        schema_dict: TStoredSchema = schema.to_dict()
        param_chunk = [
            (
                self.config,
                self.normalize_storage.config,
                self.load_storage.config,
                schema_dict,
                load_id,
                files,
            )
            for files in chunk_files
        ]
        # return stats
        summary = TWorkerRV([], [])
        # push all tasks to queue
        tasks = [
            (self.pool.submit(Normalize.w_normalize_files, *params), params)
            for params in param_chunk
        ]

        while len(tasks) > 0:
            sleep(0.3)
            # operate on copy of the list
            for task in list(tasks):
                pending, params = task
                if pending.done():
                    result: TWorkerRV = (
                        pending.result()
                    )  # Exception in task (if any) is raised here
                    try:
                        # gather schema from all manifests, validate consistency and combine
                        self.update_table(schema, result[0])
                        summary.schema_updates.extend(result.schema_updates)
                        summary.file_metrics.extend(result.file_metrics)
                        # update metrics
                        self.collector.update("Files", len(result.file_metrics))
                        self.collector.update(
                            "Items", sum(result.file_metrics, EMPTY_DATA_WRITER_METRICS).items_count
                        )
                    except CannotCoerceColumnException as exc:
                        # schema conflicts resulting from parallel executing
                        logger.warning(
                            f"Parallel schema update conflict, retrying task ({str(exc)}"
                        )
                        # delete all files produced by the task
                        for metrics in result.file_metrics:
                            os.remove(metrics.file_path)
                        # schedule the task again
                        schema_dict = schema.to_dict()
                        # TODO: it's time for a named tuple
                        params = params[:3] + (schema_dict,) + params[4:]
                        retry_pending: Future[TWorkerRV] = self.pool.submit(
                            Normalize.w_normalize_files, *params
                        )
                        tasks.append((retry_pending, params))
                    # remove finished tasks
                    tasks.remove(task)
                logger.debug(f"{len(tasks)} tasks still remaining for {load_id}...")

        return summary

    def map_single(self, schema: Schema, load_id: str, files: Sequence[str]) -> TWorkerRV:
        result = Normalize.w_normalize_files(
            self.config,
            self.normalize_storage.config,
            self.load_storage.config,
            schema.to_dict(),
            load_id,
            files,
        )
        self.update_table(schema, result.schema_updates)
        self.collector.update("Files", len(result.file_metrics))
        self.collector.update(
            "Items", sum(result.file_metrics, EMPTY_DATA_WRITER_METRICS).items_count
        )
        return result

    def spool_files(
        self, load_id: str, schema: Schema, map_f: TMapFuncType, files: Sequence[str]
    ) -> None:
        # process files in parallel or in single thread, depending on map_f
        schema_updates, writer_metrics = map_f(schema, load_id, files)
        # remove normalizer specific info
        for table in schema.tables.values():
            table.pop("x-normalizer", None)  # type: ignore[typeddict-item]
        logger.info(
            f"Saving schema {schema.name} with version {schema.stored_version}:{schema.version}"
        )
        # schema is updated, save it to schema volume
        self.schema_storage.save_schema(schema)
        # save schema new package
        self.load_storage.new_packages.save_schema(load_id, schema)
        # save schema updates even if empty
        self.load_storage.new_packages.save_schema_updates(
            load_id, merge_schema_updates(schema_updates)
        )
        # files must be renamed and deleted together so do not attempt that when process is about to be terminated
        signals.raise_if_signalled()
        logger.info("Committing storage, do not kill this process")
        # rename temp folder to processing
        self.load_storage.commit_new_load_package(load_id)
        # delete item files to complete commit
        self.normalize_storage.extracted_packages.delete_package(load_id)
        # log and update metrics
        logger.info(f"Extracted package {load_id} processed")
        job_metrics = {ParsedLoadJobFileName.parse(m.file_path): m for m in writer_metrics}
        self._step_info_complete_load_id(
            load_id,
            {
                "started_at": None,
                "finished_at": None,
                "job_metrics": {job.job_id(): metrics for job, metrics in job_metrics.items()},
                "table_metrics": {
                    table_name: sum(map(lambda pair: pair[1], metrics), EMPTY_DATA_WRITER_METRICS)
                    for table_name, metrics in itertools.groupby(
                        job_metrics.items(), lambda pair: pair[0].table_name
                    )
                },
            },
        )

    def spool_schema_files(self, load_id: str, schema: Schema, files: Sequence[str]) -> str:
        # normalized files will go here before being atomically renamed
        self.load_storage.new_packages.create_package(load_id)
        logger.info(f"Created new load package {load_id} on loading volume")
        try:
            # process parallel
            self.spool_files(
                load_id, schema.clone(update_normalizers=True), self.map_parallel, files
            )
        except CannotCoerceColumnException as exc:
            # schema conflicts resulting from parallel executing
            logger.warning(
                f"Parallel schema update conflict, switching to single thread ({str(exc)}"
            )
            # start from scratch
            self.load_storage.new_packages.delete_package(load_id)
            self.load_storage.new_packages.create_package(load_id)
            self.spool_files(load_id, schema.clone(update_normalizers=True), self.map_single, files)

        return load_id

    def run(self, pool: Optional[Executor]) -> TRunMetrics:
        # keep the pool in class instance
        self.pool = pool or NullExecutor()
        logger.info("Running file normalizing")
        # list all load packages in extracted folder
        load_ids = self.normalize_storage.extracted_packages.list_packages()
        logger.info(f"Found {len(load_ids)} load packages")
        if len(load_ids) == 0:
            return TRunMetrics(True, 0)
        for load_id in load_ids:
            # read schema from package
            schema = self.normalize_storage.extracted_packages.load_schema(load_id)
            # read all files to normalize placed as new jobs
            schema_files = self.normalize_storage.extracted_packages.list_new_jobs(load_id)
            logger.info(
                f"Found {len(schema_files)} files in schema {schema.name} load_id {load_id}"
            )
            if len(schema_files) == 0:
                # delete empty package
                self.normalize_storage.extracted_packages.delete_package(load_id)
                logger.info(f"Empty package {load_id} processed")
                continue
            with self.collector(f"Normalize {schema.name} in {load_id}"):
                self.collector.update("Files", 0, len(schema_files))
                self.collector.update("Items", 0)
                self._step_info_start_load_id(load_id)
                self.spool_schema_files(load_id, schema, schema_files)

        # return info on still pending packages (if extractor saved something in the meantime)
        return TRunMetrics(False, len(self.normalize_storage.extracted_packages.list_packages()))

    def get_load_package_info(self, load_id: str) -> LoadPackageInfo:
        """Returns information on extracted/normalized/completed package with given load_id, all jobs and their statuses."""
        try:
            return self.load_storage.get_load_package_info(load_id)
        except LoadPackageNotFound:
            return self.normalize_storage.extracted_packages.get_load_package_info(load_id)

    def get_step_info(
        self,
        pipeline: SupportsPipeline,
    ) -> NormalizeInfo:
        load_ids = list(self._load_id_metrics.keys())
        load_packages: List[LoadPackageInfo] = []
        metrics: Dict[str, List[NormalizeMetrics]] = {}
        for load_id in self._load_id_metrics.keys():
            load_package = self.get_load_package_info(load_id)
            load_packages.append(load_package)
            metrics[load_id] = self._step_info_metrics(load_id)
        return NormalizeInfo(pipeline, metrics, load_ids, load_packages, pipeline.first_run)
