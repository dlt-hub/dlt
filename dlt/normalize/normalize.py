import os
import itertools
from typing import List, Dict, Sequence, Optional, Callable
from concurrent.futures import Future, Executor

from dlt.common import logger
from dlt.common.metrics import DataWriterMetrics
from dlt.common.runtime.signals import sleep
from dlt.common.configuration import with_config, known_sections
from dlt.common.configuration.accessors import config
from dlt.common.data_writers.writers import EMPTY_DATA_WRITER_METRICS
from dlt.common.runners import TRunMetrics, Runnable, NullExecutor
from dlt.common.runtime import signals
from dlt.common.runtime.collector import Collector, NULL_COLLECTOR
from dlt.common.schema.typing import TStoredSchema, TTableSchema
from dlt.common.schema.utils import merge_schema_updates
from dlt.common.storages import (
    NormalizeStorage,
    SchemaStorage,
    LoadStorage,
    ParsedLoadJobFileName,
)
from dlt.common.schema import Schema
from dlt.common.schema.exceptions import CannotCoerceColumnException
from dlt.common.pipeline import (
    NormalizeInfo,
    NormalizeMetrics,
    SupportsPipeline,
    WithStepInfo,
)
from dlt.common.storages.exceptions import LoadPackageNotFound
from dlt.common.storages.load_package import LoadPackageInfo

from dlt.normalize.configuration import NormalizeConfiguration
from dlt.normalize.exceptions import NormalizeJobFailed
from dlt.normalize.worker import w_normalize_files, group_worker_files, TWorkerRV
from dlt.normalize.validate import validate_and_update_schema, verify_normalized_table


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
            LoadStorage.ALL_SUPPORTED_FILE_FORMATS,
            config=self.config._load_storage_config,
        )

    def map_parallel(self, schema: Schema, load_id: str, files: Sequence[str]) -> TWorkerRV:
        workers: int = getattr(self.pool, "_max_workers", 1)
        chunk_files = group_worker_files(files, workers)
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
        tasks = [(self.pool.submit(w_normalize_files, *params), params) for params in param_chunk]

        while len(tasks) > 0:
            sleep(0.3)
            # operate on copy of the list
            for task in list(tasks):
                pending, params = task
                if pending.done():
                    # collect metrics from the exception (if any)
                    if isinstance(pending.exception(), NormalizeJobFailed):
                        summary.file_metrics.extend(pending.exception().writer_metrics)  # type: ignore[attr-defined]
                    # Exception in task (if any) is raised here
                    result: TWorkerRV = pending.result()
                    try:
                        # gather schema from all manifests, validate consistency and combine
                        validate_and_update_schema(schema, result[0])
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
                            w_normalize_files, *params
                        )
                        tasks.append((retry_pending, params))
                    # remove finished tasks
                    tasks.remove(task)
                logger.debug(f"{len(tasks)} tasks still remaining for {load_id}...")

        return summary

    def map_single(self, schema: Schema, load_id: str, files: Sequence[str]) -> TWorkerRV:
        result = w_normalize_files(
            self.config,
            self.normalize_storage.config,
            self.load_storage.config,
            schema.to_dict(),
            load_id,
            files,
        )
        validate_and_update_schema(schema, result.schema_updates)
        self.collector.update("Files", len(result.file_metrics))
        self.collector.update(
            "Items", sum(result.file_metrics, EMPTY_DATA_WRITER_METRICS).items_count
        )
        return result

    def clean_x_normalizer(self, load_id: str, table_name: str, table_schema: TTableSchema) -> None:
        x_normalizer = table_schema.setdefault("x-normalizer", {})
        # drop evolve once for all tables that seen data
        x_normalizer.pop("evolve-columns-once", None)
        # mark that table have seen data only if there was data
        if "seen-data" not in x_normalizer:
            logger.info(
                f"Table {table_name} has seen data for the first time with load id {load_id}"
            )
            x_normalizer["seen-data"] = True

        # Handle column-level x-normalizer
        # drop seen-null-first flag if data type was set
        for column in table_schema.get("columns", {}).values():
            col_x_normalizer = column.setdefault("x-normalizer", {})
            if col_x_normalizer.get("seen-null-first") and "data_type" in column:
                col_x_normalizer.pop("seen-null-first", None)
            if not col_x_normalizer:
                column.pop("x-normalizer", None)

    def spool_files(
        self, load_id: str, schema: Schema, map_f: TMapFuncType, files: Sequence[str]
    ) -> None:
        # process files in parallel or in single thread, depending on map_f
        schema_updates, writer_metrics = map_f(schema, load_id, files)
        # compute metrics
        job_metrics = {ParsedLoadJobFileName.parse(m.file_path): m for m in writer_metrics}
        table_metrics: Dict[str, DataWriterMetrics] = {
            table_name: sum(map(lambda pair: pair[1], metrics), EMPTY_DATA_WRITER_METRICS)
            for table_name, metrics in itertools.groupby(
                job_metrics.items(), lambda pair: pair[0].table_name
            )
        }
        # update normalizer specific info
        for table_name in table_metrics:
            table = schema.tables[table_name]
            verify_normalized_table(schema, table, self.config.destination_capabilities)
            self.clean_x_normalizer(load_id, table_name, table)
        # schema is updated, save it to schema volume
        if schema.is_modified:
            logger.info(
                f"Saving schema {schema.name} with version {schema.stored_version}:{schema.version}"
            )
            self.schema_storage.save_schema(schema)
        else:
            logger.info(
                f"Schema {schema.name} with version {schema.version} was not modified. Save skipped"
            )
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
        self._step_info_complete_load_id(
            load_id,
            {
                "started_at": None,
                "finished_at": None,
                "job_metrics": {job.job_id(): metrics for job, metrics in job_metrics.items()},
                "table_metrics": table_metrics,
            },
        )

    def spool_schema_files(self, load_id: str, schema: Schema, files: Sequence[str]) -> str:
        # delete existing folder for the case that this is a retry
        self.load_storage.new_packages.delete_package(load_id, not_exists_ok=True)
        # normalized files will go here before being atomically renamed
        self.load_storage.import_extracted_package(
            load_id, self.normalize_storage.extracted_packages
        )
        logger.info(f"Created new load package {load_id} on loading volume with ")
        # get number of workers with default == 1 if not set (ie. NullExecutor)
        workers: int = getattr(self.pool, "_max_workers", 1)
        map_f: TMapFuncType = self.map_parallel if workers > 1 else self.map_single
        self.spool_files(load_id, schema.clone(update_normalizers=True), map_f, files)

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
            # prefer schema from schema storage if it exists
            try:
                # use live schema instance via getter if on live storage, it will also do import
                # schema as live schemas are committed before calling normalize
                storage_schema = self.schema_storage[schema.name]
                if schema.stored_version_hash != storage_schema.stored_version_hash:
                    logger.warning(
                        f"When normalizing package {load_id} with schema {schema.name}: the storage"
                        f" schema hash {storage_schema.stored_version_hash} is different from"
                        f" extract package schema hash {schema.stored_version_hash}. Storage schema"
                        " was used."
                    )
                schema = storage_schema
            except FileNotFoundError:
                pass
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
                # self.verify_package(load_id, schema, schema_files)
                self._step_info_start_load_id(load_id)
                self.spool_schema_files(load_id, schema, schema_files)

        # return info on still pending packages (if extractor saved something in the meantime)
        return TRunMetrics(False, len(self.normalize_storage.extracted_packages.list_packages()))

    # def verify_package(self, load_id, schema: Schema, schema_files: Sequence[str]) -> None:
    #     """Verifies package schema and jobs against destination capabilities"""
    #     # get all tables in schema files
    #     table_names = set(ParsedLoadJobFileName.parse(job).table_name for job in schema_files)

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
