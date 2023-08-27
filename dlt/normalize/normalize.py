import os
from typing import Any, Callable, List, Dict, Sequence, Tuple, Set
from multiprocessing.pool import AsyncResult, Pool as ProcessPool

from dlt.common import pendulum, json, logger, sleep
from dlt.common.configuration import with_config, known_sections
from dlt.common.configuration.accessors import config
from dlt.common.configuration.container import Container
from dlt.common.destination import DestinationCapabilitiesContext, TLoaderFileFormat
from dlt.common.json import custom_pua_decode
from dlt.common.runners import TRunMetrics, Runnable
from dlt.common.runtime import signals
from dlt.common.runtime.collector import Collector, NULL_COLLECTOR
from dlt.common.schema.typing import TStoredSchema, TTableSchemaColumns
from dlt.common.schema.utils import merge_schema_updates
from dlt.common.storages.exceptions import SchemaNotFoundError
from dlt.common.storages import NormalizeStorage, SchemaStorage, LoadStorage, LoadStorageConfiguration, NormalizeStorageConfiguration
from dlt.common.typing import TDataItem
from dlt.common.schema import TSchemaUpdate, Schema
from dlt.common.schema.exceptions import CannotCoerceColumnException
from dlt.common.pipeline import NormalizeInfo
from dlt.common.utils import chunks, TRowCount, merge_row_count, increase_row_count

from dlt.normalize.configuration import NormalizeConfiguration

# normalize worker wrapping function (map_parallel, map_single) return type
TMapFuncRV = Tuple[Sequence[TSchemaUpdate], TRowCount]
# normalize worker wrapping function signature
TMapFuncType = Callable[[Schema, str, Sequence[str]], TMapFuncRV]  # input parameters: (schema name, load_id, list of files to process)
# tuple returned by the worker
TWorkerRV = Tuple[List[TSchemaUpdate], int, List[str], TRowCount]


class Normalize(Runnable[ProcessPool]):

    @with_config(spec=NormalizeConfiguration, sections=(known_sections.NORMALIZE,))
    def __init__(self, collector: Collector = NULL_COLLECTOR, schema_storage: SchemaStorage = None, config: NormalizeConfiguration = config.value) -> None:
        self.config = config
        self.collector = collector
        self.pool: ProcessPool = None
        self.normalize_storage: NormalizeStorage = None
        self.load_storage: LoadStorage = None
        self.schema_storage: SchemaStorage = None
        self._row_counts: TRowCount = {}

        # setup storages
        self.create_storages()
        # create schema storage with give type
        self.schema_storage = schema_storage or SchemaStorage(self.config._schema_storage_config, makedirs=True)

    def create_storages(self) -> None:
        # pass initial normalize storage config embedded in normalize config
        self.normalize_storage = NormalizeStorage(True, config=self.config._normalize_storage_config)
        # normalize saves in preferred format but can read all supported formats
        self.load_storage = LoadStorage(True, self.config.destination_capabilities.preferred_loader_file_format, LoadStorage.ALL_SUPPORTED_FILE_FORMATS, config=self.config._load_storage_config)

    @staticmethod
    def load_or_create_schema(schema_storage: SchemaStorage, schema_name: str) -> Schema:
        try:
            schema = schema_storage.load_schema(schema_name)
            schema.update_normalizers()
            logger.info(f"Loaded schema with name {schema_name} with version {schema.stored_version}")
        except SchemaNotFoundError:
            schema = Schema(schema_name)
            logger.info(f"Created new schema with name {schema_name}")
        return schema

    @staticmethod
    def w_normalize_files(
            normalize_storage_config: NormalizeStorageConfiguration,
            loader_storage_config: LoadStorageConfiguration,
            destination_caps: DestinationCapabilitiesContext,
            stored_schema: TStoredSchema,
            load_id: str,
            extracted_items_files: Sequence[str],
        ) -> TWorkerRV:

        schema_updates: List[TSchemaUpdate] = []
        total_items = 0
        row_counts: TRowCount = {}

        # process all files with data items and write to buffered item storage
        with Container().injectable_context(destination_caps):
            schema = Schema.from_stored_schema(stored_schema)
            load_storage = LoadStorage(False, destination_caps.preferred_loader_file_format, LoadStorage.ALL_SUPPORTED_FILE_FORMATS, loader_storage_config)
            normalize_storage = NormalizeStorage(False, normalize_storage_config)

            try:
                root_tables: Set[str] = set()
                populated_root_tables: Set[str] = set()
                for extracted_items_file in extracted_items_files:
                    line_no: int = 0
                    root_table_name = NormalizeStorage.parse_normalize_file_name(extracted_items_file).table_name
                    root_tables.add(root_table_name)
                    logger.debug(f"Processing extracted items in {extracted_items_file} in load_id {load_id} with table name {root_table_name} and schema {schema.name}")
                    with normalize_storage.storage.open_file(extracted_items_file) as f:
                        # enumerate jsonl file line by line
                        items_count = 0
                        for line_no, line in enumerate(f):
                            items: List[TDataItem] = json.loads(line)
                            partial_update, items_count, r_counts = Normalize._w_normalize_chunk(load_storage, schema, load_id, root_table_name, items)
                            schema_updates.append(partial_update)
                            total_items += items_count
                            merge_row_count(row_counts, r_counts)
                            logger.debug(f"Processed {line_no} items from file {extracted_items_file}, items {items_count} of total {total_items}")
                        # if any item found in the file
                        if items_count > 0:
                            populated_root_tables.add(root_table_name)
                            logger.debug(f"Processed total {line_no + 1} lines from file {extracted_items_file}, total items {total_items}")
                        # make sure base tables are all covered
                        increase_row_count(row_counts, root_table_name, 0)
                # write empty jobs for tables without items if table exists in schema
                for table_name in root_tables - populated_root_tables:
                    if table_name not in schema.tables:
                        continue
                    logger.debug(f"Writing empty job for table {table_name}")
                    columns = schema.get_table_columns(table_name)
                    load_storage.write_empty_file(load_id, schema.name, table_name, columns)
            except Exception:
                logger.exception(f"Exception when processing file {extracted_items_file}, line {line_no}")
                raise
            finally:
                load_storage.close_writers(load_id)

        logger.info(f"Processed total {total_items} items in {len(extracted_items_files)} files")

        return schema_updates, total_items, load_storage.closed_files(), row_counts

    @staticmethod
    def _w_normalize_chunk(load_storage: LoadStorage, schema: Schema, load_id: str, root_table_name: str, items: List[TDataItem]) -> Tuple[TSchemaUpdate, int, TRowCount]:
        column_schemas: Dict[str, TTableSchemaColumns] = {}  # quick access to column schema for writers below
        schema_update: TSchemaUpdate = {}
        schema_name = schema.name
        items_count = 0
        row_counts: TRowCount = {}

        for item in items:
            for (table_name, parent_table), row in schema.normalize_data_item(item, load_id, root_table_name):
                # filter row, may eliminate some or all fields
                row = schema.filter_row(table_name, row)
                # do not process empty rows
                if row:
                    # decode pua types
                    for k, v in row.items():
                        row[k] = custom_pua_decode(v)  # type: ignore
                    # coerce row of values into schema table, generating partial table with new columns if any
                    row, partial_table = schema.coerce_row(table_name, parent_table, row)
                    # theres a new table or new columns in existing table
                    if partial_table:
                        # update schema and save the change
                        schema.update_schema(partial_table)
                        table_updates = schema_update.setdefault(table_name, [])
                        table_updates.append(partial_table)
                        # update our columns
                        column_schemas[table_name] = schema.get_table_columns(table_name)
                    # get current columns schema
                    columns = column_schemas.get(table_name)
                    if not columns:
                        columns = schema.get_table_columns(table_name)
                        column_schemas[table_name] = columns
                    # store row
                    # TODO: it is possible to write to single file from many processes using this: https://gitlab.com/warsaw/flufl.lock
                    load_storage.write_data_item(load_id, schema_name, table_name, row, columns)
                    # count total items
                    items_count += 1
                    increase_row_count(row_counts, table_name, 1)
            signals.raise_if_signalled()
        return schema_update, items_count, row_counts

    def update_schema(self, schema: Schema, schema_updates: List[TSchemaUpdate]) -> None:
        for schema_update in schema_updates:
            for table_name, table_updates in schema_update.items():
                logger.info(f"Updating schema for table {table_name} with {len(table_updates)} deltas")
                for partial_table in table_updates:
                    # merge columns
                    schema.update_schema(partial_table)

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
            remainder_l -=1
            l_idx = idx + 1
        return chunk_files

    def map_parallel(self, schema: Schema, load_id: str, files: Sequence[str]) -> TMapFuncRV:
        workers = self.pool._processes  # type: ignore
        chunk_files = self.group_worker_files(files, workers)
        schema_dict: TStoredSchema = schema.to_dict()
        config_tuple = (self.normalize_storage.config, self.load_storage.config, self.config.destination_capabilities, schema_dict)
        param_chunk = [[*config_tuple, load_id, files] for files in chunk_files]
        tasks: List[Tuple[AsyncResult[TWorkerRV], List[Any]]] = []
        row_counts: TRowCount = {}

        # return stats
        schema_updates: List[TSchemaUpdate] = []

        # push all tasks to queue
        for params in param_chunk:
            pending: AsyncResult[TWorkerRV] = self.pool.apply_async(Normalize.w_normalize_files, params)
            tasks.append((pending, params))

        while len(tasks) > 0:
            sleep(0.3)
            # operate on copy of the list
            for task in list(tasks):
                pending, params = task
                if pending.ready():
                    if pending.successful():
                        result: TWorkerRV = pending.get()
                        try:
                            # gather schema from all manifests, validate consistency and combine
                            self.update_schema(schema, result[0])
                            schema_updates.extend(result[0])
                            # update metrics
                            self.collector.update("Files", len(result[2]))
                            self.collector.update("Items", result[1])
                            # merge row counts
                            merge_row_count(row_counts, result[3])
                        except CannotCoerceColumnException as exc:
                            # schema conflicts resulting from parallel executing
                            logger.warning(f"Parallel schema update conflict, retrying task ({str(exc)}")
                            # delete all files produced by the task
                            for file in result[2]:
                                os.remove(file)
                            # schedule the task again
                            schema_dict = schema.to_dict()
                            # TODO: it's time for a named tuple
                            params[3] = schema_dict
                            retry_pending: AsyncResult[TWorkerRV] = self.pool.apply_async(Normalize.w_normalize_files, params)
                            tasks.append((retry_pending, params))
                        # remove finished tasks
                        tasks.remove(task)
                    else:
                        # raise the exception
                        pending.get()
                        raise AssertionError("unreachable code: pending.get must raise")

        return schema_updates, row_counts

    def map_single(self, schema: Schema, load_id: str, files: Sequence[str]) -> TMapFuncRV:
        result = Normalize.w_normalize_files(
            self.normalize_storage.config,
            self.load_storage.config,
            self.config.destination_capabilities,
            schema.to_dict(),
            load_id,
            files,
        )
        self.update_schema(schema, result[0])
        self.collector.update("Files", len(result[2]))
        self.collector.update("Items", result[1])
        return result[0], result[3]

    def spool_files(self, schema_name: str, load_id: str, map_f: TMapFuncType, files: Sequence[str]) -> None:
        schema = Normalize.load_or_create_schema(self.schema_storage, schema_name)

        # process files in parallel or in single thread, depending on map_f
        schema_updates, row_counts = map_f(schema, load_id, files)
        # logger.metrics("Normalize metrics", extra=get_logging_extras([self.schema_version_gauge.labels(schema_name)]))
        if len(schema_updates) > 0:
            logger.info(f"Saving schema {schema_name} with version {schema.version}, writing manifest files")
            # schema is updated, save it to schema volume
            self.schema_storage.save_schema(schema)
        # save schema to temp load folder
        self.load_storage.save_temp_schema(schema, load_id)
        # save schema updates even if empty
        self.load_storage.save_temp_schema_updates(load_id, merge_schema_updates(schema_updates))
        # files must be renamed and deleted together so do not attempt that when process is about to be terminated
        signals.raise_if_signalled()
        logger.info("Committing storage, do not kill this process")
        # rename temp folder to processing
        self.load_storage.commit_temp_load_package(load_id)
        # delete item files to complete commit
        for file in files:
            self.normalize_storage.storage.delete(file)
        # log and update metrics
        logger.info(f"Chunk {load_id} processed")
        self._row_counts = row_counts

    def spool_schema_files(self, load_id: str, schema_name: str, files: Sequence[str]) -> str:
        # normalized files will go here before being atomically renamed

        self.load_storage.create_temp_load_package(load_id)
        logger.info(f"Created temp load folder {load_id} on loading volume")

        # if pool is not present use map_single method to run normalization in single process
        map_parallel_f = self.map_parallel if self.pool else self.map_single
        try:
            # process parallel
            self.spool_files(schema_name, load_id, map_parallel_f, files)
        except CannotCoerceColumnException as exc:
            # schema conflicts resulting from parallel executing
            logger.warning(f"Parallel schema update conflict, switching to single thread ({str(exc)}")
            # start from scratch
            self.load_storage.create_temp_load_package(load_id)
            self.spool_files(schema_name, load_id, self.map_single, files)

        return load_id

    def run(self, pool: ProcessPool) -> TRunMetrics:
        # keep the pool in class instance
        self.pool = pool
        self._row_counts = {}
        logger.info("Running file normalizing")
        # list files and group by schema name, list must be sorted for group by to actually work
        files = self.normalize_storage.list_files_to_normalize_sorted()
        logger.info(f"Found {len(files)} files")
        if len(files) == 0:
            return TRunMetrics(True, 0)
        # group files by schema
        for schema_name, files_iter in self.normalize_storage.group_by_schema(files):
            schema_files = list(files_iter)
            load_id = str(pendulum.now().timestamp())
            logger.info(f"Found {len(schema_files)} files in schema {schema_name} load_id {load_id}")
            with self.collector(f"Normalize {schema_name} in {load_id}"):
                self.collector.update("Files", 0, len(schema_files))
                self.collector.update("Items", 0)
                self.spool_schema_files(load_id, schema_name, schema_files)
        # return info on still pending files (if extractor saved something in the meantime)
        return TRunMetrics(False, len(self.normalize_storage.list_files_to_normalize_sorted()))

    def get_normalize_info(self) -> NormalizeInfo:
        return NormalizeInfo(row_counts=self._row_counts)
