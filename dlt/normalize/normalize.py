from typing import Any, Callable, Type, List, Dict, Optional, Sequence, Tuple
from multiprocessing.pool import Pool as ProcessPool
from itertools import chain
from prometheus_client import Counter, CollectorRegistry, REGISTRY, Gauge

from dlt.common import pendulum, signals, json, logger
from dlt.common.json import custom_pua_decode
from dlt.cli import TRunnerArgs
from dlt.common.normalizers.json import wrap_in_dict
from dlt.common.runners import TRunMetrics, Runnable, run_pool, initialize_runner
from dlt.common.schema.typing import TTableSchemaColumns
from dlt.common.storages.exceptions import SchemaNotFoundError
from dlt.common.storages import NormalizeStorage, SchemaStorage, LoadStorage
from dlt.common.telemetry import get_logging_extras
from dlt.common.typing import StrAny, TDataItem
from dlt.common.exceptions import PoolException
from dlt.common.schema import TSchemaUpdate, Schema
from dlt.common.schema.exceptions import CannotCoerceColumnException
from dlt.common.utils import uniq_id

from dlt.normalize.configuration import configuration, NormalizeConfiguration

# normalize worker wrapping function (map_parallel, map_single) return type
TMapFuncRV = Tuple[int, List[TSchemaUpdate], List[Sequence[str]]]  # (total items processed, list of schema updates, list of processed files)
# normalize worker wrapping function signature
TMapFuncType = Callable[[str, str, Sequence[str]], TMapFuncRV]  # input parameters: (schema name, load_id, list of files to process)


class Normalize(Runnable[ProcessPool]):

    # make our gauges static
    item_counter: Counter = None
    item_gauge: Gauge = None
    schema_version_gauge: Gauge = None
    load_package_counter: Counter = None

    def __init__(self, C: NormalizeConfiguration, collector: CollectorRegistry = REGISTRY, schema_storage: SchemaStorage = None) -> None:
        self.CONFIG = C
        self.pool: ProcessPool = None
        self.normalize_storage: NormalizeStorage = None
        self.load_storage: LoadStorage = None
        self.schema_storage: SchemaStorage = None

        # setup storages
        self.create_storages()
        # create schema storage with give type
        self.schema_storage = schema_storage or SchemaStorage(self.CONFIG, makedirs=True)
        try:
            self.create_gauges(collector)
        except ValueError as v:
            # ignore re-creation of gauges
            if "Duplicated time-series" not in str(v):
                raise

    @staticmethod
    def create_gauges(registry: CollectorRegistry) -> None:
        Normalize.item_counter = Counter("normalize_item_count", "Items processed in normalize", ["schema"], registry=registry)
        Normalize.item_gauge = Gauge("normalize_last_items", "Number of items processed in last run", ["schema"], registry=registry)
        Normalize.schema_version_gauge = Gauge("normalize_schema_version", "Current schema version", ["schema"], registry=registry)
        Normalize.load_package_counter = Gauge("normalize_load_packages_created_count", "Count of load package created", ["schema"], registry=registry)

    def create_storages(self) -> None:
        self.normalize_storage = NormalizeStorage(True, self.CONFIG)
        # normalize saves in preferred format but can read all supported formats
        self.load_storage = LoadStorage(True, self.CONFIG, self.CONFIG.loader_file_format, LoadStorage.ALL_SUPPORTED_FILE_FORMATS)


    @staticmethod
    def load_or_create_schema(schema_storage: SchemaStorage, schema_name: str) -> Schema:
        try:
            schema = schema_storage.load_schema(schema_name)
            logger.info(f"Loaded schema with name {schema_name} with version {schema.stored_version}")
        except SchemaNotFoundError:
            schema = Schema(schema_name)
            logger.info(f"Created new schema with name {schema_name}")
        return schema

    @staticmethod
    def w_normalize_files(CONFIG: NormalizeConfiguration, schema_name: str, load_id: str, extracted_items_files: Sequence[str]) -> Tuple[TSchemaUpdate, int]:
        schema = Normalize.load_or_create_schema(SchemaStorage(CONFIG, makedirs=False), schema_name)
        load_storage = LoadStorage(False, CONFIG, CONFIG.loader_file_format, LoadStorage.ALL_SUPPORTED_FILE_FORMATS)
        normalize_storage = NormalizeStorage(False, CONFIG)

        schema_update: TSchemaUpdate = {}
        column_schemas: Dict[str, TTableSchemaColumns] = {}  # quick access to column schema for writers below
        total_items = 0

        # process all files with data items and write to buffered item storage
        try:
            for extracted_items_file in extracted_items_files:
                line_no: int = 0
                item: TDataItem = None
                parent_table_name = NormalizeStorage.parse_normalize_file_name(extracted_items_file).table_name
                logger.debug(f"Processing extracted items in {extracted_items_file} in load_id {load_id} with table name {parent_table_name} and schema {schema_name}")
                with normalize_storage.storage.open_file(extracted_items_file) as f:
                    # enumerate jsonl file line by line
                    for line_no, line in enumerate(f):
                        item = json.loads(line)
                        if not isinstance(item, dict):
                            item = wrap_in_dict(item)
                        for (table_name, parent_table), row in schema.normalize_data_item(schema, item, load_id, parent_table_name):
                            # filter row, may eliminate some or all fields
                            row = schema.filter_row(table_name, row)
                            # do not process empty rows
                            if row:
                                # decode pua types
                                for k, v in row.items():
                                    row[k] = custom_pua_decode(v)  # type: ignore
                                # coerce row of values into schema table, generating partial table with new columns if any
                                row, partial_table = schema.coerce_row(table_name, parent_table, row)
                                if partial_table:
                                    # update schema and save the change
                                    schema.update_schema(partial_table)
                                    table_updates = schema_update.setdefault(table_name, [])
                                    table_updates.append(partial_table)
                                # get current columns schema
                                columns = column_schemas.get(table_name)
                                if not columns:
                                    columns = schema.get_table_columns(table_name)
                                    column_schemas[table_name] = columns
                                # store row
                                load_storage.write_data_item(load_id, schema_name, table_name, row, columns)
                                # count total items
                                total_items += 1
                        if line_no > 0 and line_no % 100 == 0:
                            logger.debug(f"Processed {line_no} items from file {extracted_items_file}, total items {total_items}")
                    # if any item found in the file
                    if item:
                        logger.debug(f"Processed total {line_no + 1} lines from file {extracted_items_file}, total items {total_items}")
        except Exception:
            logger.exception(f"Exception when processing file {extracted_items_file}, line {line_no}")
            # logger.debug(f"Affected item: {item}")
            raise PoolException("normalize_files", extracted_items_file)
        finally:
            load_storage.close_writers(load_id)

        logger.debug(f"Processed total {total_items} items in {len(extracted_items_files)} files")

        return schema_update, total_items

    def map_parallel(self, schema_name: str, load_id: str, files: Sequence[str]) -> TMapFuncRV:
        # TODO: maybe we should chunk by file size, now map all files to workers
        chunk_files = [files]
        param_chunk = [(self.CONFIG, schema_name, load_id, files) for files in chunk_files]
        processed_chunks = self.pool.starmap(Normalize.w_normalize_files, param_chunk)
        return sum([t[1] for t in processed_chunks]), [t[0] for t in processed_chunks], chunk_files

    def map_single(self, schema_name: str, load_id: str, files: Sequence[str]) -> TMapFuncRV:
        processed_chunk = Normalize.w_normalize_files(self.CONFIG, schema_name, load_id, files)
        return processed_chunk[1], [processed_chunk[0]], [files]

    def update_schema(self, schema: Schema, schema_updates: List[TSchemaUpdate]) -> int:
        updates_count = 0
        for schema_update in schema_updates:
            for table_name, table_updates in schema_update.items():
                logger.debug(f"Updating schema for table {table_name} with {len(table_updates)} deltas")
                for partial_table in table_updates:
                    updates_count += 1
                    schema.update_schema(partial_table)
        return updates_count

    def spool_files(self, schema_name: str, load_id: str, map_f: TMapFuncType, files: Sequence[str]) -> None:
        # process files in parallel or in single thread, depending on map_f
        total_items, schema_updates, chunk_files = map_f(schema_name, load_id, files)

        schema = Normalize.load_or_create_schema(self.schema_storage, schema_name)
        # gather schema from all manifests, validate consistency and combine
        updates_count = self.update_schema(schema, schema_updates)
        self.schema_version_gauge.labels(schema_name).set(schema.version)
        logger.metrics("Normalize metrics", extra=get_logging_extras([self.schema_version_gauge.labels(schema_name)]))
        logger.info(f"Saving schema {schema_name} with version {schema.version}, writing manifest files")
        if updates_count > 0:
            # schema is updated, save it to schema volume
            self.schema_storage.save_schema(schema)
        # save schema to temp load folder
        self.load_storage.save_temp_schema(schema, load_id)
        # save schema updates even if empty
        self.load_storage.save_temp_schema_updates(load_id, schema_updates)
        # files must be renamed and deleted together so do not attempt that when process is about to be terminated
        signals.raise_if_signalled()
        logger.info("Committing storage, do not kill this process")
        # rename temp folder to processing
        self.load_storage.commit_temp_load_package(load_id)
        # delete item files to complete commit
        for item_file in chain.from_iterable(chunk_files):  # flatten chunks
            self.normalize_storage.storage.delete(item_file)
        # log and update metrics
        logger.info(f"Chunk {load_id} processed")
        self.load_package_counter.labels(schema_name).inc()
        self.item_counter.labels(schema_name).inc(total_items)
        self.item_gauge.labels(schema_name).set(total_items)
        logger.metrics("Normalize metrics", extra=get_logging_extras(
            [self.load_package_counter.labels(schema_name), self.item_counter.labels(schema_name), self.item_gauge.labels(schema_name)]))

    def spool_schema_files(self, schema_name: str, files: Sequence[str]) -> str:
        # normalized files will go here before being atomically renamed
        load_id = str(pendulum.now().timestamp())
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
        logger.info("Running file normalizing")
        # list files and group by schema name, list must be sorted for group by to actually work
        files = self.normalize_storage.list_files_to_normalize_sorted()
        logger.info(f"Found {len(files)} files")
        if len(files) == 0:
            return TRunMetrics(True, False, 0)
        # group files by schema
        for schema_name, files_in_schema in self.normalize_storage.get_grouped_iterator(files):
            logger.info(f"Found files in schema {schema_name}")
            self.spool_schema_files(schema_name, list(files_in_schema))
        # return info on still pending files (if extractor saved something in the meantime)
        return TRunMetrics(False, False, len(self.normalize_storage.list_files_to_normalize_sorted()))


def main(args: TRunnerArgs) -> int:
    # initialize runner
    C = configuration(args._asdict())
    initialize_runner(C)
    # create objects and gauges
    try:
        n = Normalize(C, REGISTRY)
    except Exception:
        logger.exception("init module")
        return -1
    return run_pool(C, n)


def run_main(args: TRunnerArgs) -> None:
    exit(main(args))
