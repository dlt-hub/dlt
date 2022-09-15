from typing import Any, Callable, Type, List, Dict, Optional, Sequence, Tuple
from multiprocessing.pool import Pool as ProcessPool
from itertools import chain
from prometheus_client import Counter, CollectorRegistry, REGISTRY, Gauge

from dlt.common import pendulum, signals, json, logger
from dlt.common.json import custom_pua_decode
from dlt.cli import TRunnerArgs
from dlt.common.runners import TRunMetrics, Runnable, run_pool, initialize_runner, workermethod
from dlt.common.storages.exceptions import SchemaNotFoundError
from dlt.common.storages.normalize_storage import NormalizeStorage
from dlt.common.telemetry import get_logging_extras
from dlt.common.utils import uniq_id
from dlt.common.typing import TDataItem
from dlt.common.exceptions import PoolException
from dlt.common.storages import SchemaStorage
from dlt.common.schema import TSchemaUpdate, Schema
from dlt.common.schema.exceptions import CannotCoerceColumnException
from dlt.common.storages.load_storage import LoadStorage

from dlt.normalize.configuration import configuration, NormalizeConfiguration

TMapFuncRV = Tuple[List[TSchemaUpdate], List[Sequence[str]]]
TMapFuncType = Callable[[str, str, Sequence[str]], TMapFuncRV]


class Normalize(Runnable[ProcessPool]):

    # make our gauges static
    event_counter: Counter = None
    event_gauge: Gauge = None
    schema_version_gauge: Gauge = None
    load_package_counter: Counter = None

    def __init__(self, C: Type[NormalizeConfiguration], collector: CollectorRegistry = REGISTRY, schema_storage: SchemaStorage = None) -> None:
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
            if "Duplicated timeseries" not in str(v):
                raise

    @staticmethod
    def create_gauges(registry: CollectorRegistry) -> None:
        Normalize.event_counter = Counter("normalize_event_count", "Events processed in normalize", ["schema"], registry=registry)
        Normalize.event_gauge = Gauge("normalize_last_events", "Number of events processed in last run", ["schema"], registry=registry)
        Normalize.schema_version_gauge = Gauge("normalize_schema_version", "Current schema version", ["schema"], registry=registry)
        Normalize.load_package_counter = Gauge("normalize_load_packages_created_count", "Count of load package created", ["schema"], registry=registry)

    def create_storages(self) -> None:
        self.normalize_storage = NormalizeStorage(True, self.CONFIG)
        # normalize saves in preferred format but can read all supported formats
        self.load_storage = LoadStorage(True, self.CONFIG, self.CONFIG.LOADER_FILE_FORMAT, LoadStorage.ALL_SUPPORTED_FILE_FORMATS)

    def load_or_create_schema(self, schema_name: str) -> Schema:
        try:
            schema = self.schema_storage.load_schema(schema_name)
            logger.info(f"Loaded schema with name {schema_name} with version {schema.stored_version}")
        except SchemaNotFoundError:
            schema = Schema(schema_name)
            logger.info(f"Created new schema with name {schema_name}")
        return schema

    @staticmethod
    @workermethod
    def w_normalize_files(self: "Normalize", schema_name: str, load_id: str, events_files: Sequence[str]) -> TSchemaUpdate:
        normalized_data: Dict[str, List[Any]] = {}

        schema_update: TSchemaUpdate = {}
        schema = self.load_or_create_schema(schema_name)
        file_id = uniq_id(5)

        # process all event files and store rows in memory
        for events_file in events_files:
            i: int = 0
            event: TDataItem = None
            try:
                logger.debug(f"Processing events file {events_file} in load_id {load_id} with file_id {file_id}")
                with self.normalize_storage.storage.open_file(events_file) as f:
                    events: Sequence[TDataItem] = json.load(f)
                for i, event in enumerate(events):
                    for (table_name, parent_table), row in schema.normalize_data_item(schema, event, load_id):
                        # filter row, may eliminate some or all fields
                        row = schema.filter_row(table_name, row)
                        # do not process empty rows
                        if row:
                            # decode pua types
                            for k, v in row.items():
                                row[k] = custom_pua_decode(v)  # type: ignore
                            # check if schema can be updated
                            row, partial_table = schema.coerce_row(table_name, parent_table, row)
                            if partial_table:
                                # update schema and save the change
                                schema.update_schema(partial_table)
                                table_updates = schema_update.setdefault(table_name, [])
                                table_updates.append(partial_table)
                            # store row
                            rows = normalized_data.setdefault(table_name, [])
                            rows.append(row)
                    if i % 100 == 0:
                        logger.debug(f"Processed {i} of {len(events)} events")
            except Exception:
                logger.exception(f"Exception when processing file {events_file}, event idx {i}")
                logger.debug(f"Affected event: {event}")
                raise PoolException("normalize_files", events_file)

        # save rows and return schema changes to be gathered in parent process
        for table_name, rows in normalized_data.items():
            # save into new jobs to processed as load
            table = schema.get_table_columns(table_name)
            self.load_storage.write_temp_job_file(load_id, table_name, table, file_id, rows)

        return schema_update

    def map_parallel(self, schema_name: str, load_id: str, files: Sequence[str]) -> TMapFuncRV:
        # we chunk files in a way to not exceed MAX_EVENTS_IN_CHUNK and split them equally
        # between processors
        configured_processes = self.pool._processes  # type: ignore
        chunk_files = NormalizeStorage.chunk_by_events(files, self.CONFIG.MAX_EVENTS_IN_CHUNK, configured_processes)
        logger.info(f"Obtained {len(chunk_files)} processing chunks")
        # use id of self to pass the self instance. see `Runnable` class docstrings
        param_chunk = [(id(self), schema_name, load_id, files) for files in chunk_files]
        return self.pool.starmap(Normalize.w_normalize_files, param_chunk), chunk_files

    def map_single(self, schema_name: str, load_id: str, files: Sequence[str]) -> TMapFuncRV:
        chunk_files = NormalizeStorage.chunk_by_events(files, self.CONFIG.MAX_EVENTS_IN_CHUNK, 1)
        # get in one chunk
        assert len(chunk_files) == 1
        logger.info(f"Obtained {len(chunk_files)} processing chunks")
        # use id of self to pass the self instance. see `Runnable` class docstrings
        self_id: Any = id(self)
        return [Normalize.w_normalize_files(self_id, schema_name, load_id, chunk_files[0])], chunk_files

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
        schema_updates, chunk_files = map_f(schema_name, load_id, files)

        schema = self.load_or_create_schema(schema_name)
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
        # delete event files and count events to provide metrics
        total_events = 0
        for event_file in chain.from_iterable(chunk_files):  # flatten chunks
            self.normalize_storage.storage.delete(event_file)
            total_events += NormalizeStorage.get_events_count(event_file)
        # log and update metrics
        logger.info(f"Chunk {load_id} processed")
        self.load_package_counter.labels(schema_name).inc()
        self.event_counter.labels(schema_name).inc(total_events)
        self.event_gauge.labels(schema_name).set(total_events)
        logger.metrics("Normalize metrics", extra=get_logging_extras(
            [self.load_package_counter.labels(schema_name), self.event_counter.labels(schema_name), self.event_gauge.labels(schema_name)]))

    def spool_schema_files(self, schema_name: str, files: Sequence[str]) -> str:
        # normalized files will go here before being atomically renamed
        load_id = str(pendulum.now().timestamp())
        self.load_storage.create_temp_load_package(load_id)
        logger.info(f"Created temp load folder {load_id} on loading volume")

        try:
            # process parallel
            self.spool_files(schema_name, load_id, self.map_parallel, files)
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
        logger.info(f"Found {len(files)} files, will process in chunks of {self.CONFIG.MAX_EVENTS_IN_CHUNK} of events")
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
