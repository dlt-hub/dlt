from typing import Dict, List, Optional, Tuple
from multiprocessing.pool import ThreadPool
from prometheus_client import REGISTRY, Counter, Gauge, CollectorRegistry, Summary

from dlt.common import sleep, logger
from dlt.common.configuration import with_config
from dlt.common.configuration.accessors import config
from dlt.common.typing import StrAny
from dlt.common.runners import TRunMetrics, Runnable, workermethod
from dlt.common.logger import pretty_format_exception
from dlt.common.exceptions import TerminalValueError
from dlt.common.schema import Schema
from dlt.common.schema.typing import TTableSchema
from dlt.common.storages import LoadStorage
from dlt.common.telemetry import get_logging_extras, set_gauge_all_labels
from dlt.common.destination import JobClientBase, DestinationReference, LoadJob, TLoadJobStatus, DestinationClientConfiguration

from dlt.destinations.job_client_impl import LoadEmptyJob
from dlt.destinations.exceptions import DestinationTerminalException, DestinationTransientException, LoadJobUnknownTableException

from dlt.load.configuration import LoaderConfiguration
from dlt.load.exceptions import LoadClientUnsupportedWriteDisposition, LoadClientUnsupportedFileFormats


class Load(Runnable[ThreadPool]):

    load_counter: Counter = None
    job_gauge: Gauge = None
    job_counter: Counter = None
    job_wait_summary: Summary = None

    @with_config(spec=LoaderConfiguration, namespaces=("load",))
    def __init__(
        self,
        destination: DestinationReference,
        collector: CollectorRegistry = REGISTRY,
        is_storage_owner: bool = False,
        config: LoaderConfiguration = config.value,
        initial_client_config: DestinationClientConfiguration = config.value
    ) -> None:
        self.config = config
        self.initial_client_config = initial_client_config
        self.destination = destination
        self.capabilities = destination.capabilities()
        self.pool: ThreadPool = None
        self.load_storage: LoadStorage = self.create_storage(is_storage_owner)
        self._processed_load_ids: Dict[str, StrAny] = {}
        try:
            Load.create_gauges(collector)
        except ValueError as v:
            # ignore re-creation of gauges
            if "Duplicated" not in str(v):
                raise

    def create_storage(self, is_storage_owner: bool) -> LoadStorage:
        load_storage = LoadStorage(
            is_storage_owner,
            self.capabilities.preferred_loader_file_format,
            self.capabilities.supported_loader_file_formats,
            config=self.config._load_storage_config
        )
        return load_storage

    @staticmethod
    def create_gauges(registry: CollectorRegistry) -> None:
        Load.load_counter = Counter("loader_load_package_counter", "Counts load package processed", registry=registry)
        Load.job_gauge = Gauge("loader_last_package_jobs_counter", "Counts jobs in last package per status", ["status"], registry=registry)
        Load.job_counter = Counter("loader_jobs_counter", "Counts jobs per job status", ["status"], registry=registry)
        Load.job_wait_summary = Summary("loader_jobs_wait_seconds", "Counts jobs total wait until completion", registry=registry)

    @staticmethod
    def get_load_table(schema: Schema, table_name: str, file_name: str) -> TTableSchema:
        try:
            table = schema.get_table(table_name)
            # add write disposition if not specified - in child tables
            if "write_disposition" not in table:
                table["write_disposition"] = schema.get_write_disposition(table_name)
            return table
        except KeyError:
            raise LoadJobUnknownTableException(table_name, file_name)

    @staticmethod
    @workermethod
    def w_spool_job(self: "Load", file_path: str, load_id: str, schema: Schema) -> Optional[LoadJob]:
        # open new connection for each upload
        job: LoadJob = None
        try:
            with self.destination.client(schema, self.initial_client_config) as client:
                job_info = self.load_storage.parse_job_file_name(file_path)
                if job_info.file_format not in self.capabilities.supported_loader_file_formats:
                    raise LoadClientUnsupportedFileFormats(job_info.file_format, self.capabilities.supported_loader_file_formats, file_path)
                logger.info(f"Will load file {file_path} with table name {job_info.table_name}")
                table = self.get_load_table(schema, job_info.table_name, file_path)
                if table["write_disposition"] not in ["append", "replace"]:
                    raise LoadClientUnsupportedWriteDisposition(job_info.table_name, table["write_disposition"], file_path)
                job = client.start_file_load(table, self.load_storage.storage.make_full_path(file_path))
        except (DestinationTerminalException, TerminalValueError):
            # if job irreversibly cannot be started, mark it as failed
            logger.exception(f"Terminal problem with spooling job {file_path}")
            job = LoadEmptyJob.from_file_path(file_path, "failed", pretty_format_exception())
        except (DestinationTransientException, Exception):
            # return no job so file stays in new jobs (root) folder
            logger.exception(f"Temporary problem with spooling job {file_path}")
            return None
        self.load_storage.start_job(load_id, job.file_name())
        return job

    def spool_new_jobs(self, load_id: str, schema: Schema) -> Tuple[int, List[LoadJob]]:
        # TODO: validate file type, combine files, finalize etc., this is client specific, jsonl for single table
        # can just be combined, insert_values must be finalized and then combined
        # use thread based pool as jobs processing is mostly I/O and we do not want to pickle jobs
        # TODO: combine files by providing a list of files pertaining to same table into job, so job must be
        # extended to accept a list
        load_files = self.load_storage.list_new_jobs(load_id)[:self.config.workers]
        file_count = len(load_files)
        if file_count == 0:
            logger.info(f"No new jobs found in {load_id}")
            return 0, []
        logger.info(f"Will load {file_count}, creating jobs")
        param_chunk = [(id(self), file, load_id, schema) for file in load_files]
        # exceptions should not be raised, None as job is a temporary failure
        # other jobs should not be affected
        jobs: List[LoadJob] = self.pool.starmap(Load.w_spool_job, param_chunk)
        # remove None jobs and check the rest
        return file_count, [job for job in jobs if job is not None]

    def retrieve_jobs(self, client: JobClientBase, load_id: str) -> Tuple[int, List[LoadJob]]:
        jobs: List[LoadJob] = []

        # list all files that were started but not yet completed
        started_jobs = self.load_storage.list_started_jobs(load_id)
        logger.info(f"Found {len(started_jobs)} that are already started and should be continued")
        if len(started_jobs) == 0:
            return 0, jobs

        for file_path in started_jobs:
            try:
                logger.info(f"Will retrieve {file_path}")
                job = client.restore_file_load(file_path)
            except DestinationTerminalException:
                logger.exception(f"Job retrieval for {file_path} failed, job will be terminated")
                job = LoadEmptyJob.from_file_path(file_path, "failed", pretty_format_exception())
                # proceed to appending job, do not reraise
            except (DestinationTransientException, Exception):
                # raise on all temporary exceptions, typically network / server problems
                raise
            jobs.append(job)

        self.job_gauge.labels("retrieved").inc()
        self.job_counter.labels("retrieved").inc()
        logger.metrics("progress", "retrieve jobs", extra=get_logging_extras([self.job_gauge.labels("retrieved"), self.job_counter.labels("retrieved")]))
        return len(jobs), jobs

    def complete_jobs(self, load_id: str, jobs: List[LoadJob]) -> List[LoadJob]:
        remaining_jobs: List[LoadJob] = []
        logger.info(f"Will complete {len(jobs)} for {load_id}")
        for ii in range(len(jobs)):
            job = jobs[ii]
            logger.debug(f"Checking status for job {job.file_name()}")
            status: TLoadJobStatus = job.status()
            final_location: str = None
            if status == "running":
                # ask again
                logger.debug(f"job {job.file_name()} still running")
                remaining_jobs.append(job)
            elif status == "failed":
                # try to get exception message from job
                failed_message = job.exception()
                final_location = self.load_storage.fail_job(load_id, job.file_name(), failed_message)
                logger.error(f"Job for {job.file_name()} failed terminally in load {load_id} with message {failed_message}")
            elif status == "retry":
                # try to get exception message from job
                retry_message = job.exception()
                # move back to new folder to try again
                self.load_storage.retry_job(load_id, job.file_name())
                logger.error(f"Job for {job.file_name()} retried in load {load_id} with message {retry_message}")
            elif status == "completed":
                # move to completed folder
                final_location = self.load_storage.complete_job(load_id, job.file_name())
                logger.info(f"Job for {job.file_name()} completed in load {load_id}")

            if status in ["failed", "completed"]:
                self.job_gauge.labels(status).inc()
                self.job_counter.labels(status).inc()
                self.job_wait_summary.observe(self.load_storage.job_elapsed_time_seconds(final_location))

        logger.metrics("progress", "jobs", extra=get_logging_extras([self.job_counter, self.job_gauge, self.job_wait_summary]))
        return remaining_jobs

    def run(self, pool: ThreadPool) -> TRunMetrics:
        # store pool
        self.pool = pool

        logger.info("Running file loading")
        # get list of loads and order by name ASC to execute schema updates
        loads = self.load_storage.list_packages()
        logger.info(f"Found {len(loads)} load packages")
        if len(loads) == 0:
            return TRunMetrics(True, False, 0)

        # get top job id and mark as being processed
        load_id = loads[0]
        # TODO: here full info should be gathered: load_ids, jobs, their results and possible error messages, elapsed times etc.
        self._processed_load_ids[load_id] = None
        logger.info(f"Loading schema from load package in {load_id}")
        schema = self.load_storage.load_package_schema(load_id)
        logger.info(f"Loaded schema name {schema.name} and version {schema.stored_version}")
        # initialize analytical storage ie. create dataset required by passed schema
        with self.destination.client(schema, self.initial_client_config) as client:
            logger.info(f"Client for {client.config.destination_name} will start load")
            client.initialize_storage()
            schema_update = self.load_storage.begin_schema_update(load_id)
            if schema_update:
                logger.info(f"Client for {client.config.destination_name} will update schema to package schema")
                # TODO: this should rather generate an SQL job(s) to be executed PRE loading
                client.update_storage_schema()
                self.load_storage.commit_schema_update(load_id)
            # spool or retrieve unfinished jobs
            jobs_count, jobs = self.retrieve_jobs(client, load_id)
        if not jobs:
            # jobs count is a total number of jobs including those that could not be initialized
            jobs_count, jobs = self.spool_new_jobs(load_id, schema)
            if jobs_count > 0:
                # this is a new  load package
                # TODO: this is wrong, we must check completed and failed jobs to say that
                set_gauge_all_labels(self.job_gauge, 0)
                self.job_gauge.labels("running").inc(len(jobs))
                self.job_counter.labels("running").inc(len(jobs))
                logger.metrics("progress", "jobs",
                                extra=get_logging_extras([self.job_counter.labels("running"), self.job_gauge.labels("running")])
            )
        # if there are no existing or new jobs we complete the package
        if jobs_count == 0:
            with self.destination.client(schema, self.initial_client_config) as client:
                # TODO: this script should be executed as a job (and contain also code to merge/upsert data and drop temp tables)
                # TODO: post loading jobs
                remaining_jobs = client.complete_load(load_id)
            self.load_storage.complete_load_package(load_id)
            logger.info(f"All jobs completed, archiving package {load_id}")
            self.load_counter.inc()
            metrics = get_logging_extras([self.load_counter])
            self._processed_load_ids[load_id] = metrics
            logger.metrics("stop", "jobs", extra=metrics)
        else:
            # TODO: this loop must be urgently removed.
            while True:
                remaining_jobs = self.complete_jobs(load_id, jobs)
                if len(remaining_jobs) == 0:
                    break
                # process remaining jobs again
                jobs = remaining_jobs
                # this will raise on signal
                sleep(1)

        return TRunMetrics(False, False, len(self.load_storage.list_packages()))
