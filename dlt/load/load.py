import contextlib
from copy import copy
from functools import reduce
import datetime  # noqa: 251
from typing import Dict, List, Optional, Tuple, Set, Iterator
from multiprocessing.pool import ThreadPool
import os

from dlt.common import sleep, logger
from dlt.common.configuration import with_config, known_sections
from dlt.common.configuration.accessors import config
from dlt.common.pipeline import LoadInfo, SupportsPipeline
from dlt.common.schema.utils import get_child_tables, get_top_level_table, get_write_disposition
from dlt.common.storages.load_storage import LoadPackageInfo, ParsedLoadJobFileName, TJobState
from dlt.common.typing import StrAny
from dlt.common.runners import TRunMetrics, Runnable, workermethod
from dlt.common.runtime.collector import Collector, NULL_COLLECTOR
from dlt.common.runtime.logger import pretty_format_exception
from dlt.common.exceptions import TerminalValueError, DestinationTerminalException, DestinationTransientException
from dlt.common.schema import Schema
from dlt.common.schema.typing import TTableSchema, TWriteDisposition
from dlt.common.storages import LoadStorage
from dlt.common.destination.reference import DestinationClientDwhConfiguration, FollowupJob, JobClientBase, WithStagingDataset, DestinationReference, LoadJob, NewLoadJob, TLoadJobState, DestinationClientConfiguration

from dlt.destinations.job_impl import EmptyLoadJob
from dlt.destinations.exceptions import LoadJobUnknownTableException

from dlt.load.configuration import LoaderConfiguration
from dlt.load.exceptions import LoadClientJobFailed, LoadClientJobRetry, LoadClientUnsupportedWriteDisposition, LoadClientUnsupportedFileFormats


class Load(Runnable[ThreadPool]):

    @with_config(spec=LoaderConfiguration, sections=(known_sections.LOAD,))
    def __init__(
        self,
        destination: DestinationReference,
        staging_destination: DestinationReference = None,
        collector: Collector = NULL_COLLECTOR,
        is_storage_owner: bool = False,
        config: LoaderConfiguration = config.value,
        initial_client_config: DestinationClientConfiguration = config.value,
        initial_staging_client_config: DestinationClientConfiguration = config.value
    ) -> None:
        self.config = config
        self.collector = collector
        self.initial_client_config = initial_client_config
        self.initial_staging_client_config = initial_staging_client_config
        self.destination = destination
        self.capabilities = destination.capabilities()
        self.staging_destination = staging_destination
        self.pool: ThreadPool = None
        self.load_storage: LoadStorage = self.create_storage(is_storage_owner)
        self._processed_load_ids: Dict[str, str] = {}
        """Load ids to dataset name"""


    def create_storage(self, is_storage_owner: bool) -> LoadStorage:
        supported_file_formats = self.capabilities.supported_loader_file_formats
        if self.staging_destination:
            supported_file_formats = self.staging_destination.capabilities().supported_loader_file_formats + ["reference"]
        if isinstance(self.get_destination_client(Schema("test")), WithStagingDataset):
            supported_file_formats += ["sql"]
        load_storage = LoadStorage(
            is_storage_owner,
            self.capabilities.preferred_loader_file_format,
            supported_file_formats,
            config=self.config._load_storage_config
        )
        return load_storage

    @staticmethod
    def get_load_table(schema: Schema, file_name: str) -> TTableSchema:
        table_name = LoadStorage.parse_job_file_name(file_name).table_name
        try:
            # make a copy of the schema so modifications do not affect the original document
            table = copy(schema.get_table(table_name))
            # add write disposition if not specified - in child tables
            if "write_disposition" not in table:
                table["write_disposition"] = get_write_disposition(schema.tables, table_name)
            return table
        except KeyError:
            raise LoadJobUnknownTableException(table_name, file_name)

    def get_destination_client(self, schema: Schema) -> JobClientBase:
        return self.destination.client(schema, self.initial_client_config)

    def get_staging_destination_client(self, schema: Schema) -> JobClientBase:
        return self.staging_destination.client(schema, self.initial_staging_client_config)

    def is_staging_destination_job(self, file_path: str) -> bool:
        return self.staging_destination is not None and os.path.splitext(file_path)[1][1:] in self.staging_destination.capabilities().supported_loader_file_formats

    @contextlib.contextmanager
    def maybe_with_staging_dataset(self, job_client: JobClientBase, table: TTableSchema) -> Iterator[None]:
        """Executes job client methods in context of staging dataset if `table` has `write_disposition` that requires it"""
        if isinstance(job_client, WithStagingDataset) and table["write_disposition"] in job_client.get_stage_dispositions():
            with job_client.with_staging_dataset():
                yield
        else:
            yield

    @staticmethod
    @workermethod
    def w_spool_job(self: "Load", file_path: str, load_id: str, schema: Schema) -> Optional[LoadJob]:
        job: LoadJob = None
        try:
            # if we have a staging destination and the file is not a reference, send to staging
            job_client = self.get_staging_destination_client(schema) if self.is_staging_destination_job(file_path) else self.get_destination_client(schema)
            with job_client as job_client:
                job_info = self.load_storage.parse_job_file_name(file_path)
                if job_info.file_format not in self.load_storage.supported_file_formats:
                    raise LoadClientUnsupportedFileFormats(job_info.file_format, self.capabilities.supported_loader_file_formats, file_path)
                logger.info(f"Will load file {file_path} with table name {job_info.table_name}")
                table = self.get_load_table(schema, file_path)
                if table["write_disposition"] not in ["append", "replace", "merge"]:
                    raise LoadClientUnsupportedWriteDisposition(job_info.table_name, table["write_disposition"], file_path)
                with self.maybe_with_staging_dataset(job_client, table):
                    job = job_client.start_file_load(table, self.load_storage.storage.make_full_path(file_path), load_id)
        except (DestinationTerminalException, TerminalValueError):
            # if job irreversibly cannot be started, mark it as failed
            logger.exception(f"Terminal problem when adding job {file_path}")
            job = EmptyLoadJob.from_file_path(file_path, "failed", pretty_format_exception())
        except (DestinationTransientException, Exception):
            # return no job so file stays in new jobs (root) folder
            logger.exception(f"Temporary problem when adding job {file_path}")
            job = EmptyLoadJob.from_file_path(file_path, "retry", pretty_format_exception())
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

    def retrieve_jobs(self, client: JobClientBase, load_id: str, staging_client: JobClientBase = None) -> Tuple[int, List[LoadJob]]:
        jobs: List[LoadJob] = []

        # list all files that were started but not yet completed
        started_jobs = self.load_storage.list_started_jobs(load_id)

        logger.info(f"Found {len(started_jobs)} that are already started and should be continued")
        if len(started_jobs) == 0:
            return 0, jobs

        for file_path in started_jobs:
            try:
                logger.info(f"Will retrieve {file_path}")
                client = staging_client if self.is_staging_destination_job(file_path) else client
                job = client.restore_file_load(file_path)
            except DestinationTerminalException:
                logger.exception(f"Job retrieval for {file_path} failed, job will be terminated")
                job = EmptyLoadJob.from_file_path(file_path, "failed", pretty_format_exception())
                # proceed to appending job, do not reraise
            except (DestinationTransientException, Exception):
                # raise on all temporary exceptions, typically network / server problems
                raise
            jobs.append(job)

        return len(jobs), jobs

    def get_new_jobs_info(self, load_id: str, schema: Schema, dispositions: List[TWriteDisposition] = None) -> List[ParsedLoadJobFileName]:
        jobs_info: List[ParsedLoadJobFileName] = []
        new_job_files = self.load_storage.list_new_jobs(load_id)
        for job_file in new_job_files:
            if dispositions is None or self.get_load_table(schema, job_file)["write_disposition"] in dispositions:
                jobs_info.append(LoadStorage.parse_job_file_name(job_file))
        return jobs_info

    def get_completed_table_chain(self, load_id: str, schema: Schema, top_merged_table: TTableSchema, being_completed_job_id: str = None) -> List[TTableSchema]:
        """Gets a table chain starting from the `top_merged_table` containing only tables with completed/failed jobs. None is returned if there's any job that is not completed

           Optionally `being_completed_job_id` can be passed that is considered to be completed before job itself moves in storage
        """
        # returns ordered list of tables from parent to child leaf tables
        table_chain: List[TTableSchema] = []
        # make sure all the jobs for the table chain is completed
        for table in get_child_tables(schema.tables, top_merged_table["name"]):
            table_jobs = self.load_storage.list_jobs_for_table(load_id, table["name"])
            # all jobs must be completed in order for merge to be created
            if any(job.state not in ("failed_jobs", "completed_jobs") and job.job_file_info.job_id() != being_completed_job_id for job in table_jobs):
                return None
            # if there are no jobs for the table, skip it, unless the write disposition is replace, as we need to create and clear the child tables
            if not table_jobs and top_merged_table["write_disposition"] != "replace":
                 continue
            table_chain.append(table)
        # there must be at least table
        assert len(table_chain) > 0
        return table_chain

    def create_followup_jobs(self, load_id: str, state: TLoadJobState, starting_job: LoadJob, schema: Schema) -> List[NewLoadJob]:
        jobs: List[NewLoadJob] = []
        if isinstance(starting_job, FollowupJob):
            # check for merge jobs only for jobs executing on the destination, the staging destination jobs must be excluded
            # NOTE: we may move that logic to the interface
            starting_job_file_name = starting_job.file_name()
            if state == "completed" and not self.is_staging_destination_job(starting_job_file_name):
                client = self.destination.client(schema, self.initial_client_config)
                top_job_table = get_top_level_table(schema.tables, self.get_load_table(schema, starting_job_file_name)["name"])
                # if all tables of chain completed, create follow  up jobs
                if table_chain := self.get_completed_table_chain(load_id, schema, top_job_table, starting_job.job_file_info().job_id()):
                    if follow_up_jobs := client.create_table_chain_completed_followup_jobs(table_chain):
                        jobs = jobs + follow_up_jobs
            jobs = jobs + starting_job.create_followup_jobs(state)
        return jobs

    def complete_jobs(self, load_id: str, jobs: List[LoadJob], schema: Schema) -> List[LoadJob]:
        remaining_jobs: List[LoadJob] = []
        logger.info(f"Will complete {len(jobs)} for {load_id}")
        for ii in range(len(jobs)):
            job = jobs[ii]
            logger.debug(f"Checking state for job {job.job_id()}")
            state: TLoadJobState = job.state()
            if state == "running":
                # ask again
                logger.debug(f"job {job.job_id()} still running")
                remaining_jobs.append(job)
            elif state == "failed":
                # try to get exception message from job
                failed_message = job.exception()
                self.load_storage.fail_job(load_id, job.file_name(), failed_message)
                logger.error(f"Job for {job.job_id()} failed terminally in load {load_id} with message {failed_message}")
            elif state == "retry":
                # try to get exception message from job
                retry_message = job.exception()
                # move back to new folder to try again
                self.load_storage.retry_job(load_id, job.file_name())
                logger.warning(f"Job for {job.job_id()} retried in load {load_id} with message {retry_message}")
            elif state == "completed":
                # create followup jobs
                followup_jobs = self.create_followup_jobs(load_id, state, job, schema)
                for followup_job in followup_jobs:
                    # running should be moved into "new jobs", other statuses into started
                    folder: TJobState = "new_jobs" if followup_job.state() == "running" else "started_jobs"
                    # save all created jobs
                    self.load_storage.add_new_job(load_id, followup_job.new_file_path(), job_state=folder)
                    logger.info(f"Job {job.job_id()} CREATED a new FOLLOWUP JOB {followup_job.new_file_path()} placed in {folder}")
                    # if followup job is not "running" place it in current queue to be finalized
                    if not followup_job.state() == "running":
                        remaining_jobs.append(followup_job)
                # move to completed folder after followup jobs are created
                # in case of exception when creating followup job, the loader will retry operation and try to complete again
                self.load_storage.complete_job(load_id, job.file_name())
                logger.info(f"Job for {job.job_id()} completed in load {load_id}")

            if state in ["failed", "completed"]:
                self.collector.update("Jobs")
                if state == "failed":
                    self.collector.update("Jobs", 1, message="WARNING: Some of the jobs failed!", label="Failed")

        return remaining_jobs

    def complete_package(self, load_id: str, schema: Schema, aborted: bool = False) -> None:
        # do not commit load id for aborted packages
        if not aborted:
            with self.get_destination_client(schema) as job_client:
                job_client.complete_load(load_id)
                # TODO: Load must provide a clear interface to get last loads and metrics
                # TODO: get more info ie. was package aborted, schema name etc.
                if isinstance(job_client.config, DestinationClientDwhConfiguration):
                    self._processed_load_ids[load_id] = job_client.config.normalize_dataset_name(schema)
                else:
                    self._processed_load_ids[load_id] = None
        self.load_storage.complete_load_package(load_id, aborted)
        logger.info(f"All jobs completed, archiving package {load_id} with aborted set to {aborted}")

    def get_table_chain_tables_for_write_disposition(self, load_id: str, schema: Schema, dispositions: List[TWriteDisposition]) -> Set[str]:
        """Get all jobs for tables with given write disposition and resolve the table chain"""
        result: Set[str] = set()
        table_jobs = self.get_new_jobs_info(load_id, schema, dispositions)
        for job in table_jobs:
            top_job_table = get_top_level_table(schema.tables, self.get_load_table(schema, job.job_id())["name"])
            table_chain = get_child_tables(schema.tables, top_job_table["name"])
            for table in table_chain:
                existing_jobs = self.load_storage.list_jobs_for_table(load_id, table["name"])
                # only add tables for tables that have jobs unless the disposition is replace
                if not existing_jobs and top_job_table["write_disposition"] != "replace":
                    continue
                result.add(table["name"])
        return result

    def load_single_package(self, load_id: str, schema: Schema) -> None:
        # initialize analytical storage ie. create dataset required by passed schema
        job_client: JobClientBase
        with self.get_destination_client(schema) as job_client:
            expected_update = self.load_storage.begin_schema_update(load_id)
            if expected_update is not None:
                # update the default dataset
                logger.info(f"Client for {job_client.config.destination_name} will start initialize storage")
                job_client.initialize_storage()
                logger.info(f"Client for {job_client.config.destination_name} will update schema to package schema")
                all_jobs = self.get_new_jobs_info(load_id, schema)
                all_tables = set(job.table_name for job in all_jobs)
                dlt_tables = set(t["name"] for t in schema.dlt_tables())
                # only update tables that are present in the load package
                applied_update = job_client.update_stored_schema(only_tables=all_tables | dlt_tables, expected_update=expected_update)
                truncate_tables = self.get_table_chain_tables_for_write_disposition(load_id, schema, job_client.get_truncate_destination_table_dispositions())
                job_client.initialize_storage(truncate_tables=truncate_tables)
                # initialize staging storage if needed
                if self.staging_destination:
                    with self.get_staging_destination_client(schema) as staging_client:
                        truncate_tables = self.get_table_chain_tables_for_write_disposition(load_id, schema, staging_client.get_truncate_destination_table_dispositions())
                        staging_client.initialize_storage(truncate_tables)
                # update the staging dataset if client supports this
                if isinstance(job_client, WithStagingDataset):
                    if staging_tables := self.get_table_chain_tables_for_write_disposition(load_id, schema, job_client.get_stage_dispositions()):
                        with job_client.with_staging_dataset():
                            logger.info(f"Client for {job_client.config.destination_name} will start initialize STAGING storage")
                            job_client.initialize_storage()
                            logger.info(f"Client for {job_client.config.destination_name} will UPDATE STAGING SCHEMA to package schema")
                            job_client.update_stored_schema(only_tables=staging_tables | {schema.version_table_name}, expected_update=expected_update)
                            logger.info(f"Client for {job_client.config.destination_name} will TRUNCATE STAGING TABLES: {staging_tables}")
                            job_client.initialize_storage(truncate_tables=staging_tables)
                self.load_storage.commit_schema_update(load_id, applied_update)
            # initialize staging destination and spool or retrieve unfinished jobs
            if self.staging_destination:
                with self.get_staging_destination_client(schema) as staging_client:
                    jobs_count, jobs = self.retrieve_jobs(job_client, load_id, staging_client)
            else:
                jobs_count, jobs = self.retrieve_jobs(job_client, load_id)

        if not jobs:
            # jobs count is a total number of jobs including those that could not be initialized
            jobs_count, jobs = self.spool_new_jobs(load_id, schema)
        # if there are no existing or new jobs we complete the package
        if jobs_count == 0:
            self.complete_package(load_id, schema, False)
            return
        # update counter we only care about the jobs that are scheduled to be loaded
        package_info = self.load_storage.get_load_package_info(load_id)
        total_jobs = reduce(lambda p, c: p + len(c), package_info.jobs.values(), 0)
        no_failed_jobs = len(package_info.jobs["failed_jobs"])
        no_completed_jobs = len(package_info.jobs["completed_jobs"]) + no_failed_jobs
        self.collector.update("Jobs", no_completed_jobs, total_jobs)
        if no_failed_jobs > 0:
            self.collector.update("Jobs", no_failed_jobs, message="WARNING: Some of the jobs failed!", label="Failed")
        # loop until all jobs are processed
        while True:
            try:
                remaining_jobs = self.complete_jobs(load_id, jobs, schema)
                if len(remaining_jobs) == 0:
                    # get package status
                    package_info = self.load_storage.get_load_package_info(load_id)
                    # possibly raise on failed jobs
                    if self.config.raise_on_failed_jobs:
                        if package_info.jobs["failed_jobs"]:
                            failed_job = package_info.jobs["failed_jobs"][0]
                            raise LoadClientJobFailed(load_id, failed_job.job_file_info.job_id(), failed_job.failed_message)
                    # possibly raise on too many retires
                    if self.config.raise_on_max_retries:
                        for new_job in package_info.jobs["new_jobs"]:
                            r_c = new_job.job_file_info.retry_count
                            if r_c > 0 and r_c % self.config.raise_on_max_retries == 0:
                                raise LoadClientJobRetry(load_id, new_job.job_file_info.job_id(), r_c, self.config.raise_on_max_retries)
                    break
                # process remaining jobs again
                jobs = remaining_jobs
                # this will raise on signal
                sleep(1)
            except LoadClientJobFailed:
                # the package is completed and skipped
                self.complete_package(load_id, schema, True)
                raise

    def run(self, pool: ThreadPool) -> TRunMetrics:
        # store pool
        self.pool = pool

        logger.info("Running file loading")
        # get list of loads and order by name ASC to execute schema updates
        loads = self.load_storage.list_packages()
        logger.info(f"Found {len(loads)} load packages")
        if len(loads) == 0:
            return TRunMetrics(True, 0)

        # load the schema from the package
        load_id = loads[0]
        logger.info(f"Loading schema from load package in {load_id}")
        schema = self.load_storage.load_package_schema(load_id)
        logger.info(f"Loaded schema name {schema.name} and version {schema.stored_version}")

        # get top load id and mark as being processed
        # TODO: another place where tracing must be refactored
        self._processed_load_ids[load_id] = None
        with self.collector(f"Load {schema.name} in {load_id}"):
            self.load_single_package(load_id, schema)

        return TRunMetrics(False, len(self.load_storage.list_packages()))

    def get_load_info(self, pipeline: SupportsPipeline, started_at: datetime.datetime = None) -> LoadInfo:
        # TODO: LoadInfo should hold many datasets
        load_ids = list(self._processed_load_ids.keys())
        load_packages: List[LoadPackageInfo] = []
        # get load packages and dataset_name from the last package
        _dataset_name: str = None
        for load_id, _dataset_name in self._processed_load_ids.items():
            load_packages.append(self.load_storage.get_load_package_info(load_id))

        return LoadInfo(
            pipeline,
            self.initial_client_config.destination_name,
            str(self.initial_client_config),
            self.initial_staging_client_config.destination_name if self.initial_staging_client_config else None,
            str(self.initial_staging_client_config) if self.initial_staging_client_config else None,
            self.initial_client_config.fingerprint(),
            _dataset_name,
            list(load_ids),
            load_packages,
            started_at,
            pipeline.first_run
        )
