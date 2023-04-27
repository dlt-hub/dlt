from functools import reduce
import datetime  # noqa: 251
from typing import Dict, List, Optional, Tuple
from multiprocessing.pool import ThreadPool

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
from dlt.common.exceptions import TerminalValueError
from dlt.common.schema import Schema
from dlt.common.schema.typing import TTableSchema, TWriteDisposition
from dlt.common.storages import LoadStorage
from dlt.common.destination.reference import DestinationClientDwhConfiguration, FollowupJob, JobClientBase, DestinationReference, LoadJob, NewLoadJob, TLoadJobState, DestinationClientConfiguration

from dlt.destinations.job_impl import EmptyLoadJob
from dlt.destinations.exceptions import DestinationTerminalException, DestinationTransientException, LoadJobUnknownTableException

from dlt.load.configuration import LoaderConfiguration
from dlt.load.exceptions import LoadClientJobFailed, LoadClientJobRetry, LoadClientUnsupportedWriteDisposition, LoadClientUnsupportedFileFormats


class Load(Runnable[ThreadPool]):

    @with_config(spec=LoaderConfiguration, sections=(known_sections.LOAD,))
    def __init__(
        self,
        destination: DestinationReference,
        collector: Collector = NULL_COLLECTOR,
        is_storage_owner: bool = False,
        config: LoaderConfiguration = config.value,
        initial_client_config: DestinationClientConfiguration = config.value
    ) -> None:
        self.config = config
        self.collector = collector
        self.initial_client_config = initial_client_config
        self.destination = destination
        self.capabilities = destination.capabilities()
        self.pool: ThreadPool = None
        self.load_storage: LoadStorage = self.create_storage(is_storage_owner)
        self._processed_load_ids: Dict[str, int] = {}


    def create_storage(self, is_storage_owner: bool) -> LoadStorage:
        load_storage = LoadStorage(
            is_storage_owner,
            self.capabilities.preferred_loader_file_format,
            self.capabilities.supported_loader_file_formats,
            config=self.config._load_storage_config
        )
        return load_storage

    @staticmethod
    def get_load_table(schema: Schema, file_name: str) -> TTableSchema:
        table_name = LoadStorage.parse_job_file_name(file_name).table_name
        try:
            table = schema.get_table(table_name)
            # add write disposition if not specified - in child tables
            if "write_disposition" not in table:
                table["write_disposition"] = get_write_disposition(schema.tables, table_name)
            return table
        except KeyError:
            raise LoadJobUnknownTableException(table_name, file_name)

    @staticmethod
    @workermethod
    def w_spool_job(self: "Load", file_path: str, load_id: str, schema: Schema) -> Optional[LoadJob]:
        job: LoadJob = None
        try:
            with self.destination.client(schema, self.initial_client_config) as client:
                job_info = self.load_storage.parse_job_file_name(file_path)
                if job_info.file_format not in self.capabilities.supported_loader_file_formats:
                    raise LoadClientUnsupportedFileFormats(job_info.file_format, self.capabilities.supported_loader_file_formats, file_path)
                logger.info(f"Will load file {file_path} with table name {job_info.table_name}")
                table = self.get_load_table(schema, file_path)
                if table["write_disposition"] not in ["append", "replace", "merge"]:
                    raise LoadClientUnsupportedWriteDisposition(job_info.table_name, table["write_disposition"], file_path)
                job = client.start_file_load(table, self.load_storage.storage.make_full_path(file_path))
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
                job = EmptyLoadJob.from_file_path(file_path, "failed", pretty_format_exception())
                # proceed to appending job, do not reraise
            except (DestinationTransientException, Exception):
                # raise on all temporary exceptions, typically network / server problems
                raise
            jobs.append(job)

        return len(jobs), jobs

    def get_new_jobs_info(self, load_id: str, schema: Schema, disposition: TWriteDisposition = None) -> List[ParsedLoadJobFileName]:
        jobs_info: List[ParsedLoadJobFileName] = []
        new_job_files = self.load_storage.list_new_jobs(load_id)
        for job_file in new_job_files:
            if not disposition or self.get_load_table(schema, job_file)["write_disposition"] == disposition:
                jobs_info.append(LoadStorage.parse_job_file_name(job_file))
        return jobs_info

    def create_merge_job(self, load_id: str, schema: Schema, top_merged_table: TTableSchema, starting_job: LoadJob) -> NewLoadJob:
        # returns ordered list of tables from parent to child leaf tables
        table_chain: List[TTableSchema] = []
        # make sure all the jobs for the table chain is completed
        for table in get_child_tables(schema.tables, top_merged_table["name"]):
            table_jobs = self.load_storage.list_jobs_for_table(load_id, table["name"])
            # if no jobs for table then skip the table in the chain. we assume that if parent has no jobs, the child would also have no jobs
            # so it will be eliminated by this loop as well
            if not table_jobs:
                continue
            # all jobs must be completed in order for merge to be created
            if any(job.state not in ("failed_jobs", "completed_jobs") and job.job_file_info.job_id() != starting_job.job_file_info().job_id() for job in table_jobs):
                return None
            table_chain.append(table)
        # there must be at least 1 job
        assert len(table_chain) > 0
        # all tables completed, create merge sql job
        return self.destination.client(schema, self.initial_client_config).create_merge_job(table_chain)

    def create_followup_jobs(self, load_id: str, state: TLoadJobState, starting_job: LoadJob, schema: Schema) -> List[NewLoadJob]:
        jobs: List[NewLoadJob] = []
        if isinstance(starting_job, FollowupJob):
            if state == "completed":
                top_merged_table = get_top_level_table(schema.tables, self.get_load_table(schema, starting_job.file_name())["name"])
                if top_merged_table["write_disposition"] == "merge":
                    job = self.create_merge_job(load_id, schema, top_merged_table, starting_job)
                    if job:
                        jobs.append(job)
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
            with self.destination.client(schema, self.initial_client_config) as job_client:
                job_client.complete_load(load_id)
        self.load_storage.complete_load_package(load_id, aborted)
        logger.info(f"All jobs completed, archiving package {load_id} with aborted set to {aborted}")
        self._processed_load_ids[load_id] = 1

    def load_single_package(self, load_id: str, schema: Schema) -> None:
        # initialize analytical storage ie. create dataset required by passed schema
        job_client: JobClientBase
        with self.destination.client(schema, self.initial_client_config) as job_client:
            expected_update = self.load_storage.begin_schema_update(load_id)
            if expected_update is not None:
                # update the default dataset
                logger.info(f"Client for {job_client.config.destination_name} will start initialize storage")
                job_client.initialize_storage()
                logger.info(f"Client for {job_client.config.destination_name} will update schema to package schema")
                all_jobs = self.get_new_jobs_info(load_id, schema)
                all_tables = [job.table_name for job in all_jobs]
                dlt_tables = [t["name"] for t in schema.dlt_tables()]
                # only update tables that are present in the load package
                applied_update = job_client.update_storage_schema(only_tables=set(all_tables+dlt_tables), expected_update=expected_update)
                # update the staging dataset
                merge_jobs = self.get_new_jobs_info(load_id, schema, "merge")
                if merge_jobs:
                    logger.info(f"Client for {job_client.config.destination_name} will start initialize STAGING storage")
                    job_client.initialize_storage(staging=True)
                    logger.info(f"Client for {job_client.config.destination_name} will UPDATE STAGING SCHEMA to package schema")
                    merge_tables = [job.table_name for job in merge_jobs]
                    job_client.update_storage_schema(staging=True, only_tables=set(merge_tables+dlt_tables), expected_update=expected_update)
                    logger.info(f"Client for {job_client.config.destination_name} will TRUNCATE STAGING TABLES: {merge_tables}")
                    job_client.initialize_storage(staging=True, truncate_tables=merge_tables)
                self.load_storage.commit_schema_update(load_id, applied_update)
            # spool or retrieve unfinished jobs
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
        # TODO: Load must provide a clear interface to get last loads and metrics
        # TODO: LoadInfo should hold many datasets
        load_ids = list(self._processed_load_ids.keys())
        load_packages: List[LoadPackageInfo] = []
        for load_id in load_ids:
            load_packages.append(self.load_storage.get_load_package_info(load_id))
        dataset_name = None
        if isinstance(self.initial_client_config, DestinationClientDwhConfiguration):
            dataset_name = self.initial_client_config.dataset_name

        return LoadInfo(
            pipeline,
            self.initial_client_config.destination_name,
            str(self.initial_client_config.credentials),
            dataset_name,
            list(load_ids),
            load_packages,
            started_at,
            pipeline.first_run
        )
