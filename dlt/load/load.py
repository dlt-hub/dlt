import contextlib
from functools import reduce
import datetime  # noqa: 251
from typing import Dict, List, Optional, Tuple, Set, Iterator, Iterable
from concurrent.futures import Executor
import os

from dlt.common import logger
from dlt.common.runtime.signals import sleep
from dlt.common.configuration import with_config, known_sections
from dlt.common.configuration.resolve import inject_section
from dlt.common.configuration.accessors import config
from dlt.common.pipeline import (
    LoadInfo,
    LoadMetrics,
    SupportsPipeline,
    WithStepInfo,
)
from dlt.common.schema.utils import get_top_level_table
from dlt.common.storages.load_storage import LoadPackageInfo, ParsedLoadJobFileName, TJobState
from dlt.common.storages.load_package import LoadPackageStateInjectableContext
from dlt.common.runners import TRunMetrics, Runnable, workermethod, NullExecutor
from dlt.common.runtime.collector import Collector, NULL_COLLECTOR
from dlt.common.logger import pretty_format_exception
from dlt.common.exceptions import TerminalValueError
from dlt.common.configuration.container import Container
from dlt.common.schema import Schema
from dlt.common.storages import LoadStorage
from dlt.common.destination.reference import (
    DestinationClientDwhConfiguration,
    FollowupJob,
    JobClientBase,
    WithStagingDataset,
    Destination,
    LoadJob,
    NewLoadJob,
    TLoadJobState,
    DestinationClientConfiguration,
    SupportsStagingDestination,
    TDestination,
)
from dlt.common.destination.exceptions import (
    DestinationTerminalException,
    DestinationTransientException,
)

from dlt.destinations.job_impl import EmptyLoadJob

from dlt.load.configuration import LoaderConfiguration
from dlt.load.exceptions import (
    LoadClientJobFailed,
    LoadClientJobRetry,
    LoadClientUnsupportedWriteDisposition,
    LoadClientUnsupportedFileFormats,
)
from dlt.load.utils import _extend_tables_with_table_chain, get_completed_table_chain, init_client


class Load(Runnable[Executor], WithStepInfo[LoadMetrics, LoadInfo]):
    pool: Executor

    @with_config(spec=LoaderConfiguration, sections=(known_sections.LOAD,))
    def __init__(
        self,
        destination: TDestination,
        staging_destination: TDestination = None,
        collector: Collector = NULL_COLLECTOR,
        is_storage_owner: bool = False,
        config: LoaderConfiguration = config.value,
        initial_client_config: DestinationClientConfiguration = config.value,
        initial_staging_client_config: DestinationClientConfiguration = config.value,
    ) -> None:
        self.config = config
        self.collector = collector
        self.initial_client_config = initial_client_config
        self.initial_staging_client_config = initial_staging_client_config
        self.destination = destination
        self.capabilities = destination.capabilities()
        self.staging_destination = staging_destination
        self.pool = NullExecutor()
        self.load_storage: LoadStorage = self.create_storage(is_storage_owner)
        self._loaded_packages: List[LoadPackageInfo] = []
        super().__init__()

    def create_storage(self, is_storage_owner: bool) -> LoadStorage:
        supported_file_formats = self.capabilities.supported_loader_file_formats
        if self.staging_destination:
            supported_file_formats = (
                self.staging_destination.capabilities().supported_loader_file_formats
            )
        load_storage = LoadStorage(
            is_storage_owner,
            supported_file_formats,
            config=self.config._load_storage_config,
        )
        # add internal job formats
        if issubclass(self.destination.client_class, WithStagingDataset):
            load_storage.supported_job_file_formats += ["sql"]
        if self.staging_destination:
            load_storage.supported_job_file_formats += ["reference"]

        return load_storage

    def get_destination_client(self, schema: Schema) -> JobClientBase:
        return self.destination.client(schema, self.initial_client_config)

    def get_staging_destination_client(self, schema: Schema) -> JobClientBase:
        return self.staging_destination.client(schema, self.initial_staging_client_config)

    def is_staging_destination_job(self, file_path: str) -> bool:
        return (
            self.staging_destination is not None
            and os.path.splitext(file_path)[1][1:]
            in self.staging_destination.capabilities().supported_loader_file_formats
        )

    @contextlib.contextmanager
    def maybe_with_staging_dataset(
        self, job_client: JobClientBase, use_staging: bool
    ) -> Iterator[None]:
        """Executes job client methods in context of staging dataset if `table` has `write_disposition` that requires it"""
        if isinstance(job_client, WithStagingDataset) and use_staging:
            with job_client.with_staging_dataset():
                yield
        else:
            yield

    @staticmethod
    @workermethod
    def w_spool_job(
        self: "Load", file_path: str, load_id: str, schema: Schema
    ) -> Optional[LoadJob]:
        job: LoadJob = None
        try:
            is_staging_destination_job = self.is_staging_destination_job(file_path)
            job_client = self.get_destination_client(schema)

            # if we have a staging destination and the file is not a reference, send to staging
            with (
                self.get_staging_destination_client(schema)
                if is_staging_destination_job
                else job_client
            ) as client:
                job_info = ParsedLoadJobFileName.parse(file_path)
                if job_info.file_format not in self.load_storage.supported_job_file_formats:
                    raise LoadClientUnsupportedFileFormats(
                        job_info.file_format,
                        self.capabilities.supported_loader_file_formats,
                        file_path,
                    )
                logger.info(f"Will load file {file_path} with table name {job_info.table_name}")
                table = client.prepare_load_table(job_info.table_name)
                if table["write_disposition"] not in ["append", "replace", "merge"]:
                    raise LoadClientUnsupportedWriteDisposition(
                        job_info.table_name, table["write_disposition"], file_path
                    )

                if is_staging_destination_job:
                    use_staging_dataset = isinstance(
                        job_client, SupportsStagingDestination
                    ) and job_client.should_load_data_to_staging_dataset_on_staging_destination(
                        table
                    )
                else:
                    use_staging_dataset = isinstance(
                        job_client, WithStagingDataset
                    ) and job_client.should_load_data_to_staging_dataset(table)

                with self.maybe_with_staging_dataset(client, use_staging_dataset):
                    job = client.start_file_load(
                        table,
                        self.load_storage.normalized_packages.storage.make_full_path(file_path),
                        load_id,
                    )
        except (DestinationTerminalException, TerminalValueError):
            # if job irreversibly cannot be started, mark it as failed
            logger.exception(f"Terminal problem when adding job {file_path}")
            job = EmptyLoadJob.from_file_path(file_path, "failed", pretty_format_exception())
        except (DestinationTransientException, Exception):
            # return no job so file stays in new jobs (root) folder
            logger.exception(f"Temporary problem when adding job {file_path}")
            job = EmptyLoadJob.from_file_path(file_path, "retry", pretty_format_exception())
        if job is None:
            raise DestinationTerminalException(
                f"Destination could not create a job for file {file_path}. Typically the file"
                " extension could not be associated with job type and that indicates an error in"
                " the code."
            )
        self.load_storage.normalized_packages.start_job(load_id, job.file_name())
        return job

    def spool_new_jobs(self, load_id: str, schema: Schema) -> Tuple[int, List[LoadJob]]:
        # use thread based pool as jobs processing is mostly I/O and we do not want to pickle jobs
        load_files = self.load_storage.list_new_jobs(load_id)[: self.config.workers]
        file_count = len(load_files)
        if file_count == 0:
            logger.info(f"No new jobs found in {load_id}")
            return 0, []
        logger.info(f"Will load {file_count}, creating jobs")
        param_chunk = [(id(self), file, load_id, schema) for file in load_files]
        # exceptions should not be raised, None as job is a temporary failure
        # other jobs should not be affected
        jobs = self.pool.map(Load.w_spool_job, *zip(*param_chunk))
        # remove None jobs and check the rest
        return file_count, [job for job in jobs if job is not None]

    def retrieve_jobs(
        self, client: JobClientBase, load_id: str, staging_client: JobClientBase = None
    ) -> Tuple[int, List[LoadJob]]:
        jobs: List[LoadJob] = []

        # list all files that were started but not yet completed
        started_jobs = self.load_storage.normalized_packages.list_started_jobs(load_id)

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

    def get_new_jobs_info(self, load_id: str) -> List[ParsedLoadJobFileName]:
        return [
            ParsedLoadJobFileName.parse(job_file)
            for job_file in self.load_storage.list_new_jobs(load_id)
        ]

    def create_followup_jobs(
        self, load_id: str, state: TLoadJobState, starting_job: LoadJob, schema: Schema
    ) -> List[NewLoadJob]:
        jobs: List[NewLoadJob] = []
        if isinstance(starting_job, FollowupJob):
            # check for merge jobs only for jobs executing on the destination, the staging destination jobs must be excluded
            # NOTE: we may move that logic to the interface
            starting_job_file_name = starting_job.file_name()
            if state == "completed" and not self.is_staging_destination_job(starting_job_file_name):
                client = self.destination.client(schema, self.initial_client_config)
                top_job_table = get_top_level_table(
                    schema.tables, starting_job.job_file_info().table_name
                )
                # if all tables of chain completed, create follow  up jobs
                all_jobs = self.load_storage.normalized_packages.list_all_jobs(load_id)
                if table_chain := get_completed_table_chain(
                    schema, all_jobs, top_job_table, starting_job.job_file_info().job_id()
                ):
                    if follow_up_jobs := client.create_table_chain_completed_followup_jobs(
                        table_chain
                    ):
                        jobs = jobs + follow_up_jobs
            jobs = jobs + starting_job.create_followup_jobs(state)
        return jobs

    def complete_jobs(self, load_id: str, jobs: List[LoadJob], schema: Schema) -> List[LoadJob]:
        """Run periodically in the main thread to collect job execution statuses.

        After detecting change of status, it commits the job state by moving it to the right folder
        May create one or more followup jobs that get scheduled as new jobs. New jobs are created
        only in terminal states (completed / failed)
        """
        remaining_jobs: List[LoadJob] = []

        def _schedule_followup_jobs(followup_jobs: Iterable[NewLoadJob]) -> None:
            for followup_job in followup_jobs:
                # running should be moved into "new jobs", other statuses into started
                folder: TJobState = (
                    "new_jobs" if followup_job.state() == "running" else "started_jobs"
                )
                # save all created jobs
                self.load_storage.normalized_packages.import_job(
                    load_id, followup_job.new_file_path(), job_state=folder
                )
                logger.info(
                    f"Job {job.job_id()} CREATED a new FOLLOWUP JOB"
                    f" {followup_job.new_file_path()} placed in {folder}"
                )
                # if followup job is not "running" place it in current queue to be finalized
                if not followup_job.state() == "running":
                    remaining_jobs.append(followup_job)

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
                # create followup jobs
                _schedule_followup_jobs(self.create_followup_jobs(load_id, state, job, schema))

                # try to get exception message from job
                failed_message = job.exception()
                self.load_storage.normalized_packages.fail_job(
                    load_id, job.file_name(), failed_message
                )
                logger.error(
                    f"Job for {job.job_id()} failed terminally in load {load_id} with message"
                    f" {failed_message}"
                )
            elif state == "retry":
                # try to get exception message from job
                retry_message = job.exception()
                # move back to new folder to try again
                self.load_storage.normalized_packages.retry_job(load_id, job.file_name())
                logger.warning(
                    f"Job for {job.job_id()} retried in load {load_id} with message {retry_message}"
                )
            elif state == "completed":
                # create followup jobs
                _schedule_followup_jobs(self.create_followup_jobs(load_id, state, job, schema))
                # move to completed folder after followup jobs are created
                # in case of exception when creating followup job, the loader will retry operation and try to complete again
                self.load_storage.normalized_packages.complete_job(load_id, job.file_name())
                logger.info(f"Job for {job.job_id()} completed in load {load_id}")

            if state in ["failed", "completed"]:
                self.collector.update("Jobs")
                if state == "failed":
                    self.collector.update(
                        "Jobs", 1, message="WARNING: Some of the jobs failed!", label="Failed"
                    )

        return remaining_jobs

    def complete_package(self, load_id: str, schema: Schema, aborted: bool = False) -> None:
        # do not commit load id for aborted packages
        if not aborted:
            with self.get_destination_client(schema) as job_client:
                with Container().injectable_context(
                    LoadPackageStateInjectableContext(
                        storage=self.load_storage.normalized_packages,
                        load_id=load_id,
                    )
                ):
                    job_client.complete_load(load_id)
                    self._maybe_trancate_staging_dataset(schema, job_client)

        self.load_storage.complete_load_package(load_id, aborted)
        # collect package info
        self._loaded_packages.append(self.load_storage.get_load_package_info(load_id))
        self._step_info_complete_load_id(load_id, metrics={"started_at": None, "finished_at": None})
        # delete jobs only now
        self.load_storage.maybe_remove_completed_jobs(load_id)
        logger.info(
            f"All jobs completed, archiving package {load_id} with aborted set to {aborted}"
        )

    def load_single_package(self, load_id: str, schema: Schema) -> None:
        new_jobs = self.get_new_jobs_info(load_id)
        # initialize analytical storage ie. create dataset required by passed schema
        with self.get_destination_client(schema) as job_client:
            if (expected_update := self.load_storage.begin_schema_update(load_id)) is not None:
                # init job client
                applied_update = init_client(
                    job_client,
                    schema,
                    new_jobs,
                    expected_update,
                    job_client.should_truncate_table_before_load,
                    (
                        job_client.should_load_data_to_staging_dataset
                        if isinstance(job_client, WithStagingDataset)
                        else None
                    ),
                )

                # init staging client
                if self.staging_destination:
                    assert isinstance(job_client, SupportsStagingDestination), (
                        f"Job client for destination {self.destination.destination_type} does not"
                        " implement SupportsStagingDestination"
                    )
                    with self.get_staging_destination_client(schema) as staging_client:
                        init_client(
                            staging_client,
                            schema,
                            new_jobs,
                            expected_update,
                            job_client.should_truncate_table_before_load_on_staging_destination,
                            job_client.should_load_data_to_staging_dataset_on_staging_destination,
                        )

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
        package_info = self.load_storage.normalized_packages.get_load_package_info(load_id)
        total_jobs = reduce(lambda p, c: p + len(c), package_info.jobs.values(), 0)
        no_failed_jobs = len(package_info.jobs["failed_jobs"])
        no_completed_jobs = len(package_info.jobs["completed_jobs"]) + no_failed_jobs
        self.collector.update("Jobs", no_completed_jobs, total_jobs)
        if no_failed_jobs > 0:
            self.collector.update(
                "Jobs", no_failed_jobs, message="WARNING: Some of the jobs failed!", label="Failed"
            )
        # loop until all jobs are processed
        while True:
            try:
                remaining_jobs = self.complete_jobs(load_id, jobs, schema)
                if len(remaining_jobs) == 0:
                    # get package status
                    package_info = self.load_storage.normalized_packages.get_load_package_info(
                        load_id
                    )
                    # possibly raise on failed jobs
                    if self.config.raise_on_failed_jobs:
                        if package_info.jobs["failed_jobs"]:
                            failed_job = package_info.jobs["failed_jobs"][0]
                            raise LoadClientJobFailed(
                                load_id,
                                failed_job.job_file_info.job_id(),
                                failed_job.failed_message,
                            )
                    # possibly raise on too many retries
                    if self.config.raise_on_max_retries:
                        for new_job in package_info.jobs["new_jobs"]:
                            r_c = new_job.job_file_info.retry_count
                            if r_c > 0 and r_c % self.config.raise_on_max_retries == 0:
                                raise LoadClientJobRetry(
                                    load_id,
                                    new_job.job_file_info.job_id(),
                                    r_c,
                                    self.config.raise_on_max_retries,
                                )
                    break
                # process remaining jobs again
                jobs = remaining_jobs
                # this will raise on signal
                sleep(1)
            except LoadClientJobFailed:
                # the package is completed and skipped
                self.complete_package(load_id, schema, True)
                raise

    def run(self, pool: Optional[Executor]) -> TRunMetrics:
        # store pool
        self.pool = pool or NullExecutor()

        logger.info("Running file loading")
        # get list of loads and order by name ASC to execute schema updates
        loads = self.load_storage.list_normalized_packages()
        logger.info(f"Found {len(loads)} load packages")
        if len(loads) == 0:
            return TRunMetrics(True, 0)

        # load the schema from the package
        load_id = loads[0]
        logger.info(f"Loading schema from load package in {load_id}")
        schema = self.load_storage.normalized_packages.load_schema(load_id)
        logger.info(f"Loaded schema name {schema.name} and version {schema.stored_version}")

        # get top load id and mark as being processed
        with self.collector(f"Load {schema.name} in {load_id}"):
            with Container().injectable_context(
                LoadPackageStateInjectableContext(
                    storage=self.load_storage.normalized_packages,
                    load_id=load_id,
                )
            ):
                # the same load id may be processed across multiple runs
                if not self.current_load_id:
                    self._step_info_start_load_id(load_id)
                self.load_single_package(load_id, schema)

        return TRunMetrics(False, len(self.load_storage.list_normalized_packages()))

    def _maybe_trancate_staging_dataset(self, schema: Schema, job_client: JobClientBase) -> None:
        """
        Truncate the staging dataset if one used,
        and configuration requests truncation.

        Args:
            schema (Schema): Schema to use for the staging dataset.
            job_client (JobClientBase):
                Job client to use for the staging dataset.
        """
        if not (
            isinstance(job_client, WithStagingDataset) and self.config.truncate_staging_dataset
        ):
            return

        data_tables = schema.data_table_names()
        tables = _extend_tables_with_table_chain(
            schema, data_tables, data_tables, job_client.should_load_data_to_staging_dataset
        )

        try:
            with self.get_destination_client(schema) as client:
                with client.with_staging_dataset():  # type: ignore
                    client.initialize_storage(truncate_tables=tables)

        except Exception as exc:
            logger.warn(
                f"Staging dataset truncate failed due to the following error: {exc}"
                " However, it didn't affect the data integrity."
            )

    def get_step_info(
        self,
        pipeline: SupportsPipeline,
    ) -> LoadInfo:
        # TODO: LoadInfo should hold many datasets
        load_ids = list(self._load_id_metrics.keys())
        metrics: Dict[str, List[LoadMetrics]] = {}
        # get load packages and dataset_name from the last package
        _dataset_name: str = None
        for load_package in self._loaded_packages:
            # TODO: each load id may have a separate dataset so construct a list of datasets here
            if isinstance(self.initial_client_config, DestinationClientDwhConfiguration):
                _dataset_name = self.initial_client_config.normalize_dataset_name(
                    load_package.schema
                )
            metrics[load_package.load_id] = self._step_info_metrics(load_package.load_id)

        return LoadInfo(
            pipeline,
            metrics,
            Destination.normalize_type(self.initial_client_config.destination_type),
            str(self.initial_client_config),
            self.initial_client_config.destination_name,
            self.initial_client_config.environment,
            (
                Destination.normalize_type(self.initial_staging_client_config.destination_type)
                if self.initial_staging_client_config
                else None
            ),
            (
                self.initial_staging_client_config.destination_name
                if self.initial_staging_client_config
                else None
            ),
            str(self.initial_staging_client_config) if self.initial_staging_client_config else None,
            self.initial_client_config.fingerprint(),
            _dataset_name,
            list(load_ids),
            self._loaded_packages,
            pipeline.first_run,
        )
