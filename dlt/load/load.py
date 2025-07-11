import contextlib
from functools import reduce
from typing import Dict, List, Optional, Tuple, Iterator, Sequence
from concurrent.futures import Executor
import os

from dlt.common import logger
from dlt.common.exceptions import TerminalException
from dlt.common.metrics import LoadJobMetrics
from dlt.common.runtime.signals import sleep
from dlt.common.configuration import with_config, known_sections
from dlt.common.configuration.accessors import config
from dlt.common.pipeline import LoadInfo, LoadMetrics, SupportsPipeline, WithStepInfo
from dlt.common.schema.utils import get_root_table
from dlt.common.storages.load_storage import (
    LoadPackageInfo,
    ParsedLoadJobFileName,
    TPackageJobState,
)
from dlt.common.storages.load_package import (
    LoadPackageStateInjectableContext,
    load_package_state as current_load_package,
)
from dlt.common.runners import TRunMetrics, Runnable, workermethod, NullExecutor
from dlt.common.runtime.collector import Collector, NULL_COLLECTOR
from dlt.common.logger import pretty_format_exception
from dlt.common.configuration.container import Container
from dlt.common.schema import Schema
from dlt.common.storages import LoadStorage
from dlt.common.destination import DestinationReference, AnyDestination, Destination
from dlt.common.destination.client import (
    DestinationClientDwhConfiguration,
    HasFollowupJobs,
    JobClientBase,
    WithStagingDataset,
    RunnableLoadJob,
    LoadJob,
    FollowupJobRequest,
    TLoadJobState,
    DestinationClientConfiguration,
    SupportsStagingDestination,
)
from dlt.common.destination.exceptions import (
    DestinationTerminalException,
)
from dlt.common.runtime import signals

from dlt.destinations.job_impl import FinalizedLoadJobWithFollowupJobs

from dlt.load.configuration import LoaderConfiguration
from dlt.load.exceptions import (
    LoadClientJobFailed,
    LoadClientJobRetry,
    LoadClientUnsupportedWriteDisposition,
    LoadClientUnsupportedFileFormats,
    LoadClientJobException,
    FollowupJobCreationFailedException,
    TableChainFollowupJobCreationFailedException,
)
from dlt.load.utils import (
    _extend_tables_with_table_chain,
    get_completed_table_chain,
    init_client,
    filter_new_jobs,
    get_available_worker_slots,
)


class Load(Runnable[Executor], WithStepInfo[LoadMetrics, LoadInfo]):
    pool: Executor

    @with_config(spec=LoaderConfiguration, sections=(known_sections.LOAD,))
    def __init__(
        self,
        destination: AnyDestination,
        staging_destination: AnyDestination = None,
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
        self.staging_destination = staging_destination
        self.pool = NullExecutor()
        self.load_storage: LoadStorage = self.create_storage(is_storage_owner)
        self._loaded_packages: List[LoadPackageInfo] = []
        self._job_metrics: Dict[str, LoadJobMetrics] = {}
        self._run_loop_sleep_duration: float = (
            1.0  # amount of time to sleep between querying completed jobs
        )
        super().__init__()

    def create_storage(self, is_storage_owner: bool) -> LoadStorage:
        supported_file_formats = self.destination.capabilities().supported_loader_file_formats
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
        job_info = ParsedLoadJobFileName.parse(file_path)
        file_type = job_info.file_format
        # for now we know that reference and model jobs always go do the main destination
        if file_type in ["reference", "model"]:
            return False
        return (
            self.staging_destination is not None
            and file_type in self.staging_destination.capabilities().supported_loader_file_formats
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

    def submit_job(
        self, file_path: str, load_id: str, schema: Schema, restore: bool = False
    ) -> LoadJob:
        job: LoadJob = None

        is_staging_destination_job = self.is_staging_destination_job(file_path)
        job_client = self.get_destination_client(schema)

        # if we have a staging destination and the file is not a reference, send to staging
        active_job_client = (
            self.get_staging_destination_client(schema)
            if is_staging_destination_job
            else job_client
        )

        try:
            # check file format
            job_info = ParsedLoadJobFileName.parse(file_path)
            if job_info.file_format not in self.load_storage.supported_job_file_formats:
                raise LoadClientUnsupportedFileFormats(
                    job_info.file_format,
                    self.destination.capabilities().supported_loader_file_formats,
                    file_path,
                )
            logger.info(f"Will load file {file_path} with table name {job_info.table_name}")

            # determine which dataset to use
            if is_staging_destination_job:
                use_staging_dataset = isinstance(
                    job_client, SupportsStagingDestination
                ) and job_client.should_load_data_to_staging_dataset_on_staging_destination(
                    job_info.table_name
                )
            else:
                use_staging_dataset = isinstance(
                    job_client, WithStagingDataset
                ) and job_client.should_load_data_to_staging_dataset(job_info.table_name)

            # prepare table to be loaded
            load_table = active_job_client.prepare_load_table(job_info.table_name)
            if load_table["write_disposition"] not in ["append", "replace", "merge"]:
                raise LoadClientUnsupportedWriteDisposition(
                    job_info.table_name, load_table["write_disposition"], file_path
                )
            job = active_job_client.create_load_job(
                load_table,
                self.load_storage.normalized_packages.storage.make_full_path(file_path),
                load_id,
                restore=restore,
            )
            if job is None:
                raise DestinationTerminalException(
                    f"Destination could not create a job for file {file_path}. Typically the file"
                    " extension could not be associated with job type and that indicates an error"
                    " in the code."
                )
        except (TerminalException, AssertionError):
            job = FinalizedLoadJobWithFollowupJobs.from_file_path(
                file_path, "failed", pretty_format_exception()
            )
        except Exception:
            job = FinalizedLoadJobWithFollowupJobs.from_file_path(
                file_path, "retry", pretty_format_exception()
            )

        # move to started jobs in case this is not a restored job
        if not restore:
            job._file_path = self.load_storage.normalized_packages.start_job(
                load_id, job.file_name()
            )

        # only start a thread if this job is runnable
        if isinstance(job, RunnableLoadJob):
            # set job vars
            job.set_run_vars(load_id=load_id, schema=schema, load_table=load_table)
            # submit to pool
            self.pool.submit(Load.w_run_job, *(id(self), job, is_staging_destination_job, use_staging_dataset, schema))  # type: ignore

        # sanity check: otherwise a job in an actionable state is expected
        else:
            assert job.state() in ("completed", "failed", "retry")

        return job

    @staticmethod
    @workermethod
    def w_run_job(
        self: "Load",
        job: RunnableLoadJob,
        use_staging_client: bool,
        use_staging_dataset: bool,
        schema: Schema,
    ) -> None:
        """
        Start a load job in a separate thread
        """
        active_job_client = (
            self.get_staging_destination_client(schema)
            if use_staging_client
            else self.get_destination_client(schema)
        )
        with active_job_client as client:
            with self.maybe_with_staging_dataset(client, use_staging_dataset):
                job.run_managed(active_job_client)

    def start_new_jobs(
        self, load_id: str, schema: Schema, running_jobs: Sequence[LoadJob]
    ) -> Sequence[LoadJob]:
        """
        will retrieve jobs from the new_jobs folder and start as many as there are slots available
        """
        caps = self.destination.capabilities(
            self.destination.configuration(self.initial_client_config)
        )

        # early exit if no slots available
        available_slots = get_available_worker_slots(self.config, caps, running_jobs)
        if available_slots <= 0:
            return []

        # get a list of jobs eligible to be started
        load_files = filter_new_jobs(
            self.load_storage.list_new_jobs(load_id),
            caps,
            self.config,
            running_jobs,
            available_slots,
        )

        logger.info(f"Will load additional {len(load_files)}, creating jobs")
        started_jobs: List[LoadJob] = []
        for file in load_files:
            job = self.submit_job(file, load_id, schema)
            started_jobs.append(job)

        return started_jobs

    def resume_started_jobs(self, load_id: str, schema: Schema) -> List[LoadJob]:
        """
        will check jobs in the started folder and resume them
        """
        jobs: List[LoadJob] = []

        # list all files that were started but not yet completed
        started_jobs = self.load_storage.normalized_packages.list_started_jobs(load_id)

        logger.info(f"{len(started_jobs)} started jobs found, which should be continued")
        if len(started_jobs) == 0:
            return jobs

        for file_path in started_jobs:
            job = self.submit_job(file_path, load_id, schema, restore=True)
            jobs.append(job)

        return jobs

    def get_new_jobs_info(self, load_id: str) -> List[ParsedLoadJobFileName]:
        return [
            ParsedLoadJobFileName.parse(job_file)
            for job_file in self.load_storage.list_new_jobs(load_id)
        ]

    def create_followup_jobs(
        self, load_id: str, state: TLoadJobState, starting_job: LoadJob, schema: Schema
    ) -> None:
        """
        for jobs marked as having followup jobs, find them all and store them to the new jobs folder
        where they will be picked up for execution
        """

        jobs: List[FollowupJobRequest] = []
        if isinstance(starting_job, HasFollowupJobs):
            # check for merge jobs only for jobs executing on the destination, the staging destination jobs must be excluded
            # NOTE: we may move that logic to the interface
            starting_job_file_name = starting_job.file_name()
            if state == "completed" and not self.is_staging_destination_job(starting_job_file_name):
                client = self.destination.client(schema, self.initial_client_config)
                root_job_table = get_root_table(
                    schema.tables, starting_job.job_file_info().table_name
                )
                # if all tables of chain completed, create follow up jobs
                all_jobs_states = self.load_storage.normalized_packages.list_all_jobs_with_states(
                    load_id
                )
                if table_chain := get_completed_table_chain(
                    schema, all_jobs_states, root_job_table, starting_job.job_file_info().job_id()
                ):
                    table_chain_names = [table["name"] for table in table_chain]
                    # all tables will be prepared for main dataset
                    prep_table_chain = [
                        client.prepare_load_table(table_name) for table_name in table_chain_names
                    ]
                    table_chain_jobs = [
                        # we mark all jobs as completed, as by the time the followup job runs the starting job will be in this
                        # folder too
                        self.load_storage.normalized_packages.job_to_job_info(
                            load_id, "completed_jobs", job_state[1]
                        )
                        for job_state in all_jobs_states
                        if job_state[1].table_name in table_chain_names
                        # job being completed is still in started_jobs
                        and job_state[0] in ("completed_jobs", "started_jobs")
                    ]
                    try:
                        if follow_up_jobs := client.create_table_chain_completed_followup_jobs(
                            prep_table_chain, table_chain_jobs
                        ):
                            jobs = jobs + follow_up_jobs
                    except Exception as e:
                        raise TableChainFollowupJobCreationFailedException(
                            root_table_name=prep_table_chain[0]["name"]
                        ) from e

            try:
                jobs = jobs + starting_job.create_followup_jobs(state)
            except Exception as e:
                raise FollowupJobCreationFailedException(job_id=starting_job.job_id()) from e

        # import all followup jobs to the new jobs folder
        for followup_job in jobs:
            # save all created jobs
            self.load_storage.normalized_packages.import_job(
                load_id, followup_job.new_file_path(), job_state="new_jobs"
            )
            logger.info(
                f"Job {starting_job.job_id()} CREATED a new FOLLOWUP JOB"
                f" {followup_job.new_file_path()} placed in new_jobs"
            )
        self.collector.update("Jobs", inc=0, inc_total=len(jobs))

    def complete_jobs(
        self, load_id: str, jobs: Sequence[LoadJob], schema: Schema
    ) -> Tuple[List[LoadJob], List[LoadJob], Optional[LoadClientJobException]]:
        """Run periodically in the main thread to collect job execution statuses.

        After detecting change of status, it commits the job state by moving it to the right folder
        May create one or more followup jobs that get scheduled as new jobs. New jobs are created
        only in terminal states (completed / failed)
        """
        # list of jobs still running
        remaining_jobs: List[LoadJob] = []
        # list of jobs in final state
        finalized_jobs: List[LoadJob] = []
        # if an exception condition was met, return it to the main runner
        pending_exception: Optional[LoadClientJobException] = None

        logger.info(f"Will complete {len(jobs)} for {load_id}")
        for ii in range(len(jobs)):
            job = jobs[ii]
            logger.debug(f"Checking state for job {job.job_id()}")
            state: TLoadJobState = job.state()
            if state in ("ready", "running"):
                # ask again
                logger.debug(f"job {job.job_id()} still running")
                remaining_jobs.append(job)
            elif state == "failed":
                # create followup jobs
                self.create_followup_jobs(load_id, state, job, schema)

                # preserve metrics
                metrics = job.metrics()
                if metrics:
                    self._job_metrics[job.job_id()] = metrics

                # try to get exception message from job
                failed_message = job.exception()
                self.load_storage.normalized_packages.fail_job(
                    load_id, job.file_name(), failed_message
                )
                logger.error(
                    f"Job for {job.job_id()} failed terminally in load {load_id} with message"
                    f" {failed_message}"
                )
                # schedule exception on job failure
                if self.config.raise_on_failed_jobs:
                    pending_exception = LoadClientJobFailed(
                        load_id,
                        job.job_file_info().job_id(),
                        failed_message,
                    )
                finalized_jobs.append(job)
            elif state == "retry":
                # try to get exception message from job
                retry_message = job.exception()
                # move back to new folder to try again
                self.load_storage.normalized_packages.retry_job(load_id, job.file_name())
                logger.warning(
                    f"Job for {job.job_id()} retried in load {load_id} with message {retry_message}"
                )
                # possibly schedule exception on too many retries
                if self.config.raise_on_max_retries:
                    r_c = job.job_file_info().retry_count + 1
                    if r_c > 0 and r_c % self.config.raise_on_max_retries == 0:
                        pending_exception = LoadClientJobRetry(
                            load_id,
                            job.job_id(),
                            r_c,
                            self.config.raise_on_max_retries,
                            retry_message=retry_message,
                        )
            elif state == "completed":
                # create followup jobs
                self.create_followup_jobs(load_id, state, job, schema)

                # preserve metrics
                # TODO: metrics should be persisted. this is different vs. all other steps because load step
                # may be restarted in the middle of execution
                # NOTE: we could use package state but cases with 100k jobs must be tested
                metrics = job.metrics()
                if metrics:
                    self._job_metrics[job.job_id()] = metrics

                # move to completed folder after followup jobs are created
                # in case of exception when creating followup job, the loader will retry operation and try to complete again
                self.load_storage.normalized_packages.complete_job(load_id, job.file_name())
                logger.info(f"Job for {job.job_id()} completed in load {load_id}")
                finalized_jobs.append(job)
            else:
                raise Exception("Incorrect job state")

            if state in ["failed", "completed"]:
                self.collector.update("Jobs")
                if state == "failed":
                    self.collector.update(
                        "Jobs", 1, message="WARNING: Some of the jobs failed!", label="Failed"
                    )

        return remaining_jobs, finalized_jobs, pending_exception

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
                    self._maybe_truncate_staging_dataset(schema, job_client)

        self.load_storage.complete_load_package(load_id, aborted)
        # collect package info
        self._loaded_packages.append(self.load_storage.get_load_package_info(load_id))
        # TODO: job metrics must be persisted
        self._step_info_complete_load_id(
            load_id,
            metrics={"started_at": None, "finished_at": None, "job_metrics": self._job_metrics},
        )
        # delete jobs only now
        self.load_storage.maybe_remove_completed_jobs(load_id)
        logger.info(
            f"All jobs completed, archiving package {load_id} with aborted set to {aborted}"
        )

    def init_jobs_counter(self, load_id: str) -> None:
        # update counter we only care about the jobs that are scheduled to be loaded
        package_jobs = self.load_storage.normalized_packages.get_load_package_jobs(load_id)
        total_jobs = reduce(lambda p, c: p + len(c), package_jobs.values(), 0)
        no_failed_jobs = len(package_jobs["failed_jobs"])
        no_completed_jobs = len(package_jobs["completed_jobs"]) + no_failed_jobs
        self.collector.update("Jobs", no_completed_jobs, total_jobs)
        if no_failed_jobs > 0:
            self.collector.update(
                "Jobs", no_failed_jobs, message="WARNING: Some of the jobs failed!", label="Failed"
            )

    def load_single_package(self, load_id: str, schema: Schema) -> None:
        new_jobs = self.get_new_jobs_info(load_id)

        # get dropped and truncated tables that were added in the extract step if refresh was requested
        # NOTE: if naming convention was updated those names correspond to the old naming convention
        # and they must be like that in order to drop existing tables
        dropped_tables = current_load_package()["state"].get("dropped_tables", [])
        truncated_tables = current_load_package()["state"].get("truncated_tables", [])

        self.init_jobs_counter(load_id)

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
                    drop_tables=dropped_tables,
                    truncate_tables=truncated_tables,
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
                            # should_truncate_staging,
                            job_client.should_load_data_to_staging_dataset_on_staging_destination,
                            drop_tables=dropped_tables,
                            truncate_tables=truncated_tables,
                        )
                self.load_storage.commit_schema_update(load_id, applied_update)

            # collect all unfinished jobs
            running_jobs: List[LoadJob] = self.resume_started_jobs(load_id, schema)

        # loop until all jobs are processed
        pending_exception: Optional[LoadClientJobException] = None
        while True:
            try:
                # we continuously spool new jobs and complete finished ones
                running_jobs, finalized_jobs, new_pending_exception = self.complete_jobs(
                    load_id, running_jobs, schema
                )
                pending_exception = pending_exception or new_pending_exception

                # do not spool new jobs if there was a signal or an exception was encountered
                # we inform the users how many jobs remain when shutting down, but only if the count of running jobs
                # has changed (as determined by finalized jobs)
                if signals.signal_received():
                    if finalized_jobs:
                        logger.info(
                            f"Signal received, draining running jobs. {len(running_jobs)} to go."
                        )
                elif pending_exception:
                    if finalized_jobs:
                        logger.info(
                            f"Exception for job {pending_exception.job_id} received, draining"
                            f" running jobs.{len(running_jobs)} to go."
                        )
                else:
                    running_jobs += self.start_new_jobs(load_id, schema, running_jobs)

                if len(running_jobs) == 0:
                    # if a pending exception was discovered during completion of jobs
                    # we can raise it now
                    if pending_exception:
                        raise pending_exception
                    break
                # this will raise on signal
                sleep(self._run_loop_sleep_duration)
            except LoadClientJobFailed:
                # the package is completed and skipped
                self.complete_package(load_id, schema, True)
                raise

        # no new jobs, load package done
        self.complete_package(load_id, schema, False)

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
                if self.current_load_id is None:
                    self._job_metrics = {}
                    self._step_info_start_load_id(load_id)
                self.load_single_package(load_id, schema)

        return TRunMetrics(False, len(self.load_storage.list_normalized_packages()))

    def _maybe_truncate_staging_dataset(self, schema: Schema, job_client: JobClientBase) -> None:
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
            DestinationReference.normalize_type(self.initial_client_config.destination_type),
            str(self.initial_client_config),
            self.initial_client_config.destination_name
            or Destination.to_name(self.initial_client_config.destination_type),
            self.initial_client_config.environment,
            (
                DestinationReference.normalize_type(
                    self.initial_staging_client_config.destination_type
                )
                if self.initial_staging_client_config
                else None
            ),
            (
                self.initial_staging_client_config.destination_name
                or Destination.to_name(self.initial_staging_client_config.destination_type)
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
