import random
from contextlib import contextmanager
from copy import copy
from types import TracebackType
from typing import (
    ClassVar,
    Dict,
    Iterator,
    Optional,
    Sequence,
    Type,
    Iterable,
    List,
)
import time
from dlt.common.metrics import LoadJobMetrics
from dlt.common.pendulum import pendulum
from dlt.common.schema import Schema, TSchemaTables
from dlt.common.storages import FileStorage
from dlt.common.storages.load_package import LoadJobInfo
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.exceptions import (
    DestinationTerminalException,
    DestinationTransientException,
)
from dlt.common.destination.client import (
    HasFollowupJobs,
    FollowupJobRequest,
    PreparedTableSchema,
    SupportsStagingDestination,
    TLoadJobState,
    RunnableLoadJob,
    JobClientBase,
    WithStagingDataset,
    LoadJob,
)
from dlt.common.time import precise_time
from dlt.destinations.sql_jobs import SqlMergeFollowupJob

from dlt.destinations.exceptions import (
    LoadJobNotExistsException,
)
from dlt.destinations.impl.dummy.configuration import DummyClientConfiguration
from dlt.destinations.job_impl import ReferenceFollowupJobRequest


class LoadDummyBaseJob(RunnableLoadJob):
    def __init__(self, file_name: str, config: DummyClientConfiguration) -> None:
        super().__init__(file_name)
        self.config = copy(config)
        self.start_time = precise_time()

        if self.config.fail_terminally_in_init:
            raise DestinationTerminalException(self._exception)
        if self.config.fail_transiently_in_init:
            raise Exception(self._exception)

    def run(self) -> None:
        while True:
            # simulate generic exception (equals retry)
            c_r = random.random()
            if self.config.exception_prob >= c_r:
                # this will make the job go to a retry state with a generic exception
                raise Exception("Dummy job status raised exception")

            # timeout condition (terminal)
            n = precise_time()
            if n - self.start_time > self.config.timeout:
                # this will make the the job go to a failed state
                raise DestinationTerminalException("failed due to timeout")

            # success
            c_r = random.random()
            if self.config.completed_prob >= c_r:
                # this will make the run function exit and the job go to a completed state
                break

            # retry prob
            c_r = random.random()
            if self.config.retry_prob >= c_r:
                # this will make the job go to a retry state
                raise DestinationTransientException("a random retry occurred")

            # fail prob
            c_r = random.random()
            if self.config.fail_prob >= c_r:
                # this will make the the job go to a failed state
                raise DestinationTerminalException("a random fail occurred")

            time.sleep(0.1)

    def metrics(self) -> Optional[LoadJobMetrics]:
        m = super().metrics()
        # add remote url if there's followup job
        if self.config.create_followup_jobs:
            m = m._replace(remote_url=self._file_name)
        return m


class DummyFollowupJobRequest(ReferenceFollowupJobRequest):
    def __init__(
        self, original_file_name: str, remote_paths: List[str], config: DummyClientConfiguration
    ) -> None:
        self.config = config
        if config.fail_followup_job_creation:
            raise Exception("Failed to create followup job")
        super().__init__(original_file_name=original_file_name, remote_paths=remote_paths)


class LoadDummyJob(LoadDummyBaseJob, HasFollowupJobs):
    def create_followup_jobs(self, final_state: TLoadJobState) -> List[FollowupJobRequest]:
        if self.config.create_followup_jobs and final_state == "completed":
            new_job = DummyFollowupJobRequest(
                original_file_name=self.file_name(),
                remote_paths=[self._file_name],
                config=self.config,
            )
            CREATED_FOLLOWUP_JOBS[new_job.job_id()] = new_job
            return [new_job]
        return []


JOBS: Dict[str, LoadDummyBaseJob] = {}
CREATED_FOLLOWUP_JOBS: Dict[str, FollowupJobRequest] = {}
CREATED_TABLE_CHAIN_FOLLOWUP_JOBS: Dict[str, FollowupJobRequest] = {}
RETRIED_JOBS: Dict[str, LoadDummyBaseJob] = {}


class DummyClient(JobClientBase, SupportsStagingDestination, WithStagingDataset):
    """dummy client storing jobs in memory"""

    def __init__(
        self,
        schema: Schema,
        config: DummyClientConfiguration,
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        super().__init__(schema, config, capabilities)
        self.in_staging_context = False
        self.config: DummyClientConfiguration = config

    def initialize_storage(self, truncate_tables: Iterable[str] = None) -> None:
        pass

    def is_storage_initialized(self) -> bool:
        return True

    def drop_storage(self) -> None:
        pass

    def update_stored_schema(
        self,
        only_tables: Iterable[str] = None,
        expected_update: TSchemaTables = None,
    ) -> Optional[TSchemaTables]:
        applied_update = super().update_stored_schema(only_tables, expected_update)
        if self.config.fail_schema_update:
            raise DestinationTransientException(
                "Raise on schema update due to `fail_schema_update` config flag"
            )
        return applied_update

    def create_load_job(
        self, table: PreparedTableSchema, file_path: str, load_id: str, restore: bool = False
    ) -> LoadJob:
        job_id = FileStorage.get_file_name_from_file_path(file_path)
        if restore and job_id not in JOBS:
            raise LoadJobNotExistsException(job_id)
        # return existing job if already there
        if job_id not in JOBS:
            JOBS[job_id] = self._create_job(file_path)
        else:
            job = JOBS[job_id]
            # update config of existing job in case it was changed in tests
            job.config = self.config
            RETRIED_JOBS[job_id] = job

        return JOBS[job_id]

    def create_table_chain_completed_followup_jobs(
        self,
        table_chain: Sequence[PreparedTableSchema],
        completed_table_chain_jobs: Optional[Sequence[LoadJobInfo]] = None,
    ) -> List[FollowupJobRequest]:
        """Creates a list of followup jobs that should be executed after a table chain is completed"""

        # if sql job follow up is configure we schedule a merge job that will always fail
        if self.config.fail_table_chain_followup_job_creation:
            raise Exception("Failed to create table chain followup job")
        if self.config.create_followup_table_chain_sql_jobs:
            return [SqlMergeFollowupJob.from_table_chain(table_chain, self)]  # type: ignore
        if self.config.create_followup_table_chain_reference_jobs:
            table_job_paths = [job.file_path for job in completed_table_chain_jobs]
            file_name = FileStorage.get_file_name_from_file_path(table_job_paths[0])
            job = ReferenceFollowupJobRequest(file_name, table_job_paths)
            CREATED_TABLE_CHAIN_FOLLOWUP_JOBS[job.job_id()] = job
            return [job]
        return []

    def complete_load(self, load_id: str) -> None:
        pass

    def should_load_data_to_staging_dataset(self, table_name: str) -> bool:
        return super().should_load_data_to_staging_dataset(table_name)

    def should_truncate_table_before_load_on_staging_destination(self, table_name: str) -> bool:
        return self.config.truncate_tables_on_staging_destination_before_load

    @contextmanager
    def with_staging_dataset(self) -> Iterator[JobClientBase]:
        try:
            self.in_staging_context = True
            yield self
        finally:
            self.in_staging_context = False

    def __enter__(self) -> "DummyClient":
        return self

    def __exit__(
        self, exc_type: Type[BaseException], exc_val: BaseException, exc_tb: TracebackType
    ) -> None:
        pass

    def _create_job(self, job_id: str) -> LoadDummyBaseJob:
        if ReferenceFollowupJobRequest.is_reference_job(job_id):
            return LoadDummyBaseJob(job_id, config=self.config)
        else:
            return LoadDummyJob(job_id, config=self.config)
