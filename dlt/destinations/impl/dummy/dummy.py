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

from dlt.common.pendulum import pendulum
from dlt.common.schema import Schema, TTableSchema, TSchemaTables
from dlt.common.storages import FileStorage
from dlt.common.storages.load_package import LoadJobInfo
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.exceptions import (
    DestinationTerminalException,
    DestinationTransientException,
)
from dlt.common.destination.reference import (
    HasFollowupJobs,
    FollowupJob,
    SupportsStagingDestination,
    TLoadJobState,
    RunnableLoadJob,
    JobClientBase,
    WithStagingDataset,
    LoadJob,
)

from dlt.destinations.exceptions import (
    LoadJobNotExistsException,
    LoadJobInvalidStateTransitionException,
)
from dlt.destinations.impl.dummy.configuration import DummyClientConfiguration
from dlt.destinations.job_impl import ReferenceFollowupJob


class LoadDummyBaseJob(RunnableLoadJob):
    def __init__(
        self, job_client: "DummyClient", file_name: str, config: DummyClientConfiguration
    ) -> None:
        self.config = copy(config)
        self.start_time: float = pendulum.now().timestamp()
        super().__init__(job_client, file_name)

        if self.config.fail_terminally_in_init:
            raise DestinationTerminalException(self._exception)
        if self.config.fail_transiently_in_init:
            raise Exception(self._exception)

    def run(self) -> None:
        # time.sleep(0.1)
        # this should poll the server for a job status, here we simulate various outcomes
        c_r = random.random()
        if self.config.exception_prob >= c_r:
            # this will make the job go to a retry state with a generic exception
            raise Exception("Dummy job status raised exception")
        n = pendulum.now().timestamp()
        if n - self.start_time > self.config.timeout:
            # this will make the the job go to a failed state
            raise DestinationTerminalException("failed due to timeout")
        else:
            c_r = random.random()
            if self.config.completed_prob >= c_r:
                # this will make the run function exit and the job go to a completed state
                return
            else:
                c_r = random.random()
                if self.config.retry_prob >= c_r:
                    # this will make the job go to a retry state
                    raise DestinationTransientException("a random retry occured")
                else:
                    c_r = random.random()
                    if self.config.fail_prob >= c_r:
                        # this will make the the job go to a failed state
                        raise DestinationTerminalException("a random fail occured")

    def retry(self) -> None:
        if self._state != "retry":
            raise LoadJobInvalidStateTransitionException(self._state, "retry")
        self._state = "retry"


class LoadDummyJob(LoadDummyBaseJob, HasFollowupJobs):
    def create_followup_jobs(self, final_state: TLoadJobState) -> List[FollowupJob]:
        if self.config.create_followup_jobs and final_state == "completed":
            new_job = ReferenceFollowupJob(
                original_file_name=self.file_name(), remote_paths=[self._file_name]
            )
            CREATED_FOLLOWUP_JOBS[new_job.job_id()] = new_job
            return [new_job]
        return []


JOBS: Dict[str, LoadDummyBaseJob] = {}
CREATED_FOLLOWUP_JOBS: Dict[str, FollowupJob] = {}


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
                "Raise on schema update due to fail_schema_update config flag"
            )
        return applied_update

    def get_load_job(
        self, table: TTableSchema, file_path: str, load_id: str, restore: bool = False
    ) -> LoadJob:
        job_id = FileStorage.get_file_name_from_file_path(file_path)
        if restore and job_id not in JOBS:
            raise LoadJobNotExistsException(job_id)
        # return existing job if already there
        if job_id not in JOBS:
            JOBS[job_id] = self._create_job(file_path)
        else:
            job = JOBS[job_id]
            if job.state == "retry":
                job.retry()

        return JOBS[job_id]

    def create_table_chain_completed_followup_jobs(
        self,
        table_chain: Sequence[TTableSchema],
        completed_table_chain_jobs: Optional[Sequence[LoadJobInfo]] = None,
    ) -> List[FollowupJob]:
        """Creates a list of followup jobs that should be executed after a table chain is completed"""
        return []

    def complete_load(self, load_id: str) -> None:
        pass

    def should_load_data_to_staging_dataset(self, table: TTableSchema) -> bool:
        return super().should_load_data_to_staging_dataset(table)

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
        if ReferenceFollowupJob.is_reference_job(job_id):
            return LoadDummyBaseJob(self, job_id, config=self.config)
        else:
            return LoadDummyJob(self, job_id, config=self.config)
