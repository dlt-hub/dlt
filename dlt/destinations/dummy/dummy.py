import random
from copy import copy
from types import TracebackType
from typing import ClassVar, Dict, Optional, Sequence, Type, Iterable, List

from dlt.common import pendulum
from dlt.common.schema import Schema, TTableSchema, TSchemaTables
from dlt.common.schema.typing import TWriteDisposition
from dlt.common.storages import FileStorage
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.reference import FollowupJob, NewLoadJob, TLoadJobState, LoadJob, JobClientBase

from dlt.destinations.exceptions import (LoadJobNotExistsException, LoadJobInvalidStateTransitionException,
                                            DestinationTerminalException, DestinationTransientException)

from dlt.destinations.dummy import capabilities
from dlt.destinations.dummy.configuration import DummyClientConfiguration


class LoadDummyJob(LoadJob, FollowupJob):
    def __init__(self, file_name: str, config: DummyClientConfiguration) -> None:
        self.config = copy(config)
        self._status: TLoadJobState = "running"
        self._exception: str = None
        self.start_time: float = pendulum.now().timestamp()
        super().__init__(file_name)
        # if config.fail_in_init:
        s = self.state()
        if s == "failed":
            raise DestinationTerminalException(self._exception)
        if s == "retry":
            raise DestinationTransientException(self._exception)


    def state(self) -> TLoadJobState:
        # this should poll the server for a job status, here we simulate various outcomes
        if self._status == "running":
            n = pendulum.now().timestamp()
            if n - self.start_time > self.config.timeout:
                self._status = "failed"
                self._exception = "failed due to timeout"
            else:
                c_r = random.random()
                if self.config.completed_prob >= c_r:
                    self._status = "completed"
                else:
                    c_r = random.random()
                    if self.config.retry_prob >= c_r:
                        self._status = "retry"
                        self._exception = "a random retry occured"
                    else:
                        c_r = random.random()
                        if self.config.fail_prob >= c_r:
                            self._status = "failed"
                            self._exception = "a random fail occured"

        return self._status

    def exception(self) -> str:
        # this will typically call server for error messages
        return self._exception

    def retry(self) -> None:
        if self._status != "retry":
            raise LoadJobInvalidStateTransitionException(self._status, "retry")
        self._status = "retry"


JOBS: Dict[str, LoadDummyJob] = {}


class DummyClient(JobClientBase):
    """dummy client storing jobs in memory"""

    capabilities: ClassVar[DestinationCapabilitiesContext] = capabilities()

    def __init__(self, schema: Schema, config: DummyClientConfiguration) -> None:
        super().__init__(schema, config)
        self.config: DummyClientConfiguration = config

    def initialize_storage(self, truncate_tables: Iterable[str] = None) -> None:
        pass

    def is_storage_initialized(self) -> bool:
        return True

    def drop_storage(self) -> None:
        pass

    def update_stored_schema(self, only_tables: Iterable[str] = None, expected_update: TSchemaTables = None) -> Optional[TSchemaTables]:
        applied_update = super().update_stored_schema(only_tables, expected_update)
        if self.config.fail_schema_update:
            raise DestinationTransientException("Raise on schema update due to fail_schema_update config flag")
        return applied_update

    def start_file_load(self, table: TTableSchema, file_path: str, load_id: str) -> LoadJob:
        job_id = FileStorage.get_file_name_from_file_path(file_path)
        file_name = FileStorage.get_file_name_from_file_path(file_path)
        # return existing job if already there
        if job_id not in JOBS:
            JOBS[job_id] = self._create_job(file_name)
        else:
            job = JOBS[job_id]
            if job.state == "retry":
                job.retry()

        return JOBS[job_id]

    def restore_file_load(self, file_path: str) -> LoadJob:
        job_id = FileStorage.get_file_name_from_file_path(file_path)
        if job_id not in JOBS:
            raise LoadJobNotExistsException(job_id)
        return JOBS[job_id]

    def create_table_chain_completed_followup_jobs(self, table_chain: Sequence[TTableSchema]) -> List[NewLoadJob]:
        """Creates a list of followup jobs that should be executed after a table chain is completed"""
        return []

    def complete_load(self, load_id: str) -> None:
        pass

    def __enter__(self) -> "DummyClient":
        return self

    def __exit__(self, exc_type: Type[BaseException], exc_val: BaseException, exc_tb: TracebackType) -> None:
        pass

    def _create_job(self, job_id: str) -> LoadDummyJob:
        return LoadDummyJob(
            job_id,
            config=self.config
            )
