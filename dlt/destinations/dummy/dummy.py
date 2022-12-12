import random
from types import TracebackType
from typing import Dict, Type

from dlt.common import pendulum
from dlt.common.schema import Schema
from dlt.common.storages import FileStorage
from dlt.common.schema.typing import TTableSchema
from dlt.common.destination import DestinationCapabilitiesContext, JobClientBase, LoadJob, TLoadJobStatus

from dlt.destinations.exceptions import (LoadJobNotExistsException, LoadJobInvalidStateTransitionException,
                                            DestinationTerminalException, DestinationTransientException)

from dlt.destinations.dummy import capabilities
from dlt.destinations.dummy.configuration import DummyClientConfiguration


class LoadDummyJob(LoadJob):
    def __init__(self, file_name: str, fail_prob: float = 0.0, retry_prob: float = 0.0, completed_prob: float = 1.0, timeout: float = 10.0) -> None:
        self.fail_prob = fail_prob
        self.retry_prob = retry_prob
        self.completed_prob = completed_prob
        self.timeout = timeout
        self._status: TLoadJobStatus = "running"
        self._exception: str = None
        self.start_time: float = pendulum.now().timestamp()
        super().__init__(file_name)
        s = self.status()
        if s == "failed":
            raise DestinationTerminalException(self._exception)
        if s == "retry":
            raise DestinationTransientException(self._exception)


    def status(self) -> TLoadJobStatus:
        # this should poll the server for a job status, here we simulate various outcomes
        if self._status == "running":
            n = pendulum.now().timestamp()
            if n - self.start_time > self.timeout:
                self._status = "failed"
                self._exception = "failed due to timeout"
            else:
                c_r = random.random()
                if self.completed_prob >= c_r:
                    self._status = "completed"
                else:
                    c_r = random.random()
                    if self.retry_prob >= c_r:
                        self._status = "retry"
                        self._exception = "a random retry occured"
                    else:
                        c_r = random.random()
                        if self.fail_prob >= c_r:
                            self._status = "failed"
                            self._exception = "a random fail occured"

        return self._status

    def file_name(self) -> str:
        return self._file_name

    def exception(self) -> str:
        # this will typically call server for error messages
        return self._exception

    def retry(self) -> None:
        if self._status != "retry":
            raise LoadJobInvalidStateTransitionException(self._status, "retry")
        self._status = "retry"


JOBS: Dict[str, LoadDummyJob] = {}


class DummyClient(JobClientBase):
    """
    dummy client storing jobs in memory
    """

    def __init__(self, schema: Schema, config: DummyClientConfiguration) -> None:
        super().__init__(schema, config)
        self.config: DummyClientConfiguration = config

    def initialize_storage(self) -> None:
        pass

    def is_storage_initialized(self) -> bool:
        return True

    def update_storage_schema(self) -> None:
        if self.config.fail_schema_update:
            raise DestinationTransientException("Raise on schema update due to fail_schema_update config flag")

    def start_file_load(self, table: TTableSchema, file_path: str) -> LoadJob:
        job_id = FileStorage.get_file_name_from_file_path(file_path)
        file_name = FileStorage.get_file_name_from_file_path(file_path)
        # return existing job if already there
        if job_id not in JOBS:
            JOBS[job_id] = self._create_job(file_name)
        else:
            job = JOBS[job_id]
            if job.status == "retry":
                job.retry()

        return JOBS[job_id]

    def restore_file_load(self, file_path: str) -> LoadJob:
        job_id = FileStorage.get_file_name_from_file_path(file_path)
        if job_id not in JOBS:
            raise LoadJobNotExistsException(job_id)
        return JOBS[job_id]

    def complete_load(self, load_id: str) -> None:
        pass

    def __enter__(self) -> "DummyClient":
        return self

    def __exit__(self, exc_type: Type[BaseException], exc_val: BaseException, exc_tb: TracebackType) -> None:
        pass

    def _create_job(self, job_id: str) -> LoadDummyJob:
        return LoadDummyJob(
            job_id,
            fail_prob=self.config.fail_prob,
            retry_prob=self.config.retry_prob,
            completed_prob=self.config.completed_prob,
            timeout=self.config.timeout
            )

    @classmethod
    def capabilities(cls) -> DestinationCapabilitiesContext:
        return capabilities()
