from __future__ import annotations

import os
import tempfile  # noqa: 251
from typing import TYPE_CHECKING, Callable, Dict, Generic, Iterable, List, Optional


from dlt.common import pendulum
from dlt.common.destination.client import (
    HasFollowupJobs,
    TLoadJobState,
    RunnableLoadJob,
    FollowupJobRequest,
    LoadJob,
)
from dlt.common.destination.exceptions import DestinationTerminalException
from dlt.common.storages.load_package import commit_load_package_state
from dlt.common.typing import TDataItems, TDataRecordBatch
from dlt.common.storages.load_storage import ParsedLoadJobFileName

from dlt.destinations.file_batching import (
    FileBatchIterator,
    JsonlFileBatchIterator,
    ParquetFileBatchIterator,
    TRecordBatch,
)
from dlt.destinations.impl.destination.configuration import (
    CustomDestinationClientConfiguration,
    TDestinationCallable,
)

if TYPE_CHECKING:
    from dlt.common.libs.pyarrow import pyarrow


class FinalizedLoadJob(LoadJob):
    """
    Special Load Job that should never get started and just indicates a job being in a final state.
    May also be used to indicate that nothing needs to be done.
    """

    def __init__(
        self,
        file_path: str,
        /,
        *,
        started_at: pendulum.DateTime = None,
        finished_at: pendulum.DateTime = None,
        status: TLoadJobState = "completed",
        failed_message: str = None,
        exception: BaseException = None,
    ) -> None:
        super().__init__(file_path)
        self._status = status
        self._failed_message = failed_message
        self._exception = exception
        self._started_at = started_at or pendulum.now()
        self._finished_at = finished_at or (
            pendulum.now() if self._status in ("completed", "failed") else None
        )
        assert self._status in ("completed", "failed", "retry")

    @classmethod
    def from_file_path(
        cls,
        file_path: str,
        /,
        *,
        started_at: pendulum.DateTime = None,
        finished_at: pendulum.DateTime = None,
        status: TLoadJobState = "completed",
        message: str = None,
        exception: BaseException = None,
    ) -> "FinalizedLoadJob":
        return cls(
            file_path,
            started_at=started_at,
            finished_at=finished_at,
            status=status,
            failed_message=message,
            exception=exception,
        )

    def state(self) -> TLoadJobState:
        return self._status

    def failed_message(self) -> str:
        return self._failed_message

    def exception(self) -> BaseException:
        return self._exception

    def set_final_state(self, state: TLoadJobState, failed_message: Optional[str] = None) -> None:
        assert state in ("completed", "failed")
        self._status = state
        self._failed_message = failed_message
        if failed_message:
            self._exception = DestinationTerminalException(failed_message)
        self._finished_at = pendulum.now()


class FinalizedLoadJobWithFollowupJobs(FinalizedLoadJob, HasFollowupJobs):
    pass


class FollowupJobRequestImpl(FollowupJobRequest):
    """
    Class to create a new loadjob, not stateful and not runnable
    """

    def __init__(self, file_name: str) -> None:
        self._file_path = os.path.join(tempfile.gettempdir(), file_name)
        self._parsed_file_name = ParsedLoadJobFileName.parse(file_name)
        # we only accept jobs that we can schedule as new or mark as failed.

    def _save_text_file(self, data: str) -> None:
        with open(self._file_path, "w", encoding="utf-8") as f:
            f.write(data)

    def new_file_path(self) -> str:
        """Path to a newly created temporary job file"""
        return self._file_path

    def job_id(self) -> str:
        """The job id that is derived from the file name and does not changes during job lifecycle"""
        return self._parsed_file_name.job_id()


class ReferenceFollowupJobRequest(FollowupJobRequestImpl):
    def __init__(self, original_file_name: str, remote_paths: List[str]) -> None:
        job_info = ParsedLoadJobFileName.parse(original_file_name)
        file_name = job_info.to_reference_file_name()
        self._remote_paths = remote_paths
        super().__init__(file_name)
        self._save_text_file("\n".join(remote_paths))

    @staticmethod
    def is_reference_job(file_path: str) -> bool:
        return os.path.splitext(file_path)[1][1:] == "reference"

    @staticmethod
    def resolve_references(file_path: str) -> List[str]:
        with open(file_path, "r+", encoding="utf-8") as f:
            # Reading from a file
            return f.read().split("\n")

    @staticmethod
    def resolve_reference(file_path: str) -> str:
        refs = ReferenceFollowupJobRequest.resolve_references(file_path)
        assert len(refs) == 1
        return refs[0]


class BatchedFileLoadJob(RunnableLoadJob, Generic[TRecordBatch]):
    file_batch_iterator_class: type[FileBatchIterator[TRecordBatch]]

    def __init__(self, file_path: str, batch_size: int, destination_state: Dict[str, int]) -> None:
        super().__init__(file_path)
        self._batch_size = batch_size
        self._destination_state = destination_state
        self._state_key = f"{self._parsed_file_name.table_name}.{self._parsed_file_name.file_id}"

    @property
    def _record_offset(self) -> int:
        return self._destination_state.get(self._state_key, 0)

    def iter_batches(self) -> Iterable[TRecordBatch]:
        return self.file_batch_iterator_class(
            self._file_path,
            self._batch_size,
            self._record_offset,
            list(self._load_table["columns"].keys()),
        )

    def _advance_record_offset(self, processed_count: int) -> None:
        self._destination_state[self._state_key] = self._record_offset + processed_count
        commit_load_package_state()

    def _process_batches(self, process_batch: Callable[[TRecordBatch], int]) -> None:
        for batch in self.iter_batches():
            processed_count = process_batch(batch)
            self._advance_record_offset(processed_count)


class DestinationLoadJob(BatchedFileLoadJob[TRecordBatch]):
    def __init__(
        self,
        file_path: str,
        config: CustomDestinationClientConfiguration,
        destination_state: Dict[str, int],
        destination_callable: TDestinationCallable,
        callable_requires_job_client_args: bool = False,
    ) -> None:
        super().__init__(file_path, config.batch_size, destination_state)
        self._config = config
        self._callable = destination_callable
        self._callable_requires_job_client_args = callable_requires_job_client_args

    def run(self) -> None:
        # update filepath, it will be in running jobs now
        if self._batch_size == 0:
            # on batch size zero we only call the callable with the filename
            self.call_callable_with_items(self._file_path)
            # save progress
            commit_load_package_state()
        else:

            def process_batch(batch: TRecordBatch) -> int:
                self.call_callable_with_items(batch)
                return len(batch)

            self._process_batches(process_batch)

    def call_callable_with_items(self, items: TDataItems) -> None:
        if not items:
            return
        # call callable
        if self._callable_requires_job_client_args:
            self._callable(items, self._load_table, job_client=self._job_client)  # type: ignore
        else:
            self._callable(items, self._load_table)


class DestinationParquetLoadJob(DestinationLoadJob["pyarrow.RecordBatch"]):
    file_batch_iterator_class = ParquetFileBatchIterator


class DestinationJsonlLoadJob(DestinationLoadJob[TDataRecordBatch]):
    file_batch_iterator_class = JsonlFileBatchIterator
