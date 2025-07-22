from abc import ABC, abstractmethod
import os
import tempfile  # noqa: 251
from typing import Dict, Iterable, List

from dlt.common.json import json
from dlt.common.destination.client import (
    HasFollowupJobs,
    TLoadJobState,
    RunnableLoadJob,
    FollowupJobRequest,
    LoadJob,
)
from dlt.common.storages.load_package import commit_load_package_state
from dlt.common.storages import FileStorage
from dlt.common.typing import TDataItems
from dlt.common.storages.load_storage import ParsedLoadJobFileName

from dlt.destinations.impl.destination.configuration import (
    CustomDestinationClientConfiguration,
    TDestinationCallable,
)


class FinalizedLoadJob(LoadJob):
    """
    Special Load Job that should never get started and just indicates a job being in a final state.
    May also be used to indicate that nothing needs to be done.
    """

    def __init__(
        self, file_path: str, status: TLoadJobState = "completed", exception: str = None
    ) -> None:
        self._status = status
        self._exception = exception
        self._file_path = file_path
        assert self._status in ("completed", "failed", "retry")
        super().__init__(file_path)

    @classmethod
    def from_file_path(
        cls, file_path: str, status: TLoadJobState = "completed", message: str = None
    ) -> "FinalizedLoadJob":
        return cls(file_path, status, exception=message)

    def state(self) -> TLoadJobState:
        return self._status

    def exception(self) -> str:
        return self._exception


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


class DestinationLoadJob(RunnableLoadJob, ABC):
    def __init__(
        self,
        file_path: str,
        config: CustomDestinationClientConfiguration,
        destination_state: Dict[str, int],
        destination_callable: TDestinationCallable,
        skipped_columns: List[str],
        callable_requires_job_client_args: bool = False,
    ) -> None:
        super().__init__(file_path)
        self._config = config
        self._callable = destination_callable
        self._storage_id = f"{self._parsed_file_name.table_name}.{self._parsed_file_name.file_id}"
        self._skipped_columns = skipped_columns
        self._destination_state = destination_state
        self._callable_requires_job_client_args = callable_requires_job_client_args

    def run(self) -> None:
        # update filepath, it will be in running jobs now
        try:
            if self._config.batch_size == 0:
                # on batch size zero we only call the callable with the filename
                self.call_callable_with_items(self._file_path)
            else:
                current_index = self._destination_state.get(self._storage_id, 0)
                for batch in self.get_batches(current_index):
                    self.call_callable_with_items(batch)
                    current_index += len(batch)
                    self._destination_state[self._storage_id] = current_index
        finally:
            # save progress
            commit_load_package_state()

    def call_callable_with_items(self, items: TDataItems) -> None:
        if not items:
            return
        # call callable
        if self._callable_requires_job_client_args:
            self._callable(items, self._load_table, job_client=self._job_client)  # type: ignore
        else:
            self._callable(items, self._load_table)

    @abstractmethod
    def get_batches(self, start_index: int) -> Iterable[TDataItems]:
        pass


class DestinationParquetLoadJob(DestinationLoadJob):
    def get_batches(self, start_index: int) -> Iterable[TDataItems]:
        # stream items
        from dlt.common.libs.pyarrow import pyarrow

        # guard against changed batch size after restart of loadjob
        assert (
            start_index % self._config.batch_size
        ) == 0, "Batch size was changed during processing of one load package"

        # on record batches we cannot drop columns, we need to
        # select the ones we want to keep
        keep_columns = list(self._load_table["columns"].keys())
        start_batch = start_index / self._config.batch_size
        with pyarrow.parquet.ParquetFile(self._file_path) as reader:
            for record_batch in reader.iter_batches(
                batch_size=self._config.batch_size, columns=keep_columns
            ):
                if start_batch > 0:
                    start_batch -= 1
                    continue
                yield record_batch


class DestinationJsonlLoadJob(DestinationLoadJob):
    def get_batches(self, start_index: int) -> Iterable[TDataItems]:
        current_batch: TDataItems = []

        # stream items
        with FileStorage.open_zipsafe_ro(self._file_path) as f:
            for line in f:
                encoded_json = json.typed_loads(line)
                if isinstance(encoded_json, dict):
                    encoded_json = [encoded_json]

                for item in encoded_json:
                    # find correct start position
                    if start_index > 0:
                        start_index -= 1
                        continue
                    # skip internal columns
                    for column in self._skipped_columns:
                        item.pop(column, None)
                    current_batch.append(item)
                    if len(current_batch) == self._config.batch_size:
                        yield current_batch
                        current_batch = []
                yield current_batch
