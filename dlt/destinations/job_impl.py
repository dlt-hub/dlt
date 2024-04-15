from abc import ABC, abstractmethod
import os
import tempfile  # noqa: 251
from typing import Dict, Iterable, List

from dlt.common.json import json
from dlt.common.destination.reference import NewLoadJob, FollowupJob, TLoadJobState, LoadJob
from dlt.common.schema import Schema, TTableSchema
from dlt.common.storages import FileStorage
from dlt.common.typing import TDataItems

from dlt.destinations.impl.destination.configuration import (
    CustomDestinationClientConfiguration,
    TDestinationCallable,
)

from dlt.pipeline.current import commit_load_package_state


class EmptyLoadJobWithoutFollowup(LoadJob):
    def __init__(self, file_name: str, status: TLoadJobState, exception: str = None) -> None:
        self._status = status
        self._exception = exception
        super().__init__(file_name)

    @classmethod
    def from_file_path(
        cls, file_path: str, status: TLoadJobState, message: str = None
    ) -> "EmptyLoadJobWithoutFollowup":
        return cls(FileStorage.get_file_name_from_file_path(file_path), status, exception=message)

    def state(self) -> TLoadJobState:
        return self._status

    def exception(self) -> str:
        return self._exception


class EmptyLoadJob(EmptyLoadJobWithoutFollowup, FollowupJob):
    pass


class NewLoadJobImpl(EmptyLoadJobWithoutFollowup, NewLoadJob):
    def _save_text_file(self, data: str) -> None:
        temp_file = os.path.join(tempfile.gettempdir(), self._file_name)
        with open(temp_file, "w", encoding="utf-8") as f:
            f.write(data)
        self._new_file_path = temp_file

    def new_file_path(self) -> str:
        """Path to a newly created temporary job file"""
        return self._new_file_path


class NewReferenceJob(NewLoadJobImpl):
    def __init__(
        self, file_name: str, status: TLoadJobState, exception: str = None, remote_path: str = None
    ) -> None:
        file_name = os.path.splitext(file_name)[0] + ".reference"
        super().__init__(file_name, status, exception)
        self._remote_path = remote_path
        self._save_text_file(remote_path)

    @staticmethod
    def is_reference_job(file_path: str) -> bool:
        return os.path.splitext(file_path)[1][1:] == "reference"

    @staticmethod
    def resolve_reference(file_path: str) -> str:
        with open(file_path, "r+", encoding="utf-8") as f:
            # Reading from a file
            return f.read()


class DestinationLoadJob(LoadJob, ABC):
    def __init__(
        self,
        table: TTableSchema,
        file_path: str,
        config: CustomDestinationClientConfiguration,
        schema: Schema,
        destination_state: Dict[str, int],
        destination_callable: TDestinationCallable,
        skipped_columns: List[str],
    ) -> None:
        super().__init__(FileStorage.get_file_name_from_file_path(file_path))
        self._file_path = file_path
        self._config = config
        self._table = table
        self._schema = schema
        # we create pre_resolved callable here
        self._callable = destination_callable
        self._state: TLoadJobState = "running"
        self._storage_id = f"{self._parsed_file_name.table_name}.{self._parsed_file_name.file_id}"
        self.skipped_columns = skipped_columns
        try:
            if self._config.batch_size == 0:
                # on batch size zero we only call the callable with the filename
                self.call_callable_with_items(self._file_path)
            else:
                current_index = destination_state.get(self._storage_id, 0)
                for batch in self.run(current_index):
                    self.call_callable_with_items(batch)
                    current_index += len(batch)
                    destination_state[self._storage_id] = current_index

            self._state = "completed"
        except Exception as e:
            self._state = "retry"
            raise e
        finally:
            # save progress
            commit_load_package_state()

    @abstractmethod
    def run(self, start_index: int) -> Iterable[TDataItems]:
        pass

    def call_callable_with_items(self, items: TDataItems) -> None:
        if not items:
            return
        # call callable
        self._callable(items, self._table)

    def state(self) -> TLoadJobState:
        return self._state

    def exception(self) -> str:
        raise NotImplementedError()


class DestinationParquetLoadJob(DestinationLoadJob):
    def run(self, start_index: int) -> Iterable[TDataItems]:
        # stream items
        from dlt.common.libs.pyarrow import pyarrow

        # guard against changed batch size after restart of loadjob
        assert (
            start_index % self._config.batch_size
        ) == 0, "Batch size was changed during processing of one load package"

        # on record batches we cannot drop columns, we need to
        # select the ones we want to keep
        keep_columns = list(self._table["columns"].keys())
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
    def run(self, start_index: int) -> Iterable[TDataItems]:
        current_batch: TDataItems = []

        # stream items
        with FileStorage.open_zipsafe_ro(self._file_path) as f:
            encoded_json = json.typed_loads(f.read())
            if isinstance(encoded_json, dict):
                encoded_json = [encoded_json]

            for item in encoded_json:
                # find correct start position
                if start_index > 0:
                    start_index -= 1
                    continue
                # skip internal columns
                for column in self.skipped_columns:
                    item.pop(column, None)
                current_batch.append(item)
                if len(current_batch) == self._config.batch_size:
                    yield current_batch
                    current_batch = []
            yield current_batch
