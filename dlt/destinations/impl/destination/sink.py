from abc import ABC, abstractmethod
from types import TracebackType
from typing import ClassVar, Dict, Optional, Type, Iterable, Iterable, NamedTuple, Dict
import threading

from dlt.destinations.job_impl import EmptyLoadJob
from dlt.common.typing import TDataItems
from dlt.common import json
from dlt.pipeline.current import (
    destination_state,
    commit_load_package_state,
)

from dlt.common.schema import Schema, TTableSchema, TSchemaTables
from dlt.common.schema.typing import TTableSchema
from dlt.common.storages import FileStorage
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.reference import (
    TLoadJobState,
    LoadJob,
    JobClientBase,
)

from dlt.destinations.impl.destination import capabilities
from dlt.destinations.impl.destination.configuration import SinkClientConfiguration, TSinkCallable


class DestinationLoadJob(LoadJob, ABC):
    def __init__(
        self,
        table: TTableSchema,
        file_path: str,
        config: SinkClientConfiguration,
        schema: Schema,
        destination_state: Dict[str, int],
        destination_callable: TSinkCallable,
    ) -> None:
        super().__init__(FileStorage.get_file_name_from_file_path(file_path))
        self._file_path = file_path
        self._config = config
        self._table = table
        self._schema = schema
        self._callable = destination_callable

        self._state: TLoadJobState = "running"
        self._storage_id = f"{self._parsed_file_name.table_name}.{self._parsed_file_name.file_id}"
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


class SinkParquetLoadJob(DestinationLoadJob):
    def run(self, start_index: int) -> Iterable[TDataItems]:
        # stream items
        from dlt.common.libs.pyarrow import pyarrow

        # guard against changed batch size after restart of loadjob
        assert (
            start_index % self._config.batch_size
        ) == 0, "Batch size was changed during processing of one load package"

        start_batch = start_index / self._config.batch_size
        with pyarrow.parquet.ParquetFile(self._file_path) as reader:
            for record_batch in reader.iter_batches(batch_size=self._config.batch_size):
                if start_batch > 0:
                    start_batch -= 1
                    continue
                yield record_batch


class SinkJsonlLoadJob(DestinationLoadJob):
    def run(self, start_index: int) -> Iterable[TDataItems]:
        current_batch: TDataItems = []

        # stream items
        with FileStorage.open_zipsafe_ro(self._file_path) as f:
            encoded_json = json.typed_loads(f.read())

            for item in encoded_json:
                # find correct start position
                if start_index > 0:
                    start_index -= 1
                    continue
                current_batch.append(item)
                if len(current_batch) == self._config.batch_size:
                    yield current_batch
                    current_batch = []
            yield current_batch


class SinkClient(JobClientBase):
    """Sink Client"""

    capabilities: ClassVar[DestinationCapabilitiesContext] = capabilities()

    def __init__(self, schema: Schema, config: SinkClientConfiguration) -> None:
        super().__init__(schema, config)
        self.config: SinkClientConfiguration = config

        # we create pre_resolved callable here
        self.destination_callable = self.config.destination_callable.create_resolved_partial(
            lock=True
        )

    def initialize_storage(self, truncate_tables: Iterable[str] = None) -> None:
        pass

    def is_storage_initialized(self) -> bool:
        return True

    def drop_storage(self) -> None:
        pass

    def update_stored_schema(
        self, only_tables: Iterable[str] = None, expected_update: TSchemaTables = None
    ) -> Optional[TSchemaTables]:
        return super().update_stored_schema(only_tables, expected_update)

    def start_file_load(self, table: TTableSchema, file_path: str, load_id: str) -> LoadJob:
        # save our state in destination name scope
        load_state = destination_state()
        if file_path.endswith("parquet"):
            return SinkParquetLoadJob(
                table, file_path, self.config, self.schema, load_state, self.destination_callable
            )
        if file_path.endswith("jsonl"):
            return SinkJsonlLoadJob(
                table, file_path, self.config, self.schema, load_state, self.destination_callable
            )
        return None

    def restore_file_load(self, file_path: str) -> LoadJob:
        return EmptyLoadJob.from_file_path(file_path, "completed")

    def complete_load(self, load_id: str) -> None: ...

    def __enter__(self) -> "SinkClient":
        return self

    def __exit__(
        self, exc_type: Type[BaseException], exc_val: BaseException, exc_tb: TracebackType
    ) -> None:
        pass
