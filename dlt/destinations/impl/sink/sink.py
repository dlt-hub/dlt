from abc import ABC, abstractmethod
from types import TracebackType
from typing import ClassVar, Dict, Optional, Sequence, Type, Iterable, Iterable

from dlt.destinations.job_impl import EmptyLoadJob
from dlt.common.typing import TDataItems
from dlt.common import json
from dlt.common.configuration.container import Container
from dlt.common.pipeline import StateInjectableContext
from dlt.common.pipeline import destination_state, reset_destination_state, commit_pipeline_state

from dlt.common.schema import Schema, TTableSchema, TSchemaTables
from dlt.common.schema.typing import TTableSchema
from dlt.common.storages import FileStorage
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.reference import (
    TLoadJobState,
    LoadJob,
    JobClientBase,
)

from dlt.destinations.impl.sink import capabilities
from dlt.destinations.impl.sink.configuration import SinkClientConfiguration, TSinkCallable


class SinkLoadJob(LoadJob, ABC):
    def __init__(
        self,
        table: TTableSchema,
        file_path: str,
        config: SinkClientConfiguration,
        schema: Schema,
        load_state: Dict[str, int],
    ) -> None:
        super().__init__(FileStorage.get_file_name_from_file_path(file_path))
        self._file_path = file_path
        self._config = config
        self._table = table
        self._schema = schema

        self._state: TLoadJobState = "running"
        try:
            current_index = load_state.get(self._parsed_file_name.file_id, 0)
            for batch in self.run(current_index):
                self.call_callable_with_items(batch)
                current_index += len(batch)
                load_state[self._parsed_file_name.file_id] = current_index
            self._state = "completed"
        except Exception as e:
            self._state = "retry"
            raise e
        finally:
            # save progress
            commit_pipeline_state()

    @abstractmethod
    def run(self, start_index: int) -> Iterable[TDataItems]:
        pass

    def call_callable_with_items(self, items: TDataItems) -> None:
        if not items:
            return

        # coerce items into correct format specified by schema
        coerced_items: TDataItems = []
        for item in items:
            coerced_item, table_update = self._schema.coerce_row(self._table["name"], None, item)
            assert not table_update
            coerced_items.append(coerced_item)

        # send single item on batch size 1
        if self._config.batch_size == 1:
            coerced_items = coerced_items[0]

        # call callable
        self._config.credentials.resolved_callable(coerced_items, self._table)

    def state(self) -> TLoadJobState:
        return self._state

    def exception(self) -> str:
        raise NotImplementedError()


class SinkParquetLoadJob(SinkLoadJob):
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
                batch = record_batch.to_pylist()
                yield batch


class SinkJsonlLoadJob(SinkLoadJob):
    def run(self, start_index: int) -> Iterable[TDataItems]:
        current_batch: TDataItems = []

        # stream items
        with FileStorage.open_zipsafe_ro(self._file_path) as f:
            for line in f:
                # find correct start position
                if start_index > 0:
                    start_index -= 1
                    continue
                current_batch.append(json.loads(line))
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
        load_state = destination_state().setdefault(load_id, {})
        if file_path.endswith("parquet"):
            return SinkParquetLoadJob(table, file_path, self.config, self.schema, load_state)
        if file_path.endswith("jsonl"):
            return SinkJsonlLoadJob(table, file_path, self.config, self.schema, load_state)
        return None

    def restore_file_load(self, file_path: str) -> LoadJob:
        return EmptyLoadJob.from_file_path(file_path, "completed")

    def complete_load(self, load_id: str) -> None:
        # pop all state for this load on success
        state = destination_state()
        state.pop(load_id, None)
        commit_pipeline_state()

    def __enter__(self) -> "SinkClient":
        return self

    def __exit__(
        self, exc_type: Type[BaseException], exc_val: BaseException, exc_tb: TracebackType
    ) -> None:
        pass
