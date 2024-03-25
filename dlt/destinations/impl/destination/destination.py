from abc import ABC, abstractmethod
from types import TracebackType
from typing import ClassVar, Dict, Optional, Type, Iterable, Iterable, cast, Dict, List
from copy import deepcopy

from dlt.common.destination.reference import LoadJob
from dlt.destinations.job_impl import EmptyLoadJob
from dlt.common.typing import TDataItems, AnyFun
from dlt.common import json
from dlt.pipeline.current import (
    destination_state,
    commit_load_package_state,
)
from dlt.common.configuration import create_resolved_partial

from dlt.common.schema import Schema, TTableSchema, TSchemaTables
from dlt.common.schema.typing import TTableSchema
from dlt.common.storages import FileStorage
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.reference import (
    TLoadJobState,
    LoadJob,
    DoNothingJob,
    JobClientBase,
)

from dlt.destinations.impl.destination import capabilities
from dlt.destinations.impl.destination.configuration import (
    CustomDestinationClientConfiguration,
    TDestinationCallable,
)


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


class DestinationClient(JobClientBase):
    """Sink Client"""

    capabilities: ClassVar[DestinationCapabilitiesContext] = capabilities()

    def __init__(self, schema: Schema, config: CustomDestinationClientConfiguration) -> None:
        super().__init__(schema, config)
        self.config: CustomDestinationClientConfiguration = config
        # create pre-resolved callable to avoid multiple config resolutions during execution of the jobs
        self.destination_callable = create_resolved_partial(
            cast(AnyFun, self.config.destination_callable), self.config
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
        # skip internal tables and remove columns from schema if so configured
        skipped_columns: List[str] = []
        if self.config.skip_dlt_columns_and_tables:
            if table["name"].startswith(self.schema._dlt_tables_prefix):
                return DoNothingJob(file_path)
            table = deepcopy(table)
            for column in list(table["columns"].keys()):
                if column.startswith(self.schema._dlt_tables_prefix):
                    table["columns"].pop(column)
                    skipped_columns.append(column)

        # save our state in destination name scope
        load_state = destination_state()
        if file_path.endswith("parquet"):
            return DestinationParquetLoadJob(
                table,
                file_path,
                self.config,
                self.schema,
                load_state,
                self.destination_callable,
                skipped_columns,
            )
        if file_path.endswith("jsonl"):
            return DestinationJsonlLoadJob(
                table,
                file_path,
                self.config,
                self.schema,
                load_state,
                self.destination_callable,
                skipped_columns,
            )
        return None

    def restore_file_load(self, file_path: str) -> LoadJob:
        return EmptyLoadJob.from_file_path(file_path, "completed")

    def complete_load(self, load_id: str) -> None: ...

    def __enter__(self) -> "DestinationClient":
        return self

    def __exit__(
        self, exc_type: Type[BaseException], exc_val: BaseException, exc_tb: TracebackType
    ) -> None:
        pass
