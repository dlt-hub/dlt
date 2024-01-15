import random
from copy import copy
from types import TracebackType
from typing import ClassVar, Dict, Optional, Sequence, Type, Iterable, List

from dlt.destinations.job_impl import EmptyLoadJob
from dlt.common.typing import TDataItems

from dlt.common.schema import Schema, TTableSchema, TSchemaTables
from dlt.common.schema.typing import TTableSchema
from dlt.common.storages import FileStorage
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.reference import (
    FollowupJob,
    NewLoadJob,
    TLoadJobState,
    LoadJob,
    JobClientBase,
)

from dlt.destinations.impl.sink import capabilities
from dlt.destinations.impl.sink.configuration import SinkClientConfiguration, TSinkCallable


class SinkLoadJob(LoadJob, FollowupJob):
    def __init__(
        self, table: TTableSchema, file_path: str, config: SinkClientConfiguration, schema: Schema
    ) -> None:
        super().__init__(FileStorage.get_file_name_from_file_path(file_path))
        self._file_path = file_path
        self._config = config
        self._table = table
        self._schema = schema
        self.run()

    def run(self) -> None:
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

        self._config.credentials.resolved_callable(coerced_items, self._table)

    def state(self) -> TLoadJobState:
        return "completed"

    def exception(self) -> str:
        raise NotImplementedError()


class SinkParquetLoadJob(SinkLoadJob):
    def run(self) -> None:
        # stream items
        from dlt.common.libs.pyarrow import pyarrow

        with pyarrow.parquet.ParquetFile(self._file_path) as reader:
            for record_batch in reader.iter_batches(batch_size=self._config.batch_size):
                batch = record_batch.to_pylist()
                self.call_callable_with_items(batch)


class SinkJsonlLoadJob(SinkLoadJob):
    def run(self) -> None:
        from dlt.common import json

        # stream items
        with FileStorage.open_zipsafe_ro(self._file_path) as f:
            current_batch: TDataItems = []
            for line in f:
                current_batch.append(json.loads(line))
                if len(current_batch) == self._config.batch_size:
                    self.call_callable_with_items(current_batch)
                    current_batch = []
            self.call_callable_with_items(current_batch)


# class SinkInsertValueslLoadJob(SinkLoadJob):
#     def run(self) -> None:
#         from dlt.common import json

#         # stream items
#         with FileStorage.open_zipsafe_ro(self._file_path) as f:
#             header = f.readline().strip()
#             values_mark = f.readline()

#             # properly formatted file has a values marker at the beginning
#             assert values_mark == "VALUES\n"

#             # extract column names
#             assert header.startswith("INSERT INTO") and header.endswith(")")
#             header = header[15:-1]
#             column_names = header.split(",")

#             # build batches
#             current_batch: TDataItems = []
#             current_row: str = ""
#             for line in f:
#                 current_row += line
#                 if line.endswith(");"):
#                     current_row = current_row[1:-2]
#                 elif line.endswith("),\n"):
#                     current_row = current_row[1:-3]
#                 else:
#                     continue

#                 values = current_row.split(",")
#                 values = [None if v == "NULL" else v for v in values]
#                 current_row = ""
#                 print(values)
#                 print(current_row)

#                 # zip and send to callable
#                 current_batch.append(dict(zip(column_names, values)))
#                 d = dict(zip(column_names, values))
#                 print(json.dumps(d, pretty=True))
#                 if len(current_batch) == self._config.batch_size:
#                     self.call_callable_with_items(current_batch)
#                     current_batch = []

#             self.call_callable_with_items(current_batch)


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
        if file_path.endswith("parquet"):
            return SinkParquetLoadJob(table, file_path, self.config, self.schema)
        if file_path.endswith("jsonl"):
            return SinkJsonlLoadJob(table, file_path, self.config, self.schema)
        # if file_path.endswith("insert_values"):
        #    return SinkInsertValueslLoadJob(table, file_path, self.config, self.schema)
        return EmptyLoadJob.from_file_path(file_path, "completed")

    def restore_file_load(self, file_path: str) -> LoadJob:
        return EmptyLoadJob.from_file_path(file_path, "completed")

    def create_table_chain_completed_followup_jobs(
        self, table_chain: Sequence[TTableSchema]
    ) -> List[NewLoadJob]:
        """Creates a list of followup jobs that should be executed after a table chain is completed"""
        return []

    def complete_load(self, load_id: str) -> None:
        pass

    def __enter__(self) -> "SinkClient":
        return self

    def __exit__(
        self, exc_type: Type[BaseException], exc_val: BaseException, exc_tb: TracebackType
    ) -> None:
        pass
