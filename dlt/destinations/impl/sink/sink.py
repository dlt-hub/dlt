import random
from copy import copy
from types import TracebackType
from typing import ClassVar, Dict, Optional, Sequence, Type, Iterable, List

from dlt.destinations.job_impl import EmptyLoadJob

from dlt.common.schema import Schema, TTableSchema, TSchemaTables
from dlt.common.storages import FileStorage
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.reference import (
    FollowupJob,
    NewLoadJob,
    TLoadJobState,
    LoadJob,
    JobClientBase,
)

from dlt.destinations.exceptions import (
    LoadJobNotExistsException,
)

from dlt.destinations.impl.sink import capabilities
from dlt.destinations.impl.sink.configuration import SinkClientConfiguration, TSinkCallable


class LoadSinkJob(LoadJob, FollowupJob):
    def __init__(self, file_path: str, config: SinkClientConfiguration) -> None:
        super().__init__(FileStorage.get_file_name_from_file_path(file_path))
        self._file_path = file_path
        self._config = config

        # stream items
        from dlt.common.libs.pyarrow import pyarrow

        with pyarrow.parquet.ParquetFile(file_path) as reader:
            for record_batch in reader.iter_batches(batch_size=10):
                for d in record_batch.to_pylist():
                    self._config.credentials.callable(d)

    def state(self) -> TLoadJobState:
        return "completed"

    def exception(self) -> str:
        raise NotImplementedError()


JOBS: Dict[str, LoadSinkJob] = {}


class SinkClient(JobClientBase):
    """Sink client storing jobs in memory"""

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
        return LoadSinkJob(file_path, config=self.config)

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

    def _create_job(self, job_id: str) -> LoadSinkJob:
        return LoadSinkJob(job_id, config=self.config)
