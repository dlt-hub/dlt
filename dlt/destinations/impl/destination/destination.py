from copy import deepcopy
from types import TracebackType
from typing import ClassVar, Optional, Type, Iterable, cast, List

from dlt.common.destination.reference import LoadJob
from dlt.destinations.job_impl import EmptyLoadJob
from dlt.common.typing import AnyFun
from dlt.pipeline.current import destination_state
from dlt.common.configuration import create_resolved_partial

from dlt.common.schema import Schema, TTableSchema, TSchemaTables
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.reference import (
    LoadJob,
    DoNothingJob,
    JobClientBase,
)

from dlt.destinations.impl.destination import capabilities
from dlt.destinations.impl.destination.configuration import CustomDestinationClientConfiguration
from dlt.destinations.job_impl import (
    DestinationJsonlLoadJob,
    DestinationParquetLoadJob,
)


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
        self,
        only_tables: Iterable[str] = None,
        expected_update: TSchemaTables = None,
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
