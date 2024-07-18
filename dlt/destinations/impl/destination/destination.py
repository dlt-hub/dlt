from copy import deepcopy
from types import TracebackType
from typing import ClassVar, Optional, Type, Iterable, cast, List

from dlt.destinations.job_impl import FinalizedLoadJobWithFollowupJobs, FinalizedLoadJob
from dlt.common.destination.reference import LoadJob
from dlt.common.typing import AnyFun
from dlt.common.storages.load_package import destination_state
from dlt.common.configuration import create_resolved_partial

from dlt.common.schema import Schema, TTableSchema, TSchemaTables
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.reference import (
    JobClientBase,
    LoadJob,
)

from dlt.destinations.impl.destination.configuration import CustomDestinationClientConfiguration
from dlt.destinations.job_impl import (
    DestinationJsonlLoadJob,
    DestinationParquetLoadJob,
)


class DestinationClient(JobClientBase):
    """Sink Client"""

    def __init__(
        self,
        schema: Schema,
        config: CustomDestinationClientConfiguration,
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        config.ensure_callable()
        super().__init__(schema, config, capabilities)
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

    def create_load_job(
        self, table: TTableSchema, file_path: str, load_id: str, restore: bool = False
    ) -> LoadJob:
        # skip internal tables and remove columns from schema if so configured
        if self.config.skip_dlt_columns_and_tables:
            if table["name"].startswith(self.schema._dlt_tables_prefix):
                return FinalizedLoadJob(file_path)

        skipped_columns: List[str] = []
        if self.config.skip_dlt_columns_and_tables:
            for column in list(self.schema.tables[table["name"]]["columns"].keys()):
                if column.startswith(self.schema._dlt_tables_prefix):
                    skipped_columns.append(column)

        # save our state in destination name scope
        load_state = destination_state()
        if file_path.endswith("parquet"):
            return DestinationParquetLoadJob(
                file_path,
                self.config,
                load_state,
                self.destination_callable,
                skipped_columns,
            )
        if file_path.endswith("jsonl"):
            return DestinationJsonlLoadJob(
                file_path,
                self.config,
                load_state,
                self.destination_callable,
                skipped_columns,
            )
        return None

    def prepare_load_table(
        self, table_name: str, prepare_for_staging: bool = False
    ) -> TTableSchema:
        table = super().prepare_load_table(table_name, prepare_for_staging)
        if self.config.skip_dlt_columns_and_tables:
            for column in list(table["columns"].keys()):
                if column.startswith(self.schema._dlt_tables_prefix):
                    table["columns"].pop(column)
        return table

    def complete_load(self, load_id: str) -> None: ...

    def __enter__(self) -> "DestinationClient":
        return self

    def __exit__(
        self, exc_type: Type[BaseException], exc_val: BaseException, exc_tb: TracebackType
    ) -> None:
        pass
