import os
from typing import Final, Optional, TYPE_CHECKING

from dlt.common import logger
from dlt.common.destination.client import RunnableLoadJob
from dlt.common.metrics import LoadJobMetrics
from dlt.destinations.job_impl import ReferenceFollowupJobRequest
from dlt.destinations.impl.filesystem.table_format.common import (
    LOAD_TABLE_WRITE_DISPOSITION_KEY,
    make_remote_path_for_filesystem_destination,
    make_remote_url_for_filesystem_destination,
    get_arrow_dataset_for_filesystem_source,
    get_partition_column_names_for_named_load_table,
)

if TYPE_CHECKING:
    from dlt.common.libs.pyarrow import pyarrow as pa
    from dlt.destinations.impl.filesystem.filesystem import FilesystemClient

WRITE_DISPOSITION_MERGE: Final = "merge"
DELTA_TABLE_CREATE_MODE_OVERWRITE: Final = "overwrite"


class DeltaLoadFilesystemJob(RunnableLoadJob):
    """Job which loads data to a filesystem via the LFAI Delta specification."""

    def __init__(self, file_path: str):
        super().__init__(file_path)
        self._job_client: "FilesystemClient" = None
        self.file_paths = ReferenceFollowupJobRequest.resolve_references(self._file_path)

    def make_remote_path(self) -> str:
        """Builds a remote path for a Delta table destination.
        Returns:
            remote path housing Delta table files/objects
        """

        return make_remote_path_for_filesystem_destination(self, self._job_client)

    def make_remote_url(self) -> str:
        """Builds a remote url for a Delta table destination
        Returns: remote url for use in systems like DuckDB
        """
        return make_remote_url_for_filesystem_destination(self, self._job_client)

    @property
    def arrow_dataset(self) -> "pa.Dataset":
        return get_arrow_dataset_for_filesystem_source(self)

    @property
    def _partition_columns(self) -> list[str]:
        return get_partition_column_names_for_named_load_table(self)

    def run(self) -> None:
        # create Arrow dataset from Parquet files
        from dlt.common.libs.pyarrow import pyarrow as pa
        from dlt.common.libs.deltalake import write_delta_table, merge_delta_table

        logger.info(
            f"Will copy file(s) {self.file_paths} to delta table {self.make_remote_url()} [arrow"
            f" buffer: {pa.total_allocated_bytes()}]"
        )
        source_ds = self.arrow_dataset
        delta_table = self._delta_table()

        # explicitly check if there is data
        # (https://github.com/delta-io/delta-rs/issues/2686)
        if source_ds.head(1).num_rows == 0:
            delta_table = self._create_or_evolve_delta_table(source_ds, delta_table)
        else:
            with source_ds.scanner().to_reader() as arrow_rbr:  # RecordBatchReader
                if (
                    self._load_table[LOAD_TABLE_WRITE_DISPOSITION_KEY] == WRITE_DISPOSITION_MERGE
                    and delta_table is not None
                ):
                    merge_delta_table(
                        table=delta_table,
                        data=arrow_rbr,
                        schema=self._load_table,
                    )
                else:
                    write_delta_table(
                        table_or_uri=(
                            self.make_remote_url() if delta_table is None else delta_table
                        ),
                        data=arrow_rbr,
                        write_disposition=self._load_table[LOAD_TABLE_WRITE_DISPOSITION_KEY],
                        partition_by=self._partition_columns,
                        storage_options=self._storage_options,
                    )
        # release memory ASAP by deleting objects explicitly
        del source_ds
        del delta_table
        logger.info(
            f"Copied {self.file_paths} to delta table {self.make_remote_url()} [arrow buffer:"
            f" {pa.total_allocated_bytes()}]"
        )

    @property
    def _storage_options(self) -> dict[str, str]:
        from dlt.common.libs.deltalake import _deltalake_storage_options

        return _deltalake_storage_options(self._job_client.config)

    def _delta_table(self) -> Optional["DeltaTable"]:  # type: ignore[name-defined] # noqa: F821
        from dlt.common.libs.deltalake import DeltaTable

        if DeltaTable.is_deltatable(self.make_remote_url(), storage_options=self._storage_options):
            return DeltaTable(self.make_remote_url(), storage_options=self._storage_options)
        else:
            return None

    def _create_or_evolve_delta_table(self, arrow_ds: "Dataset", delta_table: "DeltaTable") -> "DeltaTable":  # type: ignore[name-defined] # noqa: F821
        from dlt.common.libs.deltalake import (
            DeltaTable,
            ensure_delta_compatible_arrow_schema,
            _evolve_delta_table_schema,
        )

        if delta_table is None:
            return DeltaTable.create(
                table_uri=self.make_remote_url(),
                schema=ensure_delta_compatible_arrow_schema(arrow_ds.schema),
                mode=DELTA_TABLE_CREATE_MODE_OVERWRITE,
                partition_by=self._partition_columns,
                storage_options=self._storage_options,
            )
        else:
            return _evolve_delta_table_schema(delta_table, arrow_ds.schema)

    def metrics(self) -> Optional[LoadJobMetrics]:
        m = super().metrics()
        return m._replace(remote_url=self.make_remote_url())
