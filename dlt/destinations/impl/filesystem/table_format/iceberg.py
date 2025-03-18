import os
from typing import Any, Optional, TYPE_CHECKING

from dlt.common import logger
from dlt.common.metrics import LoadJobMetrics
from dlt.destinations.job_impl import ReferenceFollowupJobRequest
from dlt.common.destination.client import RunnableLoadJob
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


class IcebergLoadFilesystemJob(RunnableLoadJob):
    """Job that loads data to a filesystem via the Apache Iceberg specification."""

    def __init__(self, file_path: str):
        super().__init__(file_path)
        self._job_client: "FilesystemClient" = None
        self.file_paths = ReferenceFollowupJobRequest.resolve_references(self._file_path)

    def make_remote_path(self) -> str:
        """Builds a remote path for an Iceberg table destination.
        Returns:
            remote path housing Delta table files/objects
        """

        return make_remote_path_for_filesystem_destination(self, self._job_client)

    def make_remote_url(self) -> str:
        """Builds a remote url for an Iceberg table destination
        Returns:
            remote url for use in systems like DuckDB
        """
        return make_remote_url_for_filesystem_destination(self, self._job_client)

    @property
    def arrow_dataset(self) -> "pa.Dataset":
        return get_arrow_dataset_for_filesystem_source(self)

    @property
    def _partition_columns(self) -> list[str]:
        return get_partition_column_names_for_named_load_table(self)

    def run(self) -> None:
        from dlt.common.libs.pyiceberg import write_iceberg_table

        write_iceberg_table(
            table=self._iceberg_table(),
            data=self.arrow_dataset.to_table(),
            write_disposition=self._load_table[LOAD_TABLE_WRITE_DISPOSITION_KEY],
        )

    def _iceberg_table(self) -> "pyiceberg.table.Table":  # type: ignore[name-defined] # noqa: F821
        from dlt.common.libs.pyiceberg import get_catalog

        catalog = get_catalog(
            client=self._job_client,
            table_name=self.load_table_name,
            schema=self.arrow_dataset.schema,
            partition_columns=self._partition_columns,
        )
        return catalog.load_table(self.table_identifier)

    @property
    def table_identifier(self) -> str:
        return f"{self._job_client.dataset_name}.{self.load_table_name}"

    def metrics(self) -> Optional[LoadJobMetrics]:
        m = super().metrics()
        return m._replace(remote_url=self.make_remote_url())
