from __future__ import annotations

import functools
from abc import ABC, abstractmethod
from typing import Iterator, TYPE_CHECKING, Sequence, Tuple

from dlt.common import logger
from dlt.common.destination.capabilities import LoaderFileFormatSelector
from dlt.common.schema.typing import TTableSchema
from dlt.common.typing import TLoaderFileFormat
from dlt.destinations.job_client_impl import SqlJobClientBase

if TYPE_CHECKING:
    from adbc_driver_manager.dbapi import Connection

from dlt.common.destination.client import RunnableLoadJob


class AdbcParquetCopyJob(RunnableLoadJob, ABC):
    def __init__(self, file_path: str) -> None:
        super().__init__(file_path)
        self._job_client: SqlJobClientBase = None

    @abstractmethod
    def _connect(self) -> Connection:
        pass

    def run(self) -> None:
        from dlt.common.libs.pyarrow import pq_stream_with_new_columns
        from dlt.common.libs.pyarrow import pyarrow

        def _iter_batches(file_path: str) -> Iterator[pyarrow.RecordBatch]:
            for table in pq_stream_with_new_columns(file_path, ()):
                yield from table.to_batches()

        with self._connect() as conn, conn.cursor() as cur:
            import time

            t_ = time.time()
            rows = cur.adbc_ingest(
                self.load_table_name,
                _iter_batches(self._file_path),
                mode="append",
                catalog_name=self._job_client.sql_client.catalog_name(quote=False),
                db_schema_name=self._job_client.sql_client.fully_qualified_dataset_name(
                    quote=False
                ),
            )
            conn.commit()
            logger.warning(
                f"{rows} rows copied from {self._file_name} to {self.load_table_name} in"
                f" {time.time()-t_} s"
            )


def has_driver(driver: str) -> Tuple[bool, str]:
    """Figures out if given driver is available without actually connecting to destination"""
    try:
        import adbc_driver_manager as dm

        try:
            db = dm.AdbcDatabase(driver=driver, uri="")
            db.close()
            return True, None
        except dm.Error as pex:
            # NOT_FOUND returned when driver library can't be found
            if pex.status_code in (dm.AdbcStatusCode.NOT_FOUND, dm.AdbcStatusCode.NOT_IMPLEMENTED):
                return False, str(pex)
            return True, str(pex)
    except ImportError as import_ex:
        return False, str(import_ex)


def _loader_file_format_selector(
    driver: str,
    docs_url: str,
    prefer_parquet: bool,
    preferred_loader_file_format: TLoaderFileFormat,
    supported_loader_file_formats: Sequence[TLoaderFileFormat],
    /,
    *,
    table_schema: TTableSchema,
) -> Tuple[TLoaderFileFormat, Sequence[TLoaderFileFormat]]:
    found, err_str = has_driver(driver)
    if not found:
        supported_loader_file_formats = list(supported_loader_file_formats)
        supported_loader_file_formats.remove("parquet")

        if table_schema.get("file_format") == "parquet":
            logger.warning(
                f"parquet file format was requested for table {table_schema['name']} but ADBC"
                f" driver for {driver} was not installed:\n {err_str}\n Read more: "
                + docs_url
            )
    else:
        if prefer_parquet:
            # parquet is preferred format if driver is enabled
            preferred_loader_file_format = "parquet"

    return (preferred_loader_file_format, supported_loader_file_formats)


def make_adbc_parquet_file_format_selector(
    driver: str,
    docs_url: str,
    prefer_parquet: bool,
) -> LoaderFileFormatSelector:
    """Factory for file format selector that removes parquet from the list if `driver` not installed"""

    return functools.partial(_loader_file_format_selector, driver, docs_url, prefer_parquet)  # type: ignore[return-value]
