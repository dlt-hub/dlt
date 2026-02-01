from __future__ import annotations

import functools
from abc import ABC, abstractmethod
from typing import Iterator, TYPE_CHECKING, Sequence, Tuple

from dlt.common import logger
from dlt.common.configuration.inject import with_config
from dlt.common.destination.capabilities import LoaderFileFormatSelector
from dlt.common.schema.typing import TTableSchema
from dlt.common.typing import TLoaderFileFormat
from dlt.common.utils import without_none
from dlt.destinations.job_client_impl import SqlJobClientBase

if TYPE_CHECKING:
    from adbc_driver_manager.dbapi import Connection

from dlt.common.destination.client import HasFollowupJobs, RunnableLoadJob


# TODO: driver presence detection, driver location detection to support (see postgres factory)
# dbc and pip install drivers, connection string conversion etc. should be extracted to ADBC
# lib helper (like we do with sqlalchemy or arrow)


@with_config
def has_adbc_driver(driver: str, disable_adbc_detection: bool = False) -> Tuple[bool, str]:
    """Figures out if given driver is available without actually connecting to destination.
    Allows to disable via `disable_adbc_detection` setting in dlt config
    """
    if disable_adbc_detection:
        return False, None
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


class AdbcParquetCopyJob(RunnableLoadJob, HasFollowupJobs, ABC):
    def __init__(self, file_path: str) -> None:
        super().__init__(file_path)
        self._job_client: SqlJobClientBase = None
        # override default schema handling
        self._connect_catalog_name: str = None
        self._connect_schema_name: str = None

    @abstractmethod
    def _connect(self) -> Connection:
        pass

    def _set_catalog_and_schema(self) -> Tuple[str, str]:
        catalog_name = self._connect_catalog_name
        if catalog_name is None:
            catalog_name = self._job_client.sql_client.catalog_name(quote=False)
        elif catalog_name == "":
            # empty string disables catalog
            catalog_name = None

        schema_name = self._connect_schema_name
        if schema_name is None:
            schema_name = self._job_client.sql_client.escape_column_name(
                self._job_client.sql_client.dataset_name, quote=False, casefold=True
            )
        elif schema_name == "":
            # empty string disables schema
            schema_name = None

        return catalog_name, schema_name

    def run(self) -> None:
        from dlt.common.libs.pyarrow import pq_stream_with_new_columns
        from dlt.common.libs.pyarrow import pyarrow

        def _iter_batches(file_path: str) -> Iterator[pyarrow.RecordBatch]:
            for table in pq_stream_with_new_columns(file_path, ()):
                yield from table.to_batches()

        with self._connect() as conn, conn.cursor() as cur:
            import time

            catalog_name, schema_name = self._set_catalog_and_schema()
            kwargs = dict(catalog_name=catalog_name, db_schema_name=schema_name)

            t_ = time.time()
            rows = cur.adbc_ingest(
                self.load_table_name,
                _iter_batches(self._file_path),
                mode="append",
                **without_none(kwargs),  # type: ignore[arg-type,unused-ignore]
            )
            conn.commit()
            logger.info(
                f"{rows} rows copied from {self._file_name} to"
                f" {self.load_table_name}.{schema_name} in {time.time()-t_} s"
            )


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
    found, err_str = has_adbc_driver(driver)
    if not found:
        supported_loader_file_formats = list(supported_loader_file_formats)
        supported_loader_file_formats.remove("parquet")

        if table_schema.get("file_format") == "parquet":
            logger.warning(
                f"parquet file format was requested for table {table_schema['name']} but ADBC"
                f" driver for {driver} was not installed:\n {err_str}\n"
                " Read more: "
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
