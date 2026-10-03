"""Runtime detection for the native Arrow bulk copy path.

Kept out of `mssql.py` so the destination factory can probe for it without importing the job
client, which pulls in the driver.
"""

import functools
from typing import Optional, Sequence, Tuple

from dlt.common import logger
from dlt.common.destination.capabilities import LoaderFileFormatSelector
from dlt.common.exceptions import MissingDependencyException
from dlt.common.schema.typing import TTableSchema
from dlt.common.typing import TLoaderFileFormat


def has_native_arrow_bulk_copy() -> Tuple[bool, Optional[str]]:
    """Checks whether parquet load files can be streamed into mssql-python's Arrow bulk copy.

    Returns:
        Tuple[bool, Optional[str]]: whether the path is available, and why it is not.
    """
    try:
        from dlt.common.libs.pyarrow import pyarrow  # noqa: F401
    except MissingDependencyException as dep_ex:
        return False, str(dep_ex)

    try:
        from mssql_python import Cursor
    except ImportError as import_ex:
        return False, str(import_ex)

    if not hasattr(Cursor, "bulkcopy_arrow"):
        return False, "the installed mssql-python has no `Cursor.bulkcopy_arrow`, added in 1.13.0"
    return True, None


def _loader_file_format_selector(
    docs_url: str,
    prefer_parquet: bool,
    preferred_loader_file_format: TLoaderFileFormat,
    supported_loader_file_formats: Sequence[TLoaderFileFormat],
    /,
    *,
    table_schema: TTableSchema,
) -> Tuple[TLoaderFileFormat, Sequence[TLoaderFileFormat]]:
    found, err_str = has_native_arrow_bulk_copy()
    if not found:
        supported_loader_file_formats = list(supported_loader_file_formats)
        supported_loader_file_formats.remove("parquet")

        if table_schema.get("file_format") == "parquet":
            logger.warning(
                f"parquet file format was requested for table {table_schema['name']} but the"
                f" native Arrow bulk copy is not available:\n {err_str}\n Read more: "
                + docs_url
            )
    elif prefer_parquet:
        preferred_loader_file_format = "parquet"

    return (preferred_loader_file_format, supported_loader_file_formats)


def make_native_parquet_file_format_selector(
    docs_url: str,
    prefer_parquet: bool,
) -> LoaderFileFormatSelector:
    """Factory for a file format selector that drops parquet when Arrow bulk copy is unavailable"""

    return functools.partial(_loader_file_format_selector, docs_url, prefer_parquet)  # type: ignore[return-value]
