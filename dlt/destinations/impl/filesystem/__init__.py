from typing import Sequence, Tuple

from dlt.common.schema.typing import TTableSchema
from dlt.common.destination import DestinationCapabilitiesContext, TLoaderFileFormat


def loader_file_format_adapter(
    preferred_loader_file_format: TLoaderFileFormat,
    supported_loader_file_formats: Sequence[TLoaderFileFormat],
    /,
    *,
    table_schema: TTableSchema,
) -> Tuple[TLoaderFileFormat, Sequence[TLoaderFileFormat]]:
    if table_schema.get("table_format") == "delta":
        return ("parquet", ["parquet"])
    return (preferred_loader_file_format, supported_loader_file_formats)


def capabilities() -> DestinationCapabilitiesContext:
    return DestinationCapabilitiesContext.generic_capabilities(
        preferred_loader_file_format="jsonl",
        loader_file_format_adapter=loader_file_format_adapter,
        supported_table_formats=["delta"],
    )
