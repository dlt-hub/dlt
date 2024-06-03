from typing import Sequence, Tuple

from dlt.common.schema.typing import TSchemaTables
from dlt.common.destination import DestinationCapabilitiesContext, TLoaderFileFormat


def loader_file_format_adapter(
    preferred_loader_file_format: TLoaderFileFormat,
    supported_loader_file_formats: Sequence[TLoaderFileFormat],
    /,
    *,
    schema_tables: TSchemaTables,
) -> Tuple[TLoaderFileFormat, Sequence[TLoaderFileFormat]]:
    if any([table.get("table_format") == "delta" for table in schema_tables.values()]):
        return ("parquet", ["parquet"])
    return (preferred_loader_file_format, supported_loader_file_formats)


def capabilities() -> DestinationCapabilitiesContext:
    return DestinationCapabilitiesContext.generic_capabilities(
        preferred_loader_file_format="jsonl",
        loader_file_format_adapter=loader_file_format_adapter,
        supported_table_formats=["delta"],
    )
