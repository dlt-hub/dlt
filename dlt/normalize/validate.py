from ast import List
from typing import Optional
from dlt.common.destination.capabilities import DestinationCapabilitiesContext
from dlt.common.destination.utils import resolve_merge_strategy
from dlt.common.schema import Schema
from dlt.common.schema.typing import TTableSchema
from dlt.common.schema.utils import find_incomplete_columns, get_validity_column_names
from dlt.common.schema.exceptions import UnboundColumnException
from dlt.common import logger


# def validate_validity_column_names(
#     schema_name: str, validity_column_names: List[Optional[str]]
# ) -> None:
#     """Raises exception if configured validity column name appears in data item."""
#     for validity_column_name in validity_column_names:
#         if validity_column_name in item.keys():
#             raise ColumnNameConflictException(
#                 schema_name,
#                 "Found column in data item with same name as validity column"
#                 f' "{validity_column_name}".',
#             )


def verify_normalized_table(
    schema: Schema, table: TTableSchema, capabilities: DestinationCapabilitiesContext
) -> None:
    """Verify `table` schema is valid for next stage after normalization. Only tables that have seen data are verified.
    Verification happens before seen-data flag is set so new tables can be detected.

    1. Log warning if any incomplete nullable columns are in any data tables
    2. Raise `UnboundColumnException` on incomplete non-nullable columns (e.g. missing merge/primary key)
    3. Log warning if table format is not supported by destination capabilities
    """
    for column, nullable in find_incomplete_columns(table):
        exc = UnboundColumnException(schema.name, table["name"], column)
        if nullable:
            logger.warning(str(exc))
        else:
            raise exc

    # TODO: 3. raise if we detect name conflict for SCD2 columns
    # until we track data per column we won't be able to implement this
    # if resolve_merge_strategy(schema.tables, table, capabilities) == "scd2":
    #     for validity_column_name in get_validity_column_names(table):
    #         if validity_column_name in item.keys():
    #             raise ColumnNameConflictException(
    #                 schema_name,
    #                 "Found column in data item with same name as validity column"
    #                 f' "{validity_column_name}".',
    #             )

    supported_table_formats = capabilities.supported_table_formats or []
    if "table_format" in table and table["table_format"] not in supported_table_formats:
        logger.warning(
            "Destination does not support the configured `table_format` value "
            f"`{table['table_format']}` for table `{table['name']}`. "
            "The setting will probably be ignored."
        )
