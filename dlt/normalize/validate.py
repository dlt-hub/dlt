from dlt.common.destination.capabilities import DestinationCapabilitiesContext
from dlt.common.schema import Schema
from dlt.common.schema.typing import TTableSchema
from dlt.common.schema.utils import (
    find_incomplete_columns,
    get_first_column_name_with_prop,
    is_nested_table,
)
from dlt.common.schema.exceptions import UnboundColumnException
from dlt.common import logger


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

    parent_key = get_first_column_name_with_prop(table, "parent_key")
    if parent_key and not is_nested_table(table):
        logger.warning(
            f"Table {table['name']} has parent_key on column {parent_key} but no corresponding"
            " `parent` table hint to refer to parent table.Such table is not considered a nested"
            " table and relational normalizer will not generate linking data. The most probable"
            " cause is manual modification of the dtl schema for the table. The most probable"
            f" outcome will be NULL violation during the load process on {parent_key}."
        )
