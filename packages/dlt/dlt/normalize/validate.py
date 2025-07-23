from typing import List

from dlt.common.destination.capabilities import DestinationCapabilitiesContext
from dlt.common.schema import Schema
from dlt.common.schema.typing import TTableSchema, TSchemaUpdate, TColumnSchemaBase
from dlt.common.schema.utils import (
    ensure_compatible_tables,
    find_incomplete_columns,
    get_first_column_name_with_prop,
    is_nested_table,
)
from dlt.common.schema.exceptions import UnboundColumnException, UnboundColumnWithoutTypeException
from dlt.common import logger


def validate_and_update_schema(schema: Schema, schema_updates: List[TSchemaUpdate]) -> None:
    """Updates `schema` tables with partial tables in `schema_updates`"""
    for schema_update in schema_updates:
        for table_name, table_updates in schema_update.items():
            logger.info(f"Updating schema for table {table_name} with {len(table_updates)} deltas")
            for partial_table in table_updates:
                # ensure updates will pass
                if existing_table := schema.tables.get(partial_table["name"]):
                    ensure_compatible_tables(schema.name, existing_table, partial_table)

            for partial_table in table_updates:
                # merge columns where we expect identifiers to be normalized
                schema.update_table(partial_table, normalize_identifiers=False)


def verify_normalized_table(
    schema: Schema, table: TTableSchema, capabilities: DestinationCapabilitiesContext
) -> None:
    """Verify `table` schema is valid for next stage after normalization. Only tables that have seen data are verified.
    Verification happens before seen-data flag is set so new tables can be detected.

    1. Log warning if any incomplete nullable columns are in any data tables
    2. Raise `UnboundColumnException` on incomplete non-nullable columns (e.g. missing merge/primary key)
    3. Log warning if table format is not supported by destination capabilities
    """
    incomplete_nullable_not_seen_data: List[TColumnSchemaBase] = []
    incomplete_nullable_seen_data: List[TColumnSchemaBase] = []

    for column, nullable in find_incomplete_columns(table):
        if nullable:
            seen_null_first = column.get("x-normalizer", {}).get("seen-null-first")
            # warn if column exists in source, but has no data
            if seen_null_first is True:
                incomplete_nullable_not_seen_data.append(column)
            # warn if column doesn't exist in source
            else:
                incomplete_nullable_seen_data.append(column)
        else:
            raise UnboundColumnException(schema.name, table["name"], [column])

    if incomplete_nullable_not_seen_data:
        logger.warning(
            str(
                UnboundColumnWithoutTypeException(
                    schema.name, table["name"], incomplete_nullable_not_seen_data
                )
            )
        )

    if incomplete_nullable_seen_data:
        logger.warning(
            str(UnboundColumnException(schema.name, table["name"], incomplete_nullable_seen_data))
        )

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
            " cause is manual modification of the dlt schema for the table. The most probable"
            f" outcome will be NULL violation during the load process on {parent_key}."
        )
