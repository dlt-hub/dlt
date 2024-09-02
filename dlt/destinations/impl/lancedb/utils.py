import os
from typing import Union, Dict, List

import pyarrow as pa

from dlt import Schema
from dlt.common import logger
from dlt.common.destination.exceptions import DestinationTerminalException
from dlt.common.pendulum import __utcnow
from dlt.common.schema import TTableSchema
from dlt.common.schema.utils import get_columns_names_with_prop
from dlt.destinations.impl.lancedb.configuration import TEmbeddingProvider
from dlt.destinations.impl.lancedb.schema import TArrowDataType

EMPTY_STRING_PLACEHOLDER = "0uEoDNBpQUBwsxKbmxxB"
PROVIDER_ENVIRONMENT_VARIABLES_MAP: Dict[TEmbeddingProvider, str] = {
    "cohere": "COHERE_API_KEY",
    "gemini-text": "GOOGLE_API_KEY",
    "openai": "OPENAI_API_KEY",
    "huggingface": "HUGGINGFACE_API_KEY",
}


def set_non_standard_providers_environment_variables(
    embedding_model_provider: TEmbeddingProvider, api_key: Union[str, None]
) -> None:
    if embedding_model_provider in PROVIDER_ENVIRONMENT_VARIABLES_MAP:
        os.environ[PROVIDER_ENVIRONMENT_VARIABLES_MAP[embedding_model_provider]] = api_key or ""


def get_default_arrow_value(field_type: TArrowDataType) -> object:
    if pa.types.is_integer(field_type):
        return 0
    elif pa.types.is_floating(field_type):
        return 0.0
    elif pa.types.is_string(field_type):
        return ""
    elif pa.types.is_boolean(field_type):
        return False
    elif pa.types.is_date(field_type):
        return __utcnow().today()
    elif pa.types.is_timestamp(field_type):
        return __utcnow()
    else:
        raise ValueError(f"Unsupported data type: {field_type}")


def get_canonical_vector_database_doc_id_merge_key(
    load_table: TTableSchema,
) -> str:
    if merge_key := get_columns_names_with_prop(load_table, "merge_key"):
        if len(merge_key) > 1:
            raise DestinationTerminalException(
                "You cannot specify multiple merge keys with LanceDB orphan remove enabled:"
                f" {merge_key}"
            )
        else:
            return merge_key[0]
    elif primary_key := get_columns_names_with_prop(load_table, "primary_key"):
        # No merge key defined, warn and assume the first element of the primary key is `doc_id`.
        logger.warning(
            "Merge strategy selected without defined merge key - using the first element of the"
            f" primary key ({primary_key}) as merge key."
        )
        return primary_key[0]
    else:
        raise DestinationTerminalException(
            "You must specify at least a primary key in order to perform orphan removal."
        )


def fill_empty_source_column_values_with_placeholder(
    table: pa.Table, source_columns: List[str], placeholder: str
) -> pa.Table:
    """
    Replaces empty strings and null values in the specified source columns of an Arrow table with a placeholder string.

    Args:
        table (pa.Table): The input Arrow table.
        source_columns (List[str]): A list of column names to replace empty strings and null values in.
        placeholder (str): The placeholder string to use for replacement.

    Returns:
        pa.Table: The modified Arrow table with empty strings and null values replaced in the specified columns.
    """
    for col_name in source_columns:
        column = table[col_name]
        filled_column = pa.compute.fill_null(column, fill_value=placeholder)
        new_column = pa.compute.replace_substring_regex(
            filled_column, pattern=r"^$", replacement=placeholder
        )
        table = table.set_column(table.column_names.index(col_name), col_name, new_column)
    return table


def create_filter_condition(canonical_doc_id_field: str, id_column: pa.Array) -> str:
    def format_value(element: Union[str, int, float, pa.Scalar]) -> str:
        if isinstance(element, pa.Scalar):
            element = element.as_py()
        return "'" + element.replace("'", "''") + "'" if isinstance(element, str) else str(element)

    return f"{canonical_doc_id_field} IN ({', '.join(map(format_value, id_column))})"


def add_missing_columns_to_arrow_table(
    payload_arrow_table: pa.Table,
    target_table_schema: pa.Schema,
) -> pa.Table:
    """Add missing columns from the target schema to the payload Arrow table.

    LanceDB requires the payload to have all fields populated, even if we
    don't intend to use them in our merge operation.

    Unfortunately, we can't just create NULL fields; else LanceDB always truncates
    the target using `when_not_matched_by_source_delete`.
    This function identifies columns present in the target schema but missing from
    the payload table and adds them with either default or null values.

     Args:
         payload_arrow_table: The input Arrow table.
         target_table_schema: The schema of the target table.

     Returns:
         The modified Arrow table with added columns.

    """
    schema_difference = pa.schema(set(target_table_schema) - set(payload_arrow_table.schema))

    for field in schema_difference:
        try:
            default_value = get_default_arrow_value(field.type)
            default_array = pa.array(
                [default_value] * payload_arrow_table.num_rows, type=field.type
            )
            payload_arrow_table = payload_arrow_table.append_column(field, default_array)
        except ValueError as e:
            logger.warning(f"{e}. Using null values for field '{field.name}'.")
            payload_arrow_table = payload_arrow_table.append_column(
                field, pa.nulls(size=payload_arrow_table.num_rows, type=field.type)
            )

    return payload_arrow_table


def get_root_table_name(table: TTableSchema, schema: Schema) -> str:
    """Identify a table's root table."""
    if parent_name := table.get("parent"):
        parent = schema.get_table(parent_name)
        return get_root_table_name(parent, schema)
    else:
        return table["name"]
