import os
from typing import Union, Dict, List

import pyarrow as pa

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
            raise DestinationTerminalException(f"You cannot specify multiple merge keys with LanceDB orphan remove enabled: {merge_key}")
        else:
            return merge_key[0]
    elif primary_key := get_columns_names_with_prop(load_table, "primary_key"):
        # No merge key defined, warn and assume the first element of the primary key is `doc_id`.
        logger.warning(
            f"Merge strategy selected without defined merge key - using the first element of the primary key ({primary_key}) as merge key."
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
