import os
from typing import Union, Dict, Optional, TypeVar, Generic, Iterable, Iterator, List

import pyarrow as pa

from dlt.common import logger
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


ItemType = TypeVar("ItemType")


# LanceDB `merge_insert` expects an 'iter()' method instead of using standard iteration.
# https://github.com/lancedb/lancedb/blob/ae85008714792a6b724c75793b63273c51caba88/python/python/lancedb/table.py#L2264
class IterableWrapper(Generic[ItemType]):
    def __init__(self, iterable: Iterable[ItemType]) -> None:
        self.iterable = iterable

    def __iter__(self) -> Iterator[ItemType]:
        return iter(self.iterable)

    def iter(self) -> Iterator[ItemType]:  # noqa: A003
        return iter(self.iterable)


def get_lancedb_orphan_removal_merge_key(
    load_table: TTableSchema,
) -> Optional[Union[str, IterableWrapper[str]]]:
    if merge_key := get_columns_names_with_prop(load_table, "merge_key"):
        return merge_key[0] if len(merge_key) == 1 else IterableWrapper(merge_key)
    elif primary_key := get_columns_names_with_prop(load_table, "primary_key"):
        # No merge key defined, warn and merge on the primary key.
        logger.warning(
            "Merge strategy selected without defined merge key - using primary key as merge key."
        )
        return primary_key[0] if len(primary_key) == 1 else IterableWrapper(merge_key)
    elif unique_key := get_columns_names_with_prop(load_table, "unique"):
        logger.warning(
            "Merge strategy selected without defined merge key - using unique key as merge key."
        )
        return unique_key[0] if len(unique_key) == 1 else IterableWrapper(unique_key)
    else:
        return None


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
