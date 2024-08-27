import os
from datetime import date, datetime
from typing import Union, Dict, Optional, TypeVar, Generic, Iterable, Iterator

import pyarrow as pa

from dlt.common.destination.exceptions import DestinationTerminalException
from dlt.common.schema import TTableSchema
from dlt.common.schema.utils import get_columns_names_with_prop
from dlt.destinations.impl.lancedb.configuration import TEmbeddingProvider
from dlt.destinations.impl.lancedb.schema import TArrowDataType


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
        return date.today()
    elif pa.types.is_timestamp(field_type):
        return datetime.now()
    else:
        raise ValueError(f"Unsupported data type: {field_type}")


ItemType = TypeVar('ItemType')


# LanceDB `merge_insert` expects an 'iter()' method instead of using standard iteration.
# https://github.com/lancedb/lancedb/blob/ae85008714792a6b724c75793b63273c51caba88/python/python/lancedb/table.py#L2264
class IterableWrapper(Generic[ItemType]):
    def __init__(self, iterable: Iterable[ItemType]) -> None:
        self.iterable = iterable

    def __iter__(self) -> Iterator[ItemType]:
        return iter(self.iterable)

    def iter(self) -> Iterator[ItemType]:
        return iter(self.iterable)


def get_lancedb_merge_key(load_table: TTableSchema) -> Optional[Union[str, IterableWrapper[str]]]:
    if get_columns_names_with_prop(load_table, "primary_key"):
        raise DestinationTerminalException(
            "LanceDB destination currently does not yet support primary key constraints: "
            "https://github.com/lancedb/lancedb/issues/1120. Only `merge_key` is supported!"
        )
    if merge_key := get_columns_names_with_prop(load_table, "merge_key"):
        return merge_key[0] if len(merge_key)==1 else IterableWrapper(merge_key)
    elif unique_key := get_columns_names_with_prop(load_table, "unique"):
        return unique_key[0] if len(unique_key)==1 else IterableWrapper(unique_key)
    else:
        return None
