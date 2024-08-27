import os
import uuid
from datetime import date, datetime
from typing import Sequence, Union, Dict, List

import pyarrow as pa
import pyarrow.compute as pc

from dlt.common import logger
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


# TODO: Update `generate_arrow_uuid_column` when pyarrow 17.0.0 becomes available with vectorized operations (batched + memory-mapped)
def generate_arrow_uuid_column(
    table: pa.Table, unique_identifiers: Sequence[str], id_field_name: str, table_name: str
) -> pa.Table:
    """Generates deterministic UUID - used for deduplication, returning a new arrow
    table with added UUID column.

    Args:
        table (pa.Table): PyArrow table to generate UUIDs for.
        unique_identifiers (Sequence[str]): A list of unique identifier column names.
        id_field_name (str): Name of the new UUID column.
        table_name (str): Name of the table.

    Returns:
        pa.Table: New PyArrow table with the new UUID column.
    """

    unique_identifiers_columns = []
    for col in unique_identifiers:
        column = pc.fill_null(pc.cast(table[col], pa.string()), "")
        unique_identifiers_columns.append(column.to_pylist())

    uuids = pa.array(
        [
            str(uuid.uuid5(uuid.NAMESPACE_OID, x + table_name))
            for x in ["".join(x) for x in zip(*unique_identifiers_columns)]
        ]
    )

    return table.append_column(id_field_name, uuids)


def get_unique_identifiers_from_table_schema(table_schema: TTableSchema) -> List[str]:
    """Returns a list of merge keys for a table used for either merging or deduplication.

    Args:
        table_schema (TTableSchema): a dlt table schema.

    Returns:
        Sequence[str]: A list of unique column identifiers.
    """
    primary_keys = get_columns_names_with_prop(table_schema, "primary_key")
    merge_keys = []
    if table_schema.get("write_disposition") == "merge":
        merge_keys = get_columns_names_with_prop(table_schema, "merge_key")
    if join_keys := list(set(primary_keys + merge_keys)):
        return join_keys
    else:
        return get_columns_names_with_prop(table_schema, "unique")


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


def get_lancedb_merge_key(load_table: TTableSchema) -> str:
    if merge_key_ := get_columns_names_with_prop(load_table, "merge_key"):
        if len(merge_key_) > 1:
            DestinationTerminalException(
                "LanceDB destination does not support compound merge keys."
            )
    if primary_key := get_columns_names_with_prop(load_table, "primary_key"):
        if merge_key_:
            logger.warning(
                "LanceDB destination currently does not yet support primary key constraints:"
                " https://github.com/lancedb/lancedb/issues/1120. Only `merge_key` is supported."
                " The supplied primary key will be ignored!"
            )
        elif len(primary_key) == 1:
            logger.warning(
                "LanceDB destination currently does not yet support primary key constraints:"
                " https://github.com/lancedb/lancedb/issues/1120. Only `merge_key` is supported."
                " The primary key will be used as a proxy merge key! Please use `merge_key` instead."
            )
            return primary_key[0]
    if len(merge_key_) == 1:
        return merge_key_[0]
    elif len(unique_key := get_columns_names_with_prop(load_table, "unique")) == 1:
        return unique_key[0]
    else:
        return ""
