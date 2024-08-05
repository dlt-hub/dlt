import os
import uuid
from typing import Sequence, Union, Dict, List

import pyarrow as pa
import pyarrow.compute as pc

from dlt.common.schema import TTableSchema
from dlt.common.schema.utils import get_columns_names_with_prop
from dlt.destinations.impl.lancedb.configuration import TEmbeddingProvider


PROVIDER_ENVIRONMENT_VARIABLES_MAP: Dict[TEmbeddingProvider, str] = {
    "cohere": "COHERE_API_KEY",
    "gemini-text": "GOOGLE_API_KEY",
    "openai": "OPENAI_API_KEY",
    "huggingface": "HUGGINGFACE_API_KEY",
}


# TODO: Update `generate_arrow_uuid_column` when pyarrow 17.0.0 becomes available with vectorized operations (batched + memory-mapped)
def generate_arrow_uuid_column(
    table: pa.Table, unique_identifiers: List[str], id_field_name: str, table_name: str
) -> pa.Table:
    """Generates deterministic UUID - used for deduplication, returning a new arrow
    table with added UUID column.

    Args:
        table (pa.Table): PyArrow table to generate UUIDs for.
        unique_identifiers (List[str]): A list of unique identifier column names.
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


