import uuid
from typing import Sequence

from dlt.common.schema import TTableSchema
from dlt.common.schema.utils import get_columns_names_with_prop
from dlt.common.typing import DictStrAny


def generate_uuid(data: DictStrAny, unique_identifiers: Sequence[str], table_name: str) -> str:
    """Generates deterministic UUID - used for deduplication.

    Args:
        data (Dict[str, Any]): Arbitrary data to generate UUID for.
        unique_identifiers (Sequence[str]): A list of unique identifiers.
        table_name (str): LanceDB table name.

    Returns:
        str: A string representation of the generated UUID.
    """
    data_id = "_".join(str(data[key]) for key in unique_identifiers)
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, table_name + data_id))


def list_unique_identifiers(table_schema: TTableSchema) -> Sequence[str]:
    """Returns a list of unique identifiers for a table.

    Args:
        table_schema (TTableSchema): a dlt table schema.

    Returns:
        Sequence[str]: A list of unique column identifiers.
    """
    if table_schema.get("write_disposition") == "merge":
        if primary_keys := get_columns_names_with_prop(table_schema, "primary_key"):
            return primary_keys
    return get_columns_names_with_prop(table_schema, "unique")
