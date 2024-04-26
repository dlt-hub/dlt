from typing import Any, Literal, Set, get_args, Dict

from dlt.destinations.utils import ensure_resource
from dlt.extract import DltResource
from dlt.extract.items import TTableHintTemplate


TTableEngineType = Literal["merge_tree", "replicated_merge_tree"]

"""
The table engine (type of table) determines:

- How and where data is stored, where to write it to, and where to read it from.
- Which queries are supported, and how.
- Concurrent data access.
- Use of indexes, if present.
- Whether multithread request execution is possible.
- Data replication parameters.

See https://clickhouse.com/docs/en/engines/table-engines.
"""
TABLE_ENGINE_TYPES: Set[TTableEngineType] = set(get_args(TTableEngineType))
TABLE_ENGINE_TYPE_HINT: Literal["x-table-engine-type"] = "x-table-engine-type"


def clickhouse_adapter(data: Any, table_engine_type: TTableEngineType = None) -> DltResource:
    """Prepares data for the ClickHouse destination by specifying which table engine type
    that should be used.

    Args:
        data (Any): The data to be transformed. It can be raw data or an instance
            of DltResource. If raw data, the function wraps it into a DltResource
            object.
        table_engine_type (TTableEngineType, optional): The table index type used when creating
            the Synapse table.

    Returns:
        DltResource: A resource with applied Synapse-specific hints.

    Raises:
        ValueError: If input for `table_engine_type` is invalid.

    Examples:
        >>> data = [{"name": "Alice", "description": "Software Developer"}]
        >>> clickhouse_adapter(data, table_engine_type="merge_tree")
        [DltResource with hints applied]
    """
    resource = ensure_resource(data)

    additional_table_hints: Dict[str, TTableHintTemplate[Any]] = {}
    if table_engine_type is not None:
        if table_engine_type not in TABLE_ENGINE_TYPES:
            allowed_types = ", ".join(TABLE_ENGINE_TYPES)
            raise ValueError(
                f"Table engine type {table_engine_type} is invalid. Allowed table engine types are:"
                f" {allowed_types}."
            )
        additional_table_hints[TABLE_ENGINE_TYPE_HINT] = table_engine_type
    resource.apply_hints(additional_table_hints=additional_table_hints)
    return resource
