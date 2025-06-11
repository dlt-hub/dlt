from typing import Any, Literal, Set, Final, Dict

from dlt.common.typing import get_args
from dlt.common.exceptions import ValueErrorWithKnownValues
from dlt.extract import DltResource, resource as make_resource
from dlt.extract.items import TTableHintTemplate
from dlt.extract.hints import TResourceHints
from dlt.destinations.utils import get_resource_for_adapter

TTableIndexType = Literal["heap", "clustered_columnstore_index"]
"""
Table [index type](https://learn.microsoft.com/en-us/azure/synapse-analytics/sql-data-warehouse/sql-data-warehouse-tables-index) used when creating the Synapse table.
This regards indexes specified at the table level, not the column level.
"""
TABLE_INDEX_TYPES: Set[TTableIndexType] = set(get_args(TTableIndexType))

TABLE_INDEX_TYPE_HINT: Literal["x-table-index-type"] = "x-table-index-type"


def synapse_adapter(data: Any, table_index_type: TTableIndexType = None) -> DltResource:
    """Prepares data for the Synapse destination by specifying which table index
    type should be used.

    Args:
        data (Any): The data to be transformed. It can be raw data or an instance
            of DltResource. If raw data, the function wraps it into a DltResource
            object.
        table_index_type (TTableIndexType, optional): The table index type used when creating
            the Synapse table.

    Returns:
        DltResource: A resource with applied Synapse-specific hints.

    Raises:
        ValueError: If input for `table_index_type` is invalid.

    Examples:
        >>> data = [{"name": "Anush", "description": "Integrations Hacker"}]
        >>> synapse_adapter(data, table_index_type="clustered_columnstore_index")
        [DltResource with hints applied]
    """
    resource = get_resource_for_adapter(data)

    additional_table_hints: Dict[str, TTableHintTemplate[Any]] = {}
    if table_index_type is not None:
        if table_index_type not in TABLE_INDEX_TYPES:
            raise ValueErrorWithKnownValues("table_index_type", table_index_type, TABLE_INDEX_TYPES)

        additional_table_hints[TABLE_INDEX_TYPE_HINT] = table_index_type
    resource.apply_hints(additional_table_hints=additional_table_hints)
    return resource
