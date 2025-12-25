from typing import Any, Dict, Optional

from dlt.common.typing import NoneType
from dlt.destinations.impl.clickhouse.typing import (
    PARTITION_HINT,
    SETTINGS_HINT,
    SORT_HINT,
    TABLE_ENGINE_TYPES,
    TABLE_ENGINE_TYPE_HINT,
    TMergeTreeSettings,
    TSQLExprOrColumnSeq,
    TTableEngineType,
)
from dlt.destinations.utils import get_resource_for_adapter
from dlt.common.exceptions import ValueErrorWithKnownValues
from dlt.extract import DltResource
from dlt.extract.items import TTableHintTemplate


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


def clickhouse_adapter(
    data: Any,
    table_engine_type: Optional[TTableEngineType] = None,
    sort: Optional[TSQLExprOrColumnSeq] = None,
    partition: Optional[TSQLExprOrColumnSeq] = None,
    settings: Optional[TMergeTreeSettings] = None,
) -> DltResource:
    """Prepares data for the ClickHouse destination by specifying which table engine type
    that should be used.

    Args:
        data (Any): The data to be transformed. It can be raw data or an instance
            of DltResource. If raw data, the function wraps it into a DltResource
            object.
        table_engine_type (TTableEngineType, optional): The table index type used when creating
            the Clickhouse table.
        sort (str, optional): Sorting key SQL expression. Will be added to `ORDER BY` clause of
            table creation statement. Use normalized column names when referring to columns.
        partition (str, optional): Partition key SQL expression. Will be added to `PARTITION BY`
            clause of table creation statement. Use normalized column names when referring to
            columns.
        settings (TMergeTreeSettings, optional): Dictionary of MergeTree settings to apply to the
            table. Will be added to `SETTINGS` clause of table creation statement.

    Returns:
        DltResource: A resource with applied Clickhouse-specific hints.

    Raises:
        ValueError: If input for `table_engine_type` is invalid.

    Examples:
        >>> data = [{"name": "Alice", "description": "Software Developer"}]
        >>> clickhouse_adapter(data, table_engine_type="merge_tree")
        [DltResource with hints applied]
    """

    def raise_if_not_none_str_seq(val: Any, name: str) -> None:
        accepted_types = (NoneType, str, list, tuple)
        if not isinstance(val, accepted_types):
            raise TypeError(
                f"`{name}` must be a string or a list or tuple of strings, got"
                f" '{type(val).__name__}'"
            )

    resource = get_resource_for_adapter(data)

    additional_table_hints: Dict[str, TTableHintTemplate[Any]] = {}

    # table engine type
    if table_engine_type is not None:
        if table_engine_type not in TABLE_ENGINE_TYPES:
            raise ValueErrorWithKnownValues(
                "table_engine_type", table_engine_type, TABLE_ENGINE_TYPES
            )

        additional_table_hints[TABLE_ENGINE_TYPE_HINT] = table_engine_type

    # sort
    raise_if_not_none_str_seq(sort, "sort")
    if sort:
        additional_table_hints[SORT_HINT] = sort

    # partition
    raise_if_not_none_str_seq(partition, "partition")
    if partition:
        additional_table_hints[PARTITION_HINT] = partition

    # settings
    if not isinstance(settings, (NoneType, dict)):
        raise TypeError(
            "`settings` must be a dictionary of MergeTree settings, got"
            f" '{type(settings).__name__}'"
        )
    if settings:
        additional_table_hints[SETTINGS_HINT] = settings

    resource.apply_hints(additional_table_hints=additional_table_hints)
    return resource
