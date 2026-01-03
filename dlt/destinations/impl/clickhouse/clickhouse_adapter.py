from typing import Any, Dict, Optional

from dlt.common.typing import NoneType
from dlt.destinations.impl.clickhouse.typing import (
    CODEC_HINT,
    PARTITION_HINT,
    SETTINGS_HINT,
    SORT_HINT,
    TABLE_ENGINE_TYPES,
    TABLE_ENGINE_TYPE_HINT,
    TColumnCodecs,
    TMergeTreeSettings,
    TSQLExprOrColumnSeq,
    TTableEngineType,
)
from dlt.destinations.utils import get_resource_for_adapter
from dlt.common.exceptions import ValueErrorWithKnownValues
from dlt.extract import DltResource
from dlt.extract.items import TTableHintTemplate


def clickhouse_adapter(
    data: Any,
    table_engine_type: Optional[TTableEngineType] = None,
    sort: Optional[TSQLExprOrColumnSeq] = None,
    partition: Optional[TSQLExprOrColumnSeq] = None,
    settings: Optional[TMergeTreeSettings] = None,
    codecs: Optional[TColumnCodecs] = None,
) -> DltResource:
    """Adapts the given data by applying Clickhouse-specific hints.

    Args:
        data (Any): The data to be transformed. It can be raw data or an instance
            of DltResource. If raw data, the function wraps it into a DltResource
            object.
        table_engine_type (TTableEngineType, optional): The table index type used when creating
            the Clickhouse table.
        sort (TSQLExprOrColumnSeq, optional): Sorting key SQL expression or sequence of column
            names. Used to generated `ORDER BY` clause of table creation statement. If passing a SQL
            expression, use normalized column names when referring to columns.
        partition (TSQLExprOrColumnSeq, optional): Partition key SQL expression or sequence of
            column names. Used to generated `PARTITION BY` clause of table creation statement. If
            passing a SQL expression, use normalized column names when referring to columns.
        settings (TMergeTreeSettings, optional): Dictionary of MergeTree settings to apply to the
            table. Will be added to `SETTINGS` clause of table creation statement.
        codecs (TColumnCodecs, optional): Dictionary of codecs to apply to the table's columns.
            Will be added as `CODEC` clauses in column definitions of table creation statement.

    Returns:
        DltResource: A resource with applied Clickhouse-specific hints.

    Raises:
        ValueError: If input for `table_engine_type` is invalid.
        TypeError: If input types for `sort`, `partition`, `settings`, or `codecs` are invalid.

    Examples:
        Set table engine type:

        >>> data = [{"name": "Alice", "description": "Software Developer"}]
        >>> clickhouse_adapter(data, table_engine_type="merge_tree")

        Set sort and partition keys:

        >>> data = [{"date": "2024-01-01", "town": "Springfield", "street": "Evergreen Terrace"}]
        >>> clickhouse_adapter(
        >>>     data,
        >>>     sort=["town", "street"],  # can also be SQL expression
        >>>     partition="toYYYYMM(date)"  # can also be sequence of column names
        >>> )

        Set MergeTree settings:

        >>> clickhouse_adapter(
        >>>     data,
        >>>     settings={"allow_nullable_key": True, "max_suspicious_broken_parts": 500}
        >>> )

        Set column codecs:

        >>> clickhouse_adapter(
        >>>     data,
        >>>     codecs={"town": "LZ4HC", "street": "Delta, ZSTD(2)"}
        >>> )
    """

    def raise_if_not_none_str_seq(val: Any, name: str) -> None:
        accepted_types = (NoneType, str, list, tuple)
        if not isinstance(val, accepted_types):
            raise TypeError(
                f"`{name}` must be a string or a list or tuple of strings, got"
                f" '{type(val).__name__}'"
            )

    def raise_if_not_none_dict(val: Any, name: str) -> None:
        accepted_types = (NoneType, dict)
        if not isinstance(val, accepted_types):
            raise TypeError(f"`{name}` must be a dictionary, got '{type(val).__name__}'")

    resource = get_resource_for_adapter(data)

    columns = None
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
    raise_if_not_none_dict(settings, "settings")
    if settings:
        additional_table_hints[SETTINGS_HINT] = settings

    # codecs
    raise_if_not_none_dict(codecs, "codecs")
    if codecs:
        columns = [{"name": name, CODEC_HINT: codec} for name, codec in codecs.items()]

    resource.apply_hints(columns=columns, additional_table_hints=additional_table_hints)  # type: ignore[arg-type]
    return resource
