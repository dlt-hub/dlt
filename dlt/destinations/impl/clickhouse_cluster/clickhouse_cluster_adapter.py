from typing import Any, Dict, Literal, Optional

from dlt.destinations.impl.clickhouse.clickhouse_adapter import clickhouse_adapter
from dlt.destinations.impl.clickhouse.typing import (
    TColumnCodecs,
    TMergeTreeSettings,
    TSQLExprOrColumnSeq,
    TTableEngineType,
)
from dlt.extract.resource import DltResource
from tests.common.test_validation import TTableHintTemplate


CREATE_DISTRIBUTED_TABLES_HINT: Literal["x-create-distributed-tables"] = (
    "x-create-distributed-tables"
)
DISTRIBUTED_TABLE_SUFFIX_HINT: Literal["x-distributed-table-suffix"] = "x-distributed-table-suffix"
SHARDING_KEY_HINT: Literal["x-sharding-key"] = "x-sharding-key"

# maps ClickHouseClusterClientConfiguration keys to corresponding table hints
CONFIG_HINT_MAP = {
    "create_distributed_tables": CREATE_DISTRIBUTED_TABLES_HINT,
    "distributed_table_suffix": DISTRIBUTED_TABLE_SUFFIX_HINT,
    "sharding_key": SHARDING_KEY_HINT,
}


def clickhouse_cluster_adapter(
    data: Any,
    table_engine_type: Optional[TTableEngineType] = None,
    sort: Optional[TSQLExprOrColumnSeq] = None,
    partition: Optional[TSQLExprOrColumnSeq] = None,
    settings: Optional[TMergeTreeSettings] = None,
    codecs: Optional[TColumnCodecs] = None,
    create_distributed_tables: Optional[bool] = None,
    distributed_table_suffix: Optional[str] = None,
    sharding_key: Optional[str] = None,
) -> DltResource:
    """Adapts the given data by applying Clickhouse Cluster-specific hints.

    Args:
        data (Any): The data to be transformed. It can be raw data or an instance
            of DltResource. If raw data, the function wraps it into a DltResource
            object.
        table_engine_type (TTableEngineType, optional): The table engine type used when creating
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
        create_distributed_tables (bool, optional): Whether to create distributed tables in addition
            to standard tables.
        distributed_table_suffix (str, optional): Suffix to append to table names when creating
            distributed tables. For example, if set to `_dist`, a table named `events` will have a
            distributed table named `events_dist`.
        sharding_key (str, optional): Sharding key expression to use for distributed tables.

    Returns:
        DltResource: A resource with applied Clickhouse Cluster-specific hints.

    Raises:
        ValueError: If input for `table_engine_type` is invalid.
        TypeError: If input types for `sort`, `partition`, `settings`, or `codecs` are invalid.

    Examples:
        Set table engine type:

        >>> data = [{"name": "Alice", "description": "Software Developer"}]
        >>> clickhouse_cluster_adapter(data, table_engine_type="merge_tree")

        Set sort and partition keys:

        >>> data = [{"date": "2024-01-01", "town": "Springfield", "street": "Evergreen Terrace"}]
        >>> clickhouse_cluster_adapter(
        >>>     data,
        >>>     sort=["town", "street"],  # can also be SQL expression
        >>>     partition="toYYYYMM(date)"  # can also be sequence of column names
        >>> )

        Set MergeTree settings:

        >>> clickhouse_cluster_adapter(
        >>>     data,
        >>>     settings={"allow_nullable_key": True, "max_suspicious_broken_parts": 500}
        >>> )

        Set column codecs:

        >>> clickhouse_cluster_adapter(
        >>>     data,
        >>>     codecs={"town": "LZ4HC", "street": "Delta, ZSTD(2)"}
        >>> )

        Create distributed tables with specific suffix and sharding key:

        >>> clickhouse_cluster_adapter(
        >>>     data,
        >>>     create_distributed_tables=True,
        >>>     distributed_table_suffix="_distributed",
        >>>     sharding_key="city_id % 4"
        >>> )
    """

    resource = clickhouse_adapter(
        data,
        table_engine_type=table_engine_type,
        sort=sort,
        partition=partition,
        settings=settings,
        codecs=codecs,
    )

    additional_table_hints: Dict[str, TTableHintTemplate[Any]] = {}

    if create_distributed_tables is not None:
        additional_table_hints[CREATE_DISTRIBUTED_TABLES_HINT] = create_distributed_tables

    if distributed_table_suffix is not None:
        additional_table_hints[DISTRIBUTED_TABLE_SUFFIX_HINT] = distributed_table_suffix

    if sharding_key is not None:
        additional_table_hints[SHARDING_KEY_HINT] = sharding_key

    # convert to None if empty, to prevent removing existing hints
    additional_table_hints = additional_table_hints or None

    resource.apply_hints(additional_table_hints=additional_table_hints)

    return resource
