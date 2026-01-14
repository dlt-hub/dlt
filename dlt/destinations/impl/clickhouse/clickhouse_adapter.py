from copy import deepcopy
from typing import Any, Dict, Literal, Optional, Set, cast

import sqlglot

from dlt.common.schema.typing import TTableSchemaColumns
from dlt.common.schema.utils import merge_columns
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
    current_columns = cast(TTableSchemaColumns, resource.columns)

    columns = deepcopy(current_columns)
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
        columns = set_column_hints_from_table_hint(columns, sort, "sort")

    # partition
    raise_if_not_none_str_seq(partition, "partition")
    if partition:
        additional_table_hints[PARTITION_HINT] = partition
        columns = set_column_hints_from_table_hint(columns, partition, "partition")

    # settings
    raise_if_not_none_dict(settings, "settings")
    if settings:
        additional_table_hints[SETTINGS_HINT] = settings

    # codecs
    raise_if_not_none_dict(codecs, "codecs")
    if codecs:
        partial_codec_columns: TTableSchemaColumns = {
            name: {"name": name, CODEC_HINT: codec} for name, codec in codecs.items()  # type: ignore[typeddict-unknown-key]
        }
        columns = merge_columns(columns, partial_codec_columns, merge_columns=True)

    resource.apply_hints(columns=columns, additional_table_hints=additional_table_hints)
    return resource


def extract_column_names(sql: str) -> Set[str]:
    cols = sqlglot.parse_one(sql, dialect="clickhouse").find_all(sqlglot.exp.Column)
    return {col.name for col in cols}


def get_column_names_from_table_hint(hint: TSQLExprOrColumnSeq) -> Set[str]:
    if isinstance(hint, str):
        # hint is SQL expression; extract column names
        return extract_column_names(hint)
    # hint is sequence of column names; return as set
    return set(hint)


def set_column_hints_from_table_hint(
    columns: TTableSchemaColumns,
    hint: TSQLExprOrColumnSeq,
    hint_name: Literal["sort", "partition"],
) -> TTableSchemaColumns:
    """Sets column hints based on provided table hint.

    Modifies `columns` in place and returns it.

    When it's a `sort` table hint, it sets `sort` column hints.
    When it's a `partition` table hint, it sets `partition` column hints.

    Principles: table hint takes precedence over column hints.

    Rules:
    1. sets/overrides column hint to True for each column in table hint, even if user provided False
    2. sets nullability to False for each column in table hint, unless user provided nullable=True
    3. removes column hint if it's set to True but not in table hint
    4. retains any user-provided nullability
    """

    table_hint_columns = get_column_names_from_table_hint(hint)

    for name in table_hint_columns:
        if name in columns:  # existing column
            # rule 1
            columns[name][hint_name] = True

            # rule 2
            if columns[name].get("nullable") is not True:
                columns[name]["nullable"] = False
        else:  # new column
            # rules 1 and 2
            columns[name] = {"name": name, "nullable": False, hint_name: True}  # type: ignore[misc]

    # rule 3
    for col in columns.values():
        if col.get(hint_name) is True and col["name"] not in table_hint_columns:
            col.pop(hint_name)

    return columns
