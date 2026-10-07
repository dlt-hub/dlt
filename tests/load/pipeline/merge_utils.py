"""Helpers and data for the merge strategy tests."""

from datetime import datetime, timezone  # noqa: I251
from typing import Any, Dict, List, Optional, Sequence

import dlt
from dlt.common.pipeline import LoadInfo
from dlt.common.schema.typing import (
    DEFAULT_VALIDITY_COLUMN_NAMES,
    TLoaderMergeStrategy,
    TTableFormat,
    TTableSchemaColumns,
)
from dlt.common.time import (
    ensure_datetime,
    ensure_datetime_in_tz,
    normalize_timezone,
    reduce_pendulum_datetime_precision,
)
from dlt.common.typing import TAnyDateTime, TColumnNames
from dlt.extract import DltSource

from tests.load.utils import DestinationTestConfiguration
from tests.pipeline.utils import load_tables_to_dicts

FROM, TO = DEFAULT_VALIDITY_COLUMN_NAMES

NEW_BUCKET = "bucket = 'new'"
# Databricks rejects a subquery nested in another subquery of a DELETE or UPDATE condition
NEW_BUCKET_SUBQUERY = "bucket IN (SELECT LOWER('new'))"

# 1 and 4 are in bucket 'old' and not reloaded: a destination scope on 'new' keeps them.
# 3 is in bucket 'new' and not reloaded. 6 is in bucket 'old', so only a source filter
# discards it. 7 moves from bucket 'old' to 'new'
STORED_RECORDS = [
    {"id": 1, "bucket": "old", "v": 10},
    {"id": 2, "bucket": "new", "v": 20},
    {"id": 3, "bucket": "new", "v": 30},
    {"id": 4, "bucket": "old", "v": 40},
    {"id": 7, "bucket": "old", "v": 70},
]
LOADED_RECORDS = [
    {"id": 2, "bucket": "new", "v": 22},
    {"id": 5, "bucket": "new", "v": 50},
    {"id": 6, "bucket": "old", "v": 60},
    {"id": 7, "bucket": "new", "v": 77},
]

PARTITIONED_TARGET = [
    {"id": 1, "v": "a", "part": "p15"},
    {"id": 2, "v": "b", "part": "p15"},
    {"id": 3, "v": "c", "part": "p16"},
]
# the load carries 3 in p15, while its stored copy is in p16
PARTITIONED_INPUT = [
    {"id": 1, "v": "a2", "part": "p15"},
    {"id": 3, "v": "c2", "part": "p15"},
]
PARTITION = "part = 'p15'"
# the subquery is not correlated, because ClickHouse cannot reference the outer table
PARTITION_LOCAL_KEY = PARTITION + " AND id IN (SELECT id FROM {staging_table})"


def merge_resource(
    data: Any,
    strategy: Optional[TLoaderMergeStrategy] = None,
    *,
    name: str = "items",
    primary_key: TColumnNames = "id",
    merge_key: TColumnNames = None,
    columns: TTableSchemaColumns = None,
    table_format: TTableFormat = None,
    append: bool = False,
    **options: Any,
) -> DltSource:
    """A source with one merge resource `name` that yields `data`. `options` are merge options
    such as `source_filter`. With `append`, the resource appends to seed a table."""
    disposition: Any = {"disposition": "merge"}
    if strategy:
        disposition["strategy"] = strategy
    disposition.update({k: v for k, v in options.items() if v is not None})

    # the append seed must create the root key that later merges need on nested tables
    @dlt.source(root_key=True)
    def merge_source():
        @dlt.resource(
            name=name,
            primary_key=primary_key,
            merge_key=merge_key,
            columns=columns,
            table_format=table_format,
            write_disposition="append" if append else disposition,
        )
        def items():
            yield data

        return items

    return merge_source()


def merge_condition(
    condition: str, destination_config: DestinationTestConfiguration, placeholder: str
) -> str:
    """Qualifies the column of `condition` with `placeholder` on Delta, whose merge predicates
    name the merged tables by alias."""
    if destination_config.table_format == "delta":
        return f"{{{placeholder}}}.{condition}"
    return condition


def load_ids_by_key(pipeline: dlt.Pipeline, table_name: str, key: str = "id") -> Dict[Any, str]:
    """Returns the `_dlt_load_id` of each row in `table_name` by `key`."""
    return {
        row[key]: row["_dlt_load_id"]
        for row in load_tables_to_dicts(pipeline, table_name)[table_name]
    }


def assert_nested_rows_have_parents(
    pipeline: dlt.Pipeline, root_table: str, nested_table: str
) -> None:
    tables = load_tables_to_dicts(pipeline, root_table, nested_table)
    parent_ids = {row["_dlt_id"] for row in tables[root_table]}
    assert all(row["_dlt_root_id"] in parent_ids for row in tables[nested_table])


def get_load_package_created_at(pipeline: dlt.Pipeline, load_info: LoadInfo) -> datetime:
    """Returns `created_at` property of load package state as the context wall clock."""
    load_id = load_info.asdict()["loads_ids"][0]
    created_at = normalize_timezone(pipeline.get_load_package_state(load_id)["created_at"], False)
    caps = pipeline._get_destination_capabilities()
    return reduce_pendulum_datetime_precision(created_at, caps.timestamp_precision)


def strip_timezone(ts: TAnyDateTime) -> datetime:
    """Puts a stored value on the context wall clock: an aware one is converted, a naive one already is."""
    return normalize_timezone(ensure_datetime(ts), False)


def boundary_wall_clock(ts: TAnyDateTime) -> datetime:
    """A boundary timestamp is a UTC instant, stored as the context wall clock."""
    return normalize_timezone(ensure_datetime_in_tz(ts, timezone.utc), False)


def get_table(
    pipeline: dlt.Pipeline,
    table_name: str,
    sort_column: str = None,
    include_dlt_id: bool = False,
    ts_columns: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """Returns destination table contents as list of dictionaries."""
    ts_columns = ts_columns or []

    table = [
        {
            k: (
                strip_timezone(v)
                if isinstance(v, datetime) or (k in ts_columns and v is not None)
                else v
            )
            for k, v in r.items()
            if not k.startswith("_dlt")
            or k in DEFAULT_VALIDITY_COLUMN_NAMES
            or (k == "_dlt_id" if include_dlt_id else False)
        }
        for r in load_tables_to_dicts(pipeline, table_name)[table_name]
    ]

    if sort_column is None:
        return table
    return sorted(table, key=lambda d: d[sort_column])


def get_rows(
    pipeline: dlt.Pipeline, table_name: str, columns: Sequence[str]
) -> List[Dict[str, Any]]:
    """Returns the `columns` of each row in `table_name`, validity timestamps on the wall clock."""
    return [
        {k: v for k, v in row.items() if k in columns}
        for row in get_table(pipeline, table_name, ts_columns=[FROM, TO])
    ]
