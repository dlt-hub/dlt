"""Helpers and data for the merge strategy tests."""

from collections import Counter
from datetime import date, datetime, timezone  # noqa: I251
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

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
    normalize_timezone,
    reduce_datetime_precision,
)
from dlt.common.typing import DictStrAny, TAnyDateTime, TColumnNames
from dlt.extract import DltResource, DltSource
from dlt.sources.helpers.transform import propagate_to_nested

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
PARTITION = "part = {part}"
# the subquery is not correlated, because ClickHouse cannot reference the outer table
PARTITION_LOCAL_KEY = PARTITION + " AND id IN (SELECT id FROM {staging_table})"


def part_of(resource: DltResource, table_name: str) -> Any:
    """Returns the `part` argument of the resource."""
    return resource.explicit_args["part"]


PARTITION_PLACEHOLDERS = {"part": part_of}


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
    part: Optional[str] = None,
    **options: Any,
) -> DltSource:
    """A source with one merge resource `name` that yields `data`. `options` are merge options
    such as `source_filter`. With `append`, the resource appends to seed a table. The resource
    receives `part` as an argument."""
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
        def items(part: Optional[str] = None):
            yield data

        return items(part=part) if part else items

    return merge_source()


EVENT_TABLES = (
    "events",
    "events__lines",
    "events__lines__parts",
    "events__lines__parts__serials",
)
EVENTS_PER_DAY = 3
# `day` is a reserved word on Dremio
EVENT_DAY_SCOPE = "event_day IN {days}"


def day_events(days: Sequence[date], version: str, size: int = 2) -> List[DictStrAny]:
    """`EVENTS_PER_DAY` events of each day in `days`, each with `size` lines, parts per line
    and serials per part."""
    return [
        {
            "id": f"{day.isoformat()}-{n}",
            "event_day": day,
            "version": version,
            "lines": [
                {
                    "line": line,
                    "parts": [
                        {"part": part, "serials": [{"serial": s} for s in range(size)]}
                        for part in range(size)
                    ],
                }
                for line in range(size)
            ],
        }
        for day in days
        for n in range(EVENTS_PER_DAY)
    ]


def day_partitioned_events(data: List[DictStrAny], nested_scope: bool) -> DltSource:
    """A source of `events` with three levels of nested tables. `propagate_to_nested` copies
    `event_day` into the nested tables, and it is the partition column of all tables. The merge
    replaces the days that `data` contains. With `nested_scope`, the nested tables select the
    replaced rows by `event_day` too and have no root key."""
    days: Set[date] = set()

    def collect_day(item: DictStrAny) -> DictStrAny:
        days.add(item["event_day"])
        return item

    def days_of(resource: DltResource, table_name: str) -> List[date]:
        return sorted(days)

    scope: Any = {
        "disposition": "merge",
        "strategy": "delete-insert",
        "destination_scope": EVENT_DAY_SCOPE,
    }
    # ClickHouse rejects nullable partition keys
    event_day: Any = {
        "name": "event_day",
        "data_type": "date",
        "partition": True,
        "nullable": False,
    }
    nested_hints: Any = {
        path: dlt.mark.make_nested_hints(
            columns=[event_day], write_disposition=scope if nested_scope else None
        )
        for path in ("lines", ("lines", "parts"), ("lines", "parts", "serials"))
    }
    root_disposition: Any = {**scope, "extra_placeholders": {"days": days_of}}
    events = dlt.resource(
        data,
        name="events",
        write_disposition=root_disposition,
        columns=[event_day],
        nested_hints=nested_hints,
    )
    events.add_map(collect_day).add_map(propagate_to_nested("event_day"))

    # nested tables that delete by their own scope do not need the root key
    @dlt.source(root_key=False if nested_scope else None)
    def events_source() -> DltResource:
        return events

    return events_source()


def assert_day_events(pipeline: dlt.Pipeline, expected: Dict[date, Tuple[str, int]]) -> None:
    """Checks the version and the number of nested rows of the events of each day in
    `expected`, and that nested rows have the `event_day` of their parent row."""
    tables = load_tables_to_dicts(pipeline, *EVENT_TABLES)
    # some destinations return dates as strings
    to_day = lambda row: str(row["event_day"])[:10]  # noqa: E731
    for parent_name, table_name in zip(EVENT_TABLES, EVENT_TABLES[1:]):
        parent_days = {row["_dlt_id"]: to_day(row) for row in tables[parent_name]}
        for row in tables[table_name]:
            assert parent_days.get(row["_dlt_parent_id"]) == to_day(row)
    counts = [Counter(to_day(row) for row in tables[name]) for name in EVENT_TABLES]
    versions: Dict[str, Set[str]] = {}
    for row in tables["events"]:
        versions.setdefault(to_day(row), set()).add(row["version"])
    observed = {day: (versions[day], *(count[day] for count in counts)) for day in versions}
    assert observed == {
        day.isoformat(): ({version}, *(EVENTS_PER_DAY * size**level for level in range(4)))
        for day, (version, size) in expected.items()
    }


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
    """Returns `created_at` of the load package on the context wall clock at the precision of the
    destination, like `get_table` returns timestamps."""
    load_id = load_info.asdict()["loads_ids"][0]
    created_at = strip_timezone(pipeline.get_load_package_state(load_id)["created_at"])
    precision = pipeline._get_destination_capabilities().timestamp_precision
    return reduce_datetime_precision(created_at, precision)


def strip_timezone(ts: TAnyDateTime) -> datetime:
    """Puts a stored value on the context wall clock: an aware one is converted, a naive one already is."""
    return normalize_timezone(ensure_datetime(ts), False)


def get_table(
    pipeline: dlt.Pipeline,
    table_name: str,
    sort_column: str = None,
    include_dlt_id: bool = False,
    ts_columns: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """Returns destination table contents as list of dictionaries. Datetimes are returned on the
    context wall clock, `ts_columns` also when the destination returns them as strings."""
    ts_columns = ts_columns or []

    table = [
        {
            k: (
                strip_timezone(v)
                if (k in ts_columns and v is not None) or isinstance(v, datetime)
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
