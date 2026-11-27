---
title: Prune BigQuery partitions when merging data
description: Replace only the days in the loaded data and prune the partitions of root and nested tables
keywords: [merge, delete-insert, destination scope, partition, partition pruning, nested tables, bigquery]
---
# Prune BigQuery partitions when merging data

This example loads orders with three levels of nested tables: lines, their parts and the serial numbers of the parts. Each load replaces the days that its data contains, in the root table and in the nested tables. On BigQuery, the delete drops only the partitions of these days, which is fast and free.

In this example, you learn how to:

- replace records with a [`delete-insert` destination scope](../general-usage/merge-loading.md#replacing-a-partition-with-delete-insert)
- [fill a placeholder](../general-usage/merge-loading.md#fill-placeholders-with-values-from-the-load) with the days that the extract saw
- copy `event_day` from each order into its nested records with the `propagate_to_nested` map step
- set the partition column and the destination scope of nested tables with [`nested_hints`](../general-usage/resource.md#define-schema-of-nested-tables)
- load nested tables without the `_dlt_root_id` column

## Full source code

```py execute
from datetime import date
from typing import Any, Iterator

import dlt
from dlt.common.schema.typing import TColumnSchema, TDeleteInsertStrategyDict
from dlt.extract import DltResource
from dlt.sources.helpers.transform import propagate_to_nested

# `event_day` is the daily partition column of the root table and of the nested tables
EVENT_DAY: TColumnSchema = {
    "name": "event_day",
    "data_type": "date",
    "partition": True,
    "nullable": False,
}
# each table deletes only the days of the load
REPLACE_DAYS: TDeleteInsertStrategyDict = {
    "disposition": "merge",
    "strategy": "delete-insert",
    "destination_scope": "event_day IN {days}",
}
NESTED_TABLES = ("lines", ("lines", "parts"), ("lines", "parts", "serials"))


# every nested table deletes by its own destination scope, so it needs no `_dlt_root_id`
@dlt.source(root_key=False)
def shop(data: list[dict[str, Any]]) -> Any:
    # the days of this load, a new set for each source instance
    days: set[date] = set()

    def track_day(order: dict[str, Any]) -> dict[str, Any]:
        days.add(order["event_day"])
        return order

    def days_of(resource: DltResource, table_name: str) -> list[date]:
        # dlt calls this function after the extract step
        return sorted(days)

    @dlt.resource(
        write_disposition={**REPLACE_DAYS, "extra_placeholders": {"days": days_of}},
        columns=[EVENT_DAY],
        nested_hints={
            path: dlt.mark.make_nested_hints(columns=[EVENT_DAY], write_disposition=REPLACE_DAYS)
            for path in NESTED_TABLES
        },
    )
    def orders() -> Iterator[list[dict[str, Any]]]:
        yield data

    # copy `event_day` into the lines, parts and serials
    return orders.add_map(track_day).add_map(propagate_to_nested("event_day", max_nesting=3))


def make_orders(days: list[date], size: int) -> list[dict[str, Any]]:
    # three orders per day, each with `size` lines, parts per line and serials per part
    return [
        {
            "order_id": f"{day}-{n}",
            "event_day": day,
            "lines": [
                {
                    "line": line,
                    "parts": [
                        {"part": part, "serials": [{"serial": f"{part}-{s}"} for s in range(size)]}
                        for part in range(size)
                    ],
                }
                for line in range(size)
            ],
        }
        for day in days
        for n in range(3)
    ]


# use the "bigquery" destination to prune the partitions
pipeline = dlt.pipeline(
    pipeline_name="replace_daily_partitions",
    destination="duckdb",
    dataset_name="shop_data",
)

# load five days, each order has two lines with two parts with two serials
days = [date(2026, 10, day) for day in range(1, 6)]
pipeline.run(shop(make_orders(days, size=2)))

# replace two days, their orders now have one line with one part with one serial
replaced = [days[1], days[3]]
load_info = pipeline.run(shop(make_orders(replaced, size=1)))

# the load package keeps the days that the merge replaced
package_state = pipeline.get_load_package_state(load_info.loads_ids[0])
assert package_state["placeholders"]["orders"]["days"] == replaced

# only the replaced days changed, in the root table and in the nested tables
dataset = pipeline.dataset()
query = "SELECT event_day, COUNT(*) FROM {table} GROUP BY event_day"
for table, rows_per_day, replaced_rows_per_day in (
    ("orders", 3, 3),
    ("orders__lines", 6, 3),
    ("orders__lines__parts", 12, 3),
    ("orders__lines__parts__serials", 24, 3),
):
    counts = dict(dataset(query.format(table=table)).fetchall())
    assert counts == {day: replaced_rows_per_day if day in replaced else rows_per_day for day in days}
    # nested tables link to their parent with `_dlt_parent_id` only
    assert "_dlt_root_id" not in pipeline.default_schema.get_table_columns(table)
```

## Partition pruning on BigQuery

Before it inserts the load, dlt deletes the replaced days from each table:

```sql
DELETE FROM `shop_data`.`orders__lines` WHERE event_day IN ('2026-10-02', '2026-10-04');
DELETE FROM `shop_data`.`orders__lines__parts` WHERE event_day IN ('2026-10-02', '2026-10-04');
DELETE FROM `shop_data`.`orders__lines__parts__serials` WHERE event_day IN ('2026-10-02', '2026-10-04');
DELETE FROM `shop_data`.`orders` WHERE event_day IN ('2026-10-02', '2026-10-04');
```

On BigQuery, the `partition` hint creates tables partitioned by day. Each delete compares only the partition column to literals, so BigQuery drops the whole partitions of these days and reads no data. Such a delete changes only metadata: it is fast and processes 0 bytes, so it is free. The load works even on tables with `require_partition_filter`.

The `destination_scope` of the nested tables is required for this. Without it, dlt deletes the nested rows of the replaced orders with a subquery on the root table: `_dlt_root_id IN (SELECT _dlt_id FROM orders WHERE ...)`. This delete reads all partitions of the nested table. A nested table can use its own destination scope only because `propagate_to_nested` copies `event_day` from the order into all its nested records.

## Nested tables without `_dlt_root_id`

By default, `delete-insert` adds the `_dlt_root_id` column to nested tables to delete nested rows with their root rows. Here, every nested table deletes by its own destination scope, so dlt never reads `_dlt_root_id`. The source sets `root_key=False`, and the nested tables link to their parent rows with `_dlt_parent_id` only. This works for any nesting level, here for three levels.

If you add a nested table without a destination scope, dlt needs `_dlt_root_id` to delete its rows. Then the load fails with ``No `root_key` column (e.g. `_dlt_root_id`) in table ...``.

:::warning
If an order moves to another day, dlt does not delete its copy in the old day. See the warning in [Replacing a partition with `delete-insert`](../general-usage/merge-loading.md#replacing-a-partition-with-delete-insert).
:::
