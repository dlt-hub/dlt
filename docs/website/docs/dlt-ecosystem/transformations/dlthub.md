---
title: Transformations with dltHub
description: Python-defined transformations that run eagerly on your compute or lazily on the warehouse with dlt.hub.transformation
keywords: [transformations, dlthub, ibis, sql, incremental, lazy, eager]
---

# Transformations with dltHub

[dltHub](../../hub/transformations/index.md) extends `dlt` with `@dlt.hub.transformation` — transformations defined in Python that run **eagerly** on your compute (e.g. local DuckDB, Pandas, Polars, or Arrow) or **lazily** on the warehouse — the same code, executed where it makes the most sense. A transformation is a resource that takes a `dlt.Dataset` and yields an Ibis expression, a `Relation`, or a plain SQL query; `dlt` compiles it to the destination SQL dialect and materializes the result into a table. When source and target share the same physical location, no data is transferred to the machine running the pipeline.

The example below incrementally appends new rows from the `orders` table to `recent_orders` — each run picks up after the last processed `created_at`:

```py
import dlt
import pendulum

@dlt.hub.transformation(
    write_disposition="merge",
    primary_key="id",
    incremental=dlt.sources.incremental(
        "created_at",
        initial_value=pendulum.datetime(2000, 1, 1, tz="UTC"),
    ),
)
def recent_orders(dataset: dlt.Dataset) -> Any:
    yield dataset.table("orders")

pipeline.run(recent_orders(pipeline.dataset()))
```

A few highlights:

- **Eager or lazy execution** — run transformations eagerly on your own compute (pull data as DataFrames/Arrow tables and process them in Python) or lazily as SQL pushed down to the warehouse, where nothing leaves the destination. You pick per transformation; the decorator stays the same.
- **Incremental out of the box** — stateful cursors (as above) or scheduler-set `[start, end)` windows for idempotent retries and partition backfills on the [dltHub platform](../../hub/pipeline-operations/triggers.md#scheduler-driven-intervals).
- **Schema evolution and lineage** — result tables evolve automatically and column-level hints are forwarded from source to transformed tables.
- **Write in what you know** — Ibis expressions, raw SQL, or [Pandas/Polars DataFrames and Arrow tables](../../hub/transformations/index.md#using-pandas-or-polars-dataframes-and-arrow-tables).
- **Full resource semantics** — `write_disposition`, `primary_key`, merge loading, and grouping transformations into sources all work as with regular resources.

See the [dltHub transformations docs](../../hub/transformations/index.md) for the full guide. Use of dltHub is subject to a commercial [dltHub License](../../hub/license.md).
