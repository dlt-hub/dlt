---
title: Transformations
description: Define Python-based or mixed SQL + Python transformations on data that is **already** in your destination.
keywords: [transformation, dataset, sql, pipeline, ibis, arrow]
---
# Transformations: Reshape data after loading

`dlthub transformations` let you build new tables or full datasets from datasets that have _already_ been ingested with `dlt`. `dlt transformations` are written and run in a very similar fashion to dlt source and resources. `dlt transformations` require you to have loaded data to a location, for example a local duckdb database, a bucket or a warehouse on which the transformations may be executed. `dlt transformations` are fully supported for all of our sql destinations including all filesystem and bucket formats.

You create them with the `@dlt.hub.transformation` decorator, which has the same signature as the `@dlt.resource` decorator but yields a SQL query, including the resulting
column schema, rather than data items. dlt transformations support the same write_dispositions per destination as dlt resources do.

## Motivations

A few real-world scenarios where dlt transformations can be useful:

- **Build one-stop reporting tables** – Flatten and enrich raw data into a wide table that analysts can pivot, slice, and dice without writing SQL each time.
- **Clean data** – Remove irrelevant columns or anonymize sensitive information before sending it to a layer with lower privacy protections.
- **Normalize JSON into 3-NF** – Break out repeating attributes from nested JSON so updates are consistent and storage isn't wasted.
- **Create dimensional (star-schema) models** – Produce fact and dimension tables so BI users can drag-and-drop metrics and break them down by any dimension.
- **Generate task-specific feature sets** – Deliver slim tables tailored for personalization, forecasting, or other ML workflows.
- **Apply shared business definitions** – Encode rules such as "a *sale* is a transaction whose status became *paid* this month," ensuring every metric is counted the same way.
- **Merge heterogeneous sources** – Combine Shopify, Amazon, WooCommerce (etc.) into one canonical *orders* feed for unified inventory and revenue reporting.
- **Run transformations during ingestion pre-warehouse** – Pre-aggregate or pre-filter data before it hits the warehouse to cut compute and storage costs.
- **…and more** – Any scenario where reshaping, enriching, or aggregating existing data unlocks faster insight or cleaner downstream pipelines.


## Quick-start in three simple steps

For the example below, you can copy–paste everything into one script and run it.

:::note
It is useful to know how to use dlt [Datasets and Relations](../../../general-usage/dataset-access/dataset.md), since these are heavily used in transformations.
:::

### 1. Load some example data

The snippets below assume that we have a simple fruitshop dataset as produced by the dlt fruitshop template:

<!--@@@DLT_SNIPPET ./transformation-snippets.py::quick_start_example-->

### 2. Inspect the dataset

<!--@@@DLT_SNIPPET ./transformation-snippets.py::dataset_inspection-->

### 3. Write and run a transformation

<!--@@@DLT_SNIPPET ./transformation-snippets.py::basic_transformation-->


### 3.1 Alternatively use pure SQL for the transformation

<!--@@@DLT_SNIPPET ./transformation-snippets.py::sql_queries_short-->

That's it — `copied_customers` is now a new table in **the same** DuckDB schema with the first 5 customers when ordered by name. `dlt` has detected that we are loading into the same dataset
and executed this transformation in SQL - no data was transferred to and from the machine executing this pipeline. Additionally, the new destination table `copied_customers` was automatically evolved
to the correct new schema, and you could also set a different write disposition and even merge data from a transformation.

## Defining a transformation

:::info
Most of the following examples will be using the ibis expressions of the `dlt.Dataset`. Read the detailed [dataset docs](../../../general-usage/dataset-access/dataset.md) to learn how to use these.
:::

<!--@@@DLT_SNIPPET ./transformation-snippets.py::orders_per_user-->

* **Decorator arguments** mirror those accepted by `@dlt.resource`.
* The transformation function signature must contain at least one `dlt.Dataset` which is used inside the function to create the transformation SQL statements and calculate the resulting schema update.
* A transformation yields a `Relation` created with ibis expressions or a select query which will be materialized into the destination table. If the first item yielded is a valid sql query or relation object, data will be interpreted as a transformation. In all other cases, the transformation decorator will work like any other resource.

## Loading to other datasets


### Loading to another dataset on the same physical location

Below we load to the same DuckDB instance with a new pipeline that points to another `dataset`. dlt will be able to detect that both datasets live on the same destination and
will run the transformation as pure SQL.

<!--@@@DLT_SNIPPET ./transformation-snippets.py::loading_to_other_datasets-->

### Loading to another dataset on a different physical location

Below we load the data from our local DuckDB instance to a Postgres instance. dlt will use the query to extract the data as Parquet files and will do a regular dlt load, pushing the data to Postgres. Note that you can use the exact same transformation functions for both scenarios. This can be extremely useful when you want to avoid compute costs in warehouses by running transformations directly from a local duckdb instance or raw data in a bucket into the warehouse, as the compute will happen on the machine executing the pipeline that runs the transformations.

<!--@@@DLT_SNIPPET ./transformation-snippets.py::loading_to_other_datasets_other_engine-->


## Using transformations


### Grouping multiple transformations in a source

`dlt transformations` can be grouped like all other resources into sources and will be executed together. You can even mix regular resources and transformations in one pipeline load.

<!--@@@DLT_SNIPPET ./transformation-snippets.py::multiple_transformations-->

### Yielding multiple transformations from one transformation resource

`dlt transformations` may also yield more than one transformation instruction. If no further table name hints are supplied, the result will be a union of the yielded transformation instructions. `dlt` will take care of the necessary schema migrations, you will just need to ensure that no columns are marked as non-nullable that are missing from one of the transformation instructions:

<!--@@@DLT_SNIPPET ./transformation-snippets.py::multiple_transformation_instructions-->

### Supplying additional hints

You may supply column and table hints the same way you do for regular resources. `dlt` will derive schema hints from your query, but in some cases you may need to modify or extend them — for example, making columns nullable as in the example above, or adjusting the precision or type of a column to ensure compatibility with a specific target destination (if it differs from the source).

<!--@@@DLT_SNIPPET ./transformation-snippets.py::supply_hints-->

### Writing your queries in SQL

If you prefer to write your queries in SQL, you can omit ibis expressions by simply creating a `Relation` from a query on your dataset:

<!--@@@DLT_SNIPPET ./transformation-snippets.py::sql_queries-->

The identifiers (table and column names) used in these raw SQL expressions must correspond to the identifiers as they are present in your dlt schema, NOT in your destination database schema.

## Using Pandas or Polars DataFrames and Arrow tables

You can also write transformations directly using Pandas or Polars DataFrames and Arrow tables. Note that in this case your transformation resource behaves like a regular resource: column-level hints will not be propagated, and `dlt` will simply treat the yielded DataFrames or Arrow tables like data from any other resource. This behavior may change in the future.

<!--@@@DLT_SNIPPET ./transformation-snippets.py::arrow_dataframe_operations-->


## Incremental transformations

When source data keeps growing, rerunning the same full transformation every time is slow and expensive.

Incremental transformations let each run work on the right slice of source data instead of the whole dataset. Each slice is defined by a _cursor_: a column whose values dlt compares against a range to decide if a row is in scope. Common choices are `created_at`, `updated_at`, an increasing `id`, or the dlt-managed `_dlt_loads.inserted_at`.

There are two common ways to choose the slice:

- [Use the scheduler interval](#the-scheduler-interval). dltHub Platform uses this approach: the scheduler sets `[start, end)` interval this run is responsible for.
- [Continue from the previous run](#continue-from-the-previous-run). dltHub stores the last cursor value it processed and the next run starts after that value.

### The scheduler interval

Use a scheduler interval when the orchestrator decides what time range each run should process. This is the natural fit for cron schedules, retries, and backfills because the run does not depend on what happened in a previous run.

Set `allow_external_schedulers=True` on the cursor and dltHub Platform owns the interval: its cron schedules set `DLT_INTERVAL_START` and `DLT_INTERVAL_END`, which are picksed up to filter the source data.

Here's an example. The transformation below reads the `orders` table and writes only the rows whose `created_at` falls in the `[start, end)` window to a new table `orders_window`.

Given an `orders` table with one row per day:

| id | created_at |
| --- | --- |
| 1  | 2026-01-01 |
| 2  | 2026-01-02 |
| …  | … |
| 10 | 2026-01-10 |

and a scheduler window of `[2026-01-05, 2026-01-10)`, the run writes ids 5 to 9 to `orders_window` (id 10 is excluded by the open upper bound).

<!--@@@DLT_SNIPPET ./transformation-snippets.py::incremental_scheduler_window_definition-->

Re-running the same `[start, end)` (start is included, end is excluded) interval produces the same transformation input, which makes this pattern a good fit for partition backfills and idempotent retries.

### Continue from the previous run

Use a stateful cursor for runs not tied to an external scheduler, where each run should continue from the last successful one. dltHub stores the cursor state internally and uses it in the next run of the transformation.

The transformation below appends rows from `orders` whose `created_at` is later than the persisted `last_value` to a new table `recent_orders`.

:::note Implicit cursor
The cursor below is declared on the decorator; the body yields a bare relation (Ibis expressions and raw SQL strings work too) and dltHub applies the filter automatically. The [scheduler example above](#the-scheduler-interval) shows the alternative form, with the cursor as a function argument.
:::

<!--@@@DLT_SNIPPET ./transformation-snippets.py::incremental_stateful_cursor_definition-->

Now suppose `orders` is loaded in two batches:

| batch   | ids    | `created_at`              |
| ---     | ---    | ---                       |
| initial | 1..3   | 2026-01-01 .. 2026-01-03  |
| later   | 4..5   | 2026-01-04 .. 2026-01-05  |

The first run has no `last_value` yet, so it starts from `initial_value` (`2000-01-01`), writes the three initial rows to `recent_orders`, and advances `last_value` to `2026-01-03`. The next run sees the two later rows fall past `last_value`, appends them, and advances `last_value` to `2026-01-05`.

:::caution Set `range_start="open"` on stateful cursors
A stateful cursor persists `last_value` after each run. With the default `range_start="closed"`, the next run's filter is `cursor >= last_value`, so the row at the boundary is re-emitted every time. Set `range_start="open"` to make the filter `cursor > last_value` and exclude the boundary row.
:::

### Cursor column choices

Use a domain cursor when the source table has a column that represents creation or update order. For append-only data, `created_at` or an increasing `id` is usually enough. For mutable data, use a cursor that changes whenever the row changes, such as `updated_at`; rows whose cursor value does not advance are intentionally ignored by the next stateful run.

Use `_dlt_loads.inserted_at` when the source table has no domain timestamp and you want to process data by the time dltHub loaded it. A dotted cursor path such as `_dlt_loads.inserted_at` tells dltHub to follow the schema reference from the base table to `_dlt_loads`, join it, and filter on the joined column. The join is filter-only: columns from `_dlt_loads` are not added to the destination table.

<!--@@@DLT_SNIPPET ./transformation-snippets.py::incremental_load_time_cursor_definition-->

:::note
Under the hood, when dltHub can run the transformation directly as SQL/model job, the source query is modified to include the cursor filter. When the transformation is materialized first, for example if source and destination are different physical engines, or when you yield Python objects such as lists, Arrow tables, or DataFrames, filtering happens during extraction.
:::

### State and safety rules

- `LIMIT` is rejected on stateful relation incrementals. Advancing state from a limited result can skip rows that were not returned. Remove the limit or use an explicit bounded window.
- SQL-based cursors support `max` and `min` last-value functions. Custom Python `last_value_func` callables cannot be pushed down to SQL.
- Null handling follows `on_cursor_value_missing`. For SQL pushdown, `"include"` adds `OR cursor IS NULL`; `"exclude"` adds `AND cursor IS NOT NULL`; `"raise"` cannot raise in the middle of a query and falls back to excluding null cursor values when needed.

For lower-level cursor rules, including range inclusivity and `lag`, see [Filter to an incremental cursor](../../../general-usage/dataset-access/dataset.md#filter-to-an-incremental-cursor) and [Cursor-based incremental loading](../../../general-usage/incremental/cursor.md).


## Schema evolution and hints lineage

When executing transformations, `dlt` computes the resulting schema before the transformation is executed. This allows `dlt` to:

1. Migrate the destination schema accordingly, creating new columns or tables as needed
2. Fail early if there are schema mismatches that cannot be resolved
3. Preserve column-level hints from source to destination

### Schema evolution

For example, if your transformation joins two tables and creates new columns, `dlt` will automatically update the destination schema to accommodate these changes. If your transformation would result in incompatible schema changes (like changing a column's data type in a way that could lose data), `dlt` will fail before executing the transformation, protecting your data and saving execution and debug time.

You can inspect the computed result schema during development by looking at the result of `compute_columns_schema` on your `Relation`:

<!--@@@DLT_SNIPPET ./transformation-snippets.py::computed_schema-->

### Column level hint forwarding

When creating or updating tables with transformation resources, `dlt` will also forward certain column hints to the new tables. In our fruitshop source, we have applied a custom hint named
`x-annotation-pii` set to True for the `name` column, which indicates that this column contains PII (personally identifiable information).
Downstream of the transformation layer, we may want to know which columns originate from columns that contain private data:

<!--@@@DLT_SNIPPET ./transformation-snippets.py::column_level_lineage-->

#### Features and limitations:

* `dlt` will only forward certain types of hints to the resulting tables: custom hints starting with `x-annotation...` and type hints such as `nullable`, `data_type`, `precision`, `scale`, and `timezone`. Other hints, such as `primary_key` or `merge_keys`, will need to be set via the `columns` argument on the transformation decorator, since `dlt` does not know how the transformed tables will be used.
* `dlt` cannot forward hints for columns that result from combining multiple origin columns, such as when they are concatenated or produced through other SQL operations.


## Lifecycle of a SQL transformation

In this section, we focus on the lifecycle of transformations that yield a `Relation` object, which we call SQL transformations here. This is in contrast to Python-based transformations that yield dataframes, arrow tables, or polars frames, which go through the regular extract, normalize, and load lifecycle of a `dlt` resource.

### Extract

In the extract stage, a `Relation` yielded by a transformation is converted into a SQL string and saved as a `.model` file along with its source SQL dialect.
At this stage, the SQL string is just the user's original query — either the string that was explicitly provided or the one generated by `Relation.to_sql()`. No `dlt`-specific columns like `_dlt_id` or `_dlt_load_id` are added yet.

### Normalize

In the normalize stage, `.model` files are read and processed. The normalization process modifies your SQL queries to ensure they execute correctly and integrate with `dlt`'s features.

:::info
The normalization described here applies only to SQL-based transformations. Python-based transformations, such as those using dataframes, arrow tables, or polars frames, follow the [regular normalization process](../../../reference/explainers/how-dlt-works.md#normalize).
:::

#### Adding `dlt` columns

During normalization, `dlt` adds internal `dlt` columns to your SQL queries depending on the configuration:

- `_dlt_load_id`, which tracks which load operation created or modified each row, is **added by default**. Even if present in your query, the `_dlt_load_id` column will be **replaced with a constant value** corresponding to the current load ID. To disable this behavior, set:
    ```toml
    [normalize.model_normalizer]
    add_dlt_load_id = false
    ```
    In this case, the column will not be added or replaced.

- `_dlt_id`, a unique identifier for each row, is **not added by default**. If your query already includes a `_dlt_id` column, it will be left unchanged. To enable automatic generation of this column when it’s missing, set:
    ```toml
    [normalize.model_normalizer]
    add_dlt_id = true
    ```
    When enabled and the column is not in the query, dlt will generate a `_dlt_id`. Note that if the column is already present, it will **not** be replaced.

    The `_dlt_id` column is generated using the destination's UUID function, such as `generateUUIDv4()` in ClickHouse. For dialects without native UUID support:
     - In **Redshift**, `_dlt_id` is generated using an `MD5` hash of the load ID and row number.
     - In **SQLite**, `_dlt_id` is simulated using `lower(hex(randomblob(16)))`.


#### Query transformations

The normalization process also applies the following transformations to ensure your queries work correctly:

1. Fully qualifies all identifiers with database and dataset prefixes
2. Quotes and adjusts identifier casing to match destination requirements
3. Normalizes column names according to the selected naming convention
4. Aliases columns and tables to handle naming convention differences
5. Reorders columns to match the destination table schema
6. Fills in `NULL` values for columns that exist in the destination but aren't in your query

### Load

In the load stage, the normalized queries from `.model` files are wrapped in INSERT statements and executed on the destination.
For example, given this query from the extract stage:

```sql
SELECT
    "my_table"."id" AS "id",
    "my_table"."value" AS "value"
FROM "my_pipeline_dataset"."my_table" AS "my_table"
```

After the normalize stage processes it (adding dlt columns, wrapping in subquery, etc.) and results in:

```sql
SELECT
    _dlt_subquery."id" AS "id",
    _dlt_subquery."value" AS "value",
    '1749134128.17655' AS "_dlt_load_id",
    UUID() AS "_dlt_id"
FROM (
    SELECT
        "my_table"."id" AS "id",
        "my_table"."value" AS "value"
    FROM "my_pipeline_dataset"."my_table" AS "my_table"
    )
AS _dlt_subquery
```

The load stage executes:

```sql
INSERT INTO
    "my_pipeline_dataset"."my_transformation" ("id", "value", "_dlt_load_id", "_dlt_id")
SELECT
    _dlt_subquery."id" AS "id",
    _dlt_subquery."value" AS "value",
    '1749134128.17655' AS "_dlt_load_id",
    UUID() AS "_dlt_id"
FROM (
    SELECT
        "my_table"."id" AS "id",
        "my_table"."value" AS "value"
    FROM "my_pipeline_dataset"."my_table" AS "my_table"
    )
AS _dlt_subquery
```

The query is executed via the destination's SQL client, materializing the transformation result directly in the database.

## Examples

### Local in-transit transformations example

If you require aggregated or otherwise transformed data in your warehouse, but would like to avoid or reduce the costs of running queries across many rows in your warehouse tables, you can run some or all of your transformations "in transit" while loading data from your source. The code below demonstrates how you can extract data with our `rest_api` source to a local DuckDB instance and then forward aggregated data to a warehouse destination.

<!--@@@DLT_SNIPPET ./transformation-snippets.py::in_transit_transformations-->

This script demonstrates:
- Fetching data from a REST API using dlt's rest_api_source
- Loading raw data into a local DuckDB instance as an intermediate step
- Transforming the data by joining orders with stores and aggregating order counts directly on the local DuckDB instance, not in the destination warehouse
- Loading only the aggregated results to a production warehouse (Postgres)
- Reducing warehouse compute costs by performing transformations locally in DuckDB
- Using multiple pipelines in a single workflow for different stages of processing
