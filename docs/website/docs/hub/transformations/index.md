---
title: Transformations
description: Define Python-based or mixed SQL + Python transformations on data that is **already** in your destination.
keywords: [transformation, dataset, sql, pipeline, ibis, arrow]
---
# Transformations: Reshape data after loading

`dlt transformations` build new tables or full datasets from datasets that dlt has _already_ ingested. You write and run them in the same fashion as dlt sources and resources. A transformation needs data that you already loaded to a data location, for example a local duckdb database, a bucket, or a warehouse. dlt supports transformations on every SQL destination, and on every filesystem and bucket format.

You create them with the `@dlt.hub.transformation` decorator. This decorator has the same signature as the `@dlt.resource` decorator, but it yields a SQL query with the resulting
column schema, rather than data items. dlt transformations support the same write_dispositions per destination as dlt resources do.

## Motivations

A few real-world scenarios where dlt transformations can be useful:

- **Build one-stop reporting tables** – Flatten and enrich raw data into a wide table that analysts can pivot, slice, and dice without writing SQL each time.
- **Clean data** – Remove irrelevant columns or anonymize sensitive information before sending it to a layer with lower privacy protections.
- **Normalize JSON into 3-NF** – Break out repeating attributes from nested JSON so updates are consistent and storage is not wasted.
- **Create dimensional (star-schema) models** – Produce fact and dimension tables so BI users can drag-and-drop metrics and break them down by any dimension.
- **Generate task-specific feature sets** – Deliver slim tables tailored for personalization, forecasting, or other ML workflows.
- **Apply shared business definitions** – Encode rules such as "a *sale* is a transaction whose status became *paid* this month". Every metric then counts the same way.
- **Merge heterogeneous sources** – Combine Shopify, Amazon, WooCommerce, and more into one canonical *orders* feed for unified inventory and revenue reporting.
- **Run transformations during ingestion pre-warehouse** – Pre-aggregate or pre-filter data before it hits the warehouse to cut compute and storage costs.
- **…and more** – Any scenario where reshaping, enriching, or aggregating existing data unlocks faster insight or cleaner downstream pipelines.


## Quick start

Copy the example below into one script. Then run the script.

:::note
It is useful to know how to use dlt [Datasets and Relations](../../general-usage/dataset-access/dataset.md), since these are heavily used in transformations.
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

That is it — `copied_customers` is now a new table in **the same** DuckDB schema with the first 5 customers when ordered by name. `dlt` detected that we load into the same dataset,
and ran this transformation in SQL. No data travelled to and from the machine that runs this pipeline. `dlt` also evolved the new destination table `copied_customers`
to the correct new schema. You can also set a different write disposition, and even merge data from a transformation.

## Defining a transformation

:::info
Most of the following examples use the ibis expressions of the `dlt.Dataset`. The detailed [dataset docs](../../general-usage/dataset-access/dataset.md) describe how to use them.
:::

<!--@@@DLT_SNIPPET ./transformation-snippets.py::orders_per_user-->

* **Decorator arguments** mirror those accepted by `@dlt.resource`.
* The transformation function signature must contain at least one `dlt.Dataset`. The function uses that dataset to create the transformation SQL statements and to calculate the resulting schema update.
* A transformation yields a `Relation` created with ibis expressions or a select query, which dlt materializes into the destination table. When the first yielded item is a valid sql query or relation object, dlt interprets the data as a transformation. In all other cases, the transformation decorator works like any other resource.

## Loading to other datasets


### Loading to another dataset at the same data location

Below we load to the same DuckDB instance with a new pipeline that points to another `dataset`. dlt detects that both datasets live on the same destination,
and runs the transformation as pure SQL.

<!--@@@DLT_SNIPPET ./transformation-snippets.py::loading_to_other_datasets-->

### Loading to another dataset at a different data location

Below we load the data from our local DuckDB instance to a Postgres instance. dlt uses the query to extract the data as Parquet files, and then runs a regular dlt load to Postgres. The same transformation functions work for both scenarios. This is useful when you want to avoid warehouse compute costs. The compute then happens on the machine that runs the pipeline, over a local duckdb instance or over raw data in a bucket.

<!--@@@DLT_SNIPPET ./transformation-snippets.py::loading_to_other_datasets_other_engine-->


## Using transformations


### Grouping multiple transformations in a source

`dlt transformations` can be grouped like all other resources into sources and will be executed together. You can even mix regular resources and transformations in one pipeline load.

<!--@@@DLT_SNIPPET ./transformation-snippets.py::multiple_transformations-->

### Yielding multiple transformations from one transformation resource

A dlt transformation can also yield more than one relation. Without further table name hints, the result is a union of the yielded relations. `dlt` runs the necessary schema migrations. Make sure that no relation marks a column as non-nullable when another relation omits that column:

<!--@@@DLT_SNIPPET ./transformation-snippets.py::multiple_transformation_instructions-->

### Supplying additional hints

You can supply column and table hints the same way you do for regular resources. `dlt` derives schema hints from your query. Sometimes you must modify or extend them. Two examples are a nullable column, as above, and a change of precision or type for a target destination that differs from the source.

<!--@@@DLT_SNIPPET ./transformation-snippets.py::supply_hints-->

### Writing your queries in SQL

To write your queries in SQL, create a `Relation` from a query on your dataset. Ibis expressions are then not necessary:

<!--@@@DLT_SNIPPET ./transformation-snippets.py::sql_queries-->

The identifiers in these raw SQL expressions are the table and column names of your dlt schema. They are **not** the names of your destination database schema.

#### Write in one SQL dialect, run in another

Pass `query_dialect` to say which dialect you wrote. dlt parses the query in that dialect and emits it in the dialect of the destination, so a query you wrote for one warehouse runs on another.

<!--@@@DLT_SNIPPET ./transformation-snippets.py::sql_dialect_transpilation-->

The duckdb query above is not valid mssql. `||` is a string concatenation that mssql spells `+`, and mssql has no `LIMIT` clause. dlt emits this instead:

```sql
SELECT TOP 10 [purchases].[customer] + ' (' + [purchases].[city] + ')' AS [label], [purchases].[amount] AS [amount]
FROM [analytics].[purchases] AS [purchases]
ORDER BY [purchases].[amount] DESC
```

A model job carries the dialect of the destination, never the dialect you wrote in. Without `query_dialect`, dlt reads the query in the dialect of the destination, and a construct that only the source dialect knows reaches the destination unchanged.

## Transformations of multiple datasets

A transformation receives its input datasets as arguments, so passing **more than one** `dlt.Dataset` lets you join across them. dlt inspects where the inputs and the output live and picks how to run the join:

- when the output engine can read and write the inputs, the join runs **in-warehouse** as a model job. No data leaves the destination. One DuckDB database or one MotherDuck account is such a case.
- otherwise dlt uses **eager materialization**. dlt runs the query on the machine that runs the pipeline, and loads the result as data.

### Joining datasets on the same destination

Pass two input datasets to the transformation. Join them into a new output table. Here `crm` and `sales` are two datasets in the same DuckDB, so the join runs in-warehouse:

<!--@@@DLT_SNIPPET ./transformation-snippets.py::transformations_join_same_destination-->

#### Joining new input against the existing output

A transformation can also read the dataset it writes to. Pass the output dataset as another argument. On the first run the output has no tables yet. Guard that reference with [`schema.is_new`](../../general-usage/dataset-access/dataset.md). Join against the output only after it exists. This pattern processes the rows you have not loaded before:

<!--@@@DLT_SNIPPET ./transformation-snippets.py::transformations_incremental_output_join-->

For richer incremental patterns — cursors, scheduler windows, load-time cursors — see [Incremental transformations](#incremental-transformations).

### Joining datasets across destinations with DuckDB

dlt can join datasets that live on **different** destinations, when both use DuckDB as their query engine. dlt attaches the input dataset into the DuckDB engine of the output, under an **attach alias**. The query then resolves the input tables against that catalog. These destinations qualify:

- `duckdb`, `ducklake`, `motherduck`
- `lance`, `lancedb`
- `filesystem`, including Hugging Face `hf://` buckets and the `delta` and `iceberg` open table formats

`filesystem` qualifies for the `file`, `s3`, `az`, `abfss`, and `hf` protocols. dlt cannot attach `gs`, `sftp`, or `gdrive`, because those protocols read their data through an fsspec filesystem that only the local process holds.

How the join runs depends on whether the **output** engine can write:

- **Read-write engines** — `duckdb`, `ducklake`, and `motherduck` can materialize the result themselves. The join therefore runs in-warehouse as a model job, and the `SELECT` and the `ATTACH` statements execute on the destination. `duckdb` and `ducklake` run locally. Only `motherduck` is remote.
- **Read-only engines** — `filesystem`, `lance`, and `lancedb` can only be read through DuckDB. A transformation that writes to them therefore always uses **eager materialization**: dlt runs the join locally and writes the result as files.

**MotherDuck attaches inputs on your side.** A MotherDuck connection attaches every input except another MotherDuck database *locally*. DuckDB then splits the query between your machine and the server. The credentials of the input stay in your local session, and dlt never uploads them to MotherDuck. The rows of the input travel to your machine and on to MotherDuck as the query runs.

Datasets in the **same** MotherDuck account need no attach, because the query engine already accesses every database of that account. dlt cannot attach a dataset in a **different** MotherDuck account, because the client must set the token before it opens the connection. dlt rejects that join.

The example below joins a `filesystem` dataset (orders) into a `duckdb` output. Because the output engine can write, it runs in-warehouse:

<!--@@@DLT_SNIPPET ./transformation-snippets.py::transformations_cross_destination_lazy-->

**Secrets.** An attached input sometimes needs credentials: a MotherDuck token, cloud-bucket keys, or a catalog password. dlt encrypts those statements inside the `.model` file of the model job. The key comes from the encryption seed of the pipeline.

Without a `pipeline_salt` of your own, dlt makes a new random seed for each pipeline instance, so only that instance can load the job. When a **new** process retries the load, for example after a crash, decryption fails. dlt then asks you to set a permanent `pipeline_salt`, for example `pipelines.<pipeline_name>.pipeline_salt` in `secrets.toml`, which makes the key reproducible. Inputs that need no credentials, such as local files or another local DuckDB, carry no secrets.

**Force eager materialization.** To run the join on your machine, yield the materialized result rather than the relation. Yield an Arrow table or a DataFrame. dlt then creates no model job and serializes no credentials:

<!--@@@DLT_SNIPPET ./transformation-snippets.py::transformations_cross_destination_eager-->

## Using Pandas or Polars DataFrames and Arrow tables

You can also write transformations directly with Pandas or Polars DataFrames and Arrow tables. Your transformation resource then behaves like a regular resource. `dlt` does not propagate column-level hints, and treats the yielded DataFrames or Arrow tables like data from any other resource. This behavior can change in a future release.

<!--@@@DLT_SNIPPET ./transformation-snippets.py::arrow_dataframe_operations-->


## Incremental transformations

When source data keeps growing, rerunning the same full transformation every time is slow and expensive.

Incremental transformations let each run work on the right slice of source data instead of the whole dataset. Each slice is defined by a _cursor_: a column whose values dlt compares against a range to decide if a row is in scope. Common choices are `created_at`, `updated_at`, an increasing `id`, or the dlt-managed `_dlt_loads.inserted_at`.

There are two common ways to choose the slice:

- [Use the scheduler interval](#the-scheduler-interval). dltHub Platform uses this approach: the scheduler sets the `[start, end)` interval that this run is responsible for.
- [Continue from the previous run](#continue-from-the-previous-run). dlt stores the last cursor value it processed, and the next run starts after that value.

### The scheduler interval

When the orchestrator decides the time range of each run, use a scheduler interval. This is the natural fit for cron schedules, retries, and backfills because the run does not depend on what happened in a previous run.

Set `allow_external_schedulers=True` on the cursor. dltHub Platform then owns the interval. Its cron schedules set `DLT_INTERVAL_START` and `DLT_INTERVAL_END`, and dlt filters the source data with these values.

Here is an example. The transformation below reads the `orders` table and writes only the rows whose `created_at` falls in the `[start, end)` window to a new table `orders_window`.

Given an `orders` table with one row per day:

| id | created_at |
| --- | --- |
| 1  | 2026-01-01 |
| 2  | 2026-01-02 |
| …  | … |
| 10 | 2026-01-10 |

and a scheduler window of `[2026-01-05, 2026-01-10)`, the run writes ids 5 to 9 to `orders_window` (id 10 is excluded by the open range end).

<!--@@@DLT_SNIPPET ./transformation-snippets.py::incremental_scheduler_window_definition-->

Re-running the same `[start, end)` (start is included, end is excluded) interval produces the same transformation input, which makes this pattern a good fit for partition backfills and idempotent retries.

### Continue from the previous run

When each run must continue from the last successful run, use a stateful cursor. No external scheduler is then necessary. dlt stores the cursor state internally and uses it in the next run of the transformation.

The transformation below appends rows from `orders` whose `created_at` is later than the persisted `last_value` to a new table `recent_orders`.

:::note Implicit cursor
The cursor below is declared on the decorator. The body yields a bare relation (Ibis expressions and raw SQL strings work too), and dlt applies the filter automatically. The [scheduler example above](#the-scheduler-interval) shows the alternative form, with the cursor as a function argument.
:::

<!--@@@DLT_SNIPPET ./transformation-snippets.py::incremental_stateful_cursor_definition-->

Now suppose `orders` is loaded in two batches:

| batch   | ids    | `created_at`              |
| ---     | ---    | ---                       |
| initial | 1..3   | 2026-01-01 .. 2026-01-03  |
| later   | 4..5   | 2026-01-04 .. 2026-01-05  |

The first run has no `last_value` yet, so it starts from `initial_value` (`2000-01-01`), writes the three initial rows to `recent_orders`, and advances `last_value` to `2026-01-03`. The next run sees the two later rows fall past `last_value`, appends them, and advances `last_value` to `2026-01-05`.

:::caution Pick ranges by how your cursor behaves
Each stateful run computes `MAX(cursor)` at extraction and sets its range end to that value. The run then advances `last_value` to it. As a result, a run never sees rows that arrive after this aggregate. The **boundary row** is the row at `MAX`. The ranges decide how `dlt` handles it:

- `range_start="open"` (recommended for append): the boundary row loads in the run that records it, and no later run reads it again. The range end is always inclusive for an open start, because an open end never loads the boundary row.
- Default `range_start="closed"` with `range_end="open"`: `dlt` defers the boundary row. The run that records it excludes it. The next run that observes a greater cursor value loads it exactly once. Append creates no duplicates, but the newest rows of each run wait one cycle.
- `range_start="closed"` with `range_end="closed"`, `write_disposition="merge"` and a primary key: `dlt` loads the boundary eagerly and reads it again in every run. The merge removes the overlap. Use this combination when late rows that share the boundary cursor value must load without a wait.
- A `primary_key` on the incremental that equals the cursor column declares the cursor values unique. The boundary then loads eagerly and never replays, whatever the range settings are.
:::

### Cursor column choices

When the source table has a column for creation or update order, use a domain cursor. For append-only data, `created_at` or an increasing `id` is enough. For mutable data, use a cursor that changes with every row change, such as `updated_at`. The next stateful run ignores the rows whose cursor value does not advance.

When the source table has no domain timestamp, use `_dlt_loads.inserted_at`. dlt then processes the data by load time. A dotted cursor path tells dlt to follow the schema reference from the base table to `_dlt_loads`. dlt joins that table and filters on the joined column. The join is filter-only, and dlt adds no `_dlt_loads` column to the destination table.

<!--@@@DLT_SNIPPET ./transformation-snippets.py::incremental_load_time_cursor_definition-->

:::note
Internally, dlt modifies the source query to include the cursor filter when it runs the transformation as a model job. dlt filters during extraction in two other cases. The first case is a source and a destination at different data locations. The second case is a yield of Python objects, such as lists, Arrow tables, or DataFrames.
:::

### State and safety rules

- `LIMIT` is rejected on stateful relation incrementals. Advancing state from a limited result can skip rows that were not returned. Remove the limit or use an explicit fixed range.
- SQL-based cursors support `max` and `min` last-value functions. Custom Python `last_value_func` callables cannot be pushed down to SQL.
- Null handling follows `on_cursor_value_missing`. For SQL pushdown, `"include"` adds `OR cursor IS NULL`; `"exclude"` adds `AND cursor IS NOT NULL`; `"raise"` cannot raise in the middle of a query and falls back to excluding null cursor values when needed.

For lower-level cursor rules, including range inclusivity and `lag`, see [Filter to an incremental cursor](../../general-usage/dataset-access/dataset.md#filter-to-an-incremental-cursor) and [Cursor-based incremental loading](../../general-usage/incremental/cursor.md).


## Schema evolution and hint lineage

`dlt` computes the resulting schema before it executes the transformation. This computation lets `dlt`:

1. Migrate the destination schema accordingly, creating new columns or tables as needed
2. Fail early if there are schema mismatches that cannot be resolved
3. Preserve column-level hints from source to destination

### Schema evolution

For example, a transformation that joins two tables and creates new columns makes `dlt` update the destination schema. An incompatible schema change, such as a column type change that can lose data, makes `dlt` fail before the transformation runs. This protects your data and saves execution and debug time.

You can inspect the computed result schema during development. Read `Relation.columns_schema`, or print `Relation.columns` for the column names only:

<!--@@@DLT_SNIPPET ./transformation-snippets.py::computed_schema-->

### Column-level hint forwarding

When it creates or updates tables with transformation resources, `dlt` also forwards certain column hints to the new tables. In our fruitshop source, we apply a custom hint named
`x-annotation-pii` set to True for the `name` column, which indicates that this column contains PII (personally identifiable information).
Downstream of the transformation layer, we can then find out which columns originate from columns that contain private data:

<!--@@@DLT_SNIPPET ./transformation-snippets.py::column_level_lineage-->

#### Features and limitations

* `dlt` forwards only certain hint types to the resulting tables: custom hints that start with `x-annotation...`, and the type hints `nullable`, `data_type`, `precision`, `scale`, and `timezone`. Set other hints, such as `primary_key` or `merge_keys`, with the `columns` argument on the transformation decorator. `dlt` does not know how you will use the transformed tables.
* `dlt` cannot forward hints for columns that result from combining multiple origin columns, such as when they are concatenated or produced through other SQL operations.


## Lifecycle of a SQL transformation

This section covers the lifecycle of transformations that yield a `Relation` object. We call these SQL transformations. Python-based transformations yield dataframes, arrow tables, or polars frames. They go through the regular extract, normalize, and load lifecycle of a `dlt` resource.

### Extract

In the extract stage, `dlt` converts a `Relation` that a transformation yields into a SQL string. `dlt` saves that string as a `.model` file, together with its source SQL dialect.
At this stage, the SQL string is the user's original query — either the string that you provided or the one that `Relation.to_sql()` generated. `dlt` adds no `dlt`-specific columns such as `_dlt_id` or `_dlt_load_id` yet.

### Normalize

In the normalize stage, `dlt` reads and processes the `.model` files. The normalization process modifies your SQL queries to make sure that they execute correctly and integrate with `dlt`'s features.

:::info
The normalization described here applies only to SQL-based transformations. Python-based transformations, such as those using dataframes, arrow tables, or polars frames, follow the [regular normalization process](../../reference/explainers/how-dlt-works.md#normalize).
:::

#### Adding `dlt` columns

During normalization, `dlt` adds internal `dlt` columns to your SQL queries, based on the config:

- `_dlt_load_id`, which tracks which load operation created or modified each row, is **added by default**. Even if present in your query, the `_dlt_load_id` column will be **replaced with a constant value** corresponding to the current load ID. To disable this behavior, set:
    ```toml
    [normalize.model_normalizer]
    add_dlt_load_id = false
    ```
    In this case, the column will not be added or replaced.

- `_dlt_id`, a unique identifier for each row, is **not added by default**. If your query already includes a `_dlt_id` column, dlt leaves it unchanged. To generate this column when it is missing, set:
    ```toml
    [normalize.model_normalizer]
    add_dlt_id = true
    ```
    When enabled and the column is not in the query, dlt generates a `_dlt_id`. When the column is already present, dlt does **not** replace it.

    The `_dlt_id` column is generated using the destination's UUID function, such as `generateUUIDv4()` in ClickHouse. For dialects without native UUID support:
     - In **Redshift**, `_dlt_id` is generated using an `MD5` hash of the load ID and row number.
     - In **SQLite**, `_dlt_id` is simulated using `lower(hex(randomblob(16)))`.


#### Query transformations

The normalization process also applies the following transformations to make sure that your queries work correctly:

1. Fully qualifies all identifiers with database and dataset prefixes
2. Quotes and adjusts identifier casing to match destination requirements
3. Normalizes column names according to the selected naming convention
4. Aliases columns and tables to handle naming convention differences
5. Reorders columns to match the destination table schema
6. Fills in `NULL` values for columns that exist in the destination but are not in your query

### Load

In the load stage, `dlt` wraps the normalized queries from the `.model` files in INSERT statements, and executes them on the destination.
For example, given this query from the extract stage:

```sql
SELECT
    "my_table"."id" AS "id",
    "my_table"."value" AS "value"
FROM "my_pipeline_dataset"."my_table" AS "my_table"
```

The normalize stage adds the dlt columns and wraps the query in a subquery. The result is:

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

The destination's SQL client executes the query. This materializes the transformation result directly in the database.

## Examples

### Local in-transit transformations example

You sometimes need aggregated or otherwise transformed data in your warehouse, but you want to reduce the cost of large warehouse queries. You can then run some or all of your transformations "in transit", while you load data from your source. The code below extracts data with our `rest_api` source to a local DuckDB instance. It then forwards the aggregated data to a warehouse destination.

<!--@@@DLT_SNIPPET ./transformation-snippets.py::in_transit_transformations-->

This script:
- fetches data from a REST API with dlt's `rest_api_source`
- loads the raw data into a local DuckDB instance as an intermediate step
- joins orders with stores and aggregates order counts on the local DuckDB instance, not in the destination warehouse
- loads only the aggregated results to a production warehouse (Postgres)
- reduces warehouse compute costs, because the transformations run locally in DuckDB
- uses multiple pipelines in one workflow for different stages of processing
