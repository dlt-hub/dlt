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

```py execute

import dlt
from dlt.destinations import duckdb
from dlt._workspace._templates._single_file_templates.fruitshop_pipeline import (
    fruitshop as fruitshop_source,
)

fruitshop_pipeline = dlt.pipeline(
    "fruitshop", destination=duckdb("./test_duck.duckdb"), dev_mode=True
)
fruitshop_pipeline.run(fruitshop_source())
```

### 2. Inspect the dataset

```py notype execute
# Show row counts for every table
print(fruitshop_pipeline.dataset().row_counts().df())
"""
             table_name  row_count
0             customers         13
1  inventory_categories          3
2             inventory          6
3             purchases        100
"""
```

### 3. Write and run a transformation

```py execute
from typing import Any

@dlt.hub.transformation
def copied_customers(dataset: dlt.Dataset) -> Any:
    customers_table = dataset["customers"]
    yield customers_table.order_by("name").limit(5)

# Same pipeline & same dataset
fruitshop_pipeline.run(copied_customers(fruitshop_pipeline.dataset()))  # ty: ignore[unresolved-reference]

# show rowcounts again, we now have a new table in the schema and the destination
print(fruitshop_pipeline.dataset().row_counts().df())  # ty: ignore[unresolved-reference]
"""
             table_name  row_count
0             customers         13
1  inventory_categories          3
2             inventory          6
3             purchases        100
4      copied_customers          5
"""
```


### 3.1 Alternatively use pure SQL for the transformation

```py execute
# Convert the transformation above that selected the first 5 customers to a sql query
@dlt.hub.transformation
def copied_customers(dataset: dlt.Dataset) -> Any:
    customers_table = dataset(
        """
        SELECT *
        FROM customers
        ORDER BY name
        LIMIT 5
    """
    )
    yield customers_table

```

That is it — `copied_customers` is now a new table in **the same** DuckDB schema with the first 5 customers when ordered by name. `dlt` detected that we load into the same dataset,
and ran this transformation in SQL. No data travelled to and from the machine that runs this pipeline. `dlt` also evolved the new destination table `copied_customers`
to the correct new schema. You can also set a different write disposition, and even merge data from a transformation.

## Defining a transformation

:::info
Most of the following examples use the ibis expressions of the `dlt.Dataset`. The detailed [dataset docs](../../general-usage/dataset-access/dataset.md) describe how to use them.
:::

```py execute

@dlt.hub.transformation(name="orders_per_user", write_disposition="merge")
def orders_per_user(dataset: dlt.Dataset) -> Any:
    purchases = dataset.table("purchases").to_ibis()
    yield purchases.group_by(purchases.customer_id).aggregate(
        order_count=purchases.id.count()
    )

```

* **Decorator arguments** mirror those accepted by `@dlt.resource`.
* The transformation function signature must contain at least one `dlt.Dataset`. The function uses that dataset to create the transformation SQL statements and to calculate the resulting schema update.
* A transformation yields a `Relation` created with ibis expressions or a select query, which dlt materializes into the destination table. When the first yielded item is a valid sql query or relation object, dlt interprets the data as a transformation. In all other cases, the transformation decorator works like any other resource.

## Loading to other datasets


### Loading to another dataset at the same data location

Below we load to the same DuckDB instance with a new pipeline that points to another `dataset`. dlt detects that both datasets live on the same destination,
and runs the transformation as pure SQL.

```py execute
import dlt
from dlt.destinations import duckdb

@dlt.hub.transformation
def copied_customers(dataset: dlt.Dataset) -> Any:
    customers_table = dataset["customers"]
    yield customers_table.order_by("name").limit(5)

# Same duckdb instance, different dataset
dest_p = dlt.pipeline(
    "fruitshop_dataset",
    destination=duckdb("./test_duck.duckdb"),
    dataset_name="copied_dataset",
    dev_mode=True,
)
dest_p.run(copied_customers(fruitshop_pipeline.dataset()))    # ty: ignore[unresolved-reference]
```

### Loading to another dataset at a different data location

Below we load the data from our local DuckDB instance to a Postgres instance. dlt uses the query to extract the data as Parquet files, and then runs a regular dlt load to Postgres. The same transformation functions work for both scenarios. This is useful when you want to avoid warehouse compute costs. The compute then happens on the machine that runs the pipeline, over a local duckdb instance or over raw data in a bucket.

```py notype
# different engine (DuckDB → Postgres)
duck_p = dlt.pipeline("fruitshop_warehouse", destination="postgres")
duck_p.run(copied_customers(fruitshop_pipeline.dataset()))
```


## Using transformations


### Grouping multiple transformations in a source

`dlt transformations` can be grouped like all other resources into sources and will be executed together. You can even mix regular resources and transformations in one pipeline load.

```py notype execute
import dlt

@dlt.source
def my_transformations(dataset: dlt.Dataset) -> Any:
    @dlt.hub.transformation(write_disposition="append")
    def enriched_purchases(dataset: dlt.Dataset) -> Any:
        purchases = dataset.table("purchases").to_ibis()
        customers = dataset.table("customers").to_ibis()
        yield purchases.join(customers, purchases.customer_id == customers.id)

    @dlt.hub.transformation(write_disposition="replace")
    def total_items_sold(dataset: dlt.Dataset) -> Any:
        purchases = dataset.table("purchases").to_ibis()
        yield purchases.aggregate(total_qty=purchases.quantity.sum())

    return enriched_purchases(dataset), total_items_sold(dataset)

fruitshop_pipeline.run(my_transformations(fruitshop_pipeline.dataset()))
```

### Yielding multiple transformations from one transformation resource

A dlt transformation can also yield more than one relation. Without further table name hints, the result is a union of the yielded relations. `dlt` runs the necessary schema migrations. Make sure that no relation marks a column as non-nullable when another relation omits that column:

```py execute
import dlt

# this transformation creates a union of the customers and purchases tables
@dlt.hub.transformation(write_disposition="append")
def union_of_tables(dataset: dlt.Dataset) -> Any:
    yield dataset.table("purchases")
    yield dataset.table("customers")

```

### Supplying additional hints

You can supply column and table hints the same way you do for regular resources. `dlt` derives schema hints from your query. Sometimes you must modify or extend them. Two examples are a nullable column, as above, and a change of precision or type for a target destination that differs from the source.

```py execute
import dlt

# change precision and scale of the price column
@dlt.hub.transformation(
    write_disposition="append", columns={"price": {"precision": 10, "scale": 2}}
)
def precision_change(dataset: dlt.Dataset) -> Any:
    yield dataset.inventory

```

### Writing your queries in SQL

To write your queries in SQL, create a `Relation` from a query on your dataset. Ibis expressions are then not necessary:

```py execute
# Convert the transformation above that selected the first 5 customers to a sql query
@dlt.hub.transformation
def copied_customers(dataset: dlt.Dataset) -> Any:
    customers_table = dataset(
        """
        SELECT *
        FROM customers
        ORDER BY name
        LIMIT 5
    """
    )
    yield customers_table


# Joins and other more complex queries are also possible
@dlt.hub.transformation
def enriched_purchases(dataset: dlt.Dataset) -> Any:
    enriched_purchases = dataset(
        """
        SELECT customers.name, purchases.quantity
        FROM purchases
        JOIN customers
            ON purchases.customer_id = customers.id
        """
    )
    yield enriched_purchases

# you can use a different dialect than the destination with the query_dialect parameter.
# dlt compiles the query to the right destination dialect
@dlt.hub.transformation
def enriched_purchases_postgres(dataset: dlt.Dataset) -> Any:
    enriched_purchases = dataset(
        """
        SELECT customers.name, purchases.quantity
        FROM purchases
        JOIN customers
            ON purchases.customer_id = customers.id
        """,
        query_dialect="duckdb",
    )
    yield enriched_purchases

```

The identifiers in these raw SQL expressions are the table and column names of your dlt schema. They are **not** the names of your destination database schema.

#### Write in one SQL dialect, run in another

Pass `query_dialect` to say which dialect you wrote. dlt parses the query in that dialect and emits it in the dialect of the destination, so a query you wrote for one warehouse runs on another.

```py execute
from dlt.common.schema import Schema

schema = Schema("shop")
schema.update_table(
    {
        "name": "purchases",
        "columns": {
            "customer": {"name": "customer", "data_type": "text"},
            "city": {"name": "city", "data_type": "text"},
            "amount": {"name": "amount", "data_type": "double"},
        },
    }
)

# the query is duckdb SQL, but the dataset writes to mssql
mssql_dataset = dlt.dataset(
    dlt.destinations.mssql(credentials="mssql://user:pw@host:1433/warehouse?driver=ODBC+Driver+18+for+SQL+Server"),
    "analytics",
    schema=schema,
)

top_customers = mssql_dataset.query(
    """
    SELECT customer || ' (' || city || ')' AS label, amount
    FROM purchases
    ORDER BY amount DESC
    LIMIT 10
    """,
    query_dialect="duckdb",
)

# dlt emits the query in the dialect of the destination. `||` becomes `+`,
# `LIMIT` becomes `TOP`, and the identifiers take mssql quoting
print(top_customers.to_sql())
"""
SELECT TOP 10 [purchases].[customer] + ' (' + [purchases].[city] + ')' AS [label], [purchases].[amount] AS [amount] FROM [analytics].[purchases] AS [purchases] ORDER BY [purchases].[amount] DESC
"""
```

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

```py execute
import tempfile
import os

import dlt

# crm, sales, and marts are three datasets in the same duckdb file
db_path = os.path.join(tempfile.mkdtemp(), "shop.duckdb")

crm_pipeline = dlt.pipeline(
    "crm", destination=dlt.destinations.duckdb(db_path), dataset_name="crm_data"
)
crm_pipeline.run(
    [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}], table_name="users"
)
sales_pipeline = dlt.pipeline(
    "sales", destination=dlt.destinations.duckdb(db_path), dataset_name="sales_data"
)
sales_pipeline.run(
    [
        {"id": 10, "user_id": 1, "sku": "W-001"},
        {"id": 11, "user_id": 2, "sku": "G-001"},
    ],
    table_name="orders",
)
marts_pipeline = dlt.pipeline(
    "marts", destination=dlt.destinations.duckdb(db_path), dataset_name="marts_data"
)

# pass two input datasets. the transformation joins across them
@dlt.hub.transformation(table_name="user_orders")
def user_orders(crm: dlt.Dataset, sales: dlt.Dataset) -> Any:
    yield crm["users"].join(sales["orders"], on="users.id = orders.user_id")

# crm, sales and the marts output all live in the same duckdb, so the join
# runs in-warehouse as a model job — no data leaves the destination
marts_pipeline.run(user_orders(crm_pipeline.dataset(), sales_pipeline.dataset()))
```

#### Joining new input against the existing output

A transformation can also read the dataset it writes to. Pass the output dataset as another argument. On the first run the output has no tables yet. Guard that reference with [`schema.is_new`](../../general-usage/dataset-access/dataset.md). Join against the output only after it exists. This pattern processes the rows you have not loaded before:

```py notype execute
# a dedicated output pipeline/dataset for this transformation
known_pipeline = dlt.pipeline(
    "known", destination=dlt.destinations.duckdb(db_path), dataset_name="known_data"
)

@dlt.hub.transformation(table_name="known_users", write_disposition="append")
def known_users(crm: dlt.Dataset, out: dlt.Dataset) -> Any:
    users = crm["users"].to_ibis()
    if out.schema.is_new:
        # first run: the output has no tables yet, so build it from the source
        yield users
    else:
        # later runs: append only users not already present in the output
        existing = out["known_users"].to_ibis()
        yield users.anti_join(existing, users.id == existing.id)

# pass the output dataset as an argument so the transformation can read it
known_pipeline.run(known_users(crm_pipeline.dataset(), known_pipeline.dataset()))
```

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

```py execute
import tempfile
import os

import dlt
from dlt.common.storages.configuration import FilesystemConfiguration

tmp_dir = tempfile.mkdtemp()

orders_pipeline = dlt.pipeline(
    "orders",
    destination=dlt.destinations.filesystem(
        FilesystemConfiguration.make_file_url(os.path.join(tmp_dir, "orders"))
    ),
    dataset_name="orders_data",
)
orders_pipeline.run(
    [
        {"id": 10, "user_id": 1, "sku": "W-001"},
        {"id": 11, "user_id": 2, "sku": "G-001"},
    ],
    table_name="orders",
    loader_file_format="parquet",
)
warehouse_pipeline = dlt.pipeline(
    "warehouse",
    destination=dlt.destinations.duckdb(os.path.join(tmp_dir, "warehouse.duckdb")),
    dataset_name="warehouse_data",
)
warehouse_pipeline.run(
    [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}], table_name="users"
)

# join a filesystem dataset (orders) into a duckdb output. duckdb can write, so dlt
# attaches the filesystem dataset and runs the join in-warehouse as a model job.
@dlt.hub.transformation(table_name="user_orders")
def user_orders(warehouse: dlt.Dataset, orders: dlt.Dataset) -> Any:
    yield warehouse["users"].join(orders["orders"], on="users.id = orders.user_id")

warehouse_pipeline.run(
    user_orders(warehouse_pipeline.dataset(), orders_pipeline.dataset())
)
```

**Secrets.** An attached input sometimes needs credentials: a MotherDuck token, cloud-bucket keys, or a catalog password. dlt encrypts those statements inside the `.model` file of the model job. The key comes from the encryption seed of the pipeline.

Without a `pipeline_salt` of your own, dlt makes a new random seed for each pipeline instance, so only that instance can load the job. When a **new** process retries the load, for example after a crash, decryption fails. dlt then asks you to set a permanent `pipeline_salt`, for example `pipelines.<pipeline_name>.pipeline_salt` in `secrets.toml`, which makes the key reproducible. Inputs that need no credentials, such as local files or another local DuckDB, carry no secrets.

**Force eager materialization.** To run the join on your machine, yield the materialized result rather than the relation. Yield an Arrow table or a DataFrame. dlt then creates no model job and serializes no credentials:

```py notype execute
# to run the join locally and load plain data instead, yield the materialized
# result (an Arrow table or DataFrame) rather than the relation
@dlt.hub.transformation(table_name="user_orders_eager")
def user_orders_eager(warehouse: dlt.Dataset, orders: dlt.Dataset) -> Any:
    joined = warehouse["users"].join(
        orders["orders"], on="users.id = orders.user_id"
    )
    yield joined.arrow()

warehouse_pipeline.run(
    user_orders_eager(warehouse_pipeline.dataset(), orders_pipeline.dataset())
)
```

## Using Pandas or Polars DataFrames and Arrow tables

You can also write transformations directly with Pandas or Polars DataFrames and Arrow tables. Your transformation resource then behaves like a regular resource. `dlt` does not propagate column-level hints, and treats the yielded DataFrames or Arrow tables like data from any other resource. This behavior can change in a future release.

```py notype execute

@dlt.hub.transformation
def copied_customers(dataset: dlt.Dataset) -> Any:
    # get full customers table as arrow table
    customers = dataset.table("customers").arrow()

    # Sort the table by 'name'
    sorted_customers = customers.sort_by([("name", "ascending")])

    # Take first 5 rows
    yield sorted_customers.slice(0, 5)

# the same join with dataframes
@dlt.hub.transformation
def enriched_purchases(dataset: dlt.Dataset) -> Any:
    # get both full tables as dataframes
    purchases = dataset.table("purchases").df()
    customers = dataset.table("customers").df()

    # Merge (JOIN) the DataFrames
    result = purchases.merge(customers, left_on="customer_id", right_on="id")

    # Select only the desired columns
    yield result[["name", "quantity"]]

```


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

and a scheduler window of `[2026-01-05, 2026-01-10)`, the run writes ids 5 to 9 to `orders_window` (id 10 is excluded by the open upper bound).

```py execute
import dlt
from dlt.common.pendulum import pendulum

@dlt.hub.transformation(write_disposition="replace")
def orders_window(
    dataset: dlt.Dataset,
    window: dlt.sources.incremental[pendulum.DateTime] = dlt.sources.incremental(
        "created_at",
        initial_value=pendulum.datetime(2000, 1, 1, tz="UTC"),
        allow_external_schedulers=True,
        range_start="closed",
        range_end="open",
    ),
) -> Any:
    yield dataset.table("orders").incremental(window)

```

Re-running the same `[start, end)` (start is included, end is excluded) interval produces the same transformation input, which makes this pattern a good fit for partition backfills and idempotent retries.

### Continue from the previous run

When each run must continue from the last successful run, use a stateful cursor. No external scheduler is then necessary. dlt stores the cursor state internally and uses it in the next run of the transformation.

The transformation below appends rows from `orders` whose `created_at` is later than the persisted `last_value` to a new table `recent_orders`.

:::note Implicit cursor
The cursor below is declared on the decorator. The body yields a bare relation (Ibis expressions and raw SQL strings work too), and dlt applies the filter automatically. The [scheduler example above](#the-scheduler-interval) shows the alternative form, with the cursor as a function argument.
:::

```py execute
import dlt
from dlt.common.pendulum import pendulum

@dlt.hub.transformation(
    write_disposition="append",
    primary_key="id",
    incremental=dlt.sources.incremental(
        "created_at",
        initial_value=pendulum.datetime(2000, 1, 1, tz="UTC"),
        range_start="open",
    ),
)
def recent_orders(dataset: dlt.Dataset) -> Any:
    yield dataset.table("orders")

```

Now suppose `orders` is loaded in two batches:

| batch   | ids    | `created_at`              |
| ---     | ---    | ---                       |
| initial | 1..3   | 2026-01-01 .. 2026-01-03  |
| later   | 4..5   | 2026-01-04 .. 2026-01-05  |

The first run has no `last_value` yet, so it starts from `initial_value` (`2000-01-01`), writes the three initial rows to `recent_orders`, and advances `last_value` to `2026-01-03`. The next run sees the two later rows fall past `last_value`, appends them, and advances `last_value` to `2026-01-05`.

:::caution Set `range_start="open"` on stateful cursors
Set `range_start="open"` on every stateful cursor. The filter is then `cursor > last_value`, and it excludes the boundary row. A stateful cursor persists `last_value` after each run. With the default `range_start="closed"` the filter is `cursor >= last_value`, and the next run emits the boundary row again.
:::

### Cursor column choices

When the source table has a column for creation or update order, use a domain cursor. For append-only data, `created_at` or an increasing `id` is enough. For mutable data, use a cursor that changes with every row change, such as `updated_at`. The next stateful run ignores the rows whose cursor value does not advance.

When the source table has no domain timestamp, use `_dlt_loads.inserted_at`. dlt then processes the data by load time. A dotted cursor path tells dlt to follow the schema reference from the base table to `_dlt_loads`. dlt joins that table and filters on the joined column. The join is filter-only, and dlt adds no `_dlt_loads` column to the destination table.

```py execute
import dlt
from dlt.common.pendulum import pendulum

@dlt.hub.transformation(write_disposition="append")
def orders_by_load(
    dataset: dlt.Dataset,
    loaded_at: dlt.sources.incremental[pendulum.DateTime] = dlt.sources.incremental(
        "_dlt_loads.inserted_at",
        initial_value=pendulum.datetime(2000, 1, 1, tz="UTC"),
        range_start="open",
    ),
) -> Any:
    yield dataset.table("orders").incremental(loaded_at)

```

:::note
Internally, dlt modifies the source query to include the cursor filter when it runs the transformation as a model job. dlt filters during extraction in two other cases. The first case is a source and a destination at different data locations. The second case is a yield of Python objects, such as lists, Arrow tables, or DataFrames.
:::

### State and safety rules

- dlt rejects `LIMIT` on stateful relation incrementals. A limited result can advance the state past rows that the query did not return. Remove the limit. As an alternative, use an explicit bounded window.
- SQL-based cursors support `max` and `min` last-value functions. dlt cannot translate custom Python `last_value_func` callables to SQL.
- Null handling follows `on_cursor_value_missing`. For SQL pushdown, `"include"` adds `OR cursor IS NULL`. `"exclude"` adds `AND cursor IS NOT NULL`. `"raise"` cannot raise in the middle of a query. It excludes the null cursor values instead.

For lower-level cursor rules, including range inclusivity and `lag`, see [Filter to an incremental cursor](../../general-usage/dataset-access/dataset.md#filter-to-an-incremental-cursor) and [Cursor-based incremental loading](../../general-usage/incremental/cursor.md).


## Schema evolution and hint lineage

`dlt` computes the resulting schema before it executes the transformation. This computation lets `dlt`:

1. Migrate the destination schema accordingly, creating new columns or tables as needed
2. Fail early if there are schema mismatches that cannot be resolved
3. Preserve column-level hints from source to destination

### Schema evolution

For example, a transformation that joins two tables and creates new columns makes `dlt` update the destination schema. An incompatible schema change, such as a column type change that can lose data, makes `dlt` fail before the transformation runs. This protects your data and saves execution and debug time.

You can inspect the computed result schema during development. Read `Relation.columns_schema`, or print `Relation.columns` for the column names only:

```py notype execute
# Show the computed schema before the transformation is executed
dataset = fruitshop_pipeline.dataset()
purchases = dataset.table("purchases").to_ibis()
customers = dataset.table("customers").to_ibis()
enriched_purchases = purchases.join(
    customers, purchases.customer_id == customers.id
)
print(dataset(enriched_purchases).columns)
"""
[
    'id',
    'customer_id',
    'inventory_id',
    'quantity',
    'date',
    '_dlt_load_id',
    '_dlt_id',
    'id_right',
    'name',
    'city',
    '_dlt_load_id_right',
    '_dlt_id_right',
]
"""
```

### Column-level hint forwarding

When it creates or updates tables with transformation resources, `dlt` also forwards certain column hints to the new tables. In our fruitshop source, we apply a custom hint named
`x-annotation-pii` set to True for the `name` column, which indicates that this column contains PII (personally identifiable information).
Downstream of the transformation layer, we can then find out which columns originate from columns that contain private data:

```py notype execute
@dlt.hub.transformation(table_name="enriched_purchases_lineage")
def enriched_purchases_lineage(dataset: dlt.Dataset) -> Any:
    enriched_purchases = dataset(
        """
        SELECT customers.name, purchases.quantity
        FROM purchases
        JOIN customers
            ON purchases.customer_id = customers.id
        """
    )
    yield enriched_purchases

# run the transformation. the name column in the new table is also marked as PII
fruitshop_pipeline.run(enriched_purchases_lineage(fruitshop_pipeline.dataset()))
assert (
    fruitshop_pipeline.dataset().schema.tables["enriched_purchases_lineage"][
        "columns"
    ]["name"][
        "x-annotation-pii"  # type: ignore
    ]
    is True
)
```

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

```py
from dlt.sources.rest_api import (
    rest_api_source,
)

# loads some data from our example api at https://jaffle-shop.scalevector.ai/docs
source = rest_api_source(
    {
        "client": {
            "base_url": "https://jaffle-shop.scalevector.ai/api/v1",
        },
        "resources": [
            "stores",
            {
                "name": "orders",
                "endpoint": {
                    "path": "orders",
                    "params": {
                        "start_date": "2017-01-01",
                        "end_date": "2017-01-31",
                    },
                },
            },
        ],
    }
)

# load to a local DuckDB instance
transit_pipeline = dlt.pipeline(
    "jaffle_shop", destination="duckdb", dataset_name="in_transit"
)
transit_pipeline.run(source)

# define the aggregation transformation
@dlt.hub.transformation
def orders_per_store(dataset: dlt.Dataset) -> Any:
    orders = dataset.table("orders").to_ibis()
    stores = dataset.table("stores").to_ibis()
    yield (
        orders.join(stores, orders.store_id == stores.id)
        .group_by(stores.name)
        .aggregate(order_count=orders.id.count())
    )

# load aggregated data to a warehouse destination
warehouse_pipeline = dlt.pipeline(
    "jaffle_warehouse",
    destination="postgres",
    dataset_name="warehouse",
    dev_mode=True,
)
warehouse_pipeline.run(orders_per_store(transit_pipeline.dataset()))
```

This script:
- fetches data from a REST API with dlt's `rest_api_source`
- loads the raw data into a local DuckDB instance as an intermediate step
- joins orders with stores and aggregates order counts on the local DuckDB instance, not in the destination warehouse
- loads only the aggregated results to a production warehouse (Postgres)
- reduces warehouse compute costs, because the transformations run locally in DuckDB
- uses multiple pipelines in one workflow for different stages of processing
