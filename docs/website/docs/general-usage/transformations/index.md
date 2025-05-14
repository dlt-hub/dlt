---
title: Transforming loaded datasets
description: Define SQL-based or mixed SQL + Python transformations on data that is **already** in your destination.
keywords: [transformation, dataset, sql, pipeline, ibis, arrow]
---

# Transformations: reshape data you have already loaded

:::danger
The `dlt.transformation` decorator is currently in preview and while it may appear to work very well, it should not be considered stable. If you found this unlinked page and would like to give us feedback, please let us know in our Slack community.
:::

`dlt transformations` let you build new tables or views from datasets that have _already_ been ingested with `dlt`.  
Instead of pulling from an external API, remote database, or custom source like `@dlt.resource` does, a transformation's **input** is a `dlt.Dataset`, and its **output** is a source
that can be loaded into the same dataset as the input, another dataset on the same destination, or even to another destination on another database. Depending on the locations of the input and
output datasets, these transformations will be executed in SQL, or just the query is executed as SQL, extracted as Arrow tables, and then materialized in the destination dataset in the same
way that regular dlt resources are.

You create them with the `@dlt.transformation` decorator which has the same signature as the `@dlt.resource` decorator, but does not yield items but rather a SQL query including the resulting
column schema. dlt transformations support the same write_dispositions per destination as dlt resources do.

## Motivations

A few real-world scenarios where dlt transformations can be useful:

- **Build one-stop reporting tables** – Flatten and enrich raw data into a wide table that analysts can pivot, slice, and dice without writing SQL each time.  
- **Normalize JSON into 3-NF** – Break out repeating attributes from nested JSON so updates are consistent and storage isn't wasted.  
- **Create dimensional (star-schema) models** – Produce fact and dimension tables so BI users can drag-and-drop metrics and break them down by any dimension.  
- **Generate task-specific feature sets** – Deliver slim tables tailored for personalization, forecasting, or other ML workflows.  
- **Apply shared business definitions** – Encode rules such as "a *sale* is a transaction whose status became *paid* this month," ensuring every metric is counted the same way.  
- **Merge heterogeneous sources** – Combine Shopify, Amazon, WooCommerce (etc.) into one canonical *orders* feed for unified inventory and revenue reporting.  
- **Run transformations during ingestion pre-warehouse** – Pre-aggregate or pre-filter data before it hits the warehouse to cut compute and storage costs.  
- **…and more** – Any scenario where reshaping, enriching, or aggregating existing data unlocks faster insight or cleaner downstream pipelines.


## Quick-start in three simple steps

> The snippets below use a simple "fruit-shop" source. You can copy–paste everything into one script and run it.

### 1. Load some example data

The snippets below assume that we have a simple fruitshop dataset as produced by the dlt fruitshop template:

<!--@@@DLT_SNIPPET ./transformation-snippets.py::quick_start_example-->


### 1.1 Use the fruitshop template as a starting point

Alternatively, you can follow the code examples below by creating a new pipeline with the fruitshop template and running transformations on the resulting dataset:

```sh
dlt init fruitshop duckdb
```

### 2. Inspect the dataset

<!--@@@DLT_SNIPPET ./transformation-snippets.py::dataset_inspection-->

### 3. Write and run a transformation based on ibis expressions

<!--@@@DLT_SNIPPET ./transformation-snippets.py::basic_transformation-->


### 3.1 Alternatively use pure SQL for the transformation 

<!--@@@DLT_SNIPPET ./transformation-snippets.py::sql_queries_short-->

That's it — `copied_customers` is now a new table in **the same** DuckDB schema with the first 5 customers when ordered by name. `dlt` has detected that we are loading into the same dataset
and executed this transformation in SQL - no data was transferred to and from the machine executing this pipeline. Additionally, the new destination table `copied_customers` was automatically evolved
to the correct new schema, and you could also set a different write disposition and even merge data from a transformation.

## Defining a transformation

:::info
Most of the following examples will be using the ibis expressions of the `dlt.Dataset`. Read the detailed [dataset docs](../../general-usage/dataset-access/dataset) to learn how to use these.
:::

<!--@@@DLT_SNIPPET ./transformation-snippets.py::orders_per_user-->

* **Decorator arguments** mirror those accepted by `@dlt.resource`.
* The transformation function signature must contain at least one `dlt.Dataset` which is used inside the function to create the transformation SQL statements and calculate the resulting schema update.
* Return a `TReadableRelation` created with ibis expressions or a select query which will be materialized into the destination table. _Do **not** yield Python dictionaries._

## Loading to other datasets


### Loading to another dataset on the same physical location

Below we load to the same DuckDB instance with a new pipeline that points to another `dataset`. dlt will be able to detect that both datasets live on the same destination and
will run the transformation as pure SQL.

<!--@@@DLT_SNIPPET ./transformation-snippets.py::loading_to_other_datasets-->

### Loading to another dataset on a different physical location

Below we load the data from our local DuckDB instance to a Postgres instance. dlt will use the query to extract the data as Parquet files and will do a regular dlt load, pushing the data
to Postgres. Note that you can use the exact same transformation functions for both scenarios.

<!--@@@DLT_SNIPPET ./transformation-snippets.py::loading_to_other_datasets_other_engine-->

## Grouping multiple transformations in a source

`dlt transformations` can be grouped like all other resources into sources and will be executed together. You can even mix regular resources and transformations in one pipeline load.

<!--@@@DLT_SNIPPET ./transformation-snippets.py::multiple_transformations-->

## Writing your queries in SQL

If you prefer to write your queries in SQL, you can omit ibis expressions by simply creating a TReadableRelation from a query on your dataset:

<!--@@@DLT_SNIPPET ./transformation-snippets.py::sql_queries-->


## Using pandas dataframes or arrow tables

You can also directly write transformations using pandas or arrow. Please note that your transformation resource will act like a regular resource in this case, you will not have column level hints forward but rather dlt will just see the dataframes or arrow tables you yield and process them like from any other resource. This may change in the future.

<!--@@@DLT_SNIPPET ./transformation-snippets.py::arrow_dataframe_operations-->


## Schema evolution and hints lineage

When executing transformations, dlt computes the resulting schema before the transformation is executed. This allows dlt to:

1. Migrate the destination schema accordingly, creating new columns or tables as needed
2. Fail early if there are schema mismatches that cannot be resolved
3. Preserve column-level hints from source to destination

### Schema evolution

For example, if your transformation joins two tables and creates new columns, dlt will automatically update the destination schema to accommodate these changes. If your transformation would result in incompatible schema changes (like changing a column's data type in a way that could lose data), dlt will fail before executing the transformation, protecting your data and saving execution and debug time. 

You can inspect the computed result schema during development by looking at the result of `compute_columns_schema` on your `TReadableRelation`:

<!--@@@DLT_SNIPPET ./transformation-snippets.py::computed_schema-->

### Column level hint forwarding

When creating or updating tables with transformation resources, dlt will also forward certain column hints to the new tables. In our fruitshop source, we have applied a custom hint named
`x-pii` set to True for the `name` column, which indicates that this column contains PII (personally identifiable information). We might want to know downstream of our transformation layer
which columns resulted from origin columns that contain private data:

<!--@@@DLT_SNIPPET ./transformation-snippets.py::column_level_lineage-->

Features and limitations:

* `dlt` will only forward certain types of hints to the resulting tables: custom hints starting with `x-...` and type hints such as `nullable`, `precision`, `scale`, and `timezone`. Other hints, such as `primary_key` or `merge_keys`, will need to be set by the developer, since dlt does not know how the transformed tables will be used.
* `dlt` will not be able to forward hints for columns that are the result of combining two origin columns, for example by concatenating them or similar operations.

## Normalization

When executing transformations, dlt will add internal dlt columns to your SQL queries depending on the configuration:

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

Additionally, column names are normalized according to the naming schema selected and the identifier capabilities of the destinations. This ensures compatibility and consistent naming conventions across different data sources and destination systems.

This allows dlt to maintain data lineage and enables features like incremental loading and merging, even when working with raw SQL queries.

:::info
This feature is planned but not yet implemented. Currently, you need to explicitly include these columns in your SQL queries if you want to use them for tracking purposes.
:::

> The normalization described here, including automatic injection or replacement of dlt columns, applies only to SQL-based transformations. Python-based transformations, such as those using dataframes or arrow tables, follow a different normalization process as regular resources.

## Local in-transit transformations example

If you require aggregated or otherwise transformed data in your warehouse, but would like to avoid or reduce the costs of running queries across many rows in your warehouse tables, you can run some or all of your transformations "in transit" while loading data from your source. The code below demonstrates how you can extract data with our `rest_api` source to a local DuckDB instance and then forward aggregated data to a warehouse destination.

<!--@@@DLT_SNIPPET ./transformation-snippets.py::in_transit_transformations-->

This script demonstrates:
- Fetching data from a REST API using dlt's rest_api_source
- Loading raw data into a local DuckDB instance as an intermediate step
- Transforming the data by joining orders with stores and aggregating order counts directly on the local DuckDB instance, not in the destination warehouse
- Loading only the aggregated results to a production warehouse (Postgres)
- Reducing warehouse compute costs by performing transformations locally in DuckDB
- Using multiple pipelines in a single workflow for different stages of processing

## Incremental transformations example

:::info
This example shows how to do incremental transformations based on an incremental primary key of the original tables. We are working on simplifying incremental transformations at the moment.
:::

<!--@@@DLT_SNIPPET ./transformation-snippets.py::incremental_transformations-->

This example demonstrates how you can incrementally transform incoming new data for the customers table into a cleaned_customers table where the name column has been removed. It:
 * Uses primary key-based incremental loading to process only new data
 * Tracks the highest ID processed so far to filter out already processed records
 * Handles first-time runs with the PipelineNeverRan exception
 * Removes sensitive data (name column) during the transformation
 * Uses write_disposition="append" to add only new records to the destination table
 * Can be run repeatedly as new data arrives, processing only the delta



