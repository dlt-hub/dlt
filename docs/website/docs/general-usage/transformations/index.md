---
title: Transforming loaded datasets
description: Define SQL-based or mixed SQL + Python transformations on data that is **already** in your destination.
keywords: [transformation, dataset, sql, pipeline, ibis, arrow]
---

# Transformations: reshape data you have already loaded

:::danger
The `dlt.transformation` decorator is currently in preview and while it may appear to work very well should not be considered to be stable. If you find this unlinked page nonetheless and you would like to give us feedback, let us know in our slack community.
:::

`dlt transformations` let you build new tables or views from datasets that have _already_ been ingested with `dlt`.  
Instead of pulling from an external API, remote database or custom source like `@dlt.resource` does, a transformation's **input** is a `dlt.Dataset`, and its **output** is a source
that can be loaded into the same dataset as the input, another dataset on the same destination or even to another destination on another database. Depending on the locations of input and
output dataset, these transformations will be execute in sql or just the query is executed as sql, extracted as arrow tables and then materialized in the destination dataset in the same
way that regular dlt resources are.

You create them with the `@dlt.transformation` decorator which has the same signature as the `@dlt.resource` decorator, but does not yield items but rather a sql query including the resulting
column schema. dlt transformations support the same write_dispositions per destination as the dlt resources do.

## Motivations

A few real world scenarios where dlt transformations can be useful:

- **Build one-stop reporting tables** – Flatten and enrich raw data into a wide table that analysts can pivot, slice, and dice without writing SQL each time.  
- **Normalise JSON into 3-NF** – Break out repeating attributes from nested JSON so updates are consistent and storage isn't wasted.  
- **Create dimensional (star-schema) models** – Produce fact and dimension tables so BI users can drag-and-drop metrics and break them down by any dimension.  
- **Generate task-specific feature sets** – Deliver slim tables tailored for personalisation, forecasting, or other ML workflows.  
- **Apply shared business definitions** – Encode rules such as "a *sale* is a transaction whose status became *paid* this month," ensuring every metric is counted the same way.  
- **Merge heterogeneous sources** – Combine Shopify, Amazon, WooCommerce (etc.) into one canonical *orders* feed for unified inventory and revenue reporting.  
- **Run transformations during ingestion pre warehouse** – Pre-aggregate or pre-filter data before it hits the warehouse to cut compute and storage costs.  
- **…and more** – Any scenario where reshaping, enriching, or aggregating existing data unlocks faster insight or cleaner downstream pipelines.


## Quick-start in three simple steps

> The snippets below use a simple "fruit-shop" source. You can copy–paste everything into one script and run it.

### 1  Load some example data

The snippets below assume that we have a simple fruitshop dataset as produce by the dlt fruitshop template:

<!--@@@DLT_SNIPPET ./transformation-snippets.py::quick_start_example-->


### 2  Inspect the dataset

<!--@@@DLT_SNIPPET ./transformation-snippets.py::dataset_inspection-->

### 3  Write and run a transformation

<!--@@@DLT_SNIPPET ./transformation-snippets.py::basic_transformation-->

That's it — `copied_customers` is now a new table in **the same** Postgres schema with the first 5 customers when ordered by name. `dlt` has detected that we are loading into the same dataset
and executed this transformation in sql - no data was transferred to and from the machine executing this pipeline. Additional the new destination table `copied_customers` was automatically evolved
to the correct new schema, and you could also set a different write disposition and even merge data from a transformation.

## Defining a transformation

:::info
Most of the following examples will be using the ibis expressions of the `dlt.Dataset`. Read the detailed [dataset docs](../../general-usage/dataset-access/dataset) to learn how to use these.
:::

<!--@@@DLT_SNIPPET ./transformation-snippets.py::orders_per_user-->

* **Decorator arguments** mirror those accepted by `@dlt.resource`.
* The transformation function signature must contain at least one `dlt.Dataset` which is used inside the function to create the transformation sql statements and calculate the resulting schema update.
* Return an `TReadableRelation` created with ibis expressions or a select query which will be materialized into the destination table. _Do **not** yield Python dictionaries._

## Loading to other datasets

### Same Postgres instance, different dataset

Below we load to the same duckdb instance with a new pipeline that points to another `dataset`. dlt will be able to detect that both datasets live on the same destination and
will run the transformation as pure sql.

<!--@@@DLT_SNIPPET ./transformation-snippets.py::loading_to_other_datasets-->

## Grouping multiple transformations in a source

`dlt transformations` can be grouped like all other resources into resources and will be executed together. You can even mix regular resources and transformations in one pipeline load.

<!--@@@DLT_SNIPPET ./transformation-snippets.py::multiple_transformations-->

## Writing your queries in sql

If you prefer to write your queries in sql, you can omit ibis expressions by simply creating a TReadableRelation from a query on your dataset:

<!--@@@DLT_SNIPPET ./transformation-snippets.py::sql_queries-->

## Schema evolution and hints lineage

# TODO

## Normalization

# TODO, this also still under development

## Incremental transformations examples

# TODO

## Local in-transit transformations examples



