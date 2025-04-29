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
Instead of pulling from an external API, remote database or custom source like `@dlt.resource` does, a transformation’s **input** is a `dlt.Dataset`, and its **output** is a source
that can be loaded into the same dataset as the input, another dataset on the same destination or even to another destination on another database. Depending on the locations of input and
output dataset, these transformations will be execute in sql or just the query is executed as sql, extracted as arrow tables and then materialized in the destination dataset in the same
way that regular dlt resources are.

You create them with the `@dlt.transformation` decorator which has the same signature as the `@dlt.resource` decorator, but does not yield items but rather a sql query including the resulting
column schema. dlt transformations support the same write_dispositions per destination as the dlt resources do.

## Motivations

A few real world scenarios where dlt transformations can be useful:

- **Build one-stop reporting tables** – Flatten and enrich raw data into a wide table that analysts can pivot, slice, and dice without writing SQL each time.  
- **Normalise JSON into 3-NF** – Break out repeating attributes from nested JSON so updates are consistent and storage isn’t wasted.  
- **Create dimensional (star-schema) models** – Produce fact and dimension tables so BI users can drag-and-drop metrics and break them down by any dimension.  
- **Generate task-specific feature sets** – Deliver slim tables tailored for personalisation, forecasting, or other ML workflows.  
- **Apply shared business definitions** – Encode rules such as “a *sale* is a transaction whose status became *paid* this month,” ensuring every metric is counted the same way.  
- **Merge heterogeneous sources** – Combine Shopify, Amazon, WooCommerce (etc.) into one canonical *orders* feed for unified inventory and revenue reporting.  
- **Run transformations during ingestion pre warehouse** – Pre-aggregate or pre-filter data before it hits the warehouse to cut compute and storage costs.  
- **…and more** – Any scenario where reshaping, enriching, or aggregating existing data unlocks faster insight or cleaner downstream pipelines.


## Quick-start in three simple steps

> The snippets below use a simple “fruit-shop” source. You can copy–paste everything into one script and run it.

### 1  Load some example data

<details>

<summary>Click to display pipeline that loads some example data to create an initial dataset</summary>

```py
import dlt
from decimal import Decimal

@dlt.resource(primary_key="id")
def customers():
    yield [
        {"id": 1, "name": "simon", "city": "berlin"},
        {"id": 2, "name": "violet", "city": "london"},
        {"id": 3, "name": "tammo", "city": "new york"},
        {"id": 4, "name": "dave", "city": "berlin"},
        {"id": 5, "name": "andrea", "city": "berlin"},
        {"id": 6, "name": "marcin", "city": "berlin"},
        {"id": 7, "name": "sarah", "city": "paris"},
        {"id": 8, "name": "miguel", "city": "madrid"},
        {"id": 9, "name": "yuki", "city": "tokyo"},
        {"id": 10, "name": "olivia", "city": "sydney"},
        # … truncated, please add more …
    ]

# apply a custom hint to the name column of the customer
customers.apply_hints(columns={"name": {"x-pii": True}})  # type: ignore

@dlt.resource(primary_key="id")
def inventory():
    yield [
        {"id": 1, "name": "apple",  "price": Decimal("1.50")},
        {"id": 2, "name": "banana", "price": Decimal("1.70")},
        {"id": 3, "name": "pear",   "price": Decimal("2.50")},
    ]

@dlt.resource(primary_key="id")
def purchases():
    yield [
        {"id": 1, "customer_id": 1, "inventory_id": 1, "quantity": 1},
        {"id": 2, "customer_id": 1, "inventory_id": 2, "quantity": 2},
        {"id": 3, "customer_id": 2, "inventory_id": 3, "quantity": 3},
    ]

@dlt.source
def fruitshop():
    return customers(), inventory(), purchases()

fruit_p = dlt.pipeline("fruitshop", destination="postgres")
fruit_p.run(fruitshop)
```

</details>

### 2  Inspect the dataset

```py
# Show row counts for every table
print(fruit_p.dataset().row_counts().df())
```

### 3  Write and run a transformation

```py
import dlt
from typing import Any

@dlt.transformation()
def copied_customers(dataset: dlt.Dataset) -> Any:
    # Ibis expression: sort by name and keep first 5 rows
    customers_table = dataset["customers"]
    return customers_table.order_by("name").limit(5)

# Same pipeline & same dataset
fruit_p.run(copied_customers(fruit_p.dataset()))

# show rowcounts again, we now have a new table in the schema and the destination
print(fruit_p.dataset().row_counts().df())
```

That’s it — `copied_customers` is now a new table in **the same** Postgres schema with the first 5 customers when ordered by name. `dlt` has detected that we are loading into the same dataset
and executed this transformation in sql - no data was transferred to and from the machine executing this pipeline. Additional the new destination table `copied_customers` was automatically evolved
to the correct new schema, and you could also set a different write disposition and even merge data from a transformation.

## Defining a transformation

:::info
Most of the following examples will be using the ibis expressions of the `dlt.Dataset`. Read the detailed [dataset docs](../../general-usage/dataset-access/dataset) to learn how to use these.
:::

```py
@dlt.transformation(name="orders_per_user", write_disposition="merge")
def orders_per_user(dataset: dlt.Dataset) -> TReadableRelation:
    purchases = dataset["purchases"]
    return purchases.group_by(purchases.customer_id).aggregate(
        order_count=purchases.id.count()
    )
```

* **Decorator arguments** mirror those accepted by `@dlt.resource`.
* The transformation function signature must contain at least one `dlt.Dataset` which is used inside the function to create the transformation sql statements and calculate the resulting schema update.
* Return **either** an `TReadableRelation` or an sql select query which will be materialized into the destination table. _Do **not** yield Python dictionaries._

## Loading to other datasets

### Same Postgres instance, different dataset

Below we load to the same postgres instance with a new pipeline that points to another `dataset`. dlt will be able to detect that both datasets live on the same destination and
will run the transformation as pure sql.

```py
dest_p = dlt.pipeline("fruitshop", destination="postgres", dataset_name="copied_dataset")
dest_p.run(copied_customers(fruit_p.dataset()))
```

### Different engine (Postgres → DuckDB)

By simply pointing the same transformations to a dataset on another destination, you can also run cross destination transformations. In this case the same select query will be run on the
source dataset, but the data will be extracted as arrow tables and forwarded through the dlt pipeline the same way data from other resources is. 

```py
duck_p = dlt.pipeline("fruitshop", destination="duckdb")
duck_p.run(copied_customers(fruit_p.dataset()))
```

## Grouping multiple transformations in a source

`dlt transformations` can be grouped like all other resources into resources and will be executed together. You can even mix regular resources and transformations in one pipeline load.

```py
@dlt.source
def my_transformations(dataset: dlt.Dataset):

    @dlt.transformation(write_disposition="append")
    def enriched_purchases(dataset: dlt.Dataset):
        purchases = dataset["purchases"]
        customers = dataset["customers"]
        return purchases.join(customers, purchases.customer_id == customers.id)

    @dlt.transformation(write_disposition="replace")
    def total_items_sold(dataset: dlt.Dataset):
        purchases = dataset["purchases"]
        return purchases.aggregate(total_qty=purchases.quantity.sum())

    return enriched_purchases(dataset), total_items_sold(dataset)

fruit_p.run(my_transformations(fruit_p.dataset()))
```

## Writing your queries in sql

If you prefer to write your queries in sql, you can execute these queries directly on your dataset... #TODO

## Schema evolution and hints lineage

# TODO

## Normalization

# TODO, this also still under development

## Incremental transformations examples

# TODO

## Local in-transit transformations examples



