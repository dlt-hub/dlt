---
title: Transforming loaded datasets
description: Define SQL-based or mixed SQL + Python transformations on data that is **already** in your destination.
keywords: [transformation, dataset, sql, pipeline, ibis, arrow]
---

# Transformations: query and reshape data you already loaded

`dlt` **transformations** let you build new tables or views from datasets that have _already_ been ingested with `dlt`.  
Instead of pulling from an external API or database `@dlt.resource` does, a transformation’s **input** is a `dlt.Dataset`, and its **output** is usually

* an **Ibis** table expression (preferred),
* a raw SQL string, **or**
* a low-level `TReadableRelation`.

You create them with the `@dlt.transformation` decorator.


## Quick-start in three steps

> The snippets below use a simple “fruit-shop” source. You can copy–paste everything into one script and run it.

### 1  Load some example data

```py
import dlt
from decimal import Decimal

@dlt.resource(primary_key="id")
def customers():
    yield [
        {"id": 1, "name": "simon",  "city": "berlin"},
        {"id": 2, "name": "violet", "city": "london"},
        # … truncated …
    ]

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
    return dataset["customers"].order_by("name").limit(5)

# Same pipeline & same dataset
fruit_p.run(copied_customers(fruit_p.dataset()))
```

That’s it—`copied_customers` is now a new table in **the same** Postgres schema.



## Feature tour

| Feature                                   | Details |
|-------------------------------------------|---------|
| **Pure-SQL optimisation**                 | If source and target share the same physical engine, the transformation SQL is executed _in place_—no data movement. |
| **Cross-database support**                | When engines differ, `dlt` materialises Arrow tables and loads them like any other resource. |
| **Ibis query syntax**                     | Write transformations in pure Python and let Ibis generate dialect-correct SQL behind the scenes. |
| **Schema-hint propagation**               | Primary keys, data types, and other schema hints automatically flow through the pipeline. |



## Defining a transformation

```py
@dlt.transformation(name="orders_per_user", write_disposition="merge")
def orders_per_user(dataset: dlt.Dataset):
    purchases = dataset["purchases"]
    return purchases.group_by(purchases.customer_id).aggregate(
        order_count=purchases.id.count()
    )
```

* **Decorator arguments** mirror those accepted by `pipeline.run` (`write_disposition`, `name`, etc.).
* The function receives a `dlt.Dataset`; use subscript syntax (`dataset["table"]`) to get an Ibis table expression.
* Return **either** an Ibis expression, SQL string, or `TReadableRelation`. _Do **not** yield Python dictionaries._



## Loading to different destinations

### Same Postgres instance, different dataset

```py
dest_p = dlt.pipeline("fruitshop", destination="postgres", dataset_name="copied_dataset")
dest_p.run(copied_customers(fruit_p.dataset()))
```

### Different engine (Postgres → DuckDB)

```py
duck_p = dlt.pipeline("fruitshop", destination="duckdb")
duck_p.run(copied_customers(fruit_p.dataset()))
```

`dlt` executes the SQL in Postgres, streams the result as Arrow, and writes it to DuckDB.



## Grouping multiple transformations in a source

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

Grouping lets you ship a logical set of transformations as one reusable unit.



## Best practices

* **Use Ibis first.** You get type safety, column auto-completion, and automatic SQL generation.
* **Keep engines together** when performance matters—pure-SQL beats Arrow round-trips.
* **Chain steps.** Run loading and transformations in one pipeline call to minimise I/O.
* **Return tables, not dicts.** A transformation that yields Python objects is an error.


## Limitations

* Pure-SQL optimisation only applies when source **and** target share the engine **and** storage location.
* Cross-database mode necessarily moves data (Arrow → destination)—expect additional latency.
* Transformation functions must return SQL / Ibis tables / `TReadableRelation`; dictionaries are not supported.


