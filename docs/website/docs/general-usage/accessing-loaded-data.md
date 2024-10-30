---
title: Accessing loaded data
description: Conveniently accessing the data loaded to any destination
keywords: [destination, schema, data, access, retrieval]
---


# Accessing loaded data

:::caution
All interfaces mentioned on this page are considered experimental and are very likely to change in the next while. This message will be removed as soon as we consider the dataset accessing to be stable :)
:::

`dlt` makes accessing the data you have loaded to a destination with a pipeline available via the `dataset` interface. This is useful if you want to programmatically inspect the data in your destination or run computations with your data on your local machine. You can retrieve data as arrow tables, pandas dataframes of python tuples.

## Quickstart

```py
arrow_table = pipeline._dataset().users.select(["name", "age"]).limit(20).arrow()
```

The example above will:

1. access a lazy loaded dataset from your pipeline instance
2. select the table "users" on that dataset
3. select the columns "name" and "age" on the table
4. limit the result to the first 20 rows
5. return the result as an arrow table so you can use it in code

### A few more examples:

```py
# read the full table named "items" into an arrow table
arrow_table = pipeline._dataset().items.arrow()

# read the first 50 rows of a table into a dataframe
df = pipeline._dataset().limit(50).df()

# iterate over the full table named orders in batches of 20 as arrow tables
for batch in pipepline._dataset().orders.iter_arrow(chunk_size=20):
    print(batch)
```

## The `ReadableDataset` and the `ReadableRelation`



## Manipulating a readable relation


## Accessing data on a readable relation


## Performance considerations