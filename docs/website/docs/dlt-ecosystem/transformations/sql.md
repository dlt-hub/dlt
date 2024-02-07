---
title: Transform the data with SQL
description: Transforming the data loaded by a dlt pipeline with the dlt SQL client
keywords: [transform, sql]
---

# Transform the data using the `dlt` SQL client

A simple alternative to dbt is to query the data using the `dlt` SQL client and then performing the
transformations using Python. The `execute_sql` method allows you to execute any SQL statement,
including statements that change the database schema or data in the tables. In the example below we
insert a row into `customers` table. Note that the syntax is the same as for any standard `dbapi`
connection.

```python
pipeline = dlt.pipeline(destination="bigquery", dataset_name="crm")
try:
    with pipeline.sql_client() as client:
        client.sql_client.execute_sql(
            f"INSERT INTO customers VALUES (%s, %s, %s)",
            10,
            "Fred",
            "fred@fred.com"
        )
```

In the case of SELECT queries, the data is returned as a list of row, with the elements of a row
corresponding to selected columns.

```python
try:
    with pipeline.sql_client() as client:
        res = client.execute_sql(
            "SELECT id, name, email FROM customers WHERE id = %s",
            10
        )
        # prints columns values of first row
        print(res[0])
```

## Other transforming tools

If you want to transform the data before loading, you can use Python. If you want to transform the
data after loading, you can use SQL or one of the following:

1. [dbt](dbt/dbt.md) (recommended).
1. [Pandas.](pandas.md)
