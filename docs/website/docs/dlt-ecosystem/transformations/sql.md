---
title: Transforming data with SQL
description: Transforming the data loaded by a dlt pipeline with the dlt SQL client
keywords: [transform, sql]
---

# Transforming data using the `dlt` SQL client

A simple alternative to dbt is to query the data using the `dlt` SQL client and then perform the
transformations using SQL statements in Python. The `execute_sql` method allows you to execute any SQL statement,
including statements that change the database schema or data in the tables. In the example below, we
insert a row into the `customers` table. Note that the syntax is the same as for any standard `dbapi`
connection.

:::info
* This method will work for all SQL destinations supported by `dlt`, but not for the filesystem destination.
* Read the [SQL client docs](../../ general-usage/dataset-access/dataset) for more information on how to access data with the SQL client.
* If you are simply trying to read data, you should use the powerful [dataset interface](../../general-usage/dataset-access/dataset) instead.
:::


Typically you will use this type of transformation if you can create or update tables directly from existing tables
without any need to insert data from your Python environment. 

The example below creates a new table `aggregated_sales` that contains the total and average sales for each category and region


```py
pipeline = dlt.pipeline(destination="duckdb", dataset_name="crm")

# NOTE: this is the duckdb sql dialect, other destinations may use different expressions
with pipeline.sql_client() as client:
    client.execute_sql(
        """ CREATE OR REPLACE TABLE aggregated_sales AS
            SELECT 
                category,
                region,
                SUM(amount) AS total_sales,
                AVG(amount) AS average_sales
            FROM 
                sales
            GROUP BY 
                category, 
                region;
    """)
```

You can also use the `execute_sql` method to run select queries. The data is returned as a list of rows, with the elements of a row
corresponding to selected columns. A more convenient way to extract data is to use dlt datasets. 

```py
try:
    with pipeline.sql_client() as client:
        res = client.execute_sql(
            "SELECT id, name, email FROM customers WHERE id = %s",
            10
        )
        # Prints column values of the first row
        print(res[0])
except Exception:
    ...
```

## Other transforming tools

If you want to transform your data before loading, you can use Python. If you want to transform the
data after loading, you can use SQL or one of the following:

1. [dbt](dbt/dbt.md) (recommended).
2. [Python with DataFrames or Arrow tables](python.md).

