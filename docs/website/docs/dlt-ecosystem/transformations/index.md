---
title: Overview
description: dlt supports both ETL and ELT transformation patterns
keywords: [elt, etl, transformer, transformations]
---

`dlt` supports both Extract, Transform, Load (ETL) and Extract, Load, Transform (ELT) patterns.

In ETL, the data is transformed before being loaded into the destination. This is useful for light processing such as adding columns, removing sensitive data, or type casting. `dlt` offers built-in utilities like `add_map()` and custom processors via `@dlt.transformer`

<<<<<<< HEAD
* [Using dbt](./dbt/dbt.md) - dlt provides a convenient dbt wrapper to make integration easier.
* [Using the `dlt` SQL client](./sql.md) - dlt exposes an SQL client to transform data on your destination directly using SQL.
* [Using Python with DataFrames or Arrow tables](./python.md) - you can also transform your data using Arrow tables, Pandas or Polars DataFrames in Python.

If you need to preprocess some of your data before it is loaded, you can learn about strategies to:

* [Rename columns.](../../general-usage/customising-pipelines/renaming_columns.md)
* [Pseudonymize columns.](../../general-usage/customising-pipelines/pseudonymizing_columns.md)
* [Remove columns.](../../general-usage/customising-pipelines/removing_columns.md)

This is particularly useful if you are trying to remove data related to PII or other sensitive data, you want to remove columns that are not needed for your use case or you are using a destination that does not support certain data types in your source data.


# Learn more
<DocCardList />
=======
In ELT, the data is loaded as-is in the destination. This raw data is transformed directly on the destination where more powerful compute is available (e.g., data warehouse, data lake). `dlt` supports this via several patterns.
>>>>>>> devel

The two approaches can be used together in a single project.
