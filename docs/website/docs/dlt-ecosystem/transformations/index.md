---
title: Transforming your data
description: How to transform your data
keywords: [datasets, data, access, transformations]
---
import DocCardList from '@theme/DocCardList';

# Transforming data

If you'd like to transform your data after a pipeline load, you have 3 options available to you:

* [Using dbt](./dbt/dbt.md) - dlt provides a convenient dbt wrapper to make integration easier.
* [Using the `dlt` SQL client](./sql.md) - dlt exposes an SQL client to transform data on your destination directly using SQL.
* [Using Python with DataFrames or Arrow tables](./python.md) - you can also transform your data using Arrow tables and DataFrames in Python.

If you need to preprocess some of your data before it is loaded, you can learn about strategies to:

* [Rename columns.](../../general-usage/customising-pipelines/renaming_columns)
* [Pseudonymize columns.](../../general-usage/customising-pipelines/pseudonymizing_columns)
* [Remove columns.](../../general-usage/customising-pipelines/removing_columns)

This is particularly useful if you are trying to remove data related to PII or other sensitive data, you want to remove columns that are not needed for your use case or you are using a destination that does not support certain data types in your source data.


# Learn more
<DocCardList />

