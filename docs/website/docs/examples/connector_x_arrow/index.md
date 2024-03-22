---
title: Load mysql table with ConnectorX & Arrow
description: Load data from sql queries fast with connector x and arrow tables
keywords: [connector x, pyarrow, zero copy]
---

import Header from '../_examples-header.md';

<Header
    intro="In this example, you will learn how to use arrow tables to load data from sql queries.
    This method creates arrow tables in memory using Connector X and then loads them into destination
    supporting parquet files without copying data.
    "
    slug="connector_x_arrow"
    run_file="load_arrow"
    destination="duckdb" />

## Load mysql table with ConnectorX and Arrow

Example script below takes genome data from public **mysql** instance and then loads it into **duckdb**. Mind that your destination
must support loading of parquet files as this is the format that `dlt` uses to save arrow tables. [Connector X](https://github.com/sfu-db/connector-x) allows to
get data from several popular databases and creates in memory Arrow table which `dlt` then saves to load package and loads to the destination.
:::tip
You can yield several tables if your data is large and you need to partition your load.
:::

We'll learn:

- How to get arrow tables from [connector X](https://github.com/sfu-db/connector-x) and yield them.
- That merge and incremental loads work with arrow tables.
- How to enable [incremental loading](../../general-usage/incremental-loading) for efficient data extraction.
- How to use build in ConnectionString credentials.



### Loading code

<!--@@@DLT_SNIPPET ./code/load_arrow-snippets.py::markdown_source-->


Run the pipeline:

<!--@@@DLT_SNIPPET ./code/load_arrow-snippets.py::markdown_pipeline-->

