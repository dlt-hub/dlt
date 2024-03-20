---
title: Control nested MongoDB data
description: Learn how control nested data
keywords: [incremental loading, example]
---

import Header from '../_examples-header.md';

<Header
    intro="This example demonstrates how to control nested data using Python and the dlt library. It covers working with MongoDB, incremental loading, limiting nesting levels, and applying data type hints."
    slug="nested_data"
    run_file="nested_data"
    destination="duckdb"/>

## Control nested data

In this example, you'll find a Python script that demonstrates how to control nested data using the `dlt` library.

We'll learn how to:
- [Adjust maximum nesting level in three ways:](../../general-usage/source#reduce-the-nesting-level-of-generated-tables)
  - Limit nesting levels with dlt decorator.
  - Dynamic nesting level adjustment.
  - Apply data type hints.
- Work with [MongoDB](../../dlt-ecosystem/verified-sources/mongodb) in Python and `dlt`.
- Enable [incremental loading](../../general-usage/incremental-loading) for efficient data extraction.

### Install pymongo

```sh
 pip install pymongo>=4.3.3
```

### Loading code

<!--@@@DLT_SNIPPET code/nested_data-snippets.py::nested_data-->


### Run the pipeline

<!--@@@DLT_SNIPPET code/nested_data-snippets.py::nested_data_run-->

