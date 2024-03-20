---
title: Load Zendesk tickets incrementally
description: Learn how do incremental loading in consecutive runs
keywords: [incremental loading, example]
---

import Header from '../_examples-header.md';

<Header
    intro="In this tutorial, you will learn how to do incremental loading in consecutive runs with dlt.
    The state of your incremental loads will be persisted in
    your selected destination and restored and used on each new load,
    making it very easy to keep your loaded dataset up to date with the source."
    slug="incremental_loading"
    run_file="zendesk"
    destination="duckdb" />

## Incremental loading with the Zendesk API

In this example, you'll find a Python script that interacts with the Zendesk Support API to extract ticket events data.

We'll learn:

- How to pass [credentials](../../general-usage/credentials) as dict and how to type the `@dlt.source` function arguments.
- How to set [the nesting level](../../general-usage/source#reduce-the-nesting-level-of-generated-tables).
- How to enable [incremental loading](../../general-usage/incremental-loading) for efficient data extraction.
- How to specify [the start and end dates](../../general-usage/incremental-loading#using-dltsourcesincremental-for-backfill) for the data loading and how to [opt-in to Airflow scheduler](../../general-usage/incremental-loading#using-airflow-schedule-for-backfill-and-incremental-loading) by setting `allow_external_schedulers` to `True`.
- How to work with timestamps, specifically converting them to Unix timestamps for incremental data extraction.
- How to use the `start_time` parameter in API requests to retrieve data starting from a specific timestamp.


### Loading code

<!--@@@DLT_SNIPPET ./code/zendesk-snippets.py::markdown_source-->


Run the pipeline:


<!--@@@DLT_SNIPPET ./code/zendesk-snippets.py::markdown_pipeline-->


