---
title: "Staging: transformations"
description: Ensure the high quality of your data workflows
keywords: ["dlt+", "data quality", "tests"]
---
import DocCardList from '@theme/DocCardList';

## What is dlt+ staging?

dlt+ Staging is a complete testing and validation layer for data transformations, combining a local cache with schema enforcement, debugging tools, and integration with existing data workflows.

Modern data teams move fast, but their development workflows haven't kept up. While software engineers have **staging environments, CI/CD, and instant feedback loops,** data engineers are still **debugging transformations inside the warehouse, at full scale, burning cloud credits.**

**dlt+ Staging changes that.**

dlt+ Staging provides a staging layer for data transformations — a way to test, validate, and debug without running everything in the warehouse.

* Run transformations locally → No waiting for warehouse queries.
* Validate schema before loading → Catch mismatches early.
* Test without burning cloud costs → In-memory execution means no wasted compute.

## Local transformations

dlt+ Staging provides a local transformation cache that lets you:

* Sync raw data into an isolated test environment.
* Validate transformations without touching the warehouse.
* Test faster and iterate freely - without worrying about cloud bills.

It’s built on DuckDB, Arrow, and dbt, so it works with your existing stack.

:::tip
Learn more about dlt+ cache in the [Cache section](../../core-concepts/cache.md).
:::

dlt+ supports transformations defined in Python and dbt models. Additionally, it can automatically generate dbt models using metadata produced by dlt.

<DocCardList />

