---
title: "Project"
description: Run local transformations with dlt+ Cache
keywords: ["dlt+", "peoject", "cache", "dbt"]
---
import DocCardList from '@theme/DocCardList';

As part of dlt+, we provide a local transformation [cache](../../core-concepts/cache.md) — a staging layer for data transformations allowing you to test, validate, and debug data pipelines without running everything in the warehouse. With local transformations, you can:

* Run transformations locally, eliminating the need to wait for warehouse queries.
* Validate the schema before loading to catch mismatches early.
* Test without incurring cloud costs, as in-memory execution prevents wasted compute.

Local transformations are built on DuckDB, Arrow, and dbt, so they work with your existing stack.

:::caution
The local transformations feature is currently in the early access phase. We recommend waiting for general access before using it in production.
:::

<DocCardList />

