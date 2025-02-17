---
title: "Local transformations"
description: Run local transformations with dlt+ Cache
keywords: ["dlt+", "transformations", "cache", "dbt"]
---
import DocCardList from '@theme/DocCardList';

Modern data teams move fast, but their development workflows haven't kept up. While software engineers have staging environments, CI/CD, and instant feedback loops, data engineers are still debugging transformations inside the warehouse, at full scale, burning cloud credits.

As part of dlt+ we provide local transformation [cache](../../core-concepts/cache.md) — a staging layer for data transformations allowing to test, validate, and debug data pipelines without running everything in the warehouse. With the local transformations you can:

* Run transformations locally, eliminating the need to wait for warehouse queries.
* Validate the schema before loading to catch mismatches early.
* Test without incurring cloud costs, as in-memory execution prevents wasted compute.

Local transformations are built on DuckDB, Arrow, and dbt, so it works with your existing stack.

:::caution
The local transformations feature is currently in the early access phase. We recommend waiting for general access before using it in production.
:::

<DocCardList />

