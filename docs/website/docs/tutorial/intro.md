---
title: Tutorials
description: Build a data pipeline with dlt
keywords: [tutorial, api, github, duckdb, pipeline]
---
Welcome to the tutorial on how to efficiently use dlt to build a data pipeline. This tutorial will introduce you to the foundational concepts of dlt and guide you through basic and advanced usage scenarios.

As a practical example, we'll build a data pipeline that loads data from the GitHub API into DuckDB.

## What We'll Cover

- [Fetching data from the GitHub API](./load-data-from-an-api.md)
- [Understanding and managing data loading behaviors](./load-data-from-an-api.md#append-or-replace-your-data)
- [Incrementally loading new data and deduplicating existing data](./load-data-from-an-api.md#load-only-new-data-incremental-loading)
- [Making our data fetch more dynamic and reducing code redundancy](./grouping-resources.md)
- [Securely handling secrets](./grouping-resources.md#handle-secrets)
- [Making reusable data sources](./grouping-resources.md#configurable-sources)

## Ready to dive in?

Let's begin by loading data from an API.