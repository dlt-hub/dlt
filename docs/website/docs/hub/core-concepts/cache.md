---
title: "Cache 🧪"
description: Execute data transformations in your local cache
keywords: ["dltHub", "cache", "transformations"]
---

:::caution
🚧 This feature is under development, and the interface may change in future releases. Interested in becoming an early tester? [Join dltHub early access](https://info.dlthub.com/waiting-list)
:::

The dltHub Cache is a temporary local storage created by dltHub to enhance development workflows. It allows you to efficiently run local transformations, materialize dbt models, and test your queries before deploying them to production.

## How it works

The dltHub Cache is a powerful tool that enables users to shift parts of their data workflows earlier in the development process. Its primary use case today is [running transformations locally](../features/transformations/index.md), but we plan to support more use cases and workflows in the future.

The cache is powered by DuckDB, supporting the full DuckDB SQL dialect. You can manipulate cached data and push it back to any dlt destination.

You specify which datasets you want to pass to the cache in your dlt manifest file (`dlt.yml`). The cache automatically discovers the source schema from the data and runs your transformations using the cache and DuckDB as a query engine. Currently, you can define your transformations in dbt or Python (pandas, arrows, polars, etc.). After running your transformations, the cache will sync the results to the output dataset in your destination. The output schema is also automatically discovered (when not explicitly declared).

## Define the cache

To define a cache, you need to declare the name, inputs, and outputs in the `dlt.yml` file. For example, the following configuration defines a cache that retrieves data
from `github_events_dataset`, processes it, and writes the transformed data to `github_reports_dataset`:

```yaml
caches:
  github_events_cache:
    inputs:
      - dataset: github_events_dataset
        tables:
          items: items
    outputs:
      - dataset: github_reports_dataset
        tables:
          items: items
          items_aggregated: items_aggregated
```

:::caution
Currently, a cache usage has specific constraints. Please keep the following limitations in mind:

* The input dataset must be located on a filesystem-based destination such as [Iceberg](../ecosystem/iceberg.md), [Delta](../ecosystem/delta.md), or [Cloud storage and filesystem](../../dlt-ecosystem/destinations/filesystem.md). The cache creates live views on these tables.
* While the cache works with any output destination, you must explicitly define output tables.
* A cache can only write data using the append write disposition.
:::

You can configure input tables in the cache to specify which tables are cached locally. This allows you to run SQL queries on remote data lakes efficiently, eliminating complex data retrieval workflows.
Outputs define how processed data in the cache is pushed back to a chosen destination.

Populating and flushing the cache are discrete steps.
You can orchestrate these as part of your deployment or trigger them interactively using the cli, especially when analyzing data locally or working in a notebook.

## Why you should use it

The main use case for a cache is [local transformations](../features/transformations/index.md). This provides several advantages, like:

1. Your source schema is discovered automatically.
2. You save unnecessary computing costs by shifting transformation queries away from expensive cloud warehouses to the local machine or a cloud-deployed server.
3. You reduce the egress costs since data remains in your local system.
4. The same engine is used for transformations (i.e., SQL dialect) irrespective of where you're loading your data.
5. Metadata is easily propagated from the input to the output dataset, and dataset catalogs are maintained automatically.

