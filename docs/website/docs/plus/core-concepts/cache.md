---
title: "🧪 Cache"
description: Execute data transformations in your local cache
keywords: ["dlt+", "cache", "transformations"]
---  
  
import Link from '../../_plus_admonition.md';

<Link/>

:::note
🚧 This feature is under development. Interested in becoming an early tester? [Join dlt+ early access](https://info.dlthub.com/waiting-list)
:::
The dlt+ Cache is a temporary local storage created by dlt+ to enhance development workflows. It allows you to efficiently run local transformations, materialize dbt models, and test your queries before deploying them to production. 

## How it works

The dlt+ Cache is a powerful tool that enables users to shift parts of their data workflows earlier in the development process. Its primary use case today is [running transformations locally](../features/transformations/index.md), but we plan to support more use cases and workflows in the future.

You specify which datasets you want to pass to the cache in your dlt manifest file (`dlt.yml`). The cache automatically discovers the source schema from the data and runs your transformations using the cache and duckdb as a query engine. Currently you can define your transformations in dbt or Python (pands, arrows, polars, etc.). After running your transformations, the cache will sync the results to the output dataset in your destination. Output schema is also automatically discovered (when not explicitly declared).


## Why you should use it

Using local cache for transformations provides several advantages, like:
1. Your source schema is discovered automatically.
2. You save unnecessary computing costs by shifting transformation queries away from expensive cloud warehouses to the local machine or a cloud-deployed server.
3. You reduce the egress costs since data remains in your local system.
4. The same engine is used for transformations (i.e., SQL dialect) irrespective of where you're loading your data.
6. Metadata is easily propagated from the input to the output dataset and dataset catalogs are maintained automatically.

