---
title: Community Destinations
description: Community-contributed destinations for dlt
keywords: [community, destination, contributions]
---

# Community Destinations

We've got several destinations contributed by our community. [They are not part of the `dlt` library](https://github.com/dlt-hub/dlt/blob/devel/CONTRIBUTING.md#before-you-begin), instead we list them below.

:::caution Caveat Emptor
**Use at your own risk:** These are community-maintained forks of `dlt` that are not officially supported by the dlt team. While they extend dlt's functionality to additional databases, they may not receive the same level of testing, maintenance, or security updates as the core library. Always review the code and test thoroughly before using in production environments.
:::


## Starrocks

by: [phaethon](https://github.com/phaethon)

Implementation of Starrocks support as a separate destination. Starrocks is an MPP (Massively Parallel Processing) database designed for real-time analytics. This adapter implements two loading methods:
- **Stream Load**: Direct streaming of data into Starrocks
- **INSERT INTO SELECT FROM FILES**: Loading data from S3-compatible staging storage for improved performance with large datasets

Code:
https://github.com/phaethon/dlt/tree/starrocks

PR with discussion:
https://github.com/dlt-hub/dlt/pull/2518


## Clickhouse Distributed

by: [zstipanicev](https://github.com/zstipanicev)

When Clickhouse is deployed with replicas and distributed tables, standard DDL & DML statements need to be modified to work across the cluster. This implementation adds:
- **ON CLUSTER** clauses to DDL statements to execute across all nodes
- Creation of **base table and distributed table pairs** (base tables use ReplicatedMergeTree, distributed tables use Distributed engine)
- Modified ALTER, DROP, DELETE, UPDATE, and TRUNCATE operations to work with both table types
- Configuration options for cluster name, database prefixes, and table name patterns

This allows dlt to work seamlessly with sharded and replicated Clickhouse setups.

Code:
https://github.com/zstipanicev/dlt/tree/feat/2200-add-clickhouse-distributed-support


PR with discussion:
https://github.com/dlt-hub/dlt/pull/2573



## CrateDB

by: [CrateDB](https://github.com/crate)

CrateDB is a distributed SQL database built on top of Lucene, designed for real-time analytics on large datasets. This destination adapter wraps the PostgreSQL adapter with CrateDB-specific adjustments:
- Compatibility with CrateDB's PostgreSQL wire protocol
- Workarounds for system column naming restrictions (underscore-prefixed columns)
- Support for CrateDB's **REFRESH TABLE** statements for consistency
- Adjusted transaction handling for CrateDB's limited transaction support

Code:
https://github.com/crate/dlt-cratedb

Original PR with discussion (currently on hiatus):
https://github.com/dlt-hub/dlt/pull/2733
