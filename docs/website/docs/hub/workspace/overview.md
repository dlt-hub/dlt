---
title: Overview
---

# Workspace

The dltHub Workspace is a unified data engineering environment that extends the open-source dlt library with enterprise-grade features for managing data workflows.
It provides:
* [AI-powered workflows](../workspace/init.md)
* [built-in multiple environments support](../core-concepts/profiles-dlthub.md) (dev, prod, tests, access) through profiles that isolate configurations and data storage
* [data quality checks](../features/quality/data-quality.md)[Private preview]
* powerful transformation with [`@dlt.hub.transformation`](../features/transformations/index.md) and [dbt integration](../features/transformations/dbt-transformations.md)
* [dashboard](../../general-usage/dashboard.md) as a comprehensive observability tool
* [MCP](../features/mcp-server.md) for data exploration and semantic modeling

It automates essential tasks like data loading, quality checks, and governance while enabling seamless collaboration across teams and providing a consistent development-to-production workflow.

## Availability and licensing

Currently, dltHub Workspace is in public preview. Most of the dltHub Workspace functionality is available in open-source tier.
Some of the features, like  [`@dlt.hub.transformation`](../features/transformations/index.md) and [data quality checks](../features/quality/data-quality.md) require a license.
For more information, please visit the [installation page](../getting-started/installation.md).

## CLI support

Workspace comes with powerful cli support, some of the available commands:

* `dlt workspace show` will launch Workspace Dashboard
* `dlt workspace mcp` will launch Workspace MCP (OSS MCP) in sse mode.
* `dlt pipeline foo mcp` will launch pipeline MCP (old version of MCP, to be deprecated) in sse mode.
* `dlt pipeline foo show` will launch Workspace Dashboard and open pipeline `foo`
