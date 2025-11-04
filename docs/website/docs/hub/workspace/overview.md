---
title: Overview
---

# Workspace

The dltHub Workspace is a unified data engineering environment that extends the open-source dlt library with enterprise-grade features for managing data pipelines. It provides the tools and support you need to move from
running your first dlt pipeline to large-scale, production-ready data workflows.

It provides:
* [AI-powered workflows](../workspace/init.md)
* [built-in multiple environments support](../core-concepts/profiles-dlthub.md) (dev, prod, tests, access) through profiles that isolate configurations and data storage
* [data quality checks](../features/quality/data-quality.md)[Private preview]
* powerful transformation with [`@dlt.hub.transformation`](../features/transformations/index.md) and [dbt integration](../features/transformations/dbt-transformations.md)
* [dashboard](../../general-usage/dashboard.md) as a comprehensive observability tool
* [MCP](../features/mcp-server.md) for data exploration and semantic modeling

It automates essential tasks like data loading, quality checks, and governance while enabling seamless collaboration across teams and providing a consistent development-to-production workflow.

## Get started with the Workspace

To get started follow the instructions from the [installation page](../getting-started/installation.md).

## Availability and licensing

Currently, dltHub Workspace is in public preview. Most of the dltHub Workspace functionality is available in the [Free tier](../intro.md#tiers--licensing).
Some of the features, like  [`@dlt.hub.transformation`](../features/transformations/index.md) and [data quality checks](../features/quality/data-quality.md) require a license.
For more information, please visit the [installation page](../getting-started/installation.md).

## CLI support

Workspace comes with additional [cli support](../command-line-interface.md) that is enabled after installation.

## Next steps

* [Install dltHub Workspace.](../getting-started/installation.md)
* [Learn about the LLM-native workflow.](../../dlt-ecosystem/llm-tooling/llm-native-workflow.md)
* [Try dltHub Transformations.](../features/transformations/index.md)
