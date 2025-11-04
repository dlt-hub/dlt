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

## Get started with the Workspace

The Workspace extends the functionality of the open-source dlt library. It provides the tools and support you need to move from
running your first dlt pipeline to large-scale, production-ready data workflows.

To get started, install the open-source dlt library with the Workspace extra:

```sh
pip install "dlt[workspace]"
```

:::info
The Workspace is currently in Preview. Some of its features are experimental and disabled by default.
They are hidden behind a feature flag, which means you need to manually enable them before use.

To activate these features, create the empty `.dlt/.workspace` file in your working directory. This tells `dlt` to switch from the classic mode to the Workspace mode.
:::

Read more information on how to get started quickly in [installation](../getting-started/installation.md).

## Availability and licensing

Currently, dltHub Workspace is in public preview. Most of the dltHub Workspace functionality is available in open-source tier.
Some of the features, like  [`@dlt.hub.transformation`](../features/transformations/index.md) and [data quality checks](../features/quality/data-quality.md) require a license.
For more information, please visit the [installation page](../getting-started/installation.md).

## CLI support

Workspace includes powerful CLI commands that make it easy to interact with your projects.
Some of the available commands include:

* `dlt workspace show` will launch Workspace Dashboard
* `dlt workspace mcp` will launch Workspace MCP (OSS MCP) in sse mode.
* `dlt pipeline foo mcp` will launch pipeline MCP (old version of MCP, to be deprecated) in sse mode.
* `dlt pipeline foo show` will launch Workspace Dashboard and open pipeline `foo`

You can find more commands in the [CLI reference section](../reference.md).

## Next steps

* [Install dltHub Workspace.](../getting-started/installation.md)
* [Learn about the LLM-native workflow.](../../dlt-ecosystem/llm-tooling/llm-native-workflow.md)
* [Try dltHub Transformations.](../features/transformations/index.md)
*