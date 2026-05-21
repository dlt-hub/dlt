---
title: Overview
---

# Workspace

The dltHub Workspace is a unified data engineering environment that extends the open-source dlt library with enterprise-grade features for managing data pipelines. It provides the tools and support you need to move from running your first dlt pipeline to large-scale, production-ready data workflows.

It provides:
* [AI-powered workflows](./init.md)
* [built-in multiple environments support](../pipeline-operations/profiles.md) (dev, prod, tests, access) through profiles that isolate configurations and data storage
* [data quality metrics and checks](../data-quality/index.md)(public preview)
* powerful transformation with [`@dlt.hub.transformation`](../transformations/index.md) and [dbt integration](../transformations/dbt-transformations.md)
* [dashboard](./dashboard.md) as a comprehensive observability tool
* [dltHub platform integration](../pipeline-operations/overview.md) for easy deployment of pipelines, transformations and notebooks with no configuration — sign in at [app.dlthub.com](https://app.dlthub.com)

It automates essential tasks like data loading, quality checks, and governance while enabling seamless collaboration across teams and providing a consistent development-to-production workflow.

## Get started with the Workspace

The fastest way to start a new dltHub workspace is:

```sh
uvx dlthub-start@latest
```

This scaffolds a workspace with `.dlt/.workspace` already set, the AI Workbench, example pipelines, and `dlt[hub]` synced. For prerequisites and alternative install paths, see the [installation page](../getting-started/installation.md).

## CLI support

Workspace comes with additional [cli support](../command-line-interface.md) that is enabled after installation.

## Next steps

* [Install dltHub Workspace.](../getting-started/installation.md)
* [Learn about the agent-native workflow.](./rest-api-source.md)
* [Try dltHub Transformations.](../transformations/index.md)
