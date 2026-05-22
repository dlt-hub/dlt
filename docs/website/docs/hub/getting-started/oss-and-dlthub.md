---
title: dlt and dltHub
description: A map of the dlt and dltHub ecosystem
keywords: [dlt, dlthub, open source, source available, pricing, license, workspace, workbench, skills, ai, agent]
---

:::note
**dltHub offers two products**: dlt (open source) and **dltHub** (commercial, license-gated). This page explains both products and how they relate.

- **dlt** — the open source ingestion library, Apache 2.0.
- **dltHub** — the agentic platform that deploys, monitors, and scales dlt pipelines, with a managed runtime, data quality, transformations, and AI tooling for coding agents. **All dltHub components are license-gated.**
:::

## The two products at a glance

dlt is free and open source under Apache 2.0, dltHub is a paid product.

| Capability | dlt | dltHub |
|---|---|---|
| Build ingestion pipelines using [dlt verified sources](../../dlt-ecosystem/verified-sources) (except premium sources) | ✅ | ✅ |
| Build pipelines with the [dltHub AI Workbench](https://github.com/dlt-hub/dlthub-ai-workbench) | — | ✅ |
| [Data quality metrics & checks](../data-quality) | — | ✅ |
| Build transformation pipelines (dltHub/dbt) | — | ✅ |
| Managed runtime: deploy, run/schedule pipelines, serve data apps, monitor jobs | — | ✅ |
| Premium sources ([MS SQL](../ingestion/ms-sql)) and destinations ([Iceberg](../ingestion/iceberg), [Delta](../ingestion/delta), [Snowflake + Iceberg/Open Catalog](../ingestion/snowflake-plus)) | — | ✅ |

## dlt (OSS)

dlt is the open source ingestion library, distributed under Apache 2.0. 

| Component | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 | How to access it                                                                               | Get started                                                                                                                                                                                           |
|---|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **dlt library** | The Python pipeline engine: extract, normalize, load. Includes schema inference and evolution, incremental loading, write dispositions, pipeline state, and the `dlt pipeline …` CLI for inspection.                                                                                                                                                                                                                                                                                                                                                                                                                       | [dlt](https://github.com/dlt-hub/dlt) — Apache 2.0, on [PyPI](https://pypi.org/project/dlt/) | [Intro](../../intro.md) · [Tutorial](../../tutorial/sql-database.md) · `pip install dlt`                                                                                                                    |
| **Core sources** | Three flexible, generic sources shipped with the library that cover most ingestion scenarios out of the box: <ul><li>[`rest_api`](../../dlt-ecosystem/verified-sources/rest_api) (any REST API via declarative config for endpoints, pagination, and auth)</li> <li>[`sql_database`](../../dlt-ecosystem/verified-sources/sql_database) (30+ SQL databases via SQLAlchemy / PyArrow / pandas / ConnectorX with table reflection)</li> <li>[`filesystem`](../../dlt-ecosystem/verified-sources/filesystem) (local and cloud storage — S3, GCS, Azure Blob, Google Drive, SFTP — with native CSV / Parquet / JSONL support)</li></ul> | Distributed with [dlt library](https://github.com/dlt-hub/dlt/tree/devel/dlt/sources)        | [REST API](../../dlt-ecosystem/verified-sources/rest_api) · [SQL Database](../../dlt-ecosystem/verified-sources/sql_database) · [Filesystem & Cloud Storages](../../dlt-ecosystem/verified-sources/filesystem) |
| **Verified sources** | A curated set of dltHub-maintained connectors (for example, Kafka, MongoDB, Postgres CDC, Stripe, Hubspot, …) pulled into your project with `dlt init <source> <destination>` .                                                                                                                                                                                                                                                                                                                                                                                                                                                     | [`dlt-hub/verified-sources`](https://github.com/dlt-hub/verified-sources)                    | [Verified sources docs](../../dlt-ecosystem/verified-sources) · `dlt init -l` to list available sources                                                                                                  |

**dlt is a good fit if:** You want a lightweight, code-first ingestion library, are comfortable managing orchestration, scheduling, and operations yourself, or you need to deploy on-prem, on a VPS, or in any environment where managed cloud solution is not an option. dlt runs anywhere Python runs, with no platform dependency.


## dltHub

dltHub is a managed cloud platform for running your dlt pipelines, transformations, and notebooks. You can work with dltHub in two complementary ways:

- **Web UI** at [app.dlthub.com](https://app.dlthub.com) — sign up to deploy, schedule, monitor pipelines, manage profiles, browse datasets.
- **Locally, from the CLI or Python** — bootstrap a new workspace in one command:
  ```sh
  uvx dlthub-start@latest my-workspace
  ```
  This creates a runnable workspace with the AI Workbench, example pipelines, and the [`dlt[hub]`](installation.md) extra installed. To add dltHub to an existing project instead, run:
  ```sh
  pip install "dlt[hub]"
  ```
  Either way, you get the dltHub workspace and dashboard, the AI development tooling (`dlthub ai`, MCP server, AI Workbench), per-source contexts, and the `dlthub` library that adds data quality, transformations, and premium sources/destinations. 

Every component below is part of dltHub and requires a license. Most components are source-available under their own licenses; all are distributed through the `dlthub` PyPI package or the dltHub repositories.

| Component | What it is                                                                                                                                                                                                                                                                                                                                                                                                                                         | How to access it                                                                                                                                                                                 | Get started                                                                                                                         |
|---|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------|
| **dltHub Platform** | The hosted Web UI and managed runtime at [app.dlthub.com](https://app.dlthub.com/) — deploy and schedule pipelines, monitor runs, manage workspaces and profiles, browse datasets, collaborate.                                                                                                                                                                                                                                                            | [app.dlthub.com](https://app.dlthub.com/)                                                                                                                                                              | [Platform](https://app.dlthub.com/) · [Runtime overview](../pipeline-operations/overview.md) · [Runtime tutorial](platform-tutorial.md) |
| **AI Toolkits** | The dltHub AI Workbench: a collection of toolkits made of skills, rules, workflows, and MCP wiring that drive agentic pipeline development inside Claude Code, Cursor, and Codex.                                                                                                                                                                                                                                                                  | [`dlt-hub/dlthub-ai-workbench`](https://github.com/dlt-hub/dlthub-ai-workbench) — source-available under [its own license](https://github.com/dlt-hub/dlthub-ai-workbench/blob/master/LICENSE) | [Agent-native workflow walkthrough](../ingestion/rest-api-source.md)                                            |
| **dltHub Context** | Per-source agent contexts (specs, endpoint documentation, prompts) that prime your coding assistant for thousands of APIs. Automatically used by AI Workbench                                                                                                                                                                                                                                                                                        | Browse and copy contexts at [dlthub.com/context](https://dlthub.com/context)                                                                   | [Build a source with AI](../ingestion/init.md#agentic-setup)                                                                      |
| **`dlthub` library** | Python package shipped via `dlt[hub]`. Adds the production capabilities: [data quality](../data-quality/index.md), [Python transformations](../transformations/index.md) (`@dlt.hub.transformation`) and [dbt transformations](../transformations/dbt-transformations.md), and premium sources/destinations such as [Iceberg / DuckLake](../ingestion/iceberg.md) and [MSSQL Change Tracking](../ingestion/ms-sql.md). | On [PyPI](https://pypi.org/project/dlthub/)                                                                                                                                                    | [Installation](installation.md)                                                                                     |

**dltHub is a good fit if:** You are running pipelines in production, want a coding agent to do the heavy lifting with tooling that supports the generation of production-grade code, need transformations or data quality checks, if you want managed infrastructure, or if you are working as a team.

:::tip
If you have a specific question, feature request, or unique use case, feel free to [reach out](https://dlthub.com/contact).
:::