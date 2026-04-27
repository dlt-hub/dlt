---
title: From dlt to dltHub
description: A map of the dlt and dltHub ecosystem
keywords: [dlt, dlthub, open source, source available, pricing, license, workspace, workbench, skills, ai, agent, glossary]
---

:::note
We ship **two high-level products**: `dlt` (open source) and **dltHub** (commercial, license-gated). Each product is made of several components — this page maps them out so you know what each one is and where it lives.

- **`dlt`** — the open-source ingestion library, Apache 2.0.
- **dltHub** — the commercial platform built on top of `dlt`: a workspace layout, an AI toolkit (skills, rules, MCP server) for coding agents, transformations, data quality, and a managed runtime. **All dltHub components are license-gated.**
:::

## The two products at a glance

| | **`dlt`** | **dltHub**                                                                                                                   |
|---|---|------------------------------------------------------------------------------------------------------------------------------|
| **What it is** | The ingestion engine: sources, destinations, schema, incremental loading | A platform around `dlt`: workspace layout, AI toolkits, MCP, transformations, data quality, managed runtime                  |
| **Install** | `pip install dlt` | `pip install "dlt[hub]"`                                                                                                     |
| **License** | Apache 2.0 | Commercial dltHub license. Some components are source-available under their own licenses. The full list is [below](#dlthub). |
| **Cost** | Free | Paid. 30 days trial is available.                                                                                            |

The remainder of this page breaks each product down into its components.

## `dlt` (OSS)

`dlt` is the open-source ingestion library, distributed under Apache 2.0. You can use any of its components freely in any project — commercial or otherwise. The three components below are the entire OSS surface.

| Component | What it is                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 | Where it lives                                                                               | Get started                                                                                                                                                                                           |
|---|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **`dlt` library** | The Python pipeline engine: extract, normalize, load. Includes schema inference and evolution, incremental loading, write dispositions, pipeline state, and the `dlt pipeline …` CLI for inspection.                                                                                                                                                                                                                                                                                                                                                                                                                       | [dlt](https://github.com/dlt-hub/dlt) — Apache 2.0, on [PyPI](https://pypi.org/project/dlt/) | [Intro](../intro.md) · [Tutorial](../tutorial/sql-database.md) · `pip install dlt`                                                                                                                    |
| **Core sources** | Three flexible, generic sources shipped with the library that cover most ingestion scenarios out of the box: <ul><li>[`rest_api`](../dlt-ecosystem/verified-sources/rest_api) (any REST API via declarative config for endpoints, pagination, and auth)</li> <li>[`sql_database`](../dlt-ecosystem/verified-sources/sql_database) (30+ SQL databases via SQLAlchemy / PyArrow / pandas / ConnectorX with table reflection)</li> <li>[`filesystem`](../dlt-ecosystem/verified-sources/filesystem) (local and cloud storage — S3, GCS, Azure Blob, Google Drive, SFTP — with native CSV / Parquet / JSONL support)</li></ul> | Distributed with [dlt library](https://github.com/dlt-hub/dlt/tree/devel/dlt/sources)        | [REST API](../dlt-ecosystem/verified-sources/rest_api) · [SQL Database](../dlt-ecosystem/verified-sources/sql_database) · [Filesystem & Cloud Storages](../dlt-ecosystem/verified-sources/filesystem) |
| **Verified sources** | A curated set of dltHub-maintained connectors (e.g. Kafka, MongoDB,Postgres CDC, Stripe, Hubspot, …) pulled into your project with `dlt init <source> <destination>` .                                                                                                                                                                                                                                                                                                                                                                                                                                                     | [`dlt-hub/verified-sources`](https://github.com/dlt-hub/verified-sources)                    | [Verified sources docs](../dlt-ecosystem/verified-sources) · `dlt init -l` to list available sources                                                                                                  |

**A good fit if:** You want a lightweight, code-first ingestion library, are comfortable managing orchestration, scheduling, and operations yourself, or you need to deploy on-prem, on a VPS, or in any environment where managed cloud is not an option — `dlt` runs anywhere Python runs, with no platform dependency.


## dltHub

dltHub is the commercial platform built on top of `dlt`. You can work with dltHub in two complementary ways:

- **Web UI** at [dlthub.app](https://dlthub.app/) — sign in to start your **30-day trial** and use the platform from the browser: deploy, schedule, monitor pipelines, manage profiles, browse datasets.
- **Locally, from the CLI or Python** — install the [`dlt[hub]`](getting-started/installation.md) extra and you have everything you need:
  ```sh
  pip install "dlt[hub]"
  ```
  We've packaged all the necessary components inside this single extra: the dltHub workspace + dashboard, the AI development tooling (`dlt ai`, MCP server, AI Workbench), per-source contexts, and the closed-source `dlthub` library that adds data quality, transformations, and premium sources/destinations. A trial license can be [self-issued](getting-started/installation.md#self-licensing) for 30 days.

Every component below is part of dltHub and requires a license. Some components are source-available (you can read and audit the code under their own license); the rest ship as closed-source binaries in the `dlthub` PyPI package.

| Component | What it is                                                                                                                                                                                                                                                                                                                                                                                                                                         | Where it lives                                                                                                                                                                                 | Get started                                                                                                                         |
|---|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------|
| **dltHub Platform** | The hosted Web UI and managed runtime at [dlthub.app](https://dlthub.app/) — deploy and schedule pipelines, monitor runs, manage workspaces and profiles, browse datasets, collaborate.                                                                                                                                                                                                                                                            | [dlthub.app](https://dlthub.app/) — closed source                                                                                                                                              | [Platform](https://dlthub.app/) · [Runtime overview](runtime/overview.md) · [Runtime tutorial](getting-started/runtime-tutorial.md) |
| **AI Toolkits** | The dltHub AI Workbench: a collection of toolkits made of skills, rules, workflows, and MCP wiring that drive agentic pipeline development inside Claude Code, Cursor, and Codex.                                                                                                                                                                                                                                                                  | [`dlt-hub/dlthub-ai-workbench`](https://github.com/dlt-hub/dlthub-ai-workbench) — source-available under [its own license](https://github.com/dlt-hub/dlthub-ai-workbench/blob/master/LICENSE) | [LLM-native workflow walkthrough](../dlt-ecosystem/llm-tooling/llm-native-workflow.md)                                              |
| **dltHub Context** | Per-source LLM contexts (specs, endpoint documentation, prompts) that prime your coding assistant for thousands of APIs. Automatically used by AI Workbench                                                                                                                                                                                                                                                                                        | Browse and copy contexts at [dlthub.com/context](https://dlthub.com/context)                                                                   | [Build a source with AI](workspace/init.md#llm-native-setup)                                                                        |
| **`dlthub` library** | Closed-source Python package shipped via `dlt[hub]`. Adds the production capabilities: [data quality](features/quality/data-quality.md), [Python transformations](features/transformations/index.md) (`@dlt.hub.transformation`) and [dbt transformations](features/transformations/dbt-transformations.md), and premium sources/destinations such as [Iceberg / DuckLake](ecosystem/iceberg.md) and [MSSQL Change Tracking](ecosystem/ms-sql.md). | Closed source, on [PyPI](https://pypi.org/project/dlthub/)                                                                                                                                     | [Installation](getting-started/installation.md) · [Self-issue trial license](getting-started/installation.md#self-licensing)        |

**A good fit if:** You are running pipelines in production, want a coding agent to do the heavy lifting, need transformations or data quality checks, want managed infra, or are working as a team.

:::tip
For tier breakdown (Pro / Scale / Enterprise), see [the intro](intro.md#tiers--licensing).
If you have a specific question, feature request, or unique use case, feel free to [reach out](https://dlthub.com/contact).
:::


## Where to start?

dltHub and dlt provide a range of tooling and options depending on your use case. If you want to get started quickly while following dltHub best practices, we recommend starting with the [AI workbench](https://github.com/dlt-hub/dlthub-ai-workbench)

Install it, then let your preferred coding agent figure out what it needs for your specific use case. We’ve made sure the agent has access to the necessary context through our AI toolbox, so it can guide you through the setup and implementation.

## Glossary — the names you'll see

The ecosystem reuses a few words for related-but-distinct things. Here is the canonical mapping.

| Name | What it actually is | Where it lives | Belongs to |
|---|---|---|---|
| **`dlt`** | The Python library (extract → normalize → load) | [`dlt-hub/dlt`](https://github.com/dlt-hub/dlt), Apache 2.0 | `dlt` |
| **Verified sources** | Community/team-maintained connectors pulled in by `dlt init` | [`dlt-hub/verified-sources`](https://github.com/dlt-hub/verified-sources) | `dlt` |
| **dltHub** | The commercial platform built around `dlt` | [dlthub.com](https://dlthub.com) / [dlthub.app](https://dlthub.app/) | dltHub |
| **dltHub Platform** | The hosted Web UI + managed runtime | [dlthub.app](https://dlthub.app/) | dltHub |
| **`dlt[hub]`** (extra) | Single install extra that bundles all dltHub local components — workspace tooling, AI Workbench, MCP, plus the closed-source `dlthub` library | PyPI | dltHub |
| **Workspace** (concept) | Local project layout introduced by dltHub: `dlt.yml`, profiles, `.dlt/.workspace` flag, etc. | Your project folder | dltHub |
| **`dlt ai`** (CLI) | CLI subcommand that wires up coding assistants (Claude Code, Cursor, Codex) and installs toolkits | Shipped with `dlt[hub]` | dltHub |
| **dltHub AI Workbench** | The collection of toolkits — skills, rules, workflows, MCP integration — that drive agentic pipeline development | [`dlt-hub/dlthub-ai-workbench`](https://github.com/dlt-hub/dlthub-ai-workbench), source-available under [its own license](https://github.com/dlt-hub/dlthub-ai-workbench/blob/master/LICENSE) | dltHub |
| **Toolkit** | A bundle of skills + rules + MCP wiring for one phase of work (e.g. `rest-api-pipeline`, `data-exploration`, `dlthub-runtime`) | Workbench repo, installed via `dlt ai toolkit <name> install` | dltHub |
| **Skill** | A single prompt-driven procedure the assistant follows (e.g. `/find-source`, `/debug-pipeline`) | Inside a toolkit | dltHub |
| **dltHub Context** | Per-source LLM contexts (specs, prompts) that prime your coding assistant for a specific API | [dlthub.com/workspace](https://dlthub.com/workspace); installed locally by `dlt init` | dltHub |
| **MCP server** | Local server that exposes pipelines, schemas, secrets shape, and SQL execution to your coding assistant via Model Context Protocol | `dlt workspace mcp` / `dlt pipeline <name> mcp` | dltHub |
| **Dashboard** | Local Marimo-based UI for inspecting pipeline runs and data | `dlt pipeline <name> show` | dltHub |
| **`dlthub` library** | Closed-source Python package with data quality, transformations, premium sources/destinations | [PyPI](https://pypi.org/project/dlthub/) | dltHub |
| **dltHub Runtime** | Managed cloud runtime for deploying and scheduling pipelines | [dlthub.app](https://dlthub.app/) | dltHub |

## Next steps

- [Install dltHub](getting-started/installation.md) — covers setup for all components
- [LLM-native workflow walkthrough](../dlt-ecosystem/llm-tooling/llm-native-workflow.md) — build a REST API pipeline with a coding agent
- [Self-issue a trial license](getting-started/installation.md#self-licensing) — explore the platform before committing
- [dltHub intro](intro.md) — overview of products and tiers
- [EULA](EULA.md) — full license terms
