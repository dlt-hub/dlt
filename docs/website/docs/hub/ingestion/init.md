---
title: Initialize a pipeline
description: How to initialize and develop a dlt pipeline using CLI, workspace, or verified sources
keywords: [create a pipeline, verified source, workspace, rest client, how to, dlt init]
---

# Initialize a pipeline

This guide walks you through creating and initializing a `dlt` pipeline in dltHub Workspace—whether manually, with agentic help, or from one of the **verified sources** maintained by dltHub team.


## Overview

A `dlt` pipeline moves data from a source (like an API or database) into a destination (like DuckDB, Snowflake, or Iceberg). Initializing a pipeline is the first step in the data workflow.
You can create one in two CLI-based ways:

| Method          | Command | Best for |
|-----------------|----------|----------|
| Manual          | `dlthub pipeline init <source> <destination>` | Developers who prefer manual setup |
| Verified source | `dlthub pipeline init <verified_source> <destination>` | Prebuilt, tested connectors from the community and dltHub team |

Outside of a workspace (plain OSS `dlt`), the same scaffold is reachable as `dlt init <source> <destination>`. Inside a dltHub workspace, `dlthub pipeline init` is the canonical entry point—it adds the pipeline to the current workspace.


## Step 0: Install dlt with workspace support

Before you start, make sure you followed the [installation instructions](../getting-started/installation.md) and have a dltHub workspace initialized. The fastest way is:

```sh
uvx dlthub-init@latest
```

This scaffolds a workspace with `.dlt/.workspace` already set, the AI toolkits vendored, and `dlt[hub]` synced. See the [installation guide](../getting-started/installation.md) for the alternative paths (adding to an existing project, or enabling workspace mode by hand).

**dltHub Workspace** is a unified environment for developing, running, and maintaining data pipelines—from local development to production.

[More about dlt Workspace](../getting-started/installation.md#what-is-a-dlthub-workspace)


## Step 1: Initialize a custom pipeline

### Manual setup (standard workflow)

A lightweight, code-first approach ideal for developers comfortable with Python.

```sh
dlthub pipeline init {source_name} duckdb
````

for example:

```sh
dlthub pipeline init my_github_pipeline duckdb
```

It scaffolds the pipeline template—a minimal starter project with a single Python script that shows three quick ways to load data into DuckDB using dlt:

- fetch JSON from a public REST API (chess.com as an example) with requests,
- read a public CSV with pandas, and
- pull rows from a SQL database via SQLAlchemy.

The file also includes an optional GitHub REST client example (a `@dlt.resource` + `@dlt.source`) that can use a token from `.dlt/secrets.toml`, but will work unauthenticated at low rate limits.
It’s meant as a hands-on playground you can immediately run and then adapt into a real pipeline.

Learn how to build you own dlt pipeline with [dlt Fundamentals course.](https://dlthub.learnworlds.com/course/dlt-fundamentals)


### Agentic setup

A collaborative AI-human workflow that integrates `dlt` with AI editors and agents like:
- **Claude**
- **Cursor**
- **Codex**
- [the full list](./rest-api-source.md#setup)


Start with the [`/find-source` skill](./rest-api-source.md#find-source--discover-your-data-source) to describe your data source in natural language—the assistant identifies a verified source or researches the API, then chains into pipeline scaffolding.



[Read more about running a pipeline](../../walkthroughs/run-a-pipeline)

## Next steps: Deploy and scale

Once your pipeline runs locally:
* [Monitor via the workspace dashboard](../../general-usage/dataset-access/data-quality-dashboard)
* Set up [Profiles](../pipeline-operations/profiles.md) to manage separate dev, prod, and test environments
* [Deploy to runtime](../getting-started/platform-tutorial.md#5-run-your-first-pipeline)
