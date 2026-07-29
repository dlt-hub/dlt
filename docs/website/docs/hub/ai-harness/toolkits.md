---
title: Toolkits
description: The full catalog of dltHub AI Harness toolkits and how they fit into the ingest, validate, transform, deploy, observe cycle.
keywords: [ai harness, toolkits, catalog, development cycle, rest-api-pipeline, sql-database-pipeline, transformations, dlthub-platform]
---

# Toolkits

A toolkit is a versioned bundle of skills, rules, and an MCP server, tied together by a workflow that tells the agent which skill to run at each step and how to leverage the MCP. Each toolkit covers one job: build a REST API pipeline, add data-quality checks, deploy a workspace, and so on. Toolkits also act as guardrails, keeping the agent from diverging from proven dlt patterns and data-engineering best practices. Head to [Installation](installation.md#adding-feature-toolkits) to install them.

## The development cycle

Toolkits map onto a five-stage pipeline lifecycle. Each toolkit owns one stage and guides the agent through it end to end, starting from an **entry skill** and running the rest in sequence.

| Stage | Purpose |
| --- | --- |
| [Ingest](#ingest) | Load data from sources (REST APIs, SQL databases, files) into a destination. |
| [Validate](#validate) | Define column-level checks and load metrics to catch bad data early. |
| [Transform](#transform) | Reshape raw pipeline data into a curated model for downstream use. |
| [Deploy](#deploy) | Ship pipelines and notebooks to the dltHub platform on a schedule. |
| [Observe](#observe) | Explore loaded data and diagnose performance issues. |

## Ingest

### `rest-api-pipeline`

Build REST API pipelines with dlt. Scope, debug, and validate data. See the [worked example](../ingestion/rest-api-source.md).

<div class="plain-details">
<details>
<summary>Skills (entry: <code>/find-source</code>)</summary>

- `/find-source`: Find a dlt source for a given API or data provider.
- `/create-rest-api-pipeline`: Scaffold a REST API pipeline from the discovered source.
- `/debug-pipeline`: Inspect traces, load packages, and schema after a run.
- `/validate-data`: Validate schema and data after a successful load.
- `/view-data`: Query and explore loaded data via dataset API, ibis, and ReadableRelation.
- `/adjust-endpoint`: Remove dev limits, add incremental loading, and handle rate-limits.
- `/new-endpoint`: Add a new endpoint to an existing pipeline.
- `/optimize-rest-api-performance`: Parallelize resources, tune page size and concurrency.

</details>
</div>

### `sql-database-pipeline`

Connect to any SQL source, load tables to a destination, and tune performance with backends.

<div class="plain-details">
<details>
<summary>Skills (entry: <code>/find-source</code>)</summary>

- `/find-source`: Find and explore a SQL database source (Postgres, MySQL, MS SQL, Oracle, SQLite, any SQLAlchemy).
- `/create-sql-database-pipeline`: Scaffold a pipeline from a SQL source.
- `/debug-pipeline`: Diagnose connection failures, driver issues, and failed jobs.
- `/validate-data`: Validate schema and column mappings after a load.
- `/view-data`: Query and explore loaded data.
- `/add-table`: Add a new table or view to an existing pipeline.
- `/adjust-table`: Remove dev limits, configure incremental loading and merge keys.
- `/optimize-sql-performance`: Pick a faster backend, tune chunk size, parallelize tables.

</details>
</div>

### `filesystem-pipeline`

Load files (CSV, Parquet, JSONL, or custom) from local disk, S3, GCS, Azure, or SFTP into a destination.

<div class="plain-details">
<details>
<summary>Skills (entry: <code>/create-filesystem-pipeline</code>)</summary>

- `/create-filesystem-pipeline`: Load files (CSV, Parquet, JSONL, or custom) from local disk, S3, GCS, Azure, or SFTP.
- `/add-incremental-loading`: Filter files by modification date, switch to merge with a primary key.
- `/optimize-filesystem-performance`: Faster reader, parallel reads, narrower globs, chunked streaming.

</details>
</div>


## Validate

### `data-quality`

Inspect schema for candidates, define column-level validations and load metrics, run them on every pipeline load, and diagnose failures.

<div class="plain-details">
<details>
<summary>Skills (entry: <code>/setup-data-quality</code>)</summary>

- `/setup-data-quality`: Set up data-quality workflows for a pipeline.
- `/define-data-quality-checks`: Translate business rules and schema hints into checks and metrics.
- `/run-data-quality`: Execute defined checks against a loaded pipeline.
- `/review-data-quality`: Inspect check and metric outcomes and diagnose failures.

</details>
</div>


## Transform

### `transformations`

Transform raw dlt pipeline data into a Canonical Data Model using Kimball dimensional modeling and `@dlt.hub.transformation` functions. See the [worked example](../transformations/explore-and-transform.md).

<div class="plain-details">
<details>
<summary>Skills (entry: <code>/annotate-sources</code>)</summary>

- `/annotate-sources`: Annotate dlt pipeline sources for transformation.
- `/create-ontology`: Build a business entity graph (ontology) from annotated sources.
- `/generate-cdm`: Generate a Canonical Data Model in DBML using Kimball dimensional modeling.
- `/create-transformation`: Emit `@dlt.hub.transformation` functions that map source tables to CDM entities.
- `/debug-transformation`: Diagnose transformation failures, SQL dialect errors, silently dropped columns.
- `/incremental-transformation`: Switch from full-replace to incremental loading.

</details>
</div>


## Deploy

### `dlthub-platform`

Deploy dltHub workspaces and pipelines to the dltHub Platform. See the [worked example](../pipeline-operations/deployments.md#deploy-with-ai-harness).

<div class="plain-details">
<details>
<summary>Skills (entry: <code>/setup-runtime</code>)</summary>

- `/setup-runtime`: Verify workspace readiness for the platform (workspace file, dlt[hub] dependencies, login state).
- `/prepare-deployment`: Prepare production credentials and destinations, split dev/prod credentials.
- `/deploy-workspace`: Deploy pipelines and notebooks to the platform, with optional scheduling.
- `/debug-deployment`: Investigate failed runs, unexpected results, and job status.

</details>
</div>


## Observe

### `data-exploration`

Connect to a pipeline, profile tables, plan charts, and assemble marimo dashboards. See the [worked example](../transformations/explore-and-transform.md).

<div class="plain-details">
<details>
<summary>Skills (entry: <code>/explore-data</code>)</summary>

- `/explore-data`: Connect to a pipeline, profile tables, plan charts, write an analysis plan.
- `/build-notebook`: Assemble a marimo notebook from the analysis plan and launch it.

</details>
</div>

### `performance`

Diagnose the bottleneck stage (extract, normalize, load) and apply parallelism, workers, memory buffers, file rotation, and batching.

<div class="plain-details">
<details>
<summary>Skills (entry: <code>/optimize-performance</code>)</summary>

- `/optimize-performance`: Source-agnostic tuning: parallelism, workers, memory buffers, file rotation, batching. For source-specific tuning, see the pipeline toolkit's own optimize skill.

</details>
</div>

## What's next

- [Deploy with AI Harness](../pipeline-operations/deployments.md#deploy-with-ai-harness) walks through the `dlthub-platform` toolkit end-to-end.
